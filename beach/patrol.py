# Copyright (C) 2015  refractionPOINT
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

'''Daemon used to ensure that Actors of a certain setup stay available in the cluster.'''


from beach.beach_api import Beach
import logging
import logging.handlers
import signal
import gevent
import gevent.event
import os
from sets import Set
import traceback


class Patrol ( object ):

    def __init__( self,
                  configFile,
                  identifier = 'default',
                  sync_frequency = 15.0,
                  logging_dest = '/dev/log',
                  realm = 'global',
                  scale = None ):
        self._stopEvent = gevent.event.Event()

        self._logger = None
        self._log_level = logging.INFO
        self._log_dest = logging_dest
        self._realm = realm
        self._initLogging( self._log_level, logging_dest )
        self._threads = gevent.pool.Group()

        self._owner = 'beach.patrol/%s' % ( identifier, )
        self._entries = {}
        self._watch = {}
        self._freq = sync_frequency

        self._beach = Beach( configFile, realm = realm )

        self._scale = scale


    def _initLogging( self, level, dest ):
        self._logger = logging.getLogger()
        self._logger.setLevel( level )
        handler = logging.handlers.SysLogHandler( address = dest )
        handler.setFormatter( logging.Formatter( "%(asctime)-15s %(message)s" ) )
        self._logger.addHandler( handler )

    def _log( self, msg ):
        self._logger.info( '%s : %s', self.__class__.__name__, msg )

    def _logCritical( self, msg ):
        self._logger.error( '%s : %s', self.__class__.__name__, msg )

    def _scanForExistingActors( self ):
        tally = {}

        mtd = self._beach.getAllNodeMetadata()
        for node_mtd in mtd.itervalues():
            if mtd is None: return {}
            for aid, actor_mtd in node_mtd.get( 'data', {} ).get( 'mtd', {} ).iteritems():
                owner = actor_mtd.get( 'owner', None )
                if owner in self._entries:
                    # Looks like a version of that actor was maintained by us before
                    # so we'll add it to our roster.
                    self._watch[ aid ] = self._entries[ owner ]
                    self._log( 'adding pre-existing actor %s to patrol' % aid )
                    tally.setdefault( self._entries[ owner ].name, 0 )
                    tally[ self._entries[ owner ].name ] += 1
        return tally

    def _initializeMissingActors( self, existing ):
        if type( self._scale ) is int:
            currentScale = self._scale
        elif self._scale is not None:
            currentScale = self._scale()
        else:
            currentScale = None

        for actorEntry in self._entries.itervalues():
            actorName = actorEntry.name
            current = existing.get( actorName, 0 )
            targetNum = actorEntry.initialInstances
            if currentScale is not None and actorEntry.scalingFactor is not None:
                targetNum = int( currentScale / actorEntry.scalingFactor )
                if 0 != ( currentScale % actorEntry.scalingFactor ):
                    targetNum += 1
                if actorEntry.maxInstances is not None and targetNum > actorEntry.maxInstances:
                    targetNum =  actor.maxInstances
                self._log( 'actor %s scale %s / factor %s: %d' % ( actorName,
                                                                   currentScale, 
                                                                   actorEntry.scalingFactor,
                                                                   targetNum ) )
            if current < targetNum:
                newOwner = '%s/%s' % ( self._owner, actorName )
                self._log( 'actor %s has %d instances but requires %d, spawning' % ( actorName,
                                                                                     current,
                                                                                     targetNum ) )
                for _ in range( targetNum - current ):
                    status = self._beach.addActor( *(actorEntry.actorArgs[ 0 ]),
                                                   **(actorEntry.actorArgs[ 1 ]) )
                    self._log( 'actor launched: %s' % status )
                    if type( status ) is dict and status.get( 'status', {} ).get( 'success', False ):
                        self._watch[ status[ 'data' ][ 'uid' ] ] = actorEntry
            else:
                self._log( 'actor %s is satisfied' % actorName )

    def start( self ):
        self._stopEvent.clear()
        self._log( 'starting, patrolling %d actors' % len( self._entries ) )
        self._log( 'discovering pre-existing actors' )
        existing = self._scanForExistingActors()
        self._log( '%d pre-existing actors' % len( existing ) )
        self._initializeMissingActors( existing )
        self._log( 'starting patrol' )
        gevent.sleep(10)
        self._threads.add( gevent.spawn( self._sync ) )

    def stop( self ):
        self._log( 'stopping patrol' )
        self._stopEvent.set()
        self._threads.join( timeout = 30 )
        self._threads.kill( timeout = 10 )
        self._log( 'patrol stopped' )

    def monitor( self,
                 name,
                 initialInstances,
                 maxInstances = None,
                 scalingFactor = None,
                 relaunchOnFailure = True,
                 onFailureCall = None,
                 actorArgs = [], actorKwArgs = {} ):
        record = _PatrolEntry()
        record.name = name
        record.initialInstances = initialInstances
        record.maxInstances = maxInstances
        record.scalingFactor = scalingFactor
        record.relaunchOnFailure = relaunchOnFailure
        record.onFailureCall = onFailureCall
        actorKwArgs[ 'owner' ] = '%s/%s' % ( self._owner, name )
        record.actorArgs = ( actorArgs, actorKwArgs )

        self._entries[ '%s/%s' % ( self._owner, name ) ] = record

    def _processFallenActor( self, actorEntry ):
        if actorEntry.relaunchOnFailure:
            self._log( 'actor is set to relaunch on failure' )
            status = self._beach.addActor( *(actorEntry.actorArgs[ 0 ]), **(actorEntry.actorArgs[ 1 ]) )
            self._watch[ status[ 'data' ][ 'uid' ] ] = actorEntry
            self._log( 'actor relaunched: %s' % status )
        else:
            self._log( 'actor is not set to relaunch on failure' )

    def _sync( self ):
        while not self._stopEvent.wait( self._freq ):
            self._log( 'running sync' )
            allActors = Set()
            directory = self._beach.getDirectory()
            if type( directory ) is not dict:
                self._logCritical( 'error getting directory' )
                continue
            for actorName, dirEntries in directory.get( 'realms', {} ).get( self._realm, {} ).iteritems():
                for actorId in dirEntries.iterkeys():
                    allActors.add( actorId )
            self._log( 'found %d actors, testing for %d' % ( len( allActors ), len( self._watch ) ) )
            for actorId in self._watch.keys():
                if actorId not in allActors:
                    # An actor we were watching went down
                    self._log( 'actor %s has fallen' % actorId )
                    self._processFallenActor( self._watch[ actorId ] )
                    del( self._watch[ actorId ] )


class _PatrolEntry ( object ):
    def __init__( self ):
        self.name = None
        self.initialInstances = 1
        self.maxInstances = None
        self.scalingFactor = None
        self.relaunchOnFailure = True
        self.onFailureCall = None
        self.actorArgs = None

if __name__ == '__main__':
    import argparse

    timeToStopEvent = gevent.event.Event()

    def _stop():
        global timeToStopEvent
        timeToStopEvent.set()

    gevent.signal( signal.SIGQUIT, _stop )
    gevent.signal( signal.SIGINT, _stop )

    parser = argparse.ArgumentParser( prog = 'Patrol' )
    parser.add_argument( 'configFile',
                         type = str,
                         help = 'the main config file defining the beach cluster' )
    parser.add_argument( 'patrolFile',
                         type = argparse.FileType( 'r' ),
                         help = 'file defining the actors to patrol' )
    parser.add_argument( '--patrol-name',
                         type = str,
                         dest = 'patrolName',
                         required = False,
                         default = 'default',
                         help = ( 'an identifier for the patrol, use unique values '
                                  'for multiple potentially overlapping patrols' ) )
    parser.add_argument( '--realm',
                         type = str,
                         dest = 'realm',
                         required = False,
                         default = 'global',
                         help = ( 'the default realm used' ) )
    parser.add_argument( '--log-dest',
                         type = str,
                         required = False,
                         dest = 'logdest',
                         default = '/dev/log',
                         help = 'the destination for the logging for syslog' )
    parser.add_argument( '--set-scale',
                         type = int,
                         required = False,
                         dest = 'scale',
                         default = None,
                         help = 'the scale metric to use in conjunction with the actor\' scaling factor' )
    args = parser.parse_args()
    patrol = Patrol( args.configFile,
                     identifier = args.patrolName,
                     logging_dest =  args.logdest,
                     realm = args.realm,
                     scale = args.scale )

    try:
        exec( args.patrolFile.read(), { 'Patrol' : patrol.monitor,
                                        '__file__' : os.path.abspath( args.patrolFile.name ) } )
    except:
        patrol._logCritical( traceback.format_exc() )

    patrol.start()
    timeToStopEvent.wait()
    patrol.stop()