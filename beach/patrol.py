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
from gevent.lock import BoundedSemaphore
import logging
import logging.handlers
import signal
import gevent
import gevent.event
import os
from sets import Set
from collections import OrderedDict
import traceback
import urllib2
import hashlib
import multiprocessing

def withLogException( f, patrol = None ):
    def _tmp( *args, **kw_args ):
        try:
            return f( *args, **kw_args )
        except gevent.GreenletExit:
            raise
        except:
            patrol._log( traceback.format_exc() )
    return _tmp

class Patrol ( object ):

    def __init__( self,
                  configFile,
                  identifier = 'default',
                  sync_frequency = 15.0,
                  logging_dest = '/dev/log',
                  realm = 'global',
                  scale = None,
                  actorsRoot = None ):
        self._stopEvent = gevent.event.Event()

        self._logger = None
        self._log_level = logging.INFO
        self._log_dest = logging_dest
        self._realm = realm
        self._initLogging( self._log_level, logging_dest )
        self._threads = gevent.pool.Group()

        self._owner = 'beach.patrol/%s' % ( identifier, )
        self._mutex = BoundedSemaphore( value = 1 )
        self._entries = OrderedDict()
        self._freq = sync_frequency
        self._updateFreq = 60 * 60

        self._patrolHash = None
        self._patrolUrl = None
        self._isMonitored = False

        self._beach = Beach( configFile, realm = realm )

        self._scale = scale
        self._actorsRoot = actorsRoot
        if self._actorsRoot is not None and not self._actorsRoot.endswith( '/' ):
            self._actorsRoot += '/'


    def _initLogging( self, level, dest ):
        self._logger = logging.getLogger( 'beach.patrol' )
        self._logger.handlers = []
        self._logger.setLevel( level )
        handler = logging.handlers.SysLogHandler( address = dest )
        handler.setFormatter( logging.Formatter( "%(asctime)-15s %(message)s" ) )
        self._logger.addHandler( handler )
        self._logger.propagate = False

    def _log( self, msg ):
        self._logger.info( '%s : %s', self.__class__.__name__, msg )

    def _logCritical( self, msg ):
        self._logger.error( '%s : %s', self.__class__.__name__, msg )

    def _scanForExistingActors( self ):
        tally = {}

        mtd = self._beach.getAllNodeMetadata()
        for node_mtd in mtd.itervalues():
            if node_mtd is False: continue
            for aid, actor_mtd in node_mtd.get( 'data', {} ).get( 'mtd', {} ).iteritems():
                if self._stopEvent.wait( 0 ): break
                owner = actor_mtd.get( 'owner', None )
                if owner in self._entries:
                    tally.setdefault( self._entries[ owner ].name, 0 )
                    tally[ self._entries[ owner ].name ] += 1
        return tally

    def _getTargetActorNum( self, actorEntry, currentScale ):
        targetNum = 0

        if callable( actorEntry.initialInstances ):
            targetNum = actorEntry.initialInstances()
        else:
            targetNum = actorEntry.initialInstances        

        if currentScale is not None and actorEntry.scalingFactor is not None:
            preScaleTarget = targetNum
            targetNum = int( currentScale / actorEntry.scalingFactor )
            if 0 != ( currentScale % actorEntry.scalingFactor ):
                targetNum += 1
            if actorEntry.maxInstances is not None and targetNum > actorEntry.maxInstances:
                targetNum =  actor.maxInstances
            if preScaleTarget is not None and targetNum < preScaleTarget:
                targetNum = preScaleTarget
            #self._log( 'actor %s scale %s / factor %s: %d' % ( actorEntry.name,
            #                                                   currentScale, 
            #                                                   actorEntry.scalingFactor,
            #                                                   targetNum ) )
            # If we're only spawning a single actor, it must not be drained
            # so that availability is maintained.
            if 1 == targetNum and actorEntry.actorArgs[ 1 ].get( 'is_drainable', False ):
                actorEntry.actorArgs[ 1 ][ 'is_drainable' ] = False
                self._log( 'actor %s was set to drainable but only starting once instance to turning it off' % ( actorEntry.name, ) )

        return targetNum

    def _getEffectiveScale( self ):
        if type( self._scale ) is int:
            currentScale = self._scale
        elif self._scale is not None:
            currentScale = self._scale()
        else:
            currentScale = None
        return currentScale

    def _initializeMissingActors( self, existing ):
        currentScale = self._getEffectiveScale()

        for actorEntry in self._entries.itervalues():
            if self._stopEvent.wait( 0 ): break
            actorName = actorEntry.name
            current = existing.get( actorName, 0 )
            targetNum = self._getTargetActorNum( actorEntry, currentScale )

            if current < targetNum:
                newOwner = '%s/%s' % ( self._owner, actorName )
                self._log( 'actor %s has %d instances but requires %d, spawning' % ( actorName,
                                                                                     current,
                                                                                     targetNum ) )
                for _ in range( targetNum - current ):
                    self._spawnNewActor( actorEntry )
            else:
                #self._log( 'actor %s is satisfied (%d)' % ( actorName, targetNum ) )
                pass

    def start( self ):
        self._stopEvent.clear()
        self._log( 'starting, patrolling %d actors' % len( self._entries ) )

        self._log( 'starting patrol' )
        gevent.sleep(10)
        self._threads.add( gevent.spawn( withLogException( self._sync, patrol = self ) ) )

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
                 onFailureCall = None,
                 actorArgs = [], actorKwArgs = {} ):
        actorArgs = list( actorArgs )
        if self._actorsRoot is not None:
            actorArgs = [ self._actorsRoot + actorArgs[ 0 ] ] + actorArgs[ 1 : ]
        record = _PatrolEntry()
        record.name = name
        record.initialInstances = initialInstances
        record.maxInstances = maxInstances
        record.scalingFactor = scalingFactor
        record.onFailureCall = onFailureCall
        # If the time to drain is dynamic we keep a copy of the function.
        ttd = actorKwArgs.get( 'time_to_drain', None )
        if callable( ttd ):
            record.timeToDrainFunc = ttd
        actorKwArgs[ 'owner' ] = '%s/%s' % ( self._owner, name )
        record.actorArgs = ( actorArgs, actorKwArgs )

        self._entries[ '%s/%s' % ( self._owner, name ) ] = record

    def _spawnNewActor( self, actorEntry ):
        kwArgs = actorEntry.actorArgs[ 1 ]
        if actorEntry.timeToDrainFunc is not None:
            kwArgs = kwArgs.copy()
            kwArgs[ 'time_to_drain' ] = actorEntry.timeToDrainFunc()
        status = self._beach.addActor( *(actorEntry.actorArgs[ 0 ]), **(kwArgs) )
        if status is not False and status is not None and 'data' in status and 'uid' in status[ 'data' ]:
            self._log( 'actor launched: %s' % status )
            return True
        elif status is False:
            self._log( 'timeout waiting for actor to launch: will wait until next sync if it came online' )
        else:
            self._log( 'failed to launch actor: %s' % status )
            return False

    def _sync( self ):
        while not self._stopEvent.wait( self._freq ):
            with self._mutex:
                self._log( 'running sync' )

                # We do a single refresh of the directory.
                # Don't force since it may have already gotten a recent snapshot.
                self._beach.getDirectory()

                # To remove the directory jitter we will suspend directory refresh temporarily.
                originalTtl = self._beach._dirCacheTtl
                self._beach._dirCacheTtl = 60 * 5

                self._initializeMissingActors( self._scanForExistingActors() )

                # Restore the original ttl.
                self._beach._dirCacheTtl = originalTtl

    def remove( self, name = None ):
        with self._mutex:
            if name is not None:
                k ='%s/%s' % ( self._owner, name )
                if k not in self._entries:
                    return False

                del( self._entries[ k ] )
            else:
                self._entries = OrderedDict()

        return True

    def _getPatrolFromUrl( self, url ):
        if '://' in url:
            patrolFilePath = url
            if patrolFilePath.startswith( 'file://' ):
                patrolFilePath = 'file://%s' % os.path.abspath( patrolFilePath[ len( 'file://' ) : ] )
            patrolFile = urllib2.urlopen( patrolFilePath )
        else:
            patrolFilePath = os.path.abspath( url )
            patrolFile = open( patrolFilePath, 'r' )
        return patrolFile.read(), patrolFilePath

    def loadFromUrl( self, url, isMonitorForUpdates = False ):
        patrolContent, patrolFilePath = self._getPatrolFromUrl( url )
        self._patrolUrl = url
        self._patrolHash = hashlib.sha256( patrolContent ).hexdigest()
        exec( patrolContent, { 'Patrol' : self.monitor,
                               '__file__' : patrolFilePath,
                               'NUM_CPU_CORES' : multiprocessing.cpu_count,
                               'NUM_NODES' : self._beach.getNodeCount } )
        if isMonitorForUpdates and not self._isMonitored:
            self._isMonitored = True
            self._threads.add( gevent.spawn( withLogException( self._updatePatrol, patrol = self ) ) )

    def _updatePatrol( self ):
        while not self._stopEvent.wait( self._updateFreq ):
            try:
                patrolContent, patrolFilePath = self._getPatrolFromUrl( self._patrolUrl )
            except:
                return
            if self._patrolHash == hashlib.sha256( patrolContent ).hexdigest():
                return
            with self._mutex:
                self._entries = OrderedDict()
                self._patrolUrl = url
                self._patrolHash = hashlib.sha256( patrolContent ).hexdigest()
                exec( patrolContent, { 'Patrol' : self.monitor,
                                       '__file__' : patrolFilePath } )
            


class _PatrolEntry ( object ):
    def __init__( self ):
        self.name = None
        self.initialInstances = 1
        self.maxInstances = None
        self.scalingFactor = None
        self.onFailureCall = None
        self.actorArgs = None
        self.timeToDrainFunc = None

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
                         type = str,
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
    parser.add_argument( '--frequency',
                         type = int,
                         required = False,
                         dest = 'freq',
                         default = 15.0,
                         help = 'the frequency in seconds at which the patrol occurs' )
    args = parser.parse_args()
    patrol = Patrol( args.configFile,
                     identifier = args.patrolName,
                     logging_dest =  args.logdest,
                     realm = args.realm,
                     scale = args.scale,
                     sync_frequency = args.freq )

    try:
        patrol.loadFromUrl( args.patrolFile )
    except:
        patrol._logCritical( traceback.format_exc() )

    patrol.start()
    timeToStopEvent.wait()
    patrol.stop()