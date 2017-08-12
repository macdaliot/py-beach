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

import sys
if 'threading' in sys.modules and 'sphinx' not in sys.modules:
    import gevent.monkey
    if 0 == len( gevent.monkey.saved ):
        raise Exception('threading module loaded before patching!')
import gevent.monkey
gevent.monkey.patch_all()

import os
import signal
import syslog
import gevent
from gevent import Greenlet
from gevent.event import Event
from beach.utils import *
from beach.utils import _getIpv4ForIface
from beach.utils import _ZMREP
from beach.utils import loadModuleFrom
import zmq.green as zmq
from beach.actor import *
from beach.utils import parallelExec
import yaml
import time
import logging
import logging.handlers
from functools import wraps
import traceback

timeToStopEvent = gevent.event.Event()

def handleExceptions( f ):
    @wraps( f )
    def wrapped( *args, **kwargs ):
        while True:
            res = None
            try:
                res = f( *args, **kwargs )
            except:
                args[ 0 ].logCritical( traceback.format_exc() )
            else:
                break
        return res
    return wrapped

timeToStopEvent = Event()

def _stopAllActors():
    global timeToStopEvent
    timeToStopEvent.set()

class ActorHost ( object ):
    
    # The actorList is a list( actorNames, configFile )
    def __init__( self, configFile, instanceId, logging_level, logging_dest, interface ):
        
        # Setting the signal handler to trigger the stop event which
        # is interpreted by each actor implementation
        global timeToStopEvent
        gevent.signal( signal.SIGQUIT, _stopAllActors )
        gevent.signal( signal.SIGINT, _stopAllActors )
        gevent.signal( signal.SIGTERM, _stopAllActors )

        self.instanceId = instanceId

        self._log_level = logging_level
        self._log_dest = logging_dest
        self._initLogging( logging_level, logging_dest )

        self.log( "Initializing" )
        
        self.stopEvent = timeToStopEvent
        self.isOpen = True

        self.actors = {}

        self.py_beach_dir = None

        self.configFilePath = configFile
        self.configFile = None

        self.interface = interface
        self.ifaceIp4 = _getIpv4ForIface( self.interface )

        with open( self.configFilePath, 'r' ) as f:
            self.configFile = yaml.load( f )

        self.py_beach_dir = os.path.dirname( os.path.abspath( __file__ ) )

        os.chdir( os.path.dirname( os.path.abspath( self.configFilePath ) ) )

        self.private_key = self.configFile.get( 'private_key', None )
        if self.private_key is not None:
            with open( self.private_key, 'r' ) as f:
                key_path = self.private_key
                self.private_key = f.read()
                self.log( "Using shared key: %s" % key_path )

        self.codeDirectory = self.configFile.get( 'code_directory', './' )
        if '://' not in self.codeDirectory:
            self.codeDirectory = os.path.abspath( self.codeDirectory )

        Actor._code_directory_root = self.codeDirectory

        self.opsSocket = _ZMREP( 'ipc:///tmp/py_beach_instance_%s' % instanceId,
                                 isBind = True )
        self.log( "Listening for ops on %s" % ( 'ipc:///tmp/py_beach_instance_%s' % instanceId, ) )
        
        self.hostOpsPort = self.configFile.get( 'ops_port', 4999 )
        self.hostOpsSocket = _ZMREP( 'tcp://%s:%d' % ( self.ifaceIp4, self.hostOpsPort ),
                                     isBind = False,
                                     private_key = self.private_key )

        ActorHandle._setHostDirInfo( self.configFile.get( 'directory_port',
                                                          'ipc:///tmp/py_beach_directory_port' ),
                                     self.private_key )

        ActorHandleGroup._setHostDirInfo( 'tcp://%s:%d' % ( self.ifaceIp4, self.hostOpsPort ),
                                          self.private_key )
        
        for _ in range( 20 ):
            gevent.spawn( self.svc_receiveTasks )
        gevent.spawn( self.svc_monitorActors )

        self.log( "Now open to actors" )

        timeToStopEvent.wait()
        
        self.log( "Exiting, stopping all actors." )
        
        for actor in self.actors.values():
            actor.stop()
        
        gevent.joinall( self.actors.values() )
        self.log( "All Actors exiting, exiting." )
    
    @handleExceptions
    def svc_receiveTasks( self ):
        z = self.opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            #self.log( "Waiting for op" )
            data = z.recv()
            if data is not False and data is not None and 'req' in data:
                action = data[ 'req' ]
                #self.log( "Received new ops request: %s" % action )
                if 'keepalive' == action:
                    z.send( successMessage() )
                elif 'start_actor' == action:
                    if not self.isOpen:
                        z.send( errorMessage( 'draining' ) )
                    if 'actor_name' not in data or 'port' not in data or 'uid' not in data:
                        z.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        className = actorName[ actorName.rfind( '/' ) + 1 : ]
                        className = className[ : -3 ] if className.endswith( '.py' ) else className
                        realm = data.get( 'realm', 'global' )
                        parameters = data.get( 'parameters', {} )
                        resources = data.get( 'resources', {} )
                        ident = data.get( 'ident', None )
                        trusted = data.get( 'trusted', [] )
                        n_concurrent = data.get( 'n_concurrent', 1 )
                        is_drainable = data.get( 'is_drainable', False )
                        ip = data[ 'ip' ]
                        port = data[ 'port' ]
                        uid = data[ 'uid' ]
                        log_level = data.get( 'loglevel', None )
                        log_dest = data.get( 'logdest', None )
                        if log_level is None:
                            log_level = self._log_level
                        if log_dest is None:
                            log_dest = self._log_dest
                        try:
                            implementation = None
                            if '://' in actorName:
                                actorName = actorName if actorName.endswith( '.py' ) else '%s.py' % actorName
                                self.log( "Starting actor %s/%s at absolute %s" % ( realm, actorName, actorName ) )
                                implementation = loadModuleFrom( actorName, realm )
                            else:
                                self.log( "Starting actor %s/%s at %s/%s/%s.py" % ( realm,
                                                                                    actorName,
                                                                                    self.codeDirectory,
                                                                                    realm,
                                                                                    actorName ) )
                                implementation = loadModuleFrom( '%s/%s/%s.py' % ( self.codeDirectory,
                                                                                   realm,
                                                                                   actorName ),
                                                                 realm )

                            actor = getattr( implementation,
                                             className )( self,
                                                          realm,
                                                          ip,
                                                          port,
                                                          uid,
                                                          log_level,
                                                          log_dest,
                                                          self.configFilePath,
                                                          parameters = parameters,
                                                          resources = resources,
                                                          ident = ident,
                                                          trusted = trusted,
                                                          n_concurrent = n_concurrent,
                                                          private_key = self.private_key,
                                                          is_drainable = is_drainable )
                        except:
                            actor = None
                            self.logCritical( "Error loading actor %s: %s (%s)" % ( actorName, traceback.format_exc(), dir( implementation ) ) )

                        if actor is not None:
                            self.log( "Successfully loaded actor %s/%s" % ( realm, actorName ) )
                            self.actors[ uid ] = actor
                            actor.start()
                            z.send( successMessage( { 'uid' : uid } ) )
                        else:
                            self.logCritical( 'Error loading actor %s/%s: %s' % ( realm,
                                                                                  actorName,
                                                                                  traceback.format_exc() ) )
                            z.send( errorMessage( 'exception',
                                                  data = { 'st' : traceback.format_exc() } ) )
                elif 'kill_actor' == action:
                    if 'uid' not in data:
                        z.send( errorMessage( 'missing information to stop actor' ) )
                    else:
                        uid = data[ 'uid' ]
                        if uid in self.actors:
                            actor = self.actors[ uid ]
                            del( self.actors[ uid ] )
                            actor.stop()
                            actor.join( timeout = 10 )
                            info = None
                            if not actor.ready():
                                actor.kill( timeout = 10 )
                                info = { 'error' : 'timeout' }
                                self.log( "Actor %s timedout while exiting" % uid )
                            z.send( successMessage( data = info ) )
                        else:
                            z.send( errorMessage( 'actor not found' ) )
                elif 'get_load_info' == action:
                    info = {}
                    for uid, actor in self.actors.items():
                        info[ uid ] = ( actor._n_free_handlers, actor._n_concurrent, actor.getPending(), actor._qps, actor._q_avg )
                    z.send( successMessage( data = info ) )
                elif 'is_drainable' == action:
                    z.send( successMessage( { 'is_drainable' : self.isDrainable() } ) )
                elif 'drain' == action:
                    z.send( successMessage( { 'is_drained' : self.drain() } ) )
                else:
                    z.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                self.logCritical( "Received completely invalid request" )
                z.send( errorMessage( 'invalid request' ) )

    @handleExceptions
    def svc_monitorActors( self ):
        z = self.hostOpsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            #self.log( "Culling actors that stopped of themselves" )
            for uid, actor in self.actors.items():
                if not actor.isRunning():
                    exc = actor.getLastError()
                    if exc is not None:
                        self.logCritical("Actor %s exited with exception: %s" % ( uid, str( exc ) ) )
                    else:
                        self.log( "Actor %s is no longer running" % uid )
                    del( self.actors[ uid ] )
                    z.send( { 'req' : 'remove_actor', 'uid' : uid }, timeout = 5 )
            gevent.sleep( 30 )

    @handleExceptions
    def isDrainable( self ):
        if 0 == len( self.actors ):
            return False
        return all( x._is_drainable for x in self.actors.itervalues() )

    def _drainActor( self, actor ):
        isDrained = True
        if actor.isRunning() and hasattr( actor, 'drain' ):
            actor.drain()
            while not self.stopEvent.wait( 5 ):
                if self._n_free_handlers != self._n_concurrent:
                    continue
                isDrained = True
                for h in actor.getPending():
                    if 0 == len( h ): 
                        isDrained = False
                        break
                if isDrained: break
        return isDrained

    @handleExceptions
    def drain( self ):
        if self.isDrainable():
            self.isOpen = False
            self.log( "Draining..." )
            drained = parallelExec( self._drainActor, self.actors.values() )
            self.log( "Drained." )
            gevent.spawn_later( 2, _stopAllActors )
            return True
        else:
            self.log( "Cannot drain, some Actors are not drainable." )
            return False

    def _initLogging( self, level, dest ):
        self._logger = logging.getLogger( self.instanceId )
        self._logger.setLevel( level )
        handler = logging.handlers.SysLogHandler( address = dest )
        handler.setFormatter( logging.Formatter( "%(asctime)-15s %(message)s" ) )
        self._logger.addHandler( handler )

    def log( self, msg ):
        self._logger.info( '%s-%s : %s', self.__class__.__name__, self.instanceId, msg )

    def logCritical( self, msg ):
        self._logger.error( '%s-%s : %s', self.__class__.__name__, self.instanceId, msg )

#def _profileSave():
#    stats = GreenletProfiler.get_func_stats()
#    stats.save('profile.callgrind', type='callgrind')
#    gevent.spawn_later( 60, _profileSave )

if __name__ == '__main__':
#    import GreenletProfiler
#    GreenletProfiler.set_clock_type('cpu')
#    gevent.spawn_later( 60, _profileSave )
#    GreenletProfiler.start()
    try:
        host = ActorHost( sys.argv[ 1 ], sys.argv[ 2 ], int( sys.argv[ 3 ] ), sys.argv[ 4 ], sys.argv[ 5 ] )
    except:
        host.logCritical( "Exception: %s" % str( traceback.format_exc() ) )