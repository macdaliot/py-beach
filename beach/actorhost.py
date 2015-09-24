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
import os
import signal
import gevent
from gevent import Greenlet
from gevent.event import Event
from beach.utils import *
from beach.utils import _ZMREP
import imp
import zmq.green as zmq
from beach.actor import *
import yaml
import time
import logging
import logging.handlers
import traceback

timeToStopEvent = Event()

def _stopAllActors():
    global timeToStopEvent
    timeToStopEvent.set()

class ActorHost ( object ):
    
    # The actorList is a list( actorNames, configFile )
    def __init__( self, configFile, instanceId ):
        
        # Setting the signal handler to trigger the stop event which
        # is interpreted by each actor implementation
        global timeToStopEvent
        gevent.signal( signal.SIGQUIT, _stopAllActors )
        gevent.signal( signal.SIGINT, _stopAllActors )

        self._initLogging()
        self.instanceId = instanceId

        self.log( "Initializing" )
        
        self.stopEvent = timeToStopEvent

        self.actors = {}

        self.py_beach_dir = None

        self.configFilePath = configFile
        self.configFile = None

        with open( self.configFilePath, 'r' ) as f:
            self.configFile = yaml.load( f )

        self.py_beach_dir = os.path.dirname( os.path.abspath( __file__ ) )

        os.chdir( os.path.dirname( os.path.abspath( self.configFilePath ) ) )

        self.codeDirectory = os.path.abspath( self.configFile.get( 'code_directory', './' ) )

        self.opsSocket = _ZMREP( 'ipc:///tmp/py_beach_instance_%s' % instanceId, isBind = True )
        self.log( "Listening for ops on %s" % ( 'ipc:///tmp/py_beach_instance_%s' % instanceId, ) )
        
        self.hostOpsPort = self.configFile.get( 'ops_port', 4999 )
        self.hostOpsSocket = _ZMREP( 'tcp://127.0.0.1:%d' % self.hostOpsPort, isBind = False )

        ActorHandle._setHostDirInfo( self.configFile.get( 'directory_port',
                                                          'ipc:///tmp/py_beach_directory_port' ) )

        ActorHandleGroup._setHostDirInfo( self.configFile.get( 'directory_port',
                                                               'ipc:///tmp/py_beach_directory_port' ) )
        
        gevent.spawn( self.svc_receiveTasks )
        gevent.spawn( self.svc_monitorActors )

        self.log( "Now open to actors" )

        timeToStopEvent.wait()
        
        self.log( "Exiting, stopping all actors." )
        
        for actor in self.actors.values():
            actor.stop()
        
        gevent.joinall( self.actors.values() )
        self.log( "All Actors exiting, exiting." )
    
    def svc_receiveTasks( self ):
        z = self.opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            self.log( "Waiting for op" )
            data = z.recv()
            if data is not False and 'req' in data:
                action = data[ 'req' ]
                self.log( "Received new ops request: %s" % action )
                if 'keepalive' == action:
                    z.send( successMessage() )
                elif 'start_actor' == action:
                    if 'actor_name' not in data or 'port' not in data or 'uid' not in data:
                        z.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        className = actorName[ actorName.rfind( '/' ) + 1 : ]
                        realm = data.get( 'realm', 'global' )
                        parameters = data.get( 'parameters', {} )
                        ident = data.get( 'ident', None )
                        trusted = data.get( 'trusted', [] )
                        ip = data[ 'ip' ]
                        port = data[ 'port' ]
                        uid = data[ 'uid' ]
                        fileName = '%s/%s/%s.py' % ( self.codeDirectory, realm, actorName )
                        with open( fileName, 'r' ) as hFile:
                            fileHash = hashlib.sha1( hFile.read() ).hexdigest()
                        self.log( "Starting actor %s/%s at %s/%s/%s.py" % ( realm,
                                                                            actorName,
                                                                            self.codeDirectory,
                                                                            realm,
                                                                            actorName ) )
                        try:
                            actor = getattr( imp.load_source( '%s_%s_%s' % ( realm, actorName, fileHash ),
                                                              '%s/%s/%s.py' % ( self.codeDirectory,
                                                                                realm,
                                                                                actorName ) ),
                                             className )( self, realm, ip, port, uid, parameters, ident, trusted )
                        except:
                            actor = None

                        if actor is not None:
                            self.log( "Successfully loaded actor %s/%s" % ( realm, actorName ) )
                            self.actors[ uid ] = actor
                            actor.start()
                            z.send( successMessage() )
                        else:
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
                            z.send( successMessage( data = info ) )
                        else:
                            z.send( errorMessage( 'actor not found' ) )
                else:
                    z.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                self.logCritical( "Received completely invalid request" )
                z.send( errorMessage( 'invalid request' ) )

    def svc_monitorActors( self ):
        z = self.hostOpsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            self.log( "Culling actors that stopped of themselves" )
            for uid, actor in self.actors.iteritems():
                if not actor.isRunning():
                    del( self.actors[ uid ] )
                    z.request( { 'req' : 'remove_actor', 'uid' : uid }, timeout = 5 )
            gevent.sleep( 30 )

    def _initLogging( self ):
        logging.basicConfig( format = "%(asctime)-15s %(message)s" )
        self._logger = logging.getLogger()
        self._logger.setLevel( logging.INFO )
        self._logger.addHandler( logging.handlers.SysLogHandler() )

    def log( self, msg ):
        self._logger.info( '%s-%s : %s', self.__class__.__name__, self.instanceId, msg )

    def logCritical( self, msg ):
        self._logger.error( '%s-%s : %s', self.__class__.__name__, self.instanceId, msg )

if __name__ == '__main__':
    host = ActorHost( sys.argv[ 1 ], sys.argv[ 2 ] )