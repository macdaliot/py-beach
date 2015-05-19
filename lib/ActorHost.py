import sys
import os
import signal
import gevent
from gevent import Greenlet
from gevent.event import Event
from Utils import *
import imp


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
        
        self.stopEvent = timeToStopEvent
        self.actors = {}
        self.py_beach_dir = None
        self.configFilePath = configFile
        self.configFile = configFile
        self.codeDirectory = self.configFile.get( 'code_directory', './' )
        self.opsSocket = ZSocket( zmq.REP, 'ipc:///tmp/py_beach_instance_%d' % instanceId )
        
        self.py_beach_dir = self.configFile.get( 'py_beach_directory', './' )
        
        self.hostOpsPort = self.configFile.get( 'ops_port', 4999 )
        self.hostOpsSocket = ZSocket( zmq.REP, 'tcp://127.0.0.1:%d' % self.hostOpsPort, isBind = False )
        
        gevent.spawn( self.svc_receiveTasks )
        gevent.spawn( self.svc_monitorActors )
        
        timeToStopEvent.wait()
        
        print( "Exiting, stopping all actors." )
        
        for actor in self.actors.values():
            actor.stop()
        
        gevent.joinall( self.actors.values() )
        print( "All Actors exiting, exiting." )
    
    def svc_receiveTasks( self ):
        while not self.stopEvent.wait( 0 ):
            data = self.opsSocket.recv()
            if data is not False and 'req' in data:
                action = data[ 'req' ]
                if 'keepalive' == action:
                    self.opsSocket.send( successMessage() )
                elif 'start_actor' == action:
                    if 'actor_name' not in data or 'port' not in data or 'uid' not in data:
                        self.opsSocket.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        realm = data.get( 'realm', 'global' )
                        port = data[ 'port' ]
                        uid = data[ 'uid' ]
                        actor = getattr( importlib.load_source( '%s_%s' % ( realm, actorName ), '%s/%s.py' % ( self.codeDirectory, actorName ) ), actorName )( realm, port, uid )
                        self.actors[ uid ] = actor
                        actor.start()
                        self.opsSocket.send( successMessage() )
                elif 'kill_actor' == action:
                    if 'uid' not in data:
                        self.opsSocket.send( errorMessage( 'missing information to stop actor' ) )
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
                            self.opsSocket.send( successMessage( data = info ) )
                        else:
                            self.opsSocket.send( errorMessage( 'actor not found' ) )
                else:
                    self.opsSocket.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                self.opsSocket.send( errorMessage( 'invalid request' ) )

    def svc_monitorActors( self ):
        while not self.stopEvent.wait( 0 ):
            for uid, actor in self.actors.iteritems():
                if not actor.isRunning():
                    del( self.actors[ uid ] )
                    self.hostOpsSocket.request( { 'req' : 'remove_actor', 'uid' : uid } )
            gevent.sleep( 30 )

if __name__ == '__main__':
    host = ActorHost( sys.argv[ 1 ], sys.argv[ 2 ] )