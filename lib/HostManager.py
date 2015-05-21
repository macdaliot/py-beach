import sys
import os
import importlib
import signal
import gevent
from gevent import Greenlet
from gevent.event import Event
import yaml
import multiprocessing
from Utils import *
import time
import uuid
import random
from sets import Set
import netifaces

timeToStopEvent = Event()

def _stop():
    global timeToStopEvent
    timeToStopEvent.set()

class HostManager ( object ):
    
    # The actorList is a list( actorNames, configFile )
    def __init__( self, configFile ):
        
        # Setting the signal handler to trigger the stop event
        global timeToStopEvent
        gevent.signal( signal.SIGQUIT, _stop )
        gevent.signal( signal.SIGINT, _stop )
        
        self.stopEvent = timeToStopEvent
        self.py_beach_dir = None
        self.configFilePath = configFile
        self.configFile = configFile
        self.directory = {}
        self.tombstones = {}
        self.actorInfo = {}
        self.ports_available = Set()
        self.nProcesses = 0
        self.processes = []
        self.seedNodes = []
        self.directoryPort = None
        self.managementPort = None
        self.opsPort = 0
        self.opsSocket = None
        self.port_range = ( 0, 0 )
        self.interface = None
        self.ifaceIp4 = None
        self.nodes = {}
        self.peer_keepalive_seconds = 0
        self.instance_keepalive_seconds = 0
        self.tombstone_culling_seconds = 0
        
        # Load default configs
        with open( self.configFile, 'r' ) as f:
            self.configFile = yaml.load( f )
        
        self.py_beach_dir = self.configFile.get( 'py_beach_directory', './' )
        
        self.nProcesses = self.configFile.get( 'n_processes', 0 )
        if self.nProcesses == 0:
            self.nProcesses = multiprocessing.cpu_count()
        print( "Using %d instances per node" % self.nProcesses )
        
        self.seedNodes = self.configFile.get( 'seed_nodes', [] )
        for s in self.seedNodes:
            print( "Using seed node: %s" % s )

        self.interface = self.configFile.get( 'interface', 'eth0' )
        self.ifaceIp4 = netifaces.ifaddresses( self.interface )[ netifaces.AF_INET ][ 0 ][ 'addr' ]
        
        self.directoryPort = ZSocket( zmq.REP, self.configFile.get( 'directory_port', 'ipc:///tmp/py_beach_directory_port' ), isBind = True )
        self.managementPort = ZSocket( zmq.REP, self.configFile.get( 'management_port', 'ipc:///tmp/py_beach_management_port' ), isBind = True )
        
        self.opsPort = self.configFile.get( 'ops_port', 4999 )
        self.opsSocket = ZSocket( zmq.REP, 'tcp://%s:%d' % ( self.ifaceIp4, self.opsPort ), isBind = True )
        
        self.port_range = ( self.configFile.get( 'port_range_start', 5000 ), self.configFile.get( 'port_range_end', 6000 ) )
        self.ports_available.update( xrange( self.port_range[ 0 ], self.port_range[ 1 ] + 1 ) )
        
        self.peer_keepalive_seconds = self.configFile.get( 'peer_keepalive_seconds', 60 )
        self.instance_keepalive_seconds = self.configFile.get( 'instance_keepalive_seconds', 60 )
        self.directory_sync_seconds = self.configFile.get( 'directory_sync_seconds', 60 )
        self.tombstone_culling_seconsd = self.configFile.get( 'tombstone_culling_seconsd', 3600 )
        
        self.instance_strategy = self.configFile.get( 'instance_strategy', 'random' )
        
        # Bootstrap the seeds
        for s in self.seedNodes:
            nodeSocket = ZSocket( zmq.REQ, 'tcp://%s:%d' % ( s, self.opsPort ), isBind = False )
            self.nodes[ s ] = { 'socket' : nodeSocket }
        
        # Start services
        print( "Starting services" )
        gevent.spawn( self.svc_directory_requests )
        gevent.spawn( self.svc_instance_keepalive )
        gevent.spawn( self.svc_host_keepalive )
        gevent.spawn( self.svc_directory_sync )
        gevent.spawn( self.svc_cullTombstones )
        
        # Start the instances
        for n in range( self.nProcesses ):
            procSocket = ZSocket( zmq.REQ, 'ipc:///tmp/py_beach_instance_%d' % n, isBind = False )
            self.processes[ n ] = procSocket
            os.spawnl( os.P_DETACH, '%s/ActorHost.py %s %d' % ( self.py_beach_dir, self.configFilePath, n ) )
        
        # Wait to be signaled to exit
        print( "Up and running" )
        timeToStopEvent.wait()
        
        # Any teardown required
        
        print( "Exiting." )
    
    
    
    def _removeUidFromDirectory( self, uid ):
        isFound = False
        for r in self.directory.values():
            for c in r.values():
                if uid in c:
                    del( c[ uid ] )
                    isFound = True
                    break
            if isFound:
                self.tombstones[ uid ] = int( time.time() )
                break

        if uid in self.actorInfo:
            port = self.actorInfo[ uid ][ 'port' ]
            self.ports_available.add( port )
            del( self.actorInfo[ uid ] )

        return isFound
    
    def _getAvailablePortForUid( self, uid ):
        port = None
        
        if 0 != len( self.ports_available ):
            port = self.ports_available.pop()
            self.actorInfo.setdefault( uid, {} )[ 'port' ] =  port
        
        return port
    
    def _getInstanceForActor( self, uid, actorName, realm ):
        instance = None
        
        if self.instance_strategy == 'random':
            instance = random.randint( 0, self.nProcesses - 1 )
        
        if instance is not None:
            self.actorInfo.setdefault( uid, {} )[ 'instance' ] = instance
        
        return instance
    
    
    
    def svc_cullTombstones( self ):
        while not self.stopEvent.wait( 0 ):
            currentTime = int( time.time() )
            maxTime = self.tombstone_culling_seconds
            nextTime = currentTime
            
            for uid, ts in self.tombstones.itervalues():
                if ts < currentTime - maxTime:
                    del( self.tombstones[ uid ] )
                elif ts < nextTime:
                    nextTime = ts
            
            gevent.sleep( maxTime - ( currentTime - nextTime ) )
    
    def svc_receiveOpsTasks( self ):
        while not self.stopEvent.wait( 0 ):
            data = self.opsSocket.recv()
            if data is not False and 'req' in data:
                action = data[ 'req' ]
                if 'keepalive' == action:
                    self.opsSocket.send( successMessage() )
                elif 'start_actor' == action:
                    if 'actor_name' not in data or 'cat' not in data:
                        self.opsSocket.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        category = data[ 'cat' ]
                        realm = data.get( 'realm', 'global' )
                        uid = uuid.uuid4()
                        port = self._getAvailablePort( uid )
                        instance = self._getInstanceForActor( uid, actorName, realm )
                        newMsg = self.processes[ instance ].request( { 'req' : 'start_actor',
                                                                       'actor_name' : actorName,
                                                                       'realm' : realm,
                                                                       'uid' : uid,
                                                                       'port' : port } )
                        if isMessageSuccess( newMsg ):
                            self.directory.setdefault( realm, {} ).setdefault( category, {} )[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4, port )
                        else:
                            self._removeUidFromDirectory( uid )
                        self.opsSocket.send( newMsg )
                elif 'kill_actor' == action:
                    if 'uid' not in data:
                        self.opsSocket.send( errorMessage( 'missing information to stop actor' ) )
                    else:
                        uid = data[ 'uid' ]
                        if uid not in self.actorInfo:
                            self.opsSocket.send( errorMessage( 'actor not found' ) )
                        else:
                            instance = self.actorInfo[ uid ][ 'instance' ]
                            newMsg = self.processes[ instance ].request( { 'req' : 'kill_actor',
                                                                           'uid' : uid } )
                            if isMessageSuccess( newMsg ):
                                if not self._removeUidFromDirectory( uid ):
                                    newMsg = errorMessage( 'error removing actor from directory after stop' )
                            
                            self.opsSocket.send( newMsg )
                elif 'remove_actor' == action:
                    if 'uid' not in data:
                        self.opsSocket.send( errorMessage( 'missing information to remove actor' ) )
                    else:
                        isFound = False
                        uid = data[ 'uid' ]
                        if self._removeUidFromDirectory( uid ):
                            self.opsSocket.send( successMessage() )
                        else:
                            self.opsSocket.send( errorMessage( 'actor to stop not found' ) )
                else:
                    self.opsSocket.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                self.opsSocket.send( errorMessage( 'invalid request' ) )
    
    def svc_directory_requests( self ):
        while not self.stopEvent.wait( 0 ):
            data = self.directoryPort.recv()
            
            realm = data.get( 'realm', 'global' )
            if 'cat' in data:
                self.directoryPort.send( successMessage( data = self.directory.get( realm, {} ).get( data[ 'cat' ], {} ) ) )
            else:
                self.directoryPort.send( errorMessage( 'no category specified' ) )
    
    def svc_instance_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for n in range( len( self.processes ) ):
                
                data = self.processes[ n ].request( { 'req' : 'keepalive' } )
                
                if not isMessageSuccess( data ):
                    print( "Instance %d died, restarting it" % n )
                    os.spawnl( os.P_DETACH, '%s/ActorHost.py %s %d' % ( self.py_beach_dir, self.configFilePath, n ) )
            
            gevent.sleep( self.instance_keepalive_seconds )
    
    def svc_host_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for nodeName, node in self.nodes.iteritems():
                
                data = node[ 'socket' ].request( { 'req' : 'keepalive' } )
                
                if isMessageSuccess( data ):
                    node[ 'last_seen' ] = int( time.time() )
                else:
                    print( "Removing node %s because of timeout" % nodeName )
                    del( self.nodes[ nodeName ] )
            
            gevent.sleep( self.peer_keepalive_seconds )
    
    def svc_directory_sync( self ):
        while not self.stopEvent.wait( 0 ):
            for nodeName, node in self.nodes.iteritems():
                
                data = node[ 'socket' ].request( { 'req' : 'dir_sync' } )
                
                if isMessageSuccess( data ):
                    self.directory.update( data[ 'directory' ] )
                    for uid in data[ 'tombstones' ]:
                        self._removeUidFromDirectory( uid )
            
            gevent.sleep( self.directory_sync_seconds )
    

if __name__ == '__main__':
    hostManager = HostManager( sys.argv[ 1 ] )