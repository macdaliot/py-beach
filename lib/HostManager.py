import sys
import os
import signal
import gevent
import gevent.event
import gevent.pool
import yaml
import multiprocessing
from Utils import *
import time
import uuid
import random
from sets import Set
import syslog
import subprocess
import psutil

timeToStopEvent = gevent.event.Event()

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

        self._initLogging()
        
        self.stopEvent = timeToStopEvent
        self.py_beach_dir = None
        self.configFilePath = os.path.abspath( configFile )
        self.configFile = None
        self.directory = {}
        self.tombstones = {}
        self.actorInfo = {}
        self.ports_available = Set()
        self.nProcesses = 0
        self.processes = []
        self.initialProcesses = False
        self.seedNodes = []
        self.directoryPort = None
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
        with open( self.configFilePath, 'r' ) as f:
            self.configFile = yaml.load( f )

        self.py_beach_dir = os.path.dirname( os.path.abspath( __file__ ) )

        os.chdir( os.path.dirname( os.path.abspath( self.configFilePath ) ) )

        self.nProcesses = self.configFile.get( 'n_processes', 0 )
        if self.nProcesses == 0:
            self.nProcesses = multiprocessing.cpu_count()
        self.log( "Using %d instances per node" % self.nProcesses )
        
        self.seedNodes = self.configFile.get( 'seed_nodes', [] )
        for s in self.seedNodes:
            self.log( "Using seed node: %s" % s )

        self.interface = self.configFile.get( 'interface', 'eth0' )
        self.ifaceIp4 = getIpv4ForIface( self.interface )

        self.directoryPort = ZMREP( self.configFile.get( 'directory_port',
                                                         'ipc:///tmp/py_beach_directory_port' ),
                                    isBind = True )
        
        self.opsPort = self.configFile.get( 'ops_port', 4999 )
        self.opsSocket = ZMREP( 'tcp://%s:%d' % ( self.ifaceIp4, self.opsPort ), isBind = True )
        self.log( "Listening for ops on %s:%d" % ( self.ifaceIp4, self.opsPort ) )
        
        self.port_range = ( self.configFile.get( 'port_range_start', 5000 ), self.configFile.get( 'port_range_end', 6000 ) )
        self.ports_available.update( xrange( self.port_range[ 0 ], self.port_range[ 1 ] + 1 ) )
        
        self.peer_keepalive_seconds = self.configFile.get( 'peer_keepalive_seconds', 60 )
        self.instance_keepalive_seconds = self.configFile.get( 'instance_keepalive_seconds', 60 )
        self.directory_sync_seconds = self.configFile.get( 'directory_sync_seconds', 60 )
        self.tombstone_culling_seconds = self.configFile.get( 'tombstone_culling_seconds', 3600 )
        
        self.instance_strategy = self.configFile.get( 'instance_strategy', 'random' )
        
        # Bootstrap the seeds
        for s in self.seedNodes:
            self._connectToNode( s )
        
        # Start services
        self.log( "Starting services" )
        gevent.spawn( self.svc_directory_requests )
        gevent.spawn( self.svc_instance_keepalive )
        gevent.spawn( self.svc_host_keepalive )
        gevent.spawn( self.svc_directory_sync )
        gevent.spawn( self.svc_cullTombstones )
        gevent.spawn( self.svc_receiveOpsTasks )
        
        # Start the instances
        for n in range( self.nProcesses ):
            procSocket = ZMREQ( 'ipc:///tmp/py_beach_instance_%d' % n, isBind = False )
            self.processes.append( { 'socket' : procSocket, 'p' : None } )
            self.log( "Managing instance at: %s" % ( 'ipc:///tmp/py_beach_instance_%d' % n, ) )
        
        # Wait to be signaled to exit
        self.log( "Up and running" )
        timeToStopEvent.wait()
        
        # Any teardown required
        
        self.log( "Exiting." )

    def _connectToNode( self, ip ):
        nodeSocket = ZMREQ( 'tcp://%s:%d' % ( ip, self.opsPort ), isBind = False )
        self.nodes[ ip ] = { 'socket' : nodeSocket, 'last_seen' : None }

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
            self.log( "Culling tombstones" )
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
        z = self.opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            data = z.recv()
            if data is not False and 'req' in data:
                action = data[ 'req' ]
                self.log( "Received new ops request: %s" % action )
                if 'keepalive' == action:
                    if 'from' in data and data[ 'from' ] not in self.nodes:
                        self.log( "Discovered new node: %s" % data[ 'from' ] )
                        self._connectToNode( data[ 'from' ] )
                    z.send( successMessage() )
                elif 'start_actor' == action:
                    if 'actor_name' not in data or 'cat' not in data:
                        z.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        category = data[ 'cat' ]
                        realm = data.get( 'realm', 'global' )
                        uid = str( uuid.uuid4() )
                        port = self._getAvailablePortForUid( uid )
                        instance = self._getInstanceForActor( uid, actorName, realm )
                        newMsg = self.processes[ instance ][ 'socket' ].request( { 'req' : 'start_actor',
                                                                                   'actor_name' : actorName,
                                                                                   'realm' : realm,
                                                                                   'uid' : uid,
                                                                                   'port' : port },
                                                                                 timeout = 10 )
                        if isMessageSuccess( newMsg ):
                            self.directory.setdefault( realm,
                                                       {} ).setdefault( category,
                                                                        {} )[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4,
                                                                                                        port )
                        else:
                            self._removeUidFromDirectory( uid )
                        z.send( newMsg )
                elif 'kill_actor' == action:
                    if 'uid' not in data:
                        z.send( errorMessage( 'missing information to stop actor' ) )
                    else:
                        uid = data[ 'uid' ]
                        if uid not in self.actorInfo:
                            z.send( errorMessage( 'actor not found' ) )
                        else:
                            instance = self.actorInfo[ uid ][ 'instance' ]
                            newMsg = self.processes[ instance ][ 'socket' ].request( { 'req' : 'kill_actor',
                                                                                       'uid' : uid },
                                                                                     timeout = 10 )
                            if isMessageSuccess( newMsg ):
                                if not self._removeUidFromDirectory( uid ):
                                    newMsg = errorMessage( 'error removing actor from directory after stop' )

                            z.send( newMsg )
                elif 'remove_actor' == action:
                    if 'uid' not in data:
                        z.send( errorMessage( 'missing information to remove actor' ) )
                    else:
                        isFound = False
                        uid = data[ 'uid' ]
                        if self._removeUidFromDirectory( uid ):
                            z.send( successMessage() )
                        else:
                            z.send( errorMessage( 'actor to stop not found' ) )
                elif 'host_info' == action:
                    z.send( successMessage( { 'cpu' : psutil.cpu_percent( percpu = True,
                                                                                       interval = 2 ),
                                                           'mem' : psutil.virtual_memory()[ 'percent' ] } ) )
                elif 'get_dir' == action:
                    z.send( successMessage( { 'realms' : self.directory } ) )
                elif 'get_nodes' == action:
                    nodeList = {}
                    for k in self.nodes.keys():
                        nodeList[ k ] = { 'last_seen' : self.nodes[ k ][ 'last_seen' ] }
                    z.send( successMessage( { 'nodes' : nodeList } ) )
                else:
                    z.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                z.send( errorMessage( 'invalid request' ) )
                self.logCritical( "Received completely invalid request" )
    
    def svc_directory_requests( self ):
        z = self.directoryPort.getChild()
        while not self.stopEvent.wait( 0 ):
            data = z.recv()

            self.log( "Received directory request" )
            
            realm = data.get( 'realm', 'global' )
            if 'cat' in data:
                z.send( successMessage( data = { 'endpoints' : self.directory.get( realm, {} ).get( data[ 'cat' ], {} ) } ) )
            else:
                z.send( errorMessage( 'no category specified' ) )
    
    def svc_instance_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for n in range( len( self.processes ) ):
                self.log( "Issuing keepalive for instance %d" % n )
                data = self.processes[ n ][ 'socket' ].request( { 'req' : 'keepalive' }, timeout = 5 )
                if not isMessageSuccess( data ):
                    if self.processes[ n ][ 'p' ] is not None:
                        self.processes[ n ][ 'p' ].kill()
                    proc = subprocess.Popen( [ 'python',
                                               '%s/ActorHost.py' % self.py_beach_dir,
                                                self.configFilePath,
                                                str( n ) ] )

                    self.processes[ n ][ 'p' ] = proc

                    if self.initialProcesses:
                        self.logCritical( "Instance %d died, restarting it, pid %d" % ( n, proc.pid ) )
                    else:
                        self.log( "Initial instance %d created with pid %d" % ( n, proc.pid ) )
                        self.initialProcesses = True
            
            gevent.sleep( self.instance_keepalive_seconds )
    
    def svc_host_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for nodeName, node in self.nodes.iteritems():
                self.log( "Issuing keepalive for node %s" % nodeName )
                data = node[ 'socket' ].request( { 'req' : 'keepalive',
                                                   'from' : self.ifaceIp4 }, timeout = 10 )
                
                if isMessageSuccess( data ):
                    node[ 'last_seen' ] = int( time.time() )
                else:
                    self.log( "Removing node %s because of timeout" % nodeName )
                    del( self.nodes[ nodeName ] )
            
            gevent.sleep( self.peer_keepalive_seconds )
    
    def svc_directory_sync( self ):
        while not self.stopEvent.wait( 0 ):
            for nodeName, node in self.nodes.iteritems():
                self.log( "Issuing directory sync with node %s" % nodeName )
                data = node[ 'socket' ].request( { 'req' : 'dir_sync' } )
                
                if isMessageSuccess( data ):
                    self.directory.update( data[ 'directory' ] )
                    for uid in data[ 'tombstones' ]:
                        self._removeUidFromDirectory( uid )
            
            gevent.sleep( self.directory_sync_seconds )

    def _initLogging( self ):
        syslog.openlog( '%s-%d' % ( self.__class__.__name__, os.getpid() ), facility = syslog.LOG_USER )

    def log( self, msg ):
        syslog.syslog( syslog.LOG_INFO, msg )
        msg = '%s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )

    def logCritical( self, msg ):
        syslog.syslog( syslog.LOG_ERR, msg )
        msg = '!!! %s - %s : %s' % ( int( time.time() ), self.__class__.__name__, msg )
        print( msg )
    

if __name__ == '__main__':
    hostManager = HostManager( sys.argv[ 1 ] )