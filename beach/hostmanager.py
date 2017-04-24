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

'''The main script to run on a beach cluster node, it takes care of everything. Instantiate like:
    python -m beach.hostmanager -h
'''

import sys
import os
import signal
import gevent
import gevent.event
import gevent.pool
import yaml
import multiprocessing
from beach.utils import *
from beach.utils import _getIpv4ForIface
from beach.utils import _getPublicInterfaces
from beach.utils import _ZMREQ
from beach.utils import _ZMREP
from beach.utils import RWLock
import socket
import time
import random
from sets import Set
import logging
import logging.handlers
import subprocess
import psutil
import collections
import uuid
import syslog
from prefixtree import PrefixDict
from functools import wraps
import traceback
import copy
import inspect

timeToStopEvent = gevent.event.Event()

def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno

def handleExceptions( f ):
    @wraps( f )
    def wrapped( *args, **kwargs ):
        while True:
            res = None
            try:
                res = f( *args, **kwargs )
            except:
                print( traceback.format_exc() )
                args[ 0 ]._logCritical( traceback.format_exc() )
            else:
                break
        return res
    return wrapped

def _stop():
    global timeToStopEvent
    timeToStopEvent.set()

class HostManager ( object ):
    
    # The actorList is a list( actorNames, configFile )
    def __init__( self, configFile, logging_level, logging_dest, iface = None ):
        
        # Setting the signal handler to trigger the stop event
        global timeToStopEvent
        gevent.signal( signal.SIGQUIT, _stop )
        gevent.signal( signal.SIGINT, _stop )

        self._logger = None
        self._log_level = logging_level
        self._log_dest = logging_dest
        self._initLogging( logging_level, logging_dest )
        
        self.stopEvent = timeToStopEvent
        self.py_beach_dir = None
        self.configFilePath = os.path.abspath( configFile )
        self.configFile = None
        self.directory = {}
        self.reverseDir = {}
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
        self.isActorChanged = gevent.event.Event()
        self.isInstanceChanged = gevent.event.Event()
        self.isTombstoneChanged = gevent.event.Event()
        self.dirLock = RWLock()

        # Cleanup potentially old sockets
        os.system( 'rm /tmp/py_beach*' )

        # Load default configs
        with open( self.configFilePath, 'r' ) as f:
            self.configFile = yaml.load( f )

        self.py_beach_dir = os.path.dirname( os.path.abspath( __file__ ) )

        os.chdir( os.path.dirname( os.path.abspath( self.configFilePath ) ) )

        self.private_key = self.configFile.get( 'private_key', None )
        if self.private_key is not None:
            with open( self.private_key, 'r' ) as f:
                key_path = self.private_key
                self.private_key = f.read()
                self._log( "Using shared key: %s" % key_path )

        self.admin_token = self.configFile.get( 'admin_token', None )

        self.nProcesses = self.configFile.get( 'n_processes', 0 )
        if self.nProcesses == 0:
            self.nProcesses = multiprocessing.cpu_count()
        self._log( "Using %d instances per node" % self.nProcesses )

        if iface is not None:
            self.interface = iface
            self.ifaceIp4 = _getIpv4ForIface( self.interface )
            if self.ifaceIp4 is None:
                self._logCritical( "Could not use iface %s (from cli)." % self.interface )
                sys.exit( -1 )
        else:
            self.interface = self.configFile.get( 'interface', None )
            if self.interface is not None:
                self.ifaceIp4 = _getIpv4ForIface( self.interface )
                if self.ifaceIp4 is None:
                    self._logCritical( "Could not use iface %s (from config)." % self.interface )
                    sys.exit( -1 )

        # Building a list of interfaces to auto-detect
        defaultInterfaces = _getPublicInterfaces()
        while self.ifaceIp4 is None and 0 != len( defaultInterfaces ):
            self.interface = defaultInterfaces.pop()
            self.ifaceIp4 = _getIpv4ForIface( self.interface )
            if self.ifaceIp4 is None:
                self._log( "Failed to use interface %s." % self.interface )

        if self.ifaceIp4 is None:
            self._logCritical( "Could not find an interface to use." )
            sys.exit( -1 )

        self.seedNodes = self.configFile.get( 'seed_nodes', [] )

        if 0 == len( self.seedNodes ):
            self.seedNodes.append( self.ifaceIp4 )

        for s in self.seedNodes:
            self._log( "Using seed node: %s" % s )

        self.directoryPort = _ZMREP( self.configFile.get( 'directory_port',
                                                         'ipc:///tmp/py_beach_directory_port' ),
                                    isBind = True,
                                    private_key = self.private_key )
        
        self.opsPort = self.configFile.get( 'ops_port', 4999 )
        self.opsSocket = _ZMREP( 'tcp://%s:%d' % ( self.ifaceIp4, self.opsPort ),
                                 isBind = True,
                                 private_key = self.private_key )
        self._log( "Listening for ops on %s:%d" % ( self.ifaceIp4, self.opsPort ) )
        
        self.port_range = ( self.configFile.get( 'port_range_start', 5000 ), self.configFile.get( 'port_range_end', 6000 ) )
        self.ports_available.update( xrange( self.port_range[ 0 ], self.port_range[ 1 ] + 1 ) )
        
        self.peer_keepalive_seconds = self.configFile.get( 'peer_keepalive_seconds', 60 )
        self.instance_keepalive_seconds = self.configFile.get( 'instance_keepalive_seconds', 600 )
        self.directory_sync_seconds = self.configFile.get( 'directory_sync_seconds', 60 )
        self.tombstone_culling_seconds = self.configFile.get( 'tombstone_culling_seconds', 3600 )
        
        self.instance_strategy = self.configFile.get( 'instance_strategy', 'random' )
        
        # Bootstrap the seeds
        for s in self.seedNodes:
            self._connectToNode( s )
        
        # Start services
        self._log( "Starting services" )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_directory_requests )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_instance_keepalive )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_host_keepalive )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_directory_sync )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_cullTombstones )
        gevent.spawn_later( random.randint( 0, 3 ), self._svc_applyTombstones )
        for _ in range( 20 ):
            gevent.spawn( self._svc_receiveOpsTasks )
        gevent.spawn( self._svc_pushDirChanges )
        
        # Start the instances
        for n in range( self.nProcesses ):
            self._startInstance( isIsolated = False )
        
        # Wait to be signaled to exit
        self._log( "Up and running" )
        timeToStopEvent.wait()
        
        # Any teardown required
        for proc in self.processes:
            self._sendQuitToInstance( proc )
        
        self._log( "Exiting." )

    def _isPrivileged( self, req ):
        return ( self.admin_token is None ) or ( req.get( 'admin_token', None ) == self.admin_token )

    def _catToList( self, cat ):
        return [ x for x in str( cat ).split( '/' ) if '' != x ]

    def _sendQuitToInstance( self, instance ):
        if instance[ 'p' ] is not None:
            instance[ 'p' ].send_signal( signal.SIGQUIT )
            errorCode = instance[ 'p' ].wait()
            if 0 != errorCode:
                self._logCritical( 'actor host exited with error code: %d' % errorCode )

    def _startInstance( self, isIsolated = False ):
        instanceId = str( uuid.uuid4() )
        procSocket = _ZMREQ( 'ipc:///tmp/py_beach_instance_%s' % instanceId, isBind = False )
        instance = { 'socket' : procSocket, 'p' : None, 'isolated' : isIsolated, 'id' : instanceId }
        self.processes.append( instance )
        self._log( "Managing instance at: %s" % ( 'ipc:///tmp/py_beach_instance_%s' % instanceId, ) )
        return instance

    def _removeInstanceIfIsolated( self, instance ):
        if instance[ 'isolated' ]:
            # Isolated instances only have one Actor loaded
            # so this means we can remove this instance
            self._log( "Removing isolated host instance: %s" % instance[ 'id' ] )
            self.processes.remove( instance )
            self._sendQuitToInstance( instance )

    def _connectToNode( self, ip ):
        ip = socket.gethostbyname( ip )
        nodeSocket = _ZMREQ( 'tcp://%s:%d' % ( ip, self.opsPort ),
                             isBind = False,
                             private_key = self.private_key )
        self.nodes[ ip ] = { 'socket' : nodeSocket, 'last_seen' : None }

    def _removeUidFromDirectory( self, uid ):
        isFound = False

        with self.dirLock.writer():
            if self.reverseDir.pop( uid, None ) is not None:
                self.isActorChanged.set()
            for realm in self.directory.keys():
                for cname, c in self.directory[ realm ].items():
                    if c.pop( uid, None ) is not None:
                        isFound = True
                        if 0 == len( c ):
                            self.directory[ realm ].pop( cname, None )
                if isFound:
                    self.isActorChanged.set()
                    break


        if uid in self.actorInfo:
            port = self.actorInfo[ uid ][ 'port' ]
            self.ports_available.add( port )
            self.actorInfo.pop( uid, None )
            self.isActorChanged.set()

        return isFound

    def _addTombstone( self, tb, ts = None ):
        if ts is None:
            ts = int( time.time() )
        if tb not in self.tombstones:
            self.tombstones[ tb ] = ts
            self.isTombstoneChanged.set()

    @handleExceptions
    def _svc_applyTombstones( self ):
        while not self.stopEvent.wait( 5 ):
            if self.isTombstoneChanged.wait( 0 ):
                gevent.sleep( 5 )
                self.isTombstoneChanged.clear()
                for uid in self.tombstones.keys():
                    if uid in self.reverseDir:
                        self._removeUidFromDirectory( uid )

    def _removeInstanceActorsFromDirectory( self, instance ):
        for uid, actor in self.actorInfo.items():
            if actor[ 'instance' ] == instance:
                self._removeUidFromDirectory( uid )
                self._addTombstone( uid )
    
    def _getAvailablePortForUid( self, uid ):
        port = None
        
        if 0 != len( self.ports_available ):
            port = self.ports_available.pop()
            self.actorInfo.setdefault( uid, {} )[ 'port' ] =  port
        
        return port
    
    def _getInstanceForActor( self, isIsolated = False ):
        instance = None

        if isIsolated:
            self.nProcesses += 1
            instance = self._startInstance( isIsolated = True )
            self.isInstanceChanged.set()
        elif self.instance_strategy == 'random':
            instance = random.choice( [ x for x in self.processes if x[ 'isolated' ] is False ] )
        
        return instance

    def _setActorMtd( self, uid, instance, actorName, realm, isIsolated, owner, parameters, resources ):
        info = self.actorInfo.setdefault( uid, {} )
        info[ 'instance' ] = instance
        info[ 'instance_id' ] = instance[ 'id' ]
        info[ 'name' ] = actorName
        info[ 'realm' ] = realm
        info[ 'isolated' ] = isIsolated
        info[ 'owner' ] = owner
        info[ 'resources' ] = resources
        info[ 'params' ] = {}
        for k in parameters.keys():
            if k.startswith( '_' ):
                info[ 'params' ][ k ] = '<PRIVATE>'
            else:
                info[ 'params' ][ k ] = parameters[ k ]

    def _updateDirectoryWith( self, curDir, newDir, newReverse ):
        ourNode = 'tcp://%s:' % ( self.ifaceIp4, )
        isGhostActorsFound = False
        with self.dirLock.writer():
            for uid, dest in newReverse.iteritems():
                if uid not in self.reverseDir and uid not in self.tombstones:
                    self.reverseDir[ uid ] = dest
            for realm, catMap in newDir.iteritems():
                curDir.setdefault( realm, PrefixDict() )
                for cat, endpoints in catMap.iteritems():
                    curDir[ realm ].setdefault( cat, {} )
                    for uid, endpoint in endpoints.iteritems():
                        if uid in self.tombstones: continue
                        # Check for ghost directory entries that report to be from here
                        # but are not, may be that this node restarted.
                        if endpoint.startswith( ourNode ) and uid not in self.actorInfo:
                            self._addTombstone( uid )
                            isGhostActorsFound = True
                        elif not endpoint.startswith( ourNode ):
                            # Only add to this directory other node's info since
                            # we are authoritative here.
                            curDir[ realm ][ cat ][ uid ] = endpoint

        return curDir

    def _getDirectoryEntriesFor( self, realm, category ):
        endpoints = {}
        with self.dirLock.reader():
            cats = self.directory.get( realm, PrefixDict() )[ category : category ]
            for cat in cats:
                endpoints.update( cat )
        return endpoints
    
    @handleExceptions
    def _svc_cullTombstones( self ):
        while not self.stopEvent.wait( 0 ):
            #self._log( "Culling tombstones" )
            currentTime = int( time.time() )
            maxTime = self.tombstone_culling_seconds
            nextTime = currentTime
            
            for uid, ts in self.tombstones.items():
                if ts < currentTime - maxTime:
                    self.tombstones.pop( uid, None )
                elif ts < nextTime:
                    nextTime = ts

            gevent.sleep( maxTime - ( currentTime - nextTime ) )
    
    @handleExceptions
    def _svc_receiveOpsTasks( self ):
        z = self.opsSocket.getChild()
        while not self.stopEvent.wait( 0 ):
            data = z.recv()
            if data is not False and 'req' in data:
                action = data[ 'req' ]
                #self._log( "Received new ops request: %s" % action )
                if 'keepalive' == action:
                    z.send( successMessage() )
                    if 'from' in data and data[ 'from' ] not in self.nodes:
                        self._log( "Discovered new node: %s" % data[ 'from' ] )
                        self._connectToNode( data[ 'from' ] )
                    for other in data.get( 'others', [] ):
                        if other not in self.nodes:
                            self._log( "Discovered new node: %s" % other )
                            self._connectToNode( other )
                elif 'start_actor' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    elif 'actor_name' not in data or 'cat' not in data:
                        z.send( errorMessage( 'missing information to start actor' ) )
                    else:
                        actorName = data[ 'actor_name' ]
                        categories = data[ 'cat' ]
                        realm = data.get( 'realm', 'global' )
                        parameters = data.get( 'parameters', {} )
                        resources = data.get( 'resources', {} )
                        ident = data.get( 'ident', None )
                        trusted = data.get( 'trusted', [] )
                        n_concurrent = data.get( 'n_concurrent', 1 )
                        owner = data.get( 'owner', None )
                        isIsolated = data.get( 'isolated', False )
                        log_level = data.get( 'loglevel', None )
                        log_dest = data.get( 'logdest', None )
                        uid = str( uuid.uuid4() )
                        port = self._getAvailablePortForUid( uid )
                        instance = self._getInstanceForActor( isIsolated )
                        if instance is not None:
                            self._setActorMtd( uid, instance, actorName, realm, isIsolated, owner, parameters, resources )
                            newMsg = instance[ 'socket' ].request( { 'req' : 'start_actor',
                                                                     'actor_name' : actorName,
                                                                     'realm' : realm,
                                                                     'uid' : uid,
                                                                     'ip' : self.ifaceIp4,
                                                                     'port' : port,
                                                                     'parameters' : parameters,
                                                                     'resources' : resources,
                                                                     'ident' : ident,
                                                                     'trusted' : trusted,
                                                                     'n_concurrent' : n_concurrent,
                                                                     'isolated' : isIsolated,
                                                                     'loglevel' : log_level,
                                                                     'logdest' : log_dest },
                                                                   timeout = 30 )
                        else:
                            newMsg = False

                        if isMessageSuccess( newMsg ):
                            self._log( "New actor loaded (isolation = %s, concurrent = %d), adding to directory" % ( isIsolated, n_concurrent ) )
                            # We always add a hardcoded special category _ACTORS/actorUid to provide a way for certain special actors
                            # to talk to specific instances directly, but this is discouraged.
                            with self.dirLock.writer():
                                self.reverseDir[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4, port )
                                self.directory.setdefault( realm,
                                                           PrefixDict() ).setdefault( '_ACTORS/%s' % ( uid, ),
                                                                                      {} )[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4,
                                                                                                                      port )
                                for category in categories:
                                    self.directory.setdefault( realm,
                                                               PrefixDict() ).setdefault( category,
                                                                                          {} )[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4,
                                                                                                                          port )
                            self.isActorChanged.set()
                        else:
                            self._logCritical( 'Error loading actor %s: %s.' % ( actorName, newMsg ) )
                            self._removeUidFromDirectory( uid )
                            self._addTombstone( uid )
                        z.send( newMsg )
                elif 'kill_actor' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    elif 'uid' not in data:
                        z.send( errorMessage( 'missing information to stop actor' ) )
                    else:
                        uids = data[ 'uid' ]
                        if not isinstance( uids, collections.Iterable ):
                            uids = ( uids, )

                        failed = []

                        for uid in uids:
                            instance = self.actorInfo.get( uid, {} ).get( 'instance', None )

                            if instance is None:
                                failed.append( errorMessage( 'actor not found' ) )
                            else:
                                newMsg = instance[ 'socket' ].request( { 'req' : 'kill_actor',
                                                                         'uid' : uid },
                                                                       timeout = 20 )
                                if not isMessageSuccess( newMsg ):
                                    failed.append( newMsg )

                                if not self._removeUidFromDirectory( uid ):
                                    failed.append( errorMessage( 'error removing actor from directory after stop' ) )

                                self._addTombstone( uid )

                                self._removeInstanceIfIsolated( instance )

                        self.isActorChanged.set()

                        if 0 != len( failed ):
                            z.send( errorMessage( 'some actors failed to stop', failed ) )
                        else:
                            z.send( successMessage() )
                elif 'remove_actor' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    elif 'uid' not in data:
                        z.send( errorMessage( 'missing information to remove actor' ) )
                    else:
                        uid = data[ 'uid' ]
                        instance = self.actorInfo.get( uid, {} ).get( 'instance', None )
                        if instance is not None and self._removeUidFromDirectory( uid ):
                            z.send( successMessage() )
                            self.isActorChanged.set()
                            self._removeInstanceIfIsolated( instance )
                        else:
                            z.send( errorMessage( 'actor to stop not found' ) )
                elif 'host_info' == action:
                    z.send( successMessage( { 'info' : { 'cpu' : psutil.cpu_percent( percpu = True,
                                                                                     interval = 2 ),
                                                         'mem' : psutil.virtual_memory().percent } } ) )
                elif 'get_full_dir' == action:
                    with self.dirLock.reader():
                        z.send( successMessage( { 'realms' : self.directory, 'reverse' : self.reverseDir } ) )
                elif 'get_dir' == action:
                    realm = data.get( 'realm', 'global' )
                    if 'cat' in data:
                        z.send( successMessage( data = { 'endpoints' : self._getDirectoryEntriesFor( realm, data[ 'cat' ] ) } ) )
                    else:
                        z.send( errorMessage( 'no category specified' ) )
                elif 'get_cats_under' == action:
                    realm = data.get( 'realm', 'global' )
                    if 'cat' in data:
                        with self.dirLock.reader():
                            z.send( successMessage( data = { 'categories' : [ x for x in self.directory.get( realm, PrefixDict() ).startswith( data[ 'cat' ] ) if x != data[ 'cat' ] ] } ) )
                    else:
                        z.send( errorMessage( 'no category specified' ) )
                elif 'get_nodes' == action:
                    nodeList = {}
                    for k in self.nodes.keys():
                        nodeList[ k ] = { 'last_seen' : self.nodes[ k ][ 'last_seen' ] }
                    z.send( successMessage( { 'nodes' : nodeList } ) )
                elif 'flush' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    else:
                        resp = successMessage()
                        for uid, actor in self.actorInfo.items():
                            instance = actor[ 'instance' ]
                            newMsg = instance[ 'socket' ].request( { 'req' : 'kill_actor',
                                                                     'uid' : uid },
                                                                   timeout = 30 )
                            if not isMessageSuccess( newMsg ):
                                if newMsg is None or newMsg is False:
                                    self._log( 'stopping actor timed out, not all is lost' )
                                else:
                                    resp = errorMessage( 'error stopping actor' )

                            if not self._removeUidFromDirectory( uid ):
                                if isMessageSuccess( resp ):
                                    resp = errorMessage( 'error removing actor from directory after stop' )

                            self._removeInstanceIfIsolated( instance )

                        z.send( resp )

                        if isMessageSuccess( resp ):
                            self.isActorChanged.set()
                elif 'get_dir_sync' == action:
                    with self.dirLock.reader():
                        z.send( successMessage( { 'directory' : self.directory, 'tombstones' : self.tombstones, 'reverse' : self.reverseDir } ) )
                elif 'push_dir_sync' == action:
                    if 'directory' in data and 'tombstones' in data and 'reverse' in data:
                        z.send( successMessage() )
                        for uid, ts in data[ 'tombstones' ].iteritems():
                            self._addTombstone( uid, ts )
                        self._updateDirectoryWith( self.directory, data[ 'directory' ], data[ 'reverse' ] )
                    else:
                        z.send( errorMessage( 'missing information to update directory' ) )
                elif 'get_full_mtd' == action:
                    z.send( successMessage( { 'mtd' : self.actorInfo } ) )
                elif 'get_load_info' == action:
                    info = {}
                    for instance in self.processes:
                        tmp = instance[ 'socket' ].request( { 'req' : 'get_load_info' }, timeout = 5 )
                        if isMessageSuccess( tmp ):
                            info.update( tmp[ 'data' ] )
                    z.send( successMessage( { 'load' : info } ) )
                elif 'associate' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    else:
                        uid = data[ 'uid' ]
                        category = data[ 'category' ]
                        try:
                            info = self.actorInfo[ uid ]
                            with self.dirLock.writer():
                                self.directory.setdefault( info[ 'realm' ],
                                                           PrefixDict() ).setdefault( category,
                                                                                      {} )[ uid ] = 'tcp://%s:%d' % ( self.ifaceIp4,
                                                                                                                      info[ 'port' ] )
                        except:
                            z.send( errorMessage( 'error associating, actor hosted here?' ) )
                        else:
                            self.isActorChanged.set()
                            z.send( successMessage() )
                elif 'disassociate' == action:
                    if not self._isPrivileged( data ):
                        z.send( errorMessage( 'unprivileged' ) )
                    else:
                        uid = data[ 'uid' ]
                        category = data[ 'category' ]
                        try:
                            info = self.actorInfo[ uid ]
                            with self.dirLock.writer():
                                del( self.directory[ info[ 'realm' ] ][ category ][ uid ] )
                                if 0 == len( self.directory[ info[ 'realm' ] ][ category ] ):
                                    del( self.directory[ info[ 'realm' ] ][ category ] )
                        except:
                            z.send( errorMessage( 'error associating, actor exists in category?' ) )
                        else:
                            self.isActorChanged.set()
                            z.send( successMessage() )
                else:
                    z.send( errorMessage( 'unknown request', data = { 'req' : action } ) )
            else:
                z.send( errorMessage( 'invalid request' ) )
                self._logCritical( "Received completely invalid request" )
    
    @handleExceptions
    def _svc_directory_requests( self ):
        z = self.directoryPort.getChild()
        while not self.stopEvent.wait( 0 ):
            data = z.recv()

            #self._log( "Received directory request: %s/%s" % ( data[ 'realm' ], data[ 'cat' ] ) )
            
            realm = data.get( 'realm', 'global' )
            if 'cat' in data:
                z.send( successMessage( data = { 'endpoints' : self._getDirectoryEntriesFor( realm, data[ 'cat' ] ) } ) )
            else:
                z.send( errorMessage( 'no category specified' ) )
    
    @handleExceptions
    def _svc_instance_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for instance in self.processes[:]:
                #self._log( "Issuing keepalive for instance %s" % instance[ 'id' ] )

                if self.initialProcesses and instance[ 'p' ] is not None:
                    # Only attempt keepalive if we know of a pid for it, otherwise it must be new
                    data = instance[ 'socket' ].request( { 'req' : 'keepalive' }, timeout = 60 )
                else:
                    # For first instances, immediately trigger the instance creation
                    data = False

                if not isMessageSuccess( data ):
                    if not self.initialProcesses and instance[ 'p' ] is not None:
                        self._logCritical( "Instance %s is dead (%s)." % ( instance[ 'id' ], data ) )
                    isBrandNew = True
                    if instance[ 'p' ] is not None:
                        self._logCritical( "Killing previous instance for %s" % instance[ 'id' ] )
                        instance[ 'p' ].kill()
                        # Instance died, it means all Actors within are no longer reachable
                        self._removeInstanceActorsFromDirectory( instance )
                        self.isActorChanged.set()
                        isBrandNew = False

                    if isBrandNew or not instance[ 'isolated' ]:
                        proc = subprocess.Popen( [ 'python',
                                                   '%s/actorhost.py' % self.py_beach_dir,
                                                    self.configFilePath,
                                                    instance[ 'id' ],
                                                    str( self._log_level ),
                                                    self._log_dest,
                                                    self.interface ] )

                        instance[ 'p' ] = proc

                        if not isBrandNew:
                            self._logCritical( "Instance %s died, restarting it, pid %d" % ( instance[ 'id' ], proc.pid ) )
                        else:
                            self._log( "Initial instance %s created with pid %d (isolation = %s)" % ( instance[ 'id' ], proc.pid, instance[ 'isolated' ] ) )
                    else:
                        # This means it's an isolated Actor that died, in this case
                        # we don't restart it, we leave it to higher layers to restarts it
                        # if they want.
                        self.processes.remove( instance )

            if not self.initialProcesses:
                self.initialProcesses = True

            self.isInstanceChanged.wait( self.instance_keepalive_seconds )
            self.isInstanceChanged.clear()
    
    @handleExceptions
    def _svc_host_keepalive( self ):
        while not self.stopEvent.wait( 0 ):
            for nodeName, node in self.nodes.items():
                if nodeName != self.ifaceIp4:
                    #self._log( "Issuing keepalive for node %s" % nodeName )
                    data = node[ 'socket' ].request( { 'req' : 'keepalive',
                                                       'from' : self.ifaceIp4,
                                                       'others' : self.nodes.keys() }, timeout = 30 )

                    if isMessageSuccess( data ):
                        node[ 'last_seen' ] = int( time.time() )
                    else:
                        self._log( "Removing node %s because of timeout" % nodeName )
                        del( self.nodes[ nodeName ] )
            
            gevent.sleep( self.peer_keepalive_seconds )
    
    @handleExceptions
    def _svc_directory_sync( self ):
        nextWait = 0
        while not self.stopEvent.wait( nextWait ):
            if 0 == len( self.nodes ):
                nextWait = 1
            else:
                nextWait = self.directory_sync_seconds / len( self.nodes )
            for nodeName, node in self.nodes.items():
                if nodeName != self.ifaceIp4:
                    #self._log( "Issuing directory sync with node %s" % nodeName )
                    data = node[ 'socket' ].request( { 'req' : 'get_dir_sync' } )

                    if isMessageSuccess( data ):
                        for uid, ts in data[ 'data' ][ 'tombstones' ].iteritems():
                            self._addTombstone( uid, ts )
                        self._updateDirectoryWith( self.directory, data[ 'data' ][ 'directory' ], data[ 'data' ][ 'reverse' ] )
                    else:
                        self._log( "Failed to get directory sync with node %s" % nodeName )

    @handleExceptions
    def _svc_pushDirChanges( self ):
        while not self.stopEvent.wait( 0 ):
            self.isActorChanged.wait()
            # We "accumulate" updates for 5 seconds once they occur to limit updates pushed
            gevent.sleep( 5 )
            self.isActorChanged.clear()
            with self.dirLock.reader():
                tmpDir = copy.deepcopy( self.directory )
                tmpTomb = copy.deepcopy( self.tombstones )
                tmpReverse = copy.deepcopy( self.reverseDir )
            for nodeName, node in self.nodes.items():
                if nodeName != self.ifaceIp4:
                    #self._log( "Pushing new directory update to %s" % nodeName )
                    node[ 'socket' ].request( { 'req' : 'push_dir_sync',
                                                'directory' : tmpDir,
                                                'tombstones' : tmpTomb,
                                                'reverse' : tmpReverse } )

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
    

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser( prog = 'HostManager' )
    parser.add_argument( 'configFile',
                         type = str,
                         help = 'the main config file defining the beach cluster' )
    parser.add_argument( '--iface', '-i',
                         type = str,
                         required = False,
                         dest = 'iface',
                         help = 'override the interface used for comms found in the config file' )
    parser.add_argument( '--log-level',
                         type = int,
                         required = False,
                         dest = 'loglevel',
                         default = logging.WARNING,
                         help = 'the logging level threshold' )
    parser.add_argument( '--log-dest',
                         type = str,
                         required = False,
                         dest = 'logdest',
                         default = '/dev/log',
                         help = 'the destination for the logging for syslog' )
    args = parser.parse_args()
    hostManager = HostManager( args.configFile, args.loglevel, args.logdest, iface = args.iface )