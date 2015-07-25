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
if 'threading' in sys.modules:
        raise Exception('threading module loaded before patching!')
import gevent.monkey
gevent.monkey.patch_all()

import yaml
from beach.utils import *
from beach.utils import _ZMREQ
import zmq.green as zmq
import random
import collections
import operator
import gevent
import gevent.pool
import gevent.event
from beach.actor import ActorHandle
from beach.utils import _getIpv4ForIface

class Beach ( object ):

    def __init__( self, configFile, realm = 'global' ):
        '''Create a new interface to a beach cluster.

        :param configFile: the path to the config file of the cluster
        :param realm: the realm within the cluster you want to deal with, defaults to global
        :param extraTmpSeedNode: manually specify a seed node to interface with, only use
            if you know why you need it
        '''
        self._configFile = configFile
        self._nodes = {}
        self._realm = realm
        self._opsPort = None
        self._isInited = gevent.event.Event()
        self._vHandles = []
        self._dirCache = {}

        with open( self._configFile, 'r' ) as f:
            self._configFile = yaml.load( f )

        self._seedNodes = self._configFile.get( 'seed_nodes', [] )

        self._opsPort = self._configFile.get( 'ops_port', 4999 )

        if 0 == len( self._seedNodes ):
            self._seedNodes.append( _getIpv4ForIface( self._configFile.get( 'interface', 'eth0' ) ) )

        for s in self._seedNodes:
            self._connectToNode( s )

        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn( self._updateNodes ) )

        self._isInited.wait( 5 )

        ActorHandle._setHostDirInfo( [ 'tcp://%s:%d' % ( x, self._opsPort ) for x in self._nodes.keys() ] )

    def _connectToNode( self, host ):
        nodeSocket = _ZMREQ( 'tcp://%s:%d' % ( host, self._opsPort ), isBind = False )
        self._nodes[ host ] = { 'socket' : nodeSocket, 'info' : None }
        print( "Connected to node ops at: %s:%d" % ( host, self._opsPort ) )

    def _getHostInfo( self, zSock ):
        info = None
        resp = zSock.request( { 'req' : 'host_info' } )
        if isMessageSuccess( resp ):
            info = resp[ 'info' ]
        return info

    def _updateNodes( self ):
        toQuery = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        nodes = toQuery.request( { 'req' : 'get_nodes' }, timeout = 10 )
        if nodes is not False:
            for k in nodes[ 'nodes' ].keys():
                if k not in self._nodes:
                    self._connectToNode( k )

        for nodeName, node in self._nodes.items():
            self._nodes[ nodeName ][ 'info' ] = self._getHostInfo( node[ 'socket' ] )

        tmpDir = self.getDirectory()
        if isMessageSuccess( tmpDir ):
            self._dirCache = tmpDir[ 'realms' ].get( self._realm, {} )

        self._isInited.set()
        gevent.spawn_later( 30, self._updateNodes )

    def close( self ):
        '''Close all threads and resources of the interface.
        '''
        self._threads.kill()

    def setRealm( self, realm ):
        '''Change the realm to interface with.

        :param realm: the new realm to use

        :returns: the old realm used or None if none were specified
        '''
        old = self._realm
        self._realm = realm
        return old

    def getNodeCount( self ):
        '''Get the number of nodes we are connected to.

        :returns: the number of nodes in the cluster we are connected to
        '''
        return len( self._nodes )

    def addActor( self, actorName, category, strategy = 'random', strategy_hint = None, realm = None, parameters = None, isIsolated = False ):
        '''Spawn a new actor in the cluster.

        :param actorName: the name of the actor to spawn
        :param category: the category associated with this new actor
        :param strategy: the strategy to use to decide where to spawn the new actor,
            currently supports: random
        :param strategy_hint: a parameter to help choose a node, meaning depends on the strategy
        :param realm: the realm to add the actor in, if different than main realm set
        :param parameters: a dict of parameters that will be given to the actor when it starts,
            usually used for configurations
        :param isIsolated: if True the Actor will be spawned in its own process space to further
            isolate it from potential crashes of other Actors

        :returns: returns the reply from the node indicating if the actor was created successfully,
            use beach.utils.isMessageSuccess( response ) to check for success
        '''

        resp = None
        node = None

        thisRealm = realm if realm is not None else self._realm

        if 'random' == strategy or strategy is None:
            node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        elif 'resource' == strategy:
            # For now the simple version of this strategy is to just average the CPU and MEM %.
            node = min( self._nodes.values(), key = lambda x: ( sum( x[ 'info' ][ 'cpu' ] ) /
                                                                len( x[ 'info' ][ 'cpu' ] ) +
                                                                x[ 'info' ][ 'mem' ] ) / 2 )[ 'socket' ]
        elif 'affinity' == strategy:
            nodeList = self._dirCache.get( strategy_hint, {} ).values()
            population = {}
            for n in nodeList:
                name = n.split( ':' )[ 1 ][ 2 : ]
                population.setdefault( name, 0 )
                population[ name ] += 1
            if 0 != len( population ):
                affinityNode = population.keys()[ random.randint( 0, len( population ) - 1 ) ]
                node = self._nodes[ affinityNode ].get( 'socket', None )
            else:
                # There is nothing in play, fall back to random
                node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        elif 'repulsion' == strategy:
            possibleNodes = self._nodes.keys()

            nodeList = self._dirCache.get( strategy_hint, {} ).values()

            for n in nodeList:
                name = n.split( ':' )[ 1 ][ 2 : ]
                if name in possibleNodes:
                    del( possibleNodes[ name ] )

            if 0 != len( possibleNodes ):
                affinityNode = possibleNodes[ random.randint( 0, len( possibleNodes ) - 1 ) ]
                node = self._nodes[ affinityNode ].get( 'socket', None )
            else:
                # There is nothing in play, fall back to random
                node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]

        if node is not None:
            info = { 'req' : 'start_actor',
                     'actor_name' : actorName,
                     'realm' : thisRealm,
                     'cat' : category,
                     'isolated' : isIsolated }
            if parameters is not None:
                info[ 'parameters' ] = parameters
            resp = node.request( info, timeout = 10 )

        return resp

    def getDirectory( self ):
        '''Retrieve the directory from a random node, all nodes have a directory that
            is eventually-consistent.

        :returns: the realm directory of the cluster
        '''
        node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        resp = node.request( { 'req' : 'get_full_dir' }, timeout = 10 )
        if isMessageSuccess( resp ):
            self._dirCache = resp
        else:
            resp = False
        return resp

    def flush( self ):
        '''Unload all actors from the cluster, major operation, be careful.

        :returns: True if all actors were removed normally
        '''
        isFlushed = True
        for node in self._nodes.values():
            resp = node[ 'socket' ].request( { 'req' : 'flush' }, timeout = 30 )
            if not isMessageSuccess( resp ):
                isFlushed = False

        return isFlushed

    def getActorHandle( self, category, mode = 'random' ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random

        :returns: an ActorHandle
        '''
        v = ActorHandle( self._realm, category, mode )
        self._vHandles.append( v )
        return v

    def stopActors( self, withId = None, withCategory = None ):
        '''Stop specific actors based on a criteria.

        :param withId: a single, or list of actor IDs to be stopped
        :param withCategory: a category name to be stopped

        :returns: True if the actors were stopped normally
        '''
        isSuccess = False
        toRemove = []

        if withId is not None:
            if not isinstance( withId, collections.Iterable ):
                toRemove.append( withId )
            else:
                toRemove += withId

        tmpDir = self.getDirectory()

        if tmpDir is not False and isMessageSuccess( tmpDir ):
            if withCategory is not None:
                if not isinstance( withCategory, collections.Iterable ):
                    withCategory = ( withCategory, )
                for cat in withCategory:
                    toRemove += tmpDir[ 'realms' ].get( self._realm, {} ).get( cat, {} ).keys()

            # We take the easy way out for now by just spamming the kill to every node.
            isSuccess = True
            for node in self._nodes.values():
                resp = node[ 'socket' ].request( { 'req' : 'kill_actor', 'uid' : toRemove }, timeout = 30 )
                if not isMessageSuccess( resp ):
                    isSuccess = resp

        return isSuccess

    def getClusterHealth( self ):
        ''' Get the cached health information of every node in the cluster.

        :returns: dict of all nodes with their health information associated
        '''
        health = {}

        for name, node in self._nodes.items():
            health[ name ] = node[ 'info' ]

        return health