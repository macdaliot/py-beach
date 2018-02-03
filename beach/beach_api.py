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
import yaml
import uuid
from beach.utils import *
from beach.utils import _ZMREQ
import zmq.green as zmq
import socket
import random
import collections
import operator
import gevent
import gevent.pool
import gevent.event
import operator
from beach.actor import ActorHandle
from beach.actor import ActorHandleGroup
from beach.utils import _getIpv4ForIface
from beach.utils import _getPublicInterfaces

def eprint( msg ):
    print >> sys.stderr, msg

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
        self._lastCacheUpdate = 0
        self._lastAddActorNode = None
        self._dirCacheTtl = 2.0

        with open( self._configFile, 'r' ) as f:
            self._configFile = yaml.load( f )

        self._seedNodes = self._configFile.get( 'seed_nodes', [] )

        self._opsPort = self._configFile.get( 'ops_port', 4999 )

        self._private_key = self._configFile.get( 'private_key', None )
        if self._private_key is not None:
            key_path = os.path.join( os.path.dirname( os.path.abspath( configFile ) ), self._private_key )
            with open( key_path, 'r' ) as f:
                self._private_key = f.read()
                print( "Using shared key: %s" % key_path )

        self._admin_token = self._configFile.get( 'admin_token', None )

        if 0 == len( self._seedNodes ):
            if 'interface' not in self._configFile:
                defaultInterfaces = _getPublicInterfaces()
                mainIfaceIp = None
                while mainIfaceIp is None and 0 != len( defaultInterfaces ):
                    interface = defaultInterfaces.pop()
                    mainIfaceIp = _getIpv4ForIface( interface )
            if mainIfaceIp is None:
                eprint( "Failed to use interface %s." % self.interface )
            self._seedNodes.append( mainIfaceIp )

        for s in self._seedNodes:
            self._connectToNode( s, True )

        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn( self._updateNodes ) )

        self._isInited.wait( 5 )

        ActorHandle._setHostDirInfo( [ 'tcp://%s:%d' % ( x, self._opsPort ) for x in self._nodes.keys() ],
                                     private_key = self._private_key )
        ActorHandleGroup._setHostDirInfo( [ 'tcp://%s:%d' % ( x, self._opsPort ) for x in self._nodes.keys() ],
                                          private_key = self._private_key )

    def _connectToNode( self, host, isSeed = False ):
        host = socket.gethostbyname( host )
        nodeSocket = _ZMREQ( 'tcp://%s:%d' % ( host, self._opsPort ),
                             isBind = False,
                             private_key = self._private_key )
        self._nodes[ host ] = { 'socket' : nodeSocket, 'info' : None, 'is_seed' : isSeed, 'offline' : False }
        eprint( "Connected to node ops at: %s:%d" % ( host, self._opsPort ) )

    def _getHostInfo( self, zSock ):
        info = None
        resp = zSock.request( { 'req' : 'host_info' }, timeout = 10 )
        if isMessageSuccess( resp ):
            info = resp[ 'data' ][ 'info' ]
        return info

    def _updateNodes( self ):
        try:
            while True:
                srcNodeKey = random.choice( self._nodes.keys() )
                toQuery = self._nodes[ srcNodeKey ][ 'socket' ]
                nodes = toQuery.request( { 'req' : 'get_nodes' }, timeout = 10 )
                if isMessageSuccess( nodes ):
                    self._nodes[ srcNodeKey ][ 'offline' ] = False
                    for k in nodes[ 'data' ][ 'nodes' ].keys():
                        if k not in self._nodes:
                            self._connectToNode( k )
                elif self._nodes[ srcNodeKey ][ 'is_seed' ]:
                    # It's a seed node so we keep it in the list but indicate
                    # we couldn't actually talk to it.
                    self._nodes[ srcNodeKey ][ 'offline' ] = True
                    continue
                else:
                    # Couldn't get node list, assuming it's dead.
                    self._nodes[ srcNodeKey ][ 'offline' ] = True
                    nodeInfo = self._nodes.pop( srcNodeKey, None )
                    if nodeInfo is not None:
                        nodeInfo[ 'socket' ].close()
                    continue
                    

                for nodeName, node in self._nodes.items():
                    newInfo = self._getHostInfo( node[ 'socket' ] )
                    if newInfo is not None:
                        self._nodes[ nodeName ][ 'info' ] = newInfo
                        self._nodes[ srcNodeKey ][ 'offline' ] = False
                    elif self._nodes[ nodeName ][ 'is_seed' ]:
                        # It's a seed node so we keep it in the list but indicate
                        # we couldn't actually talk to it.
                        self._nodes[ srcNodeKey ][ 'offline' ] = True
                    else:
                        # Assuming it's dead.
                        self._nodes[ srcNodeKey ][ 'offline' ] = True
                        nodeInfo = self._nodes.pop( nodeName, None )
                        if nodeInfo is not None:
                            nodeInfo[ 'socket' ].close()

                self.getDirectory()

                self._isInited.set()
                break
        finally:
            self._threads.add( gevent.spawn_later( 30, self._updateNodes ) )

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
        self._isInited.wait( 30 )
        return len( [ k for k, v in self._nodes.iteritems() if v[ 'offline' ] is False ] )

    def _getRandomActiveNode( self ):
        return random.choice( [ x for x in self._nodes.itervalues() if x[ 'offline' ] is False ] )

    def addActor( self, actorName, category,
                  strategy = 'random',
                  strategy_hint = None,
                  realm = None,
                  parameters = None,
                  resources = None,
                  isIsolated = False,
                  secretIdent = None,
                  trustedIdents = [],
                  n_concurrent = 1,
                  is_drainable = False,
                  time_to_drain = None,
                  owner = None,
                  log_level = None,
                  log_dest = None ):
        '''Spawn a new actor in the cluster.

        :param actorName: the name of the actor to spawn
        :param category: the (or list of) category associated with this new actor
        :param strategy: the strategy to use to decide where to spawn the new actor,
            currently supports: random, resource, affinity, repulsion, roundrobin
        :param strategy_hint: a parameter to help choose a node, meaning depends on the strategy
        :param realm: the realm to add the actor in, if different than main realm set
        :param parameters: a dict of parameters that will be given to the actor when it starts,
            usually used for configurations. Magic parameter 'beach_trace_enabled' if True will
            trigger a log of the metadata of every request the Actor receives
        :param resources: the mapping of internal resource name to categories in the beach cluster
        :param isIsolated: if True the Actor will be spawned in its own process space to further
            isolate it from potential crashes of other Actors
        :param secretIdent: a string used as a semi-secret token passed in requests sent by
            vHandles produced by the Actor, can be used to segment or ward off vHandles
            originating from untrusted machines
        :param trustedIdents: list of idents to be trusted, if an empty list ALL will be trusted
        :param n_concurrent: number of concurrent requests handled by actor
        :param is_drainable: True if the actor can be requested to drain gracefully
        :param time_to_drain: number of seconds an instance is alive before draining, None disables
        :param owner: an identifier for the owner of the Actor, useful for shared environments
        :param log_level: a logging.* value indicating the custom logging level for the actor
        :param log_dest: a destination string for the syslog custom to the actor for the actor

        :returns: returns the reply from the node indicating if the actor was created successfully,
            use beach.utils.isMessageSuccess( response ) to check for success
        '''

        resp = None
        node = None

        thisRealm = realm if realm is not None else self._realm

        if type( category ) is str or type( category ) is unicode:
            category = ( category, )

        self.getDirectory()

        if 'random' == strategy or strategy is None:
            node = self._getRandomActiveNode()[ 'socket' ]
        elif 'resource' == strategy:
            # For now the simple version of this strategy is to just average the CPU and MEM %.
            node = min( self._nodes.values(), key = lambda x: ( sum( x[ 'info' ][ 'cpu' ] ) /
                                                                len( x[ 'info' ][ 'cpu' ] ) +
                                                                x[ 'info' ][ 'mem' ] ) / 2 )[ 'socket' ]
        elif 'affinity' == strategy:
            nodeList = self._dirCache[ 'realms' ].get( self._realm, {} ).get( strategy_hint, {} ).values()
            population = {}
            for n in nodeList:
                name = n.split( ':' )[ 1 ][ 2 : ]
                population.setdefault( name, 0 )
                population[ name ] += 1
            if 0 != len( population ):
                affinityNode = population.keys()[ random.randint( 0, len( population ) - 1 ) ]
                node = self._nodes[ affinityNode ].get( 'socket', None )
                # We create a temporary entry to allow us to do multiple Add in a row
                for cat in category:
                    self._dirCache[ 'realms' ].get( self._realm, {} ).setdefault( cat, {} )[ str(uuid.uuid4()) ] = '%s:XXXX' % affinityNode
            else:
                # There is nothing in play, fall back to random
                node = self._getRandomActiveNode()[ 'socket' ]
        elif 'host_affinity' == strategy:
            node = self._nodes.get( strategy_hint, None )
            if node is not None and node[ 'offline' ] is False:
                node = node[ 'socket' ]
        elif 'repulsion' == strategy:
            counts = {}
            possibleNodes = [ k for k, v in self._nodes.iteritems() if v[ 'offline' ] is False ]
            for name in possibleNodes:
                counts[ name ] = 0

            if strategy_hint is None:
                strategy_hint = category[ 0 ]

            for n in self._dirCache[ 'realms' ].get( self._realm, {} ).get( strategy_hint, {} ).values():
                name = n.split( ':' )[ 1 ][ 2 : ]
                if name in counts:
                    counts[ name ] += 1

            # Pick the node with the least number of actors.
            orderedNodes = sorted( counts.items(), key = operator.itemgetter( 1 ) )
            evenNodes = [ orderedNodes[ 0 ] ]
            for n in orderedNodes[ 1 : ]:
                if n[ 1 ] == evenNodes[ 0 ][ 1 ]:
                    evenNodes.append( n )
            random.shuffle( evenNodes )
            affinityNode = evenNodes[ 0 ][ 0 ]
            node = self._nodes[ affinityNode ].get( 'socket', None )

            # We create a temporary entry to allow us to do multiple Add in a row
            for cat in category:
                self._dirCache[ 'realms' ].setdefault( self._realm, {} ).setdefault( cat, {} )[ str( uuid.uuid4() ) ] = 'tcp://%s:XXXX' % affinityNode
        elif 'roundrobin' == strategy:
            if 0 != len( self._nodes ):
                while True:
                    curI = ( self._lastAddActorNode + 1 ) if self._lastAddActorNode is not None else 0
                    if curI >= len( self._nodes ):
                        curI = 0
                    self._lastAddActorNode = curI
                    node = self._nodes.values()[ curI ]
                    if node[ 'offline' ] is False or 1 == len( self._nodes ):
                        break
                node = node[ 'socket' ]

        if node is not None:
            info = { 'req' : 'start_actor',
                     'actor_name' : actorName,
                     'realm' : thisRealm,
                     'cat' : category,
                     'isolated' : isIsolated,
                     'n_concurrent' : n_concurrent,
                     'is_drainable' : is_drainable,
                     'time_to_drain' : time_to_drain }
            if parameters is not None:
                info[ 'parameters' ] = parameters
            if resources is not None:
                info[ 'resources' ] = resources
            if secretIdent is not None:
                info[ 'ident' ] = secretIdent
            if trustedIdents is not None:
                info[ 'trusted' ] = trustedIdents
            if owner is not None:
                info[ 'owner' ] = owner
            if log_level is not None:
                info[ 'loglevel' ] = log_level
            if log_dest is not None:
                info[ 'logdest' ] = log_dest
            if self._admin_token is not None:
                info[ 'admin_token' ] = self._admin_token
            resp = node.request( info, timeout = 30 )

        return resp

    def getDirectory( self, timeout = 10, isForce = False, nRetries = 3 ):
        '''Retrieve the directory from a random node, all nodes have a directory that
           is eventually-consistent. Side-effect of this call is to update the internal
           cache, so it can be used as a "forceRefresh".

        :returns: the realm directory of the cluster
        '''
        if isForce or ( self._lastCacheUpdate < ( time.time() - self._dirCacheTtl ) ):
            curRetry = 0
            while curRetry < nRetries:
                try:
                    node = random.choice( [ x for x in self._nodes.itervalues() if x[ 'offline' ] is False ] )[ 'socket' ]
                except:
                    node = None
                if node is not None:
                    resp = node.request( { 'req' : 'get_full_dir' }, timeout = timeout )
                    if isMessageSuccess( resp ):
                        resp = resp[ 'data' ]
                        if 'realms' in resp:
                            self._dirCache = resp
                            self._lastCacheUpdate = time.time()
                            break
                    else:
                        resp = False
                else:
                    gevent.sleep( timeout )
                curRetry += 1
        else:
            return self._dirCache
        return resp

    def flush( self ):
        '''Unload all actors from the cluster, major operation, be careful.

        :returns: True if all actors were removed normally
        '''
        isFlushed = True
        req = { 'req' : 'flush' }
        if self._admin_token is not None:
            req[ 'admin_token' ] = self._admin_token
        for node in self._nodes.values():
            if node[ 'offline' ]:
                continue
            resp = node[ 'socket' ].request( req, timeout = 30 )
            if not isMessageSuccess( resp ):
                isFlushed = False

        return isFlushed

    def getActorHandle( self, category, mode = 'random', nRetries = None, timeout = None, ident = None ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random, local
        :param nRetries: number of times the handle should attempt to retry the request if
            it times out
        :param timeout: number of seconds to wait before re-issuing a request or failing
        :param ident: identity token for trust between Actors

        :returns: an ActorHandle
        '''
        v = ActorHandle( self._realm, category, mode, nRetries = nRetries, timeout = timeout, ident = ident )
        self._vHandles.append( v )
        return v

    def stopActors( self, withId = None, withCategory = None, delay = None ):
        '''Stop specific actors based on a criteria.

        :param withId: a single, or list of actor IDs to be stopped
        :param withCategory: a category name to be stopped
        :param delay: number of seconds to wait between each actor stopped

        :returns: True if the actors were stopped normally
        '''
        isSuccess = False
        toRemove = []

        if withId is not None:
            if not isinstance( withId, collections.Iterable ):
                toRemove.append( withId )
            else:
                toRemove += withId

        tmpDir = self.getDirectory( isForce = True )

        if tmpDir is not False and 'realms' in tmpDir:
            if withCategory is not None:
                if not isinstance( withCategory, ( tuple, list ) ):
                    withCategory = ( withCategory, )
                for cat in withCategory:
                    for realmCat in tmpDir[ 'realms' ].get( self._realm, {} ).keys():
                        if realmCat.startswith( cat ):
                            toRemove += tmpDir[ 'realms' ].get( self._realm, {} ).get( realmCat, {} ).keys()

            if 0 == len( toRemove ):
                return {}

            # We take the easy way out for now by just spamming the kill to every node.
            isSuccess = {}
            req = { 'req' : 'kill_actor', 'uid' : toRemove }
            if self._admin_token is not None:
                req[ 'admin_token' ] = self._admin_token

            if delay is None:
                isSuccess = parallelExec( lambda node: node[ 'socket' ].request( req, timeout = 60 ), 
                                          ( x for x in self._nodes.itervalues() if x[ 'offline' ] is False ) )
            else:
                for k, node in self._nodes.items():
                    if node[ 'offline' ]:
                        continue
                    resp = node[ 'socket' ].request( req, timeout = 60 )
                    isSuccess[ k ] = resp
                    if delay is not None:
                        gevent.sleep( delay )

        return isSuccess

    def getClusterHealth( self ):
        ''' Get the cached health information of every node in the cluster.

        :returns: dict of all nodes with their health information associated
        '''
        health = {}

        for name, node in self._nodes.items():
            health[ name ] = node[ 'info' ]

        return health

    def getLoadInfo( self ):
        ''' Get the number of free handlers per Actor in the cluster.

        :returns: dict of all Actors by uid and the number of handler available
        '''
        load = {}

        for node in self._nodes.values():
            if node[ 'offline' ]:
                continue
            resp = node[ 'socket' ].request( { 'req' : 'get_load_info' }, timeout = 30 )
            if isMessageSuccess( resp ):
                load.update( resp[ 'data' ][ 'load' ] )

        return load

    def getAllNodeMetadata( self ):
        '''Retrieve metadata about actors from all nodes.

        :returns: the metadata of nodes of the cluster
        '''
        mtd = {}
        for nodename, node in self._nodes.items():
            if not node[ 'offline' ]:
                mtd[ nodename ] = node[ 'socket' ].request( { 'req' : 'get_full_mtd' }, timeout = 10 )

        return mtd

    def getActorHandleGroup( self, categoryRoot, mode = 'random', nRetries = None, timeout = None, ident = None ):
        '''Get a virtual handle to actors in the cluster.

        :param category: the name of the category holding actors to get the handle to
        :param mode: the method actors are queried by the handle, currently
            handles: random, local
        :param nRetries: number of times the handle should attempt to retry the request if
            it times out
        :param timeout: number of seconds to wait before re-issuing a request or failing
        :param ident: identity token for trust between Actors

        :returns: an ActorHandle
        '''
        v = ActorHandleGroup( self._realm, categoryRoot, mode, nRetries = nRetries, timeout = timeout, ident = ident )
        self._vHandles.append( v )
        return v

    def addToCategory( self, actorId , category ):
        '''Associate a specific actor with a specific category.

        :param actorId: a single actor IDs to associate
        :param category: a category name to associate with

        :returns: True if the actors association was successful
        '''

        isSuccess = False

        req = { 'req' : 'associate', 'uid' : actorId, 'category' : category }
        if self._admin_token is not None:
            req[ 'admin_token' ] = self._admin_token

        for node in self._nodes.values():
            if node[ 'offline' ]:
                continue
            resp = node[ 'socket' ].request( req, timeout = 30 )
            if isMessageSuccess( resp ):
                isSuccess = True

        return isSuccess

    def removeFromCategory( self, actorId , category ):
        '''Disassociate a specific actor with a specific category.

        :param actorId: a single actor IDs to associate
        :param category: a category name to disassociate from

        :returns: True if the actors disassociation was successful
        '''

        isSuccess = False

        req = { 'req' : 'disassociate', 'uid' : actorId, 'category' : category }
        if self._admin_token is not None:
            req[ 'admin_token' ] = self._admin_token

        for node in self._nodes.values():
            if node[ 'offline' ]:
                continue
            resp = node[ 'socket' ].request( req, timeout = 30 )
            if isMessageSuccess( resp ):
                isSuccess = True
                break

        return isSuccess