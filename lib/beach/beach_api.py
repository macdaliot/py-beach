import yaml
from beach.utils import *
import zmq.green as zmq
import random
import gevent
import gevent.pool
import gevent.event
from beach.actor import ActorHandle

class Beach ( object ):

    def __init__( self, configFile, realm = 'global', extraTmpSeedNode = None ):

        self._configFile = configFile
        self._nodes = {}
        self._realm = realm
        self._opsPort = None
        self._isInited = gevent.event.Event()
        self._vHandles = []

        with open( self._configFile, 'r' ) as f:
            self._configFile = yaml.load( f )

        self._seedNodes = self._configFile.get( 'seed_nodes', [] )

        if extraTmpSeedNode is not None:
            self._seedNodes.append( extraTmpSeedNode )

        self._opsPort = self._configFile.get( 'ops_port', 4999 )

        for s in self._seedNodes:
            self._connectToNode( s )

        self._threads = gevent.pool.Group()
        self._threads.add( gevent.spawn( self._updateNodes ) )

        self._isInited.wait( 5 )

        ActorHandle._setHostDirInfo( [ 'tcp://%s:%d' % ( x, self._opsPort ) for x in self._nodes.keys() ] )

    def _connectToNode( self, host ):
        nodeSocket = ZMREQ( 'tcp://%s:%d' % ( host, self._opsPort ), isBind = False )
        self._nodes[ host ] = { 'socket' : nodeSocket }
        print( "Connected to node ops at: %s:%d" % ( host, self._opsPort ) )

    def _updateNodes( self ):
        toQuery = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        nodes = toQuery.request( { 'req' : 'get_nodes' }, timeout = 10 )
        for k in nodes[ 'nodes' ].keys():
            if k not in self._nodes:
                self._connectToNode( k )
        self._isInited.set()
        gevent.spawn_later( 60, self._updateNodes )

    def close( self ):
        self._threads.kill()

    def setRealm( self, realm ):
        old = self._realm
        self._realm = realm
        return old

    def addActor( self, actorName, category, strategy = 'random', strategy_hint = None, realm = None ):
        resp = None

        thisRealm = realm if realm is not None else self._realm

        if 'random' == strategy or strategy is None:
            node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
            resp = node.request( { 'req' : 'start_actor',
                                   'actor_name' : actorName,
                                   'realm' : thisRealm,
                                   'cat' : category }, timeout = 10 )

        return resp

    def getDirectory( self ):
        # We pick a random node to query since all directories should be synced
        node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        return node.request( { 'req' : 'get_full_dir' }, timeout = 10 )

    def flush( self ):
        isFlushed = True
        for node in self._nodes.values():
            resp = node[ 'socket' ].request( { 'req' : 'flush' }, timeout = 30 )
            if not isMessageSuccess( resp ):
                isFlushed = False

        return isFlushed

    def getActorHandle( self, category, mode = 'random' ):
        v = ActorHandle( self._realm, category, mode )
        self._vHandles.append( v )
        return v