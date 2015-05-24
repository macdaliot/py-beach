import yaml
from Utils import *
import zmq.green as zmq
import random

class Beach ( object ):

    def __init__( self, configFile, realm = 'global', extraTmpSeedNode = None ):

        self._configFile = configFile
        self._nodes = {}
        self._realm = realm
        self._opsPort = None

        with open( self._configFile, 'r' ) as f:
            self._configFile = yaml.load( f )

        self._seedNodes = self._configFile.get( 'seed_nodes', [] )

        if extraTmpSeedNode is not None:
            self._seedNodes.append( extraTmpSeedNode )

        self._opsPort = self._configFile.get( 'ops_port', 4999 )

        for s in self._seedNodes:
            self._connectToNode( s )

    def _connectToNode( self, host ):
        nodeSocket = ZMREQ( 'tcp://%s:%d' % ( host, self._opsPort ), isBind = False )
        self._nodes[ host ] = { 'socket' : nodeSocket }
        print( "Connected to node ops at: %s:%d" % ( host, self._opsPort ) )

    def setRealm( self, realm ):
        self._realm = realm

    def addActor( self, actorName, category, strategy = 'random', strategy_hint = None, realm = None ):
        resp = None

        thisRealm = realm if realm is not None else self._realm

        if 'random' == strategy:
            node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
            resp = node.request( { 'req' : 'start_actor',
                                   'actor_name' : actorName,
                                   'realm' : thisRealm,
                                   'cat' : category }, timeout = 10 )

        return resp

    def getDirectory( self ):
        # We pick a random node to query since all directories should be synced
        node = self._nodes.values()[ random.randint( 0, len( self._nodes ) - 1 ) ][ 'socket' ]
        return node.request( { 'req' : 'get_dir' }, timeout = 10 )

    def flush( self ):
        isFlushed = True
        d = self.getDirectory()[ 'realms' ]
        for realm, cats in d.iteritems():
            for catName, actors in cats.iteritems():
                for uid, url in actors.iteritems():
                    isFlushed = False
                    host = url.split( ':' )[ 1 ][ 2: ]
                    if host in self._nodes:
                        resp = self._nodes[ host ][ 'socket' ].request( { 'req' : 'kill_actor',
                                                                          'uid' : uid },
                                                                        timeout = 30 )
                        if isMessageSuccess( resp ):
                            isFlushed = True
                        else:
                            print( "Error: %s" % str( resp ) )
                    if not isFlushed:
                        break

        return isFlushed