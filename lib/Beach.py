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
            nodeSocket = ZSocket( zmq.REQ, 'tcp://%s:%d' % ( s, self._opsPort ), isBind = False )
            self._nodes[ s ] = { 'socket' : nodeSocket }
            print( "Connected to node ops at: %s:%d" % ( s, self._opsPort ) )

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