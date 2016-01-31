import os
import time
import subprocess
import signal
from gevent.lock import Semaphore
from beach.beach_api import Beach
from beach.utils import *
from beach.utils import _getIpv4ForIface

import pytest

h_hostmanager = None
beach = None
curFileDir = os.path.join( os.path.dirname( os.path.abspath( __file__ ) ) )

@pytest.fixture( scope = 'module', autouse = True )
def beach_cluster( request ):
    global beach
    global h_hostmanager
    h_hostmanager = subprocess.Popen( [ 'python',
                                        '-m',
                                        'beach.hostmanager',
                                        os.path.join( curFileDir, 'simple.yaml' ),
                                        '--log-level',
                                        '10' ] )
    beach = Beach( os.path.join( curFileDir, 'simple.yaml' ),
                   realm = 'global' )

    def beach_teardown():
        global beach
        global h_hostmanager
        beach.close()
        h_hostmanager.send_signal( signal.SIGQUIT )

        assert( 0 == h_hostmanager.wait() )

    request.addfinalizer( beach_teardown )

def test_beach_connection():
    global beach
    assert( 1 == beach.getNodeCount() )

def test_admin_privileges():
    global beach

    tmpAdminToken = beach._admin_token
    beach._admin_token = None

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":1} )
    assert( ( not isMessageSuccess( a1 ) ) and ( a1.get( 'status', {} ).get( 'error', None ) == 'unprivileged' ) )

    beach._admin_token = tmpAdminToken

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":1} )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

    assert( beach.flush() )

def test_flushing_single_node_cluster():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":2} )
    assert( isMessageSuccess( a1 ) )

    f = beach.flush()
    assert( f )

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

def test_actor_creation():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":3}, strategy = 'roundrobin' )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers', parameters={"a":4} )
    assert( isMessageSuccess( a2 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

    assert( beach.flush() )

def test_isolated_actor_creation():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', isIsolated = True, parameters={"a":5} )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

    assert( beach.flush() )

def test_virtual_handles():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":6} )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'pongers' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    assert( beach.flush() )

def test_prefix_virtual_handles():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":7} )
    a2 = beach.addActor( 'Ping', 'pingers', parameters={"a":8} )
    a3 = beach.addActor( 'Ping', 'pingers', parameters={"a":9} )
    assert( isMessageSuccess( a1 ) )
    assert( isMessageSuccess( a2 ) )
    assert( isMessageSuccess( a3 ) )

    vHandles = beach.getActorHandle( 'p' )

    assert( 3 == vHandles.getNumAvailable() )
    assert( vHandles.isAvailable() is True )

    vHandles = beach.getActorHandle( 'p/' )
    assert( 0 == vHandles.getNumAvailable() )
    assert( vHandles.isAvailable() is False )

    assert( beach.flush() )

def test_trust():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers/trust', trustedIdents = [ 'abc' ], parameters={"a":10} )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers/notrust', trustedIdents = [ 'def' ], parameters={"a":11} )
    assert( isMessageSuccess( a2 ) )

    vHandle = beach.getActorHandle( 'pongers/trust', ident = 'abc' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    vHandle = beach.getActorHandle( 'pongers/notrust', ident = 'abc' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( not resp.isSuccess and resp.error == 'unauthorized' )

    vHandle = beach.getActorHandle( 'pongers/notrust', ident = 'def' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    assert( beach.flush() )

def test_group():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers/notrust/1.0', trustedIdents = [ 'def' ], parameters={"a":12} )
    assert( isMessageSuccess( a1 ) )
    a2 = beach.addActor( 'Pong', 'pongers/notrust/2.0', trustedIdents = [ 'def' ], parameters={"a":13} )
    assert( isMessageSuccess( a2 ) )

    g1 = beach.getActorHandleGroup( 'pongers/' )
    assert( 1 == g1.getNumAvailable() )

    a3 = beach.addActor( 'Pong', 'pongers/notrustalt/1.0', trustedIdents = [ 'def' ], parameters={"a":14} )
    assert( isMessageSuccess( a3 ) )

    g1.forceRefresh()
    assert( 2 == g1.getNumAvailable() )

    assert( beach.flush() )

def test_concurrency():
    global beach

    a1 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":15}, n_concurrent = 1 )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'sleepers' )

    # Normal query wait for resp
    resp = vHandle.request( 'nosleep', timeout = 2 )
    assert( resp.isSuccess and 'time' in resp.data )

    # Query timeout and re-query and confirm one
    # concurrent query
    resp = vHandle.request( 'sleep', timeout = 1 )
    assert( resp.isTimedOut and not resp.isSuccess )
    resp = vHandle.request( 'nosleep', timeout = 2 )
    assert( resp.isTimedOut and not resp.isSuccess )

    assert( beach.flush() )

    vHandle.close()
    vHandle = beach.getActorHandle( 'sleepers' )

    a2 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":16}, n_concurrent = 2 )
    assert( isMessageSuccess( a2 ) )

    # Now confirm first q does not block entire actor
    resp = vHandle.request( 'sleep', timeout = 1 )
    assert( resp.isTimedOut and not resp.isSuccess )
    resp = vHandle.request( 'nosleep', timeout = 2 )
    assert( resp.isSuccess and 'time' in resp.data )

    assert( beach.flush() )

def test_private_params():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":17,"_b":42,"c":43}, n_concurrent = 1 )
    assert( isMessageSuccess( a1 ) )

    mtd = beach.getAllNodeMetadata()

    mtd = mtd.values()[ 0 ].get( 'data', {} ).get( 'mtd', {} ).values()[ 0 ].get( 'params', {} )

    assert( mtd[ 'a' ] == 17 )
    assert( mtd[ 'c' ] == 43 )
    assert( mtd[ '_b' ] == '<PRIVATE>' )

    assert( beach.flush() )

def test_host_affinity():
    global beach

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

    a1 = beach.addActor( 'Ping', 'pingers',
                         parameters={"a":18},
                         strategy = 'host_affinity',
                         strategy_hint = 'nope' )
    assert( not isMessageSuccess( a1 ) )

    thisIface = _getIpv4ForIface( 'eth0' )
    if thisIface is None:
        thisIface = _getIpv4ForIface( 'en0' )
    a1 = beach.addActor( 'Ping', 'pingers',
                         parameters={"a":19},
                         strategy = 'host_affinity',
                         strategy_hint = thisIface )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

    assert( beach.flush() )

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

def test_multi_category():
    global beach

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

    a1 = beach.addActor( 'MultiPing', [ 'pingers', 'oobers' ], parameters={"a":18} )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers', {} ) ) )

    vHandlePing = beach.getActorHandle( 'pingers' )
    vHandleOob = beach.getActorHandle( 'oobers' )

    resp = vHandlePing.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    resp = vHandleOob.request( 'oob', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'cmdresult' in resp.data )

    assert( beach.removeFromCategory( a1[ 'data' ][ 'uid' ], 'oobers' ) )
    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers', {} ) ) )

    assert( beach.addToCategory( a1[ 'data' ][ 'uid' ], 'newcat' ) )
    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'newcat', {} ) ) )

    assert( beach.flush() )
