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

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":1}, resources = {'pongers':'pongers'} )
    assert( ( not isMessageSuccess( a1 ) ) and ( a1.get( 'status', {} ).get( 'error', None ) == 'unprivileged' ) )

    beach._admin_token = tmpAdminToken

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":1}, resources = {'pongers':'pongers'} )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )

    assert( beach.flush() )

def test_flushing_single_node_cluster():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":2}, resources = {'pongers':'pongers'} )
    assert( isMessageSuccess( a1 ) )

    f = beach.flush()
    assert( f )

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

def test_actor_creation():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":3}, resources = {'pongers':'pongers'}, strategy = 'roundrobin' )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers', parameters={"a":4}, resources = {} )
    assert( isMessageSuccess( a2 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

    assert( beach.flush() )

def test_isolated_actor_creation():
    global beach

    a1 = beach.addActor( 'Ping', 'pingers', isIsolated = True, parameters={"a":5}, resources = {'pongers':'pongers'} )
    assert( isMessageSuccess( a1 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

    assert( beach.flush() )

def test_virtual_handles():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":6}, resources = {} )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'pongers' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    assert( beach.flush() )

def test_prefix_virtual_handles():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":7}, resources = {} )
    a2 = beach.addActor( 'Ping', 'pingers', parameters={"a":8}, resources = {'pongers':'pongers'} )
    a3 = beach.addActor( 'Ping', 'pingers', parameters={"a":9}, resources = {'pongers':'pongers'} )
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

    a1 = beach.addActor( 'Pong', 'pongers/trust', trustedIdents = [ 'abc' ], parameters={"a":10}, resources = {} )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers/notrust', trustedIdents = [ 'def' ], parameters={"a":11}, resources = {} )
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

    a1 = beach.addActor( 'Pong', 'pongers/trust/1.0', trustedIdents = [ 'def' ], parameters={"a":12}, resources = {} )
    assert( isMessageSuccess( a1 ) )
    a2 = beach.addActor( 'Pong', 'pongers/trust/2.0', trustedIdents = [ 'def' ], parameters={"a":13}, resources = {} )
    assert( isMessageSuccess( a2 ) )

    g1 = beach.getActorHandleGroup( 'pongers/', ident = 'def' )
    assert( 1 == g1.getNumAvailable() )

    a3 = beach.addActor( 'Pong', 'pongers/trustalt/1.0', trustedIdents = [ 'def' ], parameters={"a":14}, resources = {} )
    assert( isMessageSuccess( a3 ) )

    g1.forceRefresh()
    assert( 2 == g1.getNumAvailable() )

    results = g1.request( 'ping', timeout = 2 )

    nResponses = 0
    nMax = 0
    while 2 > nResponses and 3 > nMax:
        assert( results.waitForResults( 5 ) )
        nResponses += len( results.getNewResults() )
        nMax += 1

    assert( 2 == nResponses )

    assert( beach.flush() )

def test_concurrency():
    global beach

    a1 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":15}, resources = {}, n_concurrent = 1 )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'sleepers' )

    # Normal query wait for resp
    resp = vHandle.request( 'nosleep', timeout = 2 )
    assert( resp.isSuccess and 'time' in resp.data )

    # Query timeout and re-query and confirm one
    # concurrent query
    resp = vHandle.request( 'sleep', timeout = 1 )
    assert( resp.isTimedOut and not resp.isSuccess )

    assert( beach.flush() )

def test_private_params():
    global beach

    a1 = beach.addActor( 'Pong', 'pongers', parameters={"a":17,"_b":42,"c":43}, resources = {}, n_concurrent = 1 )
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
                         resources = {},
                         strategy = 'host_affinity',
                         strategy_hint = 'nope' )
    assert( not isMessageSuccess( a1 ) )

    thisIface = _getIpv4ForIface( 'eth0' )
    if thisIface is None:
        thisIface = _getIpv4ForIface( 'en0' )
    a1 = beach.addActor( 'Ping', 'pingers',
                         parameters={"a":19},
                         resources = {'pongers':'pongers'},
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

    a1 = beach.addActor( 'MultiPing', [ 'pingers', 'oobers', 'oobers2' ], parameters={"a":18}, resources = {} )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'MultiPing', [ 'oobers', ], parameters={"a":19}, resources = {} )
    assert( isMessageSuccess( a2 ) )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 2 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers2', {} ) ) )

    vHandlePing = beach.getActorHandle( 'pingers' )
    vHandleOob = beach.getActorHandle( 'oobers' )

    resp = vHandlePing.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

    resp = vHandleOob.request( 'oob', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'cmdresult' in resp.data )

    assert( beach.removeFromCategory( a1[ 'data' ][ 'uid' ], 'oobers' ) )
    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers2', {} ) ) )

    assert( beach.addToCategory( a1[ 'data' ][ 'uid' ], 'newcat' ) )
    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'newcat', {} ) ) )

    assert( beach.flush() )

def test_pending_metric():
    global beach

    a1 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":15}, resources = {}, n_concurrent = 1 )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'sleepers' )

    # Sending a request that will stall for a bit
    vHandle.shoot( 'sleep', data = {}, timeout = 20 )
    # Make sure it's stalled
    time.sleep( 2 )
    # Make sure it's accounted for
    pending = vHandle.getPending()

    assert( 1 == len( pending ) )
    assert( 1 == pending.values()[ 0 ] )

    assert( beach.flush() )

    vHandle.close()
    assert( beach.flush() )

def test_tons_of_actors():
    global beach

    actors = []
    total = 50
    for i in range( total ):
        a = beach.addActor( 'MultiPing', [ 'pingers', 'oobers', 'oobers2' ], parameters={"a":18}, resources = {} )
        assert( isMessageSuccess( a ) )
        actors.append( a )

    d = beach.getDirectory()
    assert( total == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( total == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers', {} ) ) )
    assert( total == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'oobers2', {} ) ) )

    beach.flush()