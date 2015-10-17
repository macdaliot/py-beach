import os
import time
import subprocess
import signal
from gevent.lock import Semaphore
from beach.beach_api import Beach
from beach.utils import *

h_hostmanager = None
beach = None
curFileDir = os.path.join( os.path.dirname( os.path.abspath( __file__ ) ) )


def test_create_single_node_cluster():
    global beach
    global h_hostmanager
    h_hostmanager = subprocess.Popen( [ 'python',
                                        '-m',
                                        'beach.hostmanager',
                                        os.path.join( curFileDir, 'simple.yaml' ),
                                        '--log-level',
                                        '10' ] )

    time.sleep( 2 )

    assert( h_hostmanager.returncode is None )

def test_beach_connection():
    global beach
    beach = Beach( os.path.join( curFileDir, 'simple.yaml' ),
                   realm = 'global' )
    time.sleep( 1 )
    assert( 1 == beach.getNodeCount() )


def test_actor_creation():
    global beach
    a1 = beach.addActor( 'Ping', 'pingers', parameters={"a":1}, strategy = 'roundrobin' )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers', parameters={"a":2} )
    assert( isMessageSuccess( a2 ) )

    time.sleep( 2 )

    d = beach.getDirectory()
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

def test_isolated_actor_creation():
    global beach
    a1 = beach.addActor( 'Ping', 'pingers', isIsolated = True, parameters={"a":3} )
    assert( isMessageSuccess( a1 ) )

    time.sleep( 2 )

    d = beach.getDirectory()
    assert( 2 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 1 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

def test_virtual_handles():
    global beach
    vHandle = beach.getActorHandle( 'pongers' )
    resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
    assert( resp.isSuccess and 'time' in resp.data )

def test_prefix_virtual_handles():
    global beach
    vHandles = beach.getActorHandle( 'p' )
    gevent.sleep( 1 )
    assert( 3 == vHandles.getNumAvailable() )
    assert( vHandles.isAvailable() is True )

    vHandles = beach.getActorHandle( 'p/' )
    gevent.sleep( 1 )
    assert( 0 == vHandles.getNumAvailable() )
    assert( vHandles.isAvailable() is False )

def test_flushing_single_node_cluster():
    global beach
    f = beach.flush()
    assert( f )

    d = beach.getDirectory()
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pingers', {} ) ) )
    assert( 0 == len( d.get( 'realms', {} ).get( 'global', {} ).get( 'pongers', {} ) ) )

def test_trust():
    global beach
    a1 = beach.addActor( 'Pong', 'pongers/trust', trustedIdents = [ 'abc' ], parameters={"a":4} )
    assert( isMessageSuccess( a1 ) )

    a2 = beach.addActor( 'Pong', 'pongers/notrust', trustedIdents = [ 'def' ], parameters={"a":5} )
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

def test_group():
    global beach
    a1 = beach.addActor( 'Pong', 'pongers/notrust/1.0', trustedIdents = [ 'def' ], parameters={"a":6} )
    assert( isMessageSuccess( a1 ) )
    a2 = beach.addActor( 'Pong', 'pongers/notrust/2.0', trustedIdents = [ 'def' ], parameters={"a":7} )
    assert( isMessageSuccess( a2 ) )

    g1 = beach.getActorHandleGroup( 'pongers/' )

    gevent.sleep( 1 )
    assert( 2 == g1.getNumAvailable() )

    assert( beach.flush() )

def test_concurrency():
    global beach

    a1 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":8}, n_concurrent = 1 )
    assert( isMessageSuccess( a1 ) )

    vHandle = beach.getActorHandle( 'sleepers' )
    gevent.sleep( 1 )

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
    gevent.sleep( 1 )

    a2 = beach.addActor( 'Sleeper', 'sleepers', parameters={"a":9}, n_concurrent = 2 )
    assert( isMessageSuccess( a2 ) )

    # Now confirm first q does not block entire actor
    resp = vHandle.request( 'sleep', timeout = 1 )
    assert( resp.isTimedOut and not resp.isSuccess )
    resp = vHandle.request( 'nosleep', timeout = 2 )
    assert( resp.isSuccess and 'time' in resp.data )



def test_terminate_single_node_cluster():
    global beach
    global h_hostmanager
    beach.close()
    h_hostmanager.send_signal( signal.SIGQUIT )

    assert( 0 == h_hostmanager.wait() )