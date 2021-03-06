# Prior to using this example infrastructure script
# you should start the HostManager.py on the same host
# you intend to run this example.
# To do so simply run:
# python HostManager.py path/to/py-beach/examples/multinode/multinode.yaml
# This will set the host as a beach node.

import sys
import os
import json
import yaml
import time

# Adding the beach lib directory relatively for this example
curFileDir = os.path.dirname( os.path.abspath( __file__ ) )
sys.path.append( os.path.join( curFileDir, '..', '..' ) )

from beach.beach_api import Beach

print( "Connecting to example beach." )
beach = Beach( os.path.join( curFileDir, 'multinode.yaml' ),
               realm = 'global' )

print( "Creating ping actor in resource beach node." )
a1 = beach.addActor( 'Ping', 'pingers', strategy = 'resource' )
print( json.dumps( a1, indent = 4 ) )

print( "Creating pong actor in affinity( pingers ) beach node." )
a2 = beach.addActor( 'Pong', 'pongers', strategy = 'affinity', strategy_hint = 'pingers' )
print( json.dumps( a2, indent = 4 ) )

print( "Creating pong actor in isolation." )
a3 = beach.addActor( 'Pong', 'pongers', isIsolated = True )
print( json.dumps( a3, indent = 4 ) )

print( "Idling for a few seconds..." )
time.sleep( 15 )

print( "Querying for beach directory." )
d = beach.getDirectory()
print( json.dumps( d, indent = 4 ) )

print( "Trying some queries to the cluster." )
vHandle = beach.getActorHandle( 'pongers' )
print( "Issuing ping" )
resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )
print( "Received: %s" % str( resp ) )

time.sleep( 2 )
print( "Flushing beach." )
f = beach.flush()
print( json.dumps( f, indent = 4 ) )

beach.close()