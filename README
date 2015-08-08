# py-beach

Python private compute cloud framework with a focus on ease of deployment and expansion rather 
than pure performance.

## Design Basics
Beach enables you to deploy python Actors onto a cluster (with varying strategies) without 
having to care where they get loaded in the cluster. Additionally, it allows the Actors to
query each other by issuing requests (or broadcasts) to Categories of Actors rather than specific Actors.
This means different roles being fulfilled by Actors can leverage other roles without having
to care where in the cluster, how many and what are the other Actors. Note that categories are 
referenced as prefixes, meaning you can (and should) use the category to encode things like version
or tag of some kind, like: 'my_crypt_actor/1.2', will match 'my_crypt_actor' but will allow you to
also shutdown and upgrade certain versions (like tests and prototypes).

Cluster nodes can be added and removed at runtime (no Actor migration yet). All communications
between nodes are done in a peer-to-peer fashion, guided by a config file for the cluster defining
seed nodes similarly to Apache Cassandra.

Actors are created and managed in different Realms, allowing multiple projects to be running on
the same cluster. This means a cluster can be used as a common resource in a development team.

Security and strong segregation of Actors are NOT goals of this project. Rather, the clusters assume
a private, secure environment and non-malicious users.

## Repo Structure
The distributable package is in /beach.

Small tests / examples can be found in /examples and /tests.

Useful scripts, like installing dependancies on a simple Debian system without a package can be found
in /scripts/

## Basic Usage

### Preparing for a distribution
The recommended way of running your cluster uses some kind of shared filesystem to share a common
directory to all your nodes and sharing the following components:
- beach config file
- code directory

The beach config file will specify the parameters used in various house-keeping functions of the cluster
but more importantly the seed nodes of the cluster. 
See the example config files like /examples/multinode/multinode.yaml for more complete and self-explanatory
(for now) documentation.

The code directory is a directory containing one more level of directories. Each child directory is named
for a Realm of the cluster. The default Realm is 'global'. Those Realm directories then contain the python
code for your Actors that you want made available (not necessarily actually loaded).

So a typical cluster in a shared cooperative work environment would have a shared directory (let's say NFS)
on the LAN, accessible to the devs and cluster nodes. It might look something like:
```
/cluster.yaml
/global/
/project1/IngestActor.py
/project1/ComputeActor.py
/project1/WriterActor.py
/project2/....
/project3/....
```

### Bootstraping the cluster
The main API to the cluster is beach.beach_api.Beach. By instantiating the Beach() and pointing it to the 
config file for the cluster, the Beach will connect to the nodes and discover the full list of nodes as
well as the Actor directory.

So a good way of defining a cluster would be to create a start.py which connects using Beach and creates
the relevant actors in a relevant way. This start.py can then be checked in to your source code repository.
It should be fairly stable since although it contains instantiation orders for the cluster, it does not
contain any topographical information. This means it will run the same whether you have 1 node or 50.

### Interacting live
You can start a Command Line Interface into the cluster like so:
    python -m beach.beach_cli /path/to/configFile
The documentation for the CLI is built into the interface.

## Operating modes
### Actor spawning
- random: this will spawn a new actor somewhere randomly in the cluster
- affinity: this will try to spawn the actor on a node with actors in the category specified in
    by strategy_hint

### Actor requests
- random: will issue the request to a random actors, prioritizing actors we already have a connection to
- affinity: will always issue the request to the actor identified by a hash of the key parameter of the 
    request, allowing you to do stateful processing on a certain characteristic, but also making you more
    prone to failure if a node or an actor goes down

### Some samples

#### Sample directory
```
/start.py
/multinode.yaml
/global
/global/Ping.py
/global/Pong.py
```

#### Ping Actor
```
from beach.actor import Actor
import time

class Ping ( Actor ):

    def init( self, parameters ):
        print( "Called init of actor." )
        self.zPong = self.getActorHandle( category = 'myactors/pongers' )
        self.schedule( 5, self.pinger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def pinger( self ):
        print( "Sending ping" )
        data = self.zPong.request( 'ping', data = { 'time' : time.time() }, timeout = 10 )
        print( "Received pong: %s" % str( data ) )
```

#### Pong Actor
```
from beach.actor import Actor
import time


class Pong ( Actor ):

    def init( self, parameters ):
        print( "Called init of actor." )
        self.handle( 'ping', self.ponger )

    def deinit( self ):
        print( "Called deinit of actor." )

    def ponger( self, msg ):
        print( "Received ping: %s" % str( msg ) )
        return { 'time' : time.time() }
```

#### Startup script
```
from beach.beach_api import Beach
beach = Beach( os.path.join( curFileDir, 'multinode.yaml' ),
               realm = 'global' )
a1 = beach.addActor( 'Ping', 'myactors/pingers', strategy = 'resource', parameters = {} )
a2 = beach.addActor( 'Pong', 'myactors/pongers', strategy = 'affinity', strategy_hint = 'pingers', parameters = {} )

beach.close()
```

#### Python interface into beach
```
from beach.beach_api import Beach
beach = Beach( os.path.join( curFileDir, 'multinode.yaml' ),
               realm = 'global' )
vHandle = beach.getActorHandle( 'myactors/pongers' )
resp = vHandle.request( 'ping', data = { 'source' : 'outside' }, timeout = 10 )

beach.close()
```

## Advanced Features
### Broadcast
Instead of using a vHandle.request, you may broadcast a message to all members of a category. This
feature is mainly there to enable upcoming features (a manager). Note that when broadcasting, no
response is checked and the .broadcast call returns immediately.

### Isolated Actors
A flag called isIsolated is available to addActor. When set to True (default False), the new Actor will
be created in its own instance process on a node in the cluster. This new process will not be shared with 
any other Actor. This reduces a bit the efficiency of the system if you create a lot of isolated Actors
but it also limits the potential dammage that Actor can do to other Actors (if it crashes it doesn't 
take any other Actor down) as well as protects against other unstable Actors. This should be used
for new potentially-unstable Actors or more critical Actors.

### Retries
By default, a vHandle request() will send one request to one Actor in the cloud, if that request times out
or fails, that fact will be reported immediately to the caller. It's up to the caller to handle failure.
This new nRetries parameter allows the caller to let the vHandle retry the request up to N times (on potentially
N different Actors). This means it's critical for those requests to be idempotent. This is much nicer
for Actor programming (not worrying about failures, it's what beach was developed for after all) but
the idempotent requirement can be demanding which is why it defaults to 0 retries.