# py-beach

Python private compute cloud framework with a focus on ease of deployment and expansion rather 
than pure performance.

## Design Basics
Beach enables you to deploy python Actors onto a cluster (with varying strategies) without 
having to care where they get loaded in the cluster. Additionally, it allows the Actors to
query each other by issuing requests to Categories of Actors rather than specific Actors.
This means different roles being fulfilled by Actors can leverage other roles without having
to care where in the cluster, how many and what are the other Actors.

Cluster nodes can be added and removed at runtime (no Actor migration yet). All communications
between nodes are done in a peer-to-peer fashion, guided by a config file for the cluster defining
seed nodes similarly to Apache Cassandra.

Actors are created and managed in different Realms, allowing multiple projects to be running on
the same cluster. This means a cluster can be used as a common resource in a development team.

Security and strong segregation of Actors are NOT goals of this project. Rather, the clusters assume
a private, secure environment and non-malicious users.

## Repo Structure
The distributable package is in /lib/beach.

Small tests / examples can be found in /examples.

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