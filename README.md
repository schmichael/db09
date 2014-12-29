# db09

The dumbest possible distributed key/value store.

* Eventually consistent via read repairs
* Timestamp based versioning (LWW - last write wins)
* Token ring (2<sup>16</sup> tokens)
* Configurable replication level (RL)
* Max useful nodes = (2<sup>16<sup> ⨉ RL) (so 196,608 for RL=3)

## wat

2009 was a great year in open source distributed databases. Facebook had tossed
Cassandra over the wall a year earlier into the feeding frenzy caused by
Amazon's Dynamo paper back in 2007. The first version of Riak was released just
in time to save those people who had learned Erlang because of CouchDB to dodge
Cassandra and stay safely in Erlang's loving embrace (since staying on CouchDB
obviously wasn't going to work.) MongoDB's initial release was in 2009 as well
which promised web scale with secondary indexes and documents and memory maps
and benchmarks.

**2009 was great.** You no longer had to employ one-operator-per-node to run a
Hadoop cluster in order to be Web Scale<sup>tm</sup>.

Of course none of it worked well, but that was all part of the fun! Everyone
got to spend endless hours tuning the JVM, debugging vector clocks, cursing
compactions, discovering what "durability" meant, and becoming heros in their
chosen kingdoms.

Now the startups those heros worked for got acquired, or failed, or are just
flailing. The databases are almost following through on the promises they were
making back then. All the action is in Java Script or containers or service
orchestration.

**Enter db09.**

## Weaknesses

Basically everything. This is a toy. Never use it for anything you care about.

Here are some specific gotchas:

* Tokens are generated using a FNV-1a hash<sup>1<sup> which is a
  non-cryptographic hash.  Attackers can trivially DoS a node by generating
  keys which hash to the same token.
* The "backend" is currently an in-memory map (lol @ "mmap") so there's 0
  durability.
* You really need to bring up 1 node at a time to avoid ring ownership
  conficts... I mean it may not work at all either way so do what you want.
* No tests. Or maybe I'll add some and forget to remove this, but I doubt it.
* No tunables. To be fully 2009 compliant there should be dozens of poorly
  documented knobs which must be configured precisely for anything to work at
  all. Defaults should be provided and completely wrong for all use cases.

### Footnotes

1. After years of research, decades of of benchmarking, and centuries of
   microoptimizing assembly for every known CPU, I chose FNV-1a because it was
   in the Go standard library and sounded more professional than "CRC32".
