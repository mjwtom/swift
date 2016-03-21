# Data Dedupliation in Swift
## Deduplication Overview
Deduplication is implemented as a storage policy just like Erasure Coding.
This is not good. Because it should above the storage policy and rely the
storage policy to keep the reliability. It should be able to separate the
storage policy for the unique data chunks and the metadata since they have
different access patterns.

When a data stream comes, it performs the following steps.

* chunking
* fingerprinting
* fingerprint identification
* packing unique chunks into deduplication container
* storing the container if full

## Configuration
To enable deduplication, add the dedupliation policy into swift.conf
A typical confiuration would be like

`[DEFAULT]`

`compress = no`

`compress_method = lz4hc`

`[storage-policy:2]`

`name = <your-user-name>`

`policy_type = deduplication`

`default = yes`

We recommend to use compression on the storage node (object server). Because
compression is a slow process, which can nagetively impact the performance of
proxy-server. The default compression algorithm is lz4hc. It can be configured
to zib or lz4. Zlib has a higher compression rate while lz4 has a higher
compression speed.

## Fingerprint lookup in deduplication

There are three ways to check if the fingrprint is here or not.
× use sqlite databse
* use on disk hash table
* use on disk hash table with the lazy method

When using sqlite, fingerprints are inserted into a table.
Fingerprint will go to the table if it not found

On disk hash table and the lazy method use the same on disk data structure.
The different is that the lazy method buffers the fingerprints instead of
looking up them on disk immediately. For more information, please look the
lazy deduplication paper.


## Rsyslog
In the template, the system is configured to send log information to the rsyslog
via the UDP soccket. So in the rsyslog configuration file, we need to open the
port for it.

The deduplication engine records the deduplciation information into the log.
For every container it record all the information once. It is configurable.

It is good to use the rsyslog to record the information. I also make my own log
for the debug or profile information.

## Profiling
I tried to use cProfile. But I found the xprofile middleware seems work find. I
like the mini web it provides.


## Bugs I made (please check here if you find your system does not work)
Remember to start the memcached server before start the cluster.
