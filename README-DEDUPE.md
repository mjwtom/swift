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

`[storage-policy:2]`

`name = <your-user-name>`

`policy_type = deduplication`

`default = yes`

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


## Bugs I made
Remember to start the memcached server before start the cluster.
