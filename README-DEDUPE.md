## Deduplication Overview
Deduplication is implemented as a storage policy just like Erasure Coding.
This is not good. Because it should above the storage policy and rely the
storage policy to keep the reliability. It should be able to separate the
storage policy for the unique data chunks and the metadata since they have
different access patterns.

## Configuration
To enable deduplication, add the dedupliation policy into swift.conf
A typical confiuration would be like

``[storage-policy:2]
name = mjwtom
policy_type = deduplication
default = yes``

## Fingerprint lookup in deduplication

There are three ways to check if the fingrprint is here or not.
× use sqlite databse
* use on disk hash table
* use on disk hash table with the lazy method

When using sqlite, fingerprints are inserted into a table.
Fingerprint will go to the table if it not found
