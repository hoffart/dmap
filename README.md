# DMap - A Minimalistic Disk-Backed Java Key-Value Store

## Overview
A very simple disk-backed key-value store written in Java. 

The map can be built only once, afterwards it is read-only, thus the use-cases 
are somewhat limited to a scenario where the key-value pairs need to be served
read-only.

I wrote this mainly to have a simplistic store that is memory efficient and
does not come with a lot of overhead as Lucene or Berkeley DB. The main 
trade-off is speed and memory efficiency (DMap) versus flexibility 
(Lucene, BDB).

Keys and values are byte[], on disk the overhead are one additional int per 
key and value for keeping the size, plus one additional int per key-value-pair
for keeping the offset. The underlying file is put into memory on demand using
a java.nio.MappedByteBuffer, with the benefit that everything can be preloaded
on startup.

## Example
```
// Build dmap.
File mapFile = new File("FILEPATH");
byte[] key = ByteBuffer.allocate(4).putInt(2).array();
byte[] value = ByteBuffer.allocate(4).putInt(23).array();
DMapBuilder dmapBuilder = new DMapBuilder(mapFile);
dmapBuilder.add(key, value);
// Finalize map.
dmapBuilder.build();

// Get key.
DMap dmap = new DMap(mapFile);
byte[] retrieved = dmap.get(key); 
```

## Usage Scenario

Read-only, random access key reads with no iteration.

## Upcoming Features

 * Special int-int mode for more efficient storage.
 * String-String mode.
 * Make DMapBuilder appendable.