# LevelDB From Scratch

This is a simple Level DB implementation in Go aiming for helping people
understand LevelDB. It is not production ready, feature complete, or
optimized. The goal would be understandability.

To help with that, several branches are created to show how each step is
done.

## Step 1: MemTable

MemTable is a in-memory key value store. In LevelDB, new key value pairs
are inserted into MemTable first. When the table is full, it would be
finalized as a SSTable and written to disk.

In this step, we focus on MemTable without implementing SSTable, WAL,
etc.

# Step 2: SSTable

SSTable means SortedStringTable. It is stored as an on-disk file. When a
MemTable is full, it would be persisted as a level-0 SSTable. With more
kvs inserted, more level-0 SSTables would be created. These SSTables may
have duplicated keys, and consume more storage. This is resolved by a
process called compaction. Level-0 SSTables would be compacted into
level-1 SSTables, which would then be compacted into level-2 SSTables,
etc.

While doing lookup by a specific key, we would scan the MemTable first
because it is contains the most up-to-date value. If the key doesn't
exist, we should scan level-0 SSTables, level-1 SSTables, ... level-n
SSTables in order.

The same level can have multiple SSTables containing the same key (this
can only happen for level 0, will get back to this later). To tell which
one is more up-to-date, we need to be able to sort level-0 SStables. To
do that, we assign a monotonically increasing `GEN` to each SSTable (
just use `GEN` as the file name). SSTables having higher `GEN` values
are more up-to-date.

# Step 3: Persist MemTable when it's full

In this step, we convert a full MemTable into a SSTable. The SSTable
would be saved as a file on disk. Meanwhile, a new empty MemTable should
be created to handle new inserts.

As mentioned in step 2, we would assign a `GEN` to each SSTable as the
file name. In this step, we need to implement this assignment. It can be
easily done by maintaining a global counter in memory. The tricky part
is what to do when the server restarts. In this case, we need to
initialize the counter to one plus the max `GEN` in the file system.