# LevelDB From Scratch

This is a simple Level DB implementation in Go aiming for helping people
understand LevelDB. It is not production ready, feature complete, or
optimized. The goal would be understandability.

To help with that, several branches are created to show how each step is
done.

## Step 1: MemTable

MemTable is a in-memory key value store. In LevelDB, new key value pairs
are inserted into MemTable first. When it is full, it would be finalized
as a SSTable and written to disk. In this step, we focus on MemTable
only. It can be implemented as a simple map. However, we need to pay
attention on the deletion of keys.

# Step 2: Create SSTable from KVs

SSTable means SortedStringTable. It is stored as an on-disk file. When a
MemTable is full, it would be persisted as a level-0 SSTable. In this
step, we focus on creating an SSTable from a sorted list of KVs. Since
SSTables are stored as files, we need to assign a name to each SSTable.
In this project, we assign a monotonic increasing `Gen` to each SSTable
so that newer SSTables have higher `Gen`.

# Step 3: Persist MemTable when it's full

In this step, we convert a full MemTable into a SSTable using the
function we implemented in step2. It is pretty straightforward.

# Step 4: Read SSTable (without any fancy optimization)

This step is a simple sequential scan of the SSTable. No fancy stuff
like index, bloom filter, etc. It has already been done in tests of
previous steps.

# Step 5: Implement the top-level DB using MemTable and SSTable

In this step, we create the top-level user facing DB wrapping up the
MemTable and SSTable we've implemented. We will only have MemTable and
level-0 SSTables in this step. Compaction won't be included. The tricky
part here is that when a MemTable is full and being persisted, the
SSTable is not ready for read. We need to make sure this MemTable is
also scanned while doing lookups.

# Step 6: Compaction

We only have level-0 SSTables right now. It has several issues:

- They may have duplicated keys.
- They have overlaps. While doing lookup, we need to scan all SSTables.

In this step, we implement compaction. When the number of SSTables on
level-0 exceeds a specified limit, a compaction starts. We first
calculates the range of the SSTable triggering the compaction (i.e. min
key to max key). Then we find all overlapping SSTables at level 0 and
level 1. All overlapping level-0 & level-1 SSTables would be deleted.
The KVs in these SSTables would be compacted (only keep the latest value
for each key) and merged into new level-1 SSTables. After this is done,
we check if level-1 has too many SSTables, and repeat the same step for
level-1, level-2 ... until we handle all levels.

# Step 7: WAL

We should have a working DB now, but it is not crash safe. If the server
crash, all data in MemTable are lost. To support crash recovery, the
common solution is using write-ahead logs (a.k.a. WAL). The idea is
simple, before updating the MemTable, we persist the data into the
WAL first. Since the WAL is a file on disk, if the server crash, we can
always rebuild the MemTable from the WAL.

One thing to note is that we don't put all WAL data into MemTable. For
KVs already written into SSTables, we shouldn't put them into MemTable
again. Otherwise, we will have duplicated data (won't affect
correctness, but make the recovery slower and use more storage). To tell
which KVs have been written into SSTables, we can also record the
creating SSTable operation into the WAL. All KVs before the operation
should be in the SSTables. Recording this also helps recover compactions
if server crashes.
