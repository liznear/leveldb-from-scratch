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
