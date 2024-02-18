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