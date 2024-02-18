package table

// value represents a stored value in the table.
//
// If a key is deleted, we store a special value to indicate that the key is deleted. Otherwise, we may miss
// the deleting. Just consider this case. "key1" is inserted and persisted into SSTables. Now, we don't have any
// "key1" data in the MemTable. If we delete "key1" now, we should store this deletion operation in MemTable.
// Otherwise, if we read "key1", we would find nothing in MemTable, and return the old value found in SSTables.
type value struct {
	deleted bool
	data    []byte
}

func newValue(v []byte) value {
	return value{
		data: v,
	}
}

func newDeletedValue() value {
	return value{
		deleted: true,
	}
}
