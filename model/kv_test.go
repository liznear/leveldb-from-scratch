package model

import (
	"reflect"
	"testing"
)

func TestKV(t *testing.T) {
	tcs := []struct {
		name  string
		key   string
		value []byte
	}{
		{
			name:  "NilValue",
			key:   "",
			value: nil,
		},
		{
			name:  "Empty",
			key:   "",
			value: []byte{},
		},
		{
			name:  "EmptyKey",
			key:   "",
			value: []byte("World"),
		},
		{
			name:  "EmptyValue",
			key:   "Hello",
			value: []byte{},
		},
		{
			name:  "KeyValue",
			key:   "Hello",
			value: []byte("World"),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kv := KV{
				Key:   NewKey(tc.key),
				Value: NewValue(tc.value),
			}
			bytes := kv.ToBytes()
			got, end, err := FromBytes(bytes)
			if err != nil {
				t.Fatalf("Fail to convert kv (%q:%v) to bytes: %v", tc.key, tc.value, err)
			}
			if got.Key.Data != tc.key {
				t.Errorf("Got key %q, want %q", got.Key.Data, tc.key)
			}
			if got.Value.Deleted {
				t.Errorf("Got deleted value, want %v", tc.value)
			}
			if !reflect.DeepEqual(got.Value.Data, tc.value) {
				t.Errorf("Got value %v, want %v", got.Value.Data, tc.value)
			}
			if end != len(bytes) {
				t.Errorf("Still has %d bytes not processed", len(bytes)-end)
			}
		})
	}
}

func TestKV_Deleted(t *testing.T) {
	t.Parallel()
	kv := KV{
		Key:   NewKey("key"),
		Value: NewDeletedValue(),
	}
	bytes := kv.ToBytes()
	got, end, err := FromBytes(bytes)
	if err != nil {
		t.Fatalf("Fail to convert deleted value to bytes: %v", err)
	}
	if got.Key.Data != kv.Key.Data {
		t.Errorf("Got key %q, want %q", got.Key.Data, kv.Key.Data)
	}
	if !got.Value.Deleted {
		t.Errorf("Got un-deleted value %v", got.Value.Data)
	}
	if end != len(bytes) {
		t.Errorf("Still has %d bytes not processed", len(bytes)-end)
	}
}

func TestKV_BatchFromBytes(t *testing.T) {
	t.Parallel()
	kvs := []KV{
		{
			Key:   NewKey("Key1"),
			Value: NewValue([]byte("Value1")),
		},
		{
			Key:   NewKey("Key2"),
			Value: NewValue([]byte("Value2")),
		},
	}
	var bs []byte
	for _, kv := range kvs {
		bs = append(bs, kv.ToBytes()...)
	}
	got, end, err := BatchFromBytes(bs)
	if err != nil {
		t.Fatalf("Fail to convert %v to multiple kvs: %v", bs, err)
	}
	if len(got) != len(kvs) {
		t.Errorf("Got %d kvs, want %d", len(got), len(kvs))
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], kvs[i]) {
			t.Errorf("%d: Got %v, want %v", i, got[i], kvs[i])
		}
	}
	if end != len(bs) {
		t.Errorf("Still has %d bytes not processed", len(bs)-end)
	}
}
