package utils

import (
	"sync"
)

// ConcurrentSlice is a wrapper for a slice can be safely shared between goroutines
type ConcurrentSlice struct {
	sync.RWMutex
	items []interface{}
	tombstones []bool
	maxSize int // ignored if not specified
}

// ConcurrentSliceItem is a wrapper for a slice item
type ConcurrentSliceItem struct {
	Index     int
	Value     interface{}
	Tombstone bool
}

// NewConcurrentSlice creates a new concurrent map
func NewConcurrentSlice(size int, fixed bool) *ConcurrentSlice {
	cs := &ConcurrentSlice{
		items: make([]interface{}, size),
		tombstones: make([]bool, size),
	}
	if fixed {
		cs.maxSize = size
	}
	return cs
}


// Append is for adding a new item to the slice in a thread-safe fashion
// @param item is the item to append
func (cs *ConcurrentSlice) Append(item interface{}) {
	cs.Lock()
	defer cs.Unlock()

	cs.items = append(cs.items, item)
	cs.tombstones = append(cs.tombstones, false)

	if cs.maxSize > 0 && len(cs.items) > cs.maxSize {
		cs.items = cs.items[1:]
		cs.tombstones = cs.tombstones[1:]
	}
}

// Iter is for getting the iterator for the elements in the slice
// return a channel containing the items
func (cs *ConcurrentSlice) Iter() <-chan ConcurrentSliceItem {
	c := make(chan ConcurrentSliceItem, cs.Len())

	f := func() {
		cs.RLock()
		defer cs.RUnlock()
		for index, value := range cs.items {
			c <- ConcurrentSliceItem{index, value, cs.tombstones[index]}
		}
		close(c)
	}
	go f()

	return c
}

// Len returns the length of the slice
func (cs *ConcurrentSlice) Len() int {
	cs.RLock()
	defer cs.RUnlock()
	return len(cs.items)
}

// Get returns the element at the given index
func (cs *ConcurrentSlice) Get(idx int) interface{} {
	cs.RLock()
	defer cs.RUnlock()
	return cs.items[idx]
}

// MarkTombstone inform the concurrent slice that the element at index idx will be removed on next Sync call
func (cs *ConcurrentSlice) MarkTombstone(idx int) {
	cs.Lock()
	defer cs.Unlock()
	cs.tombstones[idx] = true
}

// Synchronize the slice with the given tombstone list
func (cs *ConcurrentSlice) Sync() {
	cs.Lock()
	defer cs.Unlock()
	// Allocate a new elements list
	sl := make([]interface{}, 0)
	for idx, tomb := range cs.tombstones {
		if !tomb {
			sl = append(sl, cs.items[idx])
		}
	}
	cs.items = sl
	// Reset tombstone markers
	cs.tombstones = make([]bool, len(cs.items))
}