package utils

import "sync"

// ConcurrentSlice is a wrapper for a slice can be safely shared between goroutines
type ConcurrentSlice struct {
	sync.RWMutex
	items []interface{}
	maxSize int // ignored if not specified
}

// ConcurrentSliceItem is a wrapper for a slice item
type ConcurrentSliceItem struct {
	Index int
	Value interface{}
}

// NewConcurrentSlice creates a new concurrent map
func NewConcurrentSlice(size int, fixed bool) *ConcurrentSlice {
	cs := &ConcurrentSlice{items: make([]interface{}, size)}
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

	if cs.maxSize > 0 && len(cs.items) > cs.maxSize {
		cs.items = cs.items[1:]
	}
}

// Iter is for getting the iterator for the elements in the slice
// return a channel containing the items
func (cs *ConcurrentSlice) Iter() <-chan ConcurrentSliceItem {
	c := make(chan ConcurrentSliceItem)

	f := func() {
		cs.RLock()
		defer cs.RUnlock()
		for index, value := range cs.items {
			c <- ConcurrentSliceItem{index, value}
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

// Remove remove one element from the slice
func (cs *ConcurrentSlice) Remove(idx int) {
	cs.Lock()
	defer cs.Unlock()
	// Replace element to remove with the last one
	cs.items[idx] = cs.items[len(cs.items)-1]
	cs.items = cs.items[:len(cs.items)-1]
}