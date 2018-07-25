package utils

import "sync"

// ConcurrentSlice is a wrapper for a slice can be safely shared between goroutines
type ConcurrentSlice struct {
	sync.RWMutex
	items []interface{}
}

// ConcurrentSliceItem is a wrapper for a slice item
type ConcurrentSliceItem struct {
	Index int
	Value interface{}
}

/**
* Append is for adding a new item to the slice in a thread-safe fashion
* @param item is the item to append
 */
func (cs *ConcurrentSlice) Append(item interface{}) {
	cs.Lock()
	defer cs.Unlock()

	cs.items = append(cs.items, item)
}

/**
* Iter is for getting the iterator for the elements in the slice
* return a channel containing the items
 */
func (cs *ConcurrentSlice) Iter() <-chan ConcurrentSliceItem {
	c := make(chan ConcurrentSliceItem)

	f := func() {
		cs.Lock()
		defer cs.Unlock()
		for index, value := range cs.items {
			c <- ConcurrentSliceItem{index, value}
		}
		close(c)
	}
	go f()

	return c
}


