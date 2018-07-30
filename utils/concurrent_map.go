package utils

import "sync"

// ConcurrentMap is a wrapper for a map can be safely shared between goroutines
type ConcurrentMap struct {
	sync.RWMutex
	items map[string]interface{}
}

// ConcurrentMapItem is a wrapper for a slice item
type ConcurrentMapItem struct {
	Key   string
	Value interface{}
}

// Set is for adding/updating a new <key, value> pair into the map
// @param key is the key for the dictionary
// @param value is any object
func (cm *ConcurrentMap) Set(key string, value interface{}) {
	cm.Lock()
	defer cm.Unlock()

	cm.items[key] = value
}

// Get is for getting an object from the dictionary
// @param key is the key of the desired object
func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	cm.Lock()
	defer cm.Unlock()

	value, ok := cm.items[key]

	return value, ok
}

func (cm *ConcurrentMap) Delete(key string) {
	cm.Lock()
	defer cm.Unlock()

	delete(cm.items, key)
}

// Iter is for getting the iterator for the elements in the map
// return a channel containing the items
func (cm *ConcurrentMap) Iter() <-chan ConcurrentMapItem {
	c := make(chan ConcurrentMapItem)

	f := func() {
		cm.Lock()
		defer cm.Unlock()

		for k, v := range cm.items {
			c <- ConcurrentMapItem{k, v}
		}
		close(c)
	}
	go f()

	return c
}