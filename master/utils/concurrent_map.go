// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

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

// NewConcurrentMap creates a new concurrent map
func NewConcurrentMap() *ConcurrentMap {
	cm := &ConcurrentMap{
		items: make(map[string]interface{}),
	}

	return cm
}

// Len computes the length of the concurrent map
func (cm *ConcurrentMap) Len() int {
	cm.Lock()
	defer cm.Unlock()

	return len(cm.items)
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

// Delete if for deleting a <key, value> pair from the map
// @param key is the key for the dictionary
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