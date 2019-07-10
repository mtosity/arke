package util

import (
	"sync"
)

type ConcurrentMap struct {
	sync.Mutex
	items map[string]interface{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		items: make(map[string]interface{}),
	}
}

func (cm *ConcurrentMap) Add(key string, item interface{}) {
	cm.Lock()
	defer cm.Unlock()
	cm.items[key] = item
}

func (cm *ConcurrentMap) Delete(key string) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.items, key)
}

func (cm *ConcurrentMap) Get(key string) interface{} {
	cm.Lock()
	defer cm.Unlock()
	return cm.items[key]
}
