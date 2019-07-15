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

func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	cm.Lock()
	defer cm.Unlock()
	if val, ok := cm.items[key]; ok {
		return val, true
	}
	return nil, false
}

func (cm *ConcurrentMap) GetList() []string {
	cm.Lock()
	defer cm.Unlock()
	var items []string
	for k, _ := range cm.items {
		items = append(items, k)
	}
	return items
}
