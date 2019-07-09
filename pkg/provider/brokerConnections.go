package provider

import (
	"log"
	"sync"
)

type BrokerConnections struct {
	sync.Mutex
	deets map[string]*BrokerDetails
}

func NewBrokerConnections() *BrokerConnections {
	return &BrokerConnections{
		deets: map[string]*BrokerDetails{},
	}
}

/*
 * Broker connections map access
 */

func (bc *BrokerConnections) Add(key string, bd *BrokerDetails) {
	bc.Lock()
	defer bc.Unlock()
	bc.deets[key] = bd
}

func (bc *BrokerConnections) Delete(key string) {
	bc.Lock()
	defer bc.Unlock()
	delete(bc.deets, key)
}

func (bc *BrokerConnections) Get(key string) *BrokerDetails {
	bc.Lock()
	defer bc.Unlock()
	return bc.deets[key]
}

func (bc *BrokerConnections) Destroy(key string) {
	deet := bc.Get(key)
	deet.Disconnect()
	bc.Delete(key)
}

func (bc *BrokerConnections) ListConnections() {
	for k, v := range bc.deets {
		log.Printf("%s::%v", k, v)
	}
}
