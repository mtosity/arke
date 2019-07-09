package provider

import (
	"sync"

	"github.com/streadway/amqp"
)

type ActiveMessages struct {
	sync.Mutex
	messages map[string]amqp.Delivery
}

func NewActiveMessages() *ActiveMessages {
	return &ActiveMessages{
		messages: map[string]amqp.Delivery{},
	}
}

func (am *ActiveMessages) Add(key string, msg amqp.Delivery) {
	am.Lock()
	defer am.Unlock()
	am.messages[key] = msg
}

func (am *ActiveMessages) Delete(key string) {
	am.Lock()
	defer am.Unlock()
	delete(am.messages, key)
}

func (am *ActiveMessages) Get(key string) amqp.Delivery {
	am.Lock()
	defer am.Unlock()
	return am.messages[key]
}
