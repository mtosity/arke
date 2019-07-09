package provider

import (
	"log"

	"github.com/streadway/amqp"
)

type BrokerDetails struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	ErrorChannel chan *amqp.Error
	ClientUUID   string
}

func (bd *BrokerDetails) Disconnect() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("recovered: %v", err)
			return
		}
	}()
	if bd.Connection != nil && !bd.Connection.IsClosed() {
		log.Printf("Closing connection for %s", bd.ClientUUID)
		bd.Channel.Close()
		bd.Connection.Close()
	}
}
