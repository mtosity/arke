package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

const providerName string = "amqp091"

type amqp091provider struct {
	provider.Provider
	connections    *util.ConcurrentMap
	activeMessages *util.ConcurrentMap
}

type BrokerDetails struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	ErrorChannel chan *amqp.Error
	ClientUUID   string
}

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAMQP091Provider)
}

/*
 * AMQP 0-9-1 provider code
 */

// NewAMQP091Provider returns a new amqp091 provider
func NewAMQP091Provider() provider.Provider {
	connections := util.NewConcurrentMap()
	activeMessages := util.NewConcurrentMap()
	prov := &amqp091provider{connections: connections, activeMessages: activeMessages}
	// go prov.monitor()
	return prov
}

// func (prov *amqp091provider) monitor() {
// 	for {
// 		log.Printf("---")
// 		log.Printf("Number of active messages: %d", len(prov.activeMessages.messages))
// 		log.Printf("Number of broker connections: %d", len(prov.connections.deets))
// 		time.Sleep(5 * time.Second)
// 	}
// }

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientUUID, err := util.GetClientUUID(ctx)
	if err != nil {
		log.Println(err.Error())
		return &BrokerDetails{}, err
	}

	if bd, ok := prov.connections.Get(clientUUID); ok {
		return bd.(*BrokerDetails), nil
	}

	return &BrokerDetails{}, errors.New("could not retrieve broker details for this connection")
}

// Ack ack a message
func (prov *amqp091provider) Ack(ctx *context.Context, msg *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	log.Printf("Ack message with UUID : %s", msg.GetUuid())
	if rmu, ok := prov.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Ack(false)
	} else {
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		log.Println(err.Error())
		bd.ErrorChannel <- err.(*amqp.Error)

		prov.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	prov.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Nack ack a message
func (prov *amqp091provider) Nack(ctx *context.Context, msg *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	log.Printf("Nack message with UUID : %s", msg.GetUuid())

	if rmu, ok := prov.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Nack(false, true)
	} else {
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		log.Println(err.Error())
		bd.ErrorChannel <- err.(*amqp.Error)

		prov.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	prov.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration) *pb.Error {
	var conn *amqp.Connection
	var err error
	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}
	if string(cf.GetCaCertificate()) != "" {
		tlsConfig := new(tls.Config)
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(cf.GetCaCertificate())
		// FIXME: DO NOT DO THIS (cert in k8s deployment is not created with hostname I am using for testing)
		tlsConfig.InsecureSkipVerify = true
		connStr := fmt.Sprintf("amqps://%s:%s@%s:%d/%s", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)
		log.Printf("Connecting to broker with URI : %s", connStr)
		conn, err = amqp.DialTLS(connStr, tlsConfig)
	} else {
		connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)
		log.Printf("Connecting to broker with URI : %s", connStr)
		conn, err = amqp.Dial(connStr)
	}

	if err == nil {

		clientUUID, err := util.GetClientUUID(*ctx)
		if err != nil {
			log.Print("no client-id metadata")
			return &pb.Error{Message: err.Error()}
		}
		bd := BrokerDetails{
			Connection:   conn,
			ErrorChannel: make(chan *amqp.Error),
			ClientUUID:   clientUUID,
		}
		channel, err := conn.Channel()
		if err != nil {
			// rabbitChannel.Close()
			bd.Connection.Close()
			fmt.Printf("Failed to open a channel: %s", err.Error())
			return &pb.Error{Message: err.Error()}
		}
		channel.Qos(int(cf.GetPrefetchCount()), 0, true)
		bd.Channel = channel
		conn.NotifyClose(bd.ErrorChannel)
		channel.NotifyClose(bd.ErrorChannel)
		prov.connections.Add(bd.ClientUUID, &bd)
		log.Printf("Client %s is connected", clientUUID)
		return nil
	}

	return &pb.Error{Message: err.Error()}

}

// Subscribe subscribe to a stream of messages from the broker
func (prov *amqp091provider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	log.Printf("Binding to Queue :")
	log.Printf("Queue : %s", source.GetName())
	log.Printf("Key : %s", source.GetAddress().GetSubject())
	log.Printf("Exchange : %s", source.GetAddress().GetName())
	_, qErr := bd.Channel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), false, false, nil)
	bErr := bd.Channel.QueueBind(source.GetName(), source.GetAddress().GetSubject(), source.GetAddress().GetName(), true, nil)
	log.Printf("Error from queue create : %s", qErr)
	log.Printf("Error from bind : %s", bErr)
	log.Printf("Client subscribed : %s", source.GetName())
	messages, _ := bd.Channel.Consume(
		source.GetName(), // queue name
		"",               // consumer string
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)

	for msg := range messages {
		var messageUUID string
		msgUUID := msg.Headers["MessageUUID"]
		if msgUUID == nil {
			log.Printf("Invalid message, no MessageUUID header: %s", msg.Body)
			messageUUID = util.GenUUID()
		} else {
			messageUUID = msgUUID.(string)
		}
		message := &pb.Message{Uuid: messageUUID, Body: msg.Body}
		prov.activeMessages.Add(messageUUID, msg)
		log.Printf("Delivering %s", messageUUID)
		messageChannel <- message
	}
	return nil
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx *context.Context) {
	clientUUID, err := util.GetClientUUID(*ctx)
	if err != nil {
		return
	}
	var bd *BrokerDetails
	// := prov.connections.Get(clientUUID).(*BrokerDetails)
	if bdu, ok := prov.connections.Get(clientUUID); ok {
		bd = bdu.(*BrokerDetails)
	} else {
		return
	}

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
	prov.connections.Delete(clientUUID)
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx *context.Context, message *pb.Message) (bool, *pb.Error) {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return false, &pb.Error{Message: err.Error()}
	}
	address := message.GetAddress()
	deliveryMode := 1
	if message.GetPersistent() {
		deliveryMode = 2
	}

	messageUUID := util.GenUUID()
	message.Uuid = messageUUID

	log.Printf("Sending message to %s:%s", address.GetName(), address.GetSubject())
	err = bd.Channel.Publish(
		address.GetName(),    // exchange
		address.GetSubject(), // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         message.GetBody(),
			DeliveryMode: uint8(deliveryMode),
			Headers: amqp.Table{
				"MessageUUID": messageUUID,
			},
		})

	if err != nil {
		switch err {
		case *amqp.ErrClosed:
			log.Printf("amqp closed: %s", err)
		default:
			log.Printf("default: %s", err)
		}
		log.Printf("Failed to publish a message %s", messageUUID)
		log.Println(err.Error())

		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return false, errMsg
	}
	return true, nil
}
