package amqp091

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

type activeMessages struct {
	sync.Mutex
	messages map[string]amqp.Delivery
}

type brokerConnections struct {
	sync.Mutex
	deets map[string]*brokerDetails
}

type brokerDetails struct {
	connection   *amqp.Connection
	channel      *amqp.Channel
	errorChannel chan *amqp.Error
	clientUUID   string
}

type amqp091provider struct {
	provider.Provider
	connections    *brokerConnections
	activeMessages *activeMessages
}

/*
 * Broker connections map access
 */
func (bd *brokerDetails) disconnect() {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	if bd.connection != nil && !bd.connection.IsClosed() {
		bd.connection.Close()
	}
}

func (bc *brokerConnections) add(key string, bd *brokerDetails) {
	bc.Lock()
	defer bc.Unlock()
	bc.deets[key] = bd
}

func (bc *brokerConnections) delete(key string) {
	bc.Lock()
	defer bc.Unlock()
	delete(bc.deets, key)
}

func (bc *brokerConnections) get(key string) *brokerDetails {
	bc.Lock()
	defer bc.Unlock()
	return bc.deets[key]
}

func (bc *brokerConnections) destroy(key string) {
	deet := bc.get(key)
	deet.disconnect()
	bc.delete(key)
}

func newBrokerConnections() *brokerConnections {
	return &brokerConnections{
		deets: map[string]*brokerDetails{},
	}
}

func (bc *brokerConnections) listConnections() {
	for k, v := range bc.deets {
		log.Printf("%s::%v", k, v)
	}
}

/*
 * Actively consuming messages map
 */
func newActiveMessages() *activeMessages {
	return &activeMessages{
		messages: map[string]amqp.Delivery{},
	}
}

func (am *activeMessages) add(key string, msg amqp.Delivery) {
	am.Lock()
	defer am.Unlock()
	am.messages[key] = msg
}

func (am *activeMessages) delete(key string) {
	am.Lock()
	defer am.Unlock()
	delete(am.messages, key)
}

func (am *activeMessages) get(key string) amqp.Delivery {
	am.Lock()
	defer am.Unlock()
	return am.messages[key]
}

/*
 * AMQP 0-9-1 provider code
 */

// NewAMQP091Provider returns a new amqp091 provider
func NewAMQP091Provider() provider.Provider {
	connections := newBrokerConnections()
	activeMessages := newActiveMessages()
	return &amqp091provider{connections: connections, activeMessages: activeMessages}
}

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*brokerDetails, error) {
	clientUUID, err := util.GetClientUUID(ctx)
	if err != nil {
		log.Println(err.Error())
		return &brokerDetails{}, err
	}
	bd := prov.connections.get(clientUUID)
	if bd == nil {
		return &brokerDetails{}, nil
	}
	return bd, nil
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
	rm := prov.activeMessages.get(msg.GetUuid())
	err = rm.Ack(false)
	if err != nil {
		log.Println(err.Error())
		bd.errorChannel <- err.(*amqp.Error)

		prov.activeMessages.delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	prov.activeMessages.delete(msg.GetUuid())
	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration) *pb.Error {
	var conn *amqp.Connection
	var err error
	if string(cf.GetCACertificate()) != "" {
		tlsConfig := new(tls.Config)
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(cf.GetCACertificate())
		// FIXME: DO NOT DO THIS (cert in k8s deployment is not created with hostname I am using for testing)
		tlsConfig.InsecureSkipVerify = true
		connStr := fmt.Sprintf("amqps://%s:%s@%s:%d/", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort())
		conn, err = amqp.DialTLS(connStr, tlsConfig)
	} else {
		connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort())
		conn, err = amqp.Dial(connStr)
	}

	if err == nil {

		clientUUID, err := util.GetClientUUID(*ctx)
		if err != nil {
			log.Print("no client-id metadata")
			return &pb.Error{Message: err.Error()}
		}
		bd := brokerDetails{
			connection:   conn,
			errorChannel: make(chan *amqp.Error),
			clientUUID:   clientUUID,
		}
		channel, err := conn.Channel()
		if err != nil {
			// rabbitChannel.Close()
			bd.connection.Close()
			fmt.Printf("Failed to open a channel: %s", err.Error())
			return &pb.Error{Message: err.Error()}
		}
		channel.Qos(int(cf.GetPrefetchCount()), 0, true)
		bd.channel = channel
		conn.NotifyClose(bd.errorChannel)
		channel.NotifyClose(bd.errorChannel)
		prov.connections.add(bd.clientUUID, &bd)
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
	_, qErr := bd.channel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), false, false, nil)
	bErr := bd.channel.QueueBind(source.GetName(), source.GetAddress().GetSubject(), source.GetAddress().GetName(), true, nil)
	log.Printf("Error from queue create : %s", qErr)
	log.Printf("Error from bind : %s", bErr)
	log.Printf("Client subscribed : %s", source.GetName())
	messages, _ := bd.channel.Consume(
		source.GetName(), // queue name
		"",               // consumer string
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)

	forever := make(chan bool)

	go func() {
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
			prov.activeMessages.add(messageUUID, msg)
			log.Printf("Delivering %s", messageUUID)
			messageChannel <- message
		}
	}()
	<-forever
	prov.connections.destroy(bd.clientUUID)
	return nil
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx *context.Context) {
	clientUUID, err := util.GetClientUUID(*ctx)
	if err != nil {
		return
	}
	prov.connections.destroy(clientUUID)
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
	err = bd.channel.Publish(
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
		// return &pb.MessageResponse{Success: false, Error: errMsg}, err
	}
	return true, nil
}
