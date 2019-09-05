package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

const providerName string = "amqp091"

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expiration"}

var supportedSourceOptions map[string]bool

type amqp091provider struct {
	provider.Provider
	connections *util.ConcurrentMap
}

type BrokerDetails struct {
	Connection     *amqp.Connection
	Channel        *amqp.Channel
	ClientUUID     string
	knownExchanges *util.ConcurrentMap
	activeMessages *util.ConcurrentMap
	prefetchCount  int
}

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAMQP091Provider)

	supportedSourceOptions = make(map[string]bool)
	for _, option := range supportedSourceOptionsList {
		supportedSourceOptions[option] = true
	}
}

/*
 * AMQP 0-9-1 provider code
 */

// NewAMQP091Provider returns a new amqp091 provider
func NewAMQP091Provider() provider.Provider {
	connections := util.NewConcurrentMap()
	prov := &amqp091provider{connections: connections}
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
		return &pb.Error{Message: err.Error()}
	}

	log.Printf("Ack message with UUID : %s", msg.GetUuid())
	if rmu, ok := bd.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Ack(false)
	} else {
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		log.Printf("Error acking message: %s", err.Error())

		bd.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	bd.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Nack ack a message
func (prov *amqp091provider) Nack(ctx *context.Context, msg *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	log.Printf("Nack message with UUID : %s", msg.GetUuid())
	if rmu, ok := bd.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Nack(false, true)
	} else {
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		log.Printf("Error nacking message: %s", err.Error())

		bd.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	bd.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	var conn *amqp.Connection
	var err error
	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	if tlsSkipVerify { // force TLS and also skip verification if true
		tlsConfig := new(tls.Config)
		tlsConfig.InsecureSkipVerify = true
		connStr := fmt.Sprintf("amqps://%s:%s@%s:%d/%s", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)
		log.Printf("Connecting to broker with URI : %s", connStr)
		conn, err = amqp.DialTLS(connStr, tlsConfig)
	} else if string(cf.GetCaCertificate()) != "" { // force verification if CA certificate is sent
		tlsConfig := new(tls.Config)
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(cf.GetCaCertificate())
		connStr := fmt.Sprintf("amqps://%s:%s@%s:%d/%s", cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)
		log.Printf("Connecting to broker with URI : %s", connStr)
		conn, err = amqp.DialTLS(connStr, tlsConfig)
	} else { // no tls
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

		knownExchanges := util.NewConcurrentMap()
		activeMessages := util.NewConcurrentMap()
		bd := BrokerDetails{
			Connection:     conn,
			ClientUUID:     clientUUID,
			knownExchanges: knownExchanges,
			prefetchCount:  int(cf.GetPrefetchCount()),
			activeMessages: activeMessages,
		}
		channel, err := conn.Channel()
		if err != nil {
			// rabbitChannel.Close()
			bd.Connection.Close()
			fmt.Printf("Failed to open a channel: %s", err.Error())
			return &pb.Error{Message: err.Error()}
		}
		channel.Qos(bd.prefetchCount, 0, true)
		bd.Channel = channel
		prov.connections.Add(bd.ClientUUID, &bd)
		log.Printf("Client %s is connected", clientUUID)
		return nil
	}

	return &pb.Error{Message: err.Error()}

}

func (prov *amqp091provider) declareExchange(address *pb.Address, bd *BrokerDetails) error {

	// don't try to declare an exchange with amq. in the name
	if strings.Contains(address.GetName(), "amq.") {
		return nil
	}

	_, ok := bd.knownExchanges.Get(address.GetName())

	if !ok {
		exchangeType := "topic"
		switch address.GetType() {
		case pb.Address_TOPIC:
			exchangeType = "topic"
		case pb.Address_FILTER:
			exchangeType = "headers"
		case pb.Address_QUEUE:
			exchangeType = "direct"
		default:
			return fmt.Errorf("%s is not a valid address type", address.GetType())
		}
		log.Printf("Declaring exchange %s", address.GetName())
		err := bd.Channel.ExchangeDeclare(address.GetName(), exchangeType, address.GetDurable(), address.GetAutoDelete(), false, false, nil)
		if err != nil {
			log.Printf("Error creating exchange: %s", err.Error())
			return err
		}
		bd.knownExchanges.Add(address.GetName(), true)
	}
	return nil
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *amqp091provider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {

	if source.GetAddress().GetName() == "" {
		return &pb.Error{Message: "address name not defined"}
	}

	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	amqpChannel, err := bd.Connection.Channel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()
	amqpChannel.Qos(bd.prefetchCount, 0, false)

	prov.declareExchange(source.GetAddress(), bd)

	log.Printf("Binding to Queue :")
	log.Printf("Queue : %s", source.GetName())
	log.Printf("Key : %s", source.GetAddress().GetSubjects())
	log.Printf("Exchange : %s", source.GetAddress().GetName())
	matchHeaders := make(amqp.Table)

	if source.GetAddress().GetType() == pb.Address_FILTER {
		matches := source.Filter.GetMatches()
		for _, match := range matches {
			log.Printf("match: %v", match)
			matchHeaders[match.GetName()] = match.GetValue()
		}

		if len(matchHeaders) > 0 {
			matchHeaders["x-match"] = "all"
			if source.Filter.GetType() == pb.Filter_ANY {
				matchHeaders["x-match"] = "any"
			}
		}
	}

	args := make(amqp.Table)
	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return &pb.Error{Message: "Value for MessageTTL option must be a quoted integer"}
			}
			args["x-message-ttl"] = val
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return &pb.Error{Message: "Value for Expires option must be a quoted integer"}
			}
			args["x-expires"] = val
		case "DeadLetterAddress":
			args["x-dead-letter-exchange"] = value
		case "DeadLetterSubject":
			args["x-dead-letter-routing-key"] = value
		default:
			return &pb.Error{Message: fmt.Sprintf("%s is an unsupported source option", option)}
		}
	}

	log.Printf("Arguments (matches): %s", matchHeaders)
	_, qErr := amqpChannel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), false, false, nil)
	log.Printf("Error from queue create : %s", qErr)
	for _, subject := range source.GetAddress().GetSubjects() {

		bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), true, matchHeaders)
		log.Printf("Error from bind : %s", bErr)
	}
	log.Printf("Client subscribed : %s", source.GetName())
	messages, err := amqpChannel.Consume(
		source.GetName(),      // queue name
		"",                    // consumer string
		false,                 // auto-ack
		source.GetExclusive(), // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)

	if err != nil {
		log.Printf("Error subscribing to queue: %v", err)
		return &pb.Error{Message: err.Error()}
	}

	for msg := range messages {
		messageUUID := util.GenUUID()
		headers := make(map[string]string)
		for header, value := range msg.Headers {
			// make everything a string
			headers[header] = fmt.Sprintf("%v", value)
		}
		if msg.ContentType != "" {
			headers["Content-Type"] = msg.ContentType
		}
		if msg.ContentEncoding != "" {
			headers["Content-Encoding"] = msg.ContentEncoding
		}
		message := &pb.Message{Uuid: messageUUID, Body: msg.Body, Headers: headers}
		bd.activeMessages.Add(messageUUID, msg)
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
			return
		}
	}()
	if bd.Connection != nil && !bd.Connection.IsClosed() {
		log.Printf("Closing connection for %s", bd.ClientUUID)
		// bd.Channel.Close()
		bd.Connection.Close()
	}
	prov.connections.Delete(clientUUID)
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) (bool, *pb.Error) {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return false, &pb.Error{Message: err.Error()}
	}
	amqpChannel, err := bd.Connection.Channel()
	if err != nil {
		return false, &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	for {
		message := <-messageChannel
		address := message.GetAddress()
		deliveryMode := 1
		if message.GetPersistent() {
			deliveryMode = 2
		}

		prov.declareExchange(message.GetAddress(), bd)

		amqpMessage := amqp.Publishing{
			Body:         message.GetBody(),
			DeliveryMode: uint8(deliveryMode),
		}
		headers := amqp.Table{}

		for headerName, headerValue := range message.GetHeaders() {
			headers[headerName] = headerValue
			switch headerName {
			case "Content-Type":
				amqpMessage.ContentType = headerValue
			case "Content-Encoding":
				amqpMessage.ContentEncoding = headerValue
			}
		}

		amqpMessage.Headers = headers

		log.Printf("Sending message to %s:%s", address.GetName(), address.GetSubjects())
		err = amqpChannel.Publish(
			address.GetName(),        // exchange
			address.GetSubjects()[0], // routing key
			false,                    // mandatory
			false,                    // immediate
			amqpMessage)

		if err != nil {
			switch err {
			case *amqp.ErrClosed:
				log.Printf("amqp closed: %s", err)
			default:
				log.Printf("default: %s", err)
			}
			log.Println("Failed to publish a message")
			log.Println(err.Error())

			errMsg := &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			errChan <- errMsg
		}
		errChan <- nil
	}

}

// SupportSourceOptions returns a map[string]bool of support options for Source.Options
func (prov *amqp091provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}
