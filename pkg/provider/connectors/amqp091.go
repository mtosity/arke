package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

const providerName string = "amqp091"

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires"}

var supportedSourceOptions map[string]bool

// NewAmqpConn091 allow overriding the connection for mocking in tests
var NewAmqpConn091 = NewAmqp091Connection

// GetClientIdentifier Set function as a variable so we can replace the GetClientIdentifier method in unit tests
var GetClientIdentifier = util.GetClientIdentifier

type amqp091provider struct {
	provider.Provider
	connections *util.ConcurrentMap
}

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	Connection       Amqp091ConnectionShim
	ErrorChannel     chan Amqp091Error
	RetryChannel     *Amqp091ChannelShim
	ClientIdentifier string
	knownExchanges   *util.ConcurrentMap
	knownQueues      *util.ConcurrentMap
	knownBindings    *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	state            uint16
	connectionConfig *pb.ConnectionConfiguration
	tlsSkipVerify    bool
	ActiveStreams    int
	consumed         int
	produced         int
	clientDisconnect bool
	lastPubSubEvent  time.Time
}

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAMQP091Provider)

	supportedSourceOptions = make(map[string]bool)
	for _, option := range supportedSourceOptionsList {
		supportedSourceOptions[option] = true
	}
	if !strings.HasSuffix(os.Args[0], ".test") {
		go connectionCleaner()
	}
}

// every 30 seconds check the list of active connections
// if a client has 0 active streams and hasn't created or
// deleted a stream in over 30 seconds, disconnect it.
// Severed client connections may hang around for up to 60
// seconds since we are checking every 30.
func connectionCleaner() {
	provy, _ := provider.GetProvider("amqp091")
	prov := provy.(*amqp091provider)
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			for _, connId := range prov.connections.GetList() {
				if conn, ok := prov.connections.Get(connId); ok {
					bd := conn.(*BrokerDetails)
					util.Logger.Debugf("Client %v has %d open streams", connId, bd.ActiveStreams)
					lastKnown := time.Since(bd.lastPubSubEvent)
					if bd.ActiveStreams < 1 && lastKnown > 30*time.Second {
						util.Logger.Debugf("Client %v has had no streams open for %v. Assuming dead. Disconnecting.", connId, lastKnown)
						prov.disconnectClientByIdentifier(connId)
					}
				}
			}
		}
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
// 		util.Logger.Printf("---")
// 		util.Logger.Printf("Number of active messages: %d", len(prov.activeMessages.messages))
// 		util.Logger.Printf("Number of broker connections: %d", len(prov.connections.deets))
// 		time.Sleep(5 * time.Second)
// 	}
// }

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.ErrorI("error.noclientuuid", err.Error())
		return &BrokerDetails{}, err
	}

	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return bd, nil
	}

	return &BrokerDetails{}, fmt.Errorf("could not retrieve broker details for this connection: %s", clientIdentifier)
}

func (prov *amqp091provider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		return bd.(*BrokerDetails)
	}
	return nil
}

func (prov *amqp091provider) ClientExists(clientIdentifier string) bool {
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return true
	}
	return false
}

// Ack ack a message
func (prov *amqp091provider) Ack(ctx *context.Context, msgid string) *pb.Error {
	defer func() *pb.Error {
		if err := recover(); err != nil {
			debug.PrintStack()
			util.Logger.Debugf("recovered: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	// util.Logger.Printf("Ack message with UUID : %s", msg.GetUuid())
	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(Amqp091Message)
		util.Logger.Debugf("Acking message %s with tag %d", msgid, rm.DeliveryTag)
		err = rm.Ack()
	} else {
		util.Logger.DebugI("debug.acknomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.ErrorI("error.ack", err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	util.Logger.DebugI("debug.ackmessage", bd.ClientIdentifier, msgid)
	bd.activeMessages.Delete(msgid)
	return nil
}

// Nack nack a message
func (prov *amqp091provider) Nack(ctx *context.Context, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(Amqp091Message)
		err = rm.Nack(false)
	} else {
		util.Logger.DebugI("debug.nacknomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.ErrorI("error.nack", err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	util.Logger.DebugI("debug.nackmessage", bd.ClientIdentifier, msgid)
	bd.activeMessages.Delete(msgid)
	return nil
}

func (prov *amqp091provider) Retry(ctx *context.Context, origSource *pb.Source, msgid string, delay int32) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(Amqp091Message)

		// setup exchange/queue/binding
		subjects := make([]string, 0)
		subjects = append(subjects, "#")
		options := map[string]string{"MessageTTL": strconv.Itoa(int(delay) * 1000), "DeadLetterAddress": ""}
		sourceName := fmt.Sprintf("%s.retry.%ds", origSource.GetAddress().GetName(), delay)

		retrySource := &pb.Source{
			Name:    sourceName,
			Options: options,
			Address: &pb.Address{
				Subjects: subjects,
				Type:     pb.Address_TOPIC,
				Name:     sourceName,
			},
		}

		if bd.RetryChannel == nil {
			bd.Lock()
			retryChannel, err := bd.Connection.NewChannel()
			if err != nil {
				bd.Unlock()
				return &pb.Error{Message: err.Error()}
			}
			bd.RetryChannel = &retryChannel
			bd.Unlock()
		}
		amqpChannel := *bd.RetryChannel

		defer func(bd *BrokerDetails) *pb.Error {
			if err := recover(); err != nil {
				bd.Lock()
				bd.RetryChannel = nil
				bd.Unlock()
				debug.PrintStack()
				util.Logger.Debugf("recovered: %v", err)
				return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
			}
			return nil
		}(bd)

		declareErr := prov.declareExchange(retrySource.GetAddress(), bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to declare retry exchange [%s]", retrySource.GetAddress().GetName())
		}
		declareErr = prov.declareQueue(retrySource, bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to declare retry queue [%s]", retrySource.GetName())
		}
		declareErr = prov.declareBinding(retrySource, bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to bind retry queue [%s] to exchange [%s]", retrySource.GetName(), retrySource.GetAddress().GetName())
		}

		retryErr := amqpChannel.Publish(retrySource.Address.GetName(), origSource.GetName(), rm)
		if retryErr != nil {
			util.Logger.Debugf("Failed to publish retry message [%s], requeueing instead.", msgid)
			_ = rm.Nack(true)
		} else {
			_ = rm.Nack(false)
		}
		util.Logger.DebugI("debug.retrymessage", bd.ClientIdentifier, msgid, delay)
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.DebugI("debug.retrynomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientIdentifier, err := GetClientIdentifier(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	activeMessages := util.NewConcurrentMap()

	bd := BrokerDetails{
		connectionConfig: cf,
		ClientIdentifier: clientIdentifier,
		ErrorChannel:     make(chan Amqp091Error),
		activeMessages:   activeMessages,
		tlsSkipVerify:    tlsSkipVerify,
		produced:         0,
		consumed:         0,
		ActiveStreams:    0,
		clientDisconnect: false,
	}

	_, bdErr := bd.connect()
	if bdErr != nil {
		util.Logger.ErrorI("error.brokerconnect", bdErr.Error())
		return &pb.Error{Message: bdErr.Error()}
	}
	prov.connections.Add(bd.ClientIdentifier, &bd)

	return nil

}

func addressTypeToAmqpType(aType pb.Address_TargetType) (string, error) {

	exchangeType := "topic"
	switch aType {
	case pb.Address_TOPIC:
		exchangeType = "topic"
	case pb.Address_FILTER:
		exchangeType = "headers"
	case pb.Address_QUEUE:
		exchangeType = "direct"
	default:
		util.Logger.ErrorI("error.addresstype", aType.String())
		return "", fmt.Errorf("%s is not a valid address type", aType)
	}
	return exchangeType, nil
}

func (bd *BrokerDetails) exchangeKnown(name string) bool {

	_, ok := bd.knownExchanges.Get(name)
	return ok
}

func (bd *BrokerDetails) queueKnown(name string) bool {

	_, ok := bd.knownQueues.Get(name)
	return ok
}

func (bd *BrokerDetails) bindingKnown(name string) bool {

	_, ok := bd.knownBindings.Get(name)
	return ok
}

func (bd *BrokerDetails) updateLastPubSubEvent() {
	bd.lastPubSubEvent = time.Now()
}

func (bd *BrokerDetails) incrementStreamCount() {
	bd.ActiveStreams++
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) decrementStreamCount() {
	bd.ActiveStreams--
	bd.updateLastPubSubEvent()
}

func (prov *amqp091provider) declareExchange(address *pb.Address, bd *BrokerDetails, amqpChannel Amqp091ChannelShim, force bool) error {

	// don't try to declare an exchange with amq. in the name
	if strings.Contains(address.GetName(), "amq.") {
		return nil
	}

	known := bd.exchangeKnown(address.GetName())

	if !known || force {

		exchangeType, err := addressTypeToAmqpType(address.GetType())

		if err != nil {
			return err
		}
		util.Logger.InfoI("info.exchangedeclare", address.GetName())

		err = amqpChannel.ExchangeDeclare(address.GetName(), exchangeType, address.GetDurable(), address.GetAutoDelete())
		if err != nil {
			util.Logger.ErrorI("error.exchangedeclare", err.Error())
			return err
		}

		bd.knownExchanges.Add(address.GetName(), true)
	}

	if parent := address.GetParentAddress(); parent != nil {

		known = bd.exchangeKnown(parent.GetName())
		if !known || force {
			err := prov.declareExchange(parent, bd, amqpChannel, force)
			if err != nil {
				util.Logger.ErrorI("error.exchangedeclare", err.Error())
			}

			// Bind each subject from the Address exchange to the ParentAddress exchange
			for _, subject := range address.GetSubjects() {
				util.Logger.InfoI("info.exchangebind", address.GetName(), parent.GetName(), subject)
				err = amqpChannel.ExchangeBind(address.GetName(), subject, parent.GetName())
				if err != nil {
					return err
				}
			}
			bd.knownExchanges.Add(parent.GetName(), true)
		}
	}
	return nil
}

func (prov *amqp091provider) declareQueue(source *pb.Source, bd *BrokerDetails, amqpChannel Amqp091ChannelShim, force bool) error {
	known := bd.queueKnown(source.GetName())
	if known && !force {
		return nil
	}

	args := make(Amqp091Table)
	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("Value for MessageTTL option must be a quoted integer")
			}
			args["x-message-ttl"] = val
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("Value for Expires option must be a quoted integer")
			}
			args["x-expires"] = val
		case "DeadLetterAddress":
			args["x-dead-letter-exchange"] = value
		case "DeadLetterSubject":
			args["x-dead-letter-routing-key"] = value
		default:
			return fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	qErr := amqpChannel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), source.GetExclusive(), args)
	if qErr != nil {
		util.Logger.ErrorI("error.queuedeclare", qErr.Error())
	}
	bd.knownQueues.Add(source.GetName(), true)

	return nil
}

func (prov *amqp091provider) declareBinding(source *pb.Source, bd *BrokerDetails, amqpChannel Amqp091ChannelShim, force bool) error {
	knownBindingKey := fmt.Sprintf("%s:%s", source.GetName(), strings.Join(source.Address.GetSubjects(), ":"))
	known := bd.bindingKnown(knownBindingKey)
	if known && !force {
		return nil
	}

	// If the address has subjects, bind to each subject.
	// But if the address has no subjects, bind without a subject. Don't do both.
	util.Logger.InfoI("info.binding", source.GetName(), strings.Join(source.GetAddress().GetSubjects(), ","), source.GetAddress().GetName())

	matchHeadersList := make([]Amqp091Table, 0)

	if source.GetAddress().GetType() == pb.Address_FILTER {
		for _, filter := range source.GetFilters() {
			matchHeaders := make(Amqp091Table)
			matches := filter.GetMatches()
			for _, match := range matches {
				util.Logger.Debugf("match: %v", match)
				matchHeaders[match.GetName()] = match.GetValue()
			}

			if len(matchHeaders) > 0 {
				matchHeaders["x-match"] = "all"
				if filter.GetType() == pb.Filter_ANY {
					matchHeaders["x-match"] = "any"
				}
			}

			if len(matchHeaders) > 0 {
				util.Logger.Debugf("Arguments (matches): %s", matchHeaders)
			}

			matchHeadersList = append(matchHeadersList, matchHeaders)
		}
	}

	subjects := source.GetAddress().GetSubjects()
	if len(subjects) == 0 {
		// If subjects aren't included in the address, fake an empty one so
		// we ensure we bind even if there are no filters
		subjects = append(subjects, "")
	}

	for _, subject := range subjects {
		if len(matchHeadersList) > 0 {
			for _, matchHeaders := range matchHeadersList {
				bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), matchHeaders)
				if bErr != nil {
					util.Logger.ErrorI("error.queuebind", bErr.Error())
				}
			}
		} else {
			bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), nil)
			if bErr != nil {
				util.Logger.ErrorI("error.queuebind", bErr.Error())
			}
		}
	}
	bd.knownBindings.Add(knownBindingKey, true)
	return nil
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *amqp091provider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message, stopChannel <-chan bool) *pb.Error {

	if source.GetAddress().GetName() == "" {
		return &pb.Error{Message: "address name not defined"}
	}

	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()

	amqpChannel, err := bd.Connection.NewChannel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	if source.GetPrefetchCount() > 0 {
		amqpChannel.SetPrefetch(int(source.GetPrefetchCount()))
	}

	err = prov.declareExchange(source.GetAddress(), bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	err = prov.declareQueue(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	err = prov.declareBinding(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	messages, err := amqpChannel.Consume(
		source.GetName(),      // queue name
		false,                 // auto-ack
		source.GetExclusive(), // exclusive
	)

	if err != nil {
		util.Logger.ErrorI("error.clientsubscribe", bd.ClientIdentifier, source.GetName(), err.Error())
		return &pb.Error{Message: err.Error()}
	}

	util.Logger.InfoI("info.clientsubscribe", bd.ClientIdentifier, source.GetName())

	connErrChan := make(chan Amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)
	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- NewAmqp091Error("Subscribe done", 2001):
			return
		default:
			return

		}
	}()

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	defer func() *pb.Error {
		if err := recover(); err != nil {
			debug.PrintStack()
			util.Logger.Debugf("recovered: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	for {
		select {
		case stop, ok := <-stopChannel:
			if !ok || stop {
				// channel is closed, so stop
				return nil
			}
		case chanErr, ok := <-connErrChan:
			if !ok {
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if &chanErr != nil {
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				return nil
			}
		case msg, ok := <-messages:
			if !ok {
				// Message channel closed
				return nil
			}
			// Sometimes we get a message with a DeliveryTag == 0, which is bad and I'm not sure
			// how this actually happens
			if msg.DeliveryTag == 0 {
				continue
			}
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
			message := &pb.Message{Uuid: messageUUID, Body: msg.Body, Headers: headers, Address: source.GetAddress()}
			bd.activeMessages.Add(messageUUID, msg)
			messageChannel <- message
			bd.consumed++
		}
	}
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx *context.Context) {
	clientIdentifier, err := GetClientIdentifier(*ctx)
	if err != nil {
		return
	}

	prov.disconnectClientByIdentifier(clientIdentifier)
}

func (prov *amqp091provider) disconnectClientByIdentifier(clientIdentifier string) {

	var bd *BrokerDetails
	if bdu, ok := prov.connections.Get(clientIdentifier); ok {
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
		util.Logger.InfoI("info.clientdisconnect", bd.ClientIdentifier)
		bd.clientDisconnect = true
		bd.Connection.Close()
	}
	prov.connections.Delete(clientIdentifier)
	bd = nil
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()

	amqpChannel, err := bd.Connection.NewChannel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	connErrChan := make(chan Amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)

	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- NewAmqp091Error("Publish done", 2002):
			return
		default:
			return

		}
	}()

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	for {
		select {
		case chanErr, ok := <-connErrChan:
			if !ok {
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if &chanErr != nil {
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				return nil
			}
		case message := <-messageChannel:
			if message == nil {
				// nil message means shut it down
				return nil
			}
			address := message.GetAddress()
			deliveryMode := 1
			if message.GetPersistent() {
				deliveryMode = 2
			}

			err = prov.declareExchange(message.GetAddress(), bd, amqpChannel, false)
			if err != nil {
				errChan <- &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				continue
			}

			amqpMessage := Amqp091Message{}
			amqpMessage.Body = message.GetBody()
			amqpMessage.DeliveryMode = deliveryMode

			headers := Amqp091Table{}

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

			// util.Logger.Printf("Sending message to %s:%s", address.GetName(), address.GetSubjects())
			err = amqpChannel.Publish(
				address.GetName(),        // exchange
				address.GetSubjects()[0], // routing key
				amqpMessage)

			if err != nil {
				util.Logger.ErrorI("error.publish", err.Error())

				errMsg := &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				errChan <- errMsg
			} else {
				util.Logger.DebugI("debug.clientpublished", bd.ClientIdentifier)
				bd.produced++
			}
			errChan <- nil
		}
	}

}

// SupportSourceOptions returns a map[string]bool of support options for Source.Options
func (prov *amqp091provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

// WaitForConnect returns true if connected, false if connection fails
func (prov *amqp091provider) WaitForConnect(ctx *context.Context) bool {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		util.Logger.Debugf("Could not retrieve broker details in WaitForConnect")
		return false
	}

	reconnect := false

	for start := time.Now(); time.Since(start) < CONNECT_TIMEOUT*time.Second; {
		if bd.state == CONNECTED {
			util.Logger.InfoI("info.clientconnected", bd.ClientIdentifier)
			return true
		}
		bd, err = prov.getBrokerDetails(*ctx)
		if err != nil {
			util.Logger.InfoI("info.clientdetailsgone", bd.ClientIdentifier)
			return false
		}

		if bd.state == CLOSED {
			bd.connect()
		}

		if reconnect {
			sleepRandomReconnect()
		}

		reconnect = true

	}
	return false
}

func sleepRandomReconnect() {

	rand.Seed(time.Now().UnixNano())
	splay := time.Duration(rand.Intn(ReconnectDelay-100)+100) * time.Millisecond
	time.Sleep(splay)
}

// connectionWatcher Called at the end of BrokerDetails.connect(), we monitor the bd.ErrorChannel and try to reconnect
// if we get an error on the channel. Receiving nil on the channel means we've closed because of the client
func (bd *BrokerDetails) connectionWatcher() {

	err, ok := <-bd.ErrorChannel

	bd.Lock()
	if !ok || (&err != nil && err.Code() != 0) {
		bd.state = DISCONNECTED
		sleepRandomReconnect()
		bd.Unlock()
		bd.connect()
		return
	}
	bd.Unlock()
}

func (bd *BrokerDetails) connect() (bool, error) {

	if bd.clientDisconnect {
		return false, nil
	}

	if bd.state == CONNECTING {
		for start := time.Now(); time.Since(start) < 30*time.Second; {
			switch bd.state {
			case CONNECTED:
				return true, nil
			case CONNECTING:
				time.Sleep(100 * time.Millisecond)
				continue
			case CLOSED:
				return false, nil
			case DISCONNECTED:
				break
			}
		}
	}

	bd.Lock()
	defer bd.Unlock()
	if bd.state == CONNECTED {
		return true, nil
	}

	bd.state = CONNECTING
	var conn Amqp091ConnectionShim
	var err error

	cf := bd.connectionConfig

	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	util.Logger.InfoI("info.clientconnect", bd.ClientIdentifier, cf.GetHost())

	tlsEnabled := false
	scheme := "amqp"

	// Use TLS in these scenarios:
	// * ConnectionConfiguration.TLS = true
	// * ConnectionConfiguration.CaCertificate is not empty
	if cf.GetTls() || len(cf.GetCaCertificate()) > 0 {
		tlsEnabled = true
		scheme = "amqps"
	}

	var connStr string
	var tlsConfig = &tls.Config{}

	if tlsEnabled && bd.tlsSkipVerify { // force TLS and also skip verification if true
		util.Logger.Debugf("%s connecting with TLS enabled but verification off: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
		tlsConfig.InsecureSkipVerify = true

	} else if tlsEnabled && string(cf.GetCaCertificate()) != "" { // force verification if CA certificate is sent
		util.Logger.Debugf("%s connecting with TLS and provided certificate: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(cf.GetCaCertificate())

	} else if tlsEnabled { // Regular TLS with cert verification against system certs
		if caBundlePath := os.Getenv("CA_BUNDLE"); caBundlePath != "" {
			caBundle, err := ioutil.ReadFile(caBundlePath)
			if err != nil {
				return false, fmt.Errorf("Could not read CA_BUNDLE %s: %s", caBundlePath, err.Error())
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
		}
		util.Logger.Debugf("%s connecting with TLS using system certs: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())

	} else { // no tls
		util.Logger.Debugf("%s connecting without TLS: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
	}

	connStr = fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, cf.GetCredentials().GetUsername(),
		cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)

	conn = NewAmqpConn091(connStr, bd.ClientIdentifier, tlsConfig)
	err = conn.Connect()

	if err != nil {
		util.Logger.ErrorI("error.brokerconnect", err.Error())
		bd.state = CLOSED
		return false, err
	}

	bd.Connection = conn
	bd.ErrorChannel = make(chan Amqp091Error)
	bd.ErrorChannel = bd.Connection.NotifyClose(bd.ErrorChannel) // this looks unneeded but it aids in unit testing
	go bd.connectionWatcher()
	bd.state = CONNECTED
	bd.knownExchanges = util.NewConcurrentMap()
	bd.knownQueues = util.NewConcurrentMap()
	bd.knownBindings = util.NewConcurrentMap()

	util.Logger.InfoI("info.clientconnected", bd.ClientIdentifier)

	return true, nil

}

func (prov *amqp091provider) Stats() *provider.Stats {

	stats := &provider.Stats{}
	stats.Clients = make([]*provider.ClientStats, 0)
	for _, connID := range prov.connections.GetList() {
		clientStat := &provider.ClientStats{}
		connRaw, exists := prov.connections.Get(connID)
		if !exists {
			continue
		}
		conn := connRaw.(*BrokerDetails)
		clientStat.ID = conn.ClientIdentifier
		clientStat.ActiveMessages = conn.activeMessages.Length()
		clientStat.Streams = conn.ActiveStreams
		clientStat.Produced = conn.produced
		clientStat.Consumed = conn.consumed
		stats.Clients = append(stats.Clients, clientStat)

	}
	return stats
}
