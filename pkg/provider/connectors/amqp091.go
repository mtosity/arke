package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

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

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	Connection       *amqp.Connection
	ErrorChannel     chan *amqp.Error
	Channel          *amqp.Channel
	ClientUUID       string
	knownExchanges   *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	prefetchCount    int
	state            uint16
	connectionConfig *pb.ConnectionConfiguration
	tlsSkipVerify    bool
	ActiveStreams    int
	consumed         int
	produced         int
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
// 		util.Logger.Printf("---")
// 		util.Logger.Printf("Number of active messages: %d", len(prov.activeMessages.messages))
// 		util.Logger.Printf("Number of broker connections: %d", len(prov.connections.deets))
// 		time.Sleep(5 * time.Second)
// 	}
// }

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientUUID, err := util.GetClientUUID(ctx)
	if err != nil {
		util.Logger.ErrorI("error.noclientuuid", err.Error())
		return &BrokerDetails{}, err
	}

	if bd, ok := prov.connections.Get(clientUUID); ok {
		return bd.(*BrokerDetails), nil
	}

	return &BrokerDetails{}, fmt.Errorf("could not retrieve broker details for this connection: %s", clientUUID)
}

// Ack ack a message
func (prov *amqp091provider) Ack(ctx *context.Context, msg *pb.Message) *pb.Error {
	defer func() *pb.Error {
		if err := recover(); err != nil {
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
	if rmu, ok := bd.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Ack(false)
	} else {
		util.Logger.DebugI("debug.acknomessage", bd.ClientUUID, msg.GetUuid())
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		util.Logger.ErrorI("error.ack", err.Error())

		bd.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	util.Logger.DebugI("debug.ackmessage", bd.ClientUUID, msg.GetUuid())
	bd.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Nack ack a message
func (prov *amqp091provider) Nack(ctx *context.Context, msg *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msg.GetUuid()); ok {
		rm := rmu.(amqp.Delivery)
		err = rm.Nack(false, true)
	} else {
		util.Logger.DebugI("debug.nacknomessage", bd.ClientUUID, msg.GetUuid())
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msg.GetUuid())}
	}

	if err != nil {
		util.Logger.ErrorI("error.nack", err.Error())

		bd.activeMessages.Delete(msg.GetUuid())
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}
	util.Logger.DebugI("debug.nackmessage", bd.ClientUUID, msg.GetUuid())
	bd.activeMessages.Delete(msg.GetUuid())
	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientUUID, err := util.GetClientUUID(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	activeMessages := util.NewConcurrentMap()

	bd := BrokerDetails{
		connectionConfig: cf,
		ClientUUID:       clientUUID,
		prefetchCount:    int(cf.GetPrefetchCount()),
		ErrorChannel:     make(chan *amqp.Error),
		activeMessages:   activeMessages,
		tlsSkipVerify:    tlsSkipVerify,
		produced:         0,
		consumed:         0,
		ActiveStreams:    0,
	}

	_, bdErr := bd.connect()
	if bdErr != nil {
		util.Logger.ErrorI("error.brokerconnect", bdErr.Error())
		return &pb.Error{Message: bdErr.Error()}
	}
	prov.connections.Add(bd.ClientUUID, &bd)

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

func (bd *BrokerDetails) exchangeKnow(name string) bool {

	_, ok := bd.knownExchanges.Get(name)
	return ok
}

func (prov *amqp091provider) declareExchange(address *pb.Address, bd *BrokerDetails) error {

	// don't try to declare an exchange with amq. in the name
	if strings.Contains(address.GetName(), "amq.") {
		return nil
	}

	known := bd.exchangeKnow(address.GetName())

	if !known {

		exchangeType, err := addressTypeToAmqpType(address.GetType())

		if err != nil {
			return err
		}

		amqpChannel, err := bd.Connection.Channel()
		if err != nil {
			return err
		}
		defer amqpChannel.Close()

		util.Logger.InfoI("info.exchangedeclare", address.GetName())

		err = amqpChannel.ExchangeDeclare(address.GetName(), exchangeType, address.GetDurable(), address.GetAutoDelete(), false, false, nil)
		if err != nil {
			util.Logger.ErrorI("error.exchangedeclare", err.Error())
			return err
		}

		bd.knownExchanges.Add(address.GetName(), true)
	}

	if parent := address.GetParentAddress(); parent != nil {

		known = bd.exchangeKnow(parent.GetName())
		if !known {
			amqpChannel, err := bd.Connection.Channel()
			if err != nil {
				return err
			}
			defer amqpChannel.Close()

			err = prov.declareExchange(parent, bd)
			if err != nil {
				return err
			}

			// Bind each subject from the Address exchange to the ParentAddress exchange
			for _, subject := range address.GetSubjects() {
				util.Logger.Debugf("Binding exchange %s to %s with key %s", address.GetName(), parent.GetName(), subject)
				err = amqpChannel.ExchangeBind(address.GetName(), subject, parent.GetName(), false, nil)
				if err != nil {
					return err
				}
			}
			bd.knownExchanges.Add(parent.GetName(), true)
		}
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

	err = prov.declareExchange(source.GetAddress(), bd)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	util.Logger.InfoI("info.binding", source.GetName(), strings.Join(source.GetAddress().GetSubjects(), ","), source.GetAddress().GetName())
	matchHeaders := make(amqp.Table)

	if source.GetAddress().GetType() == pb.Address_FILTER {
		matches := source.Filter.GetMatches()
		for _, match := range matches {
			util.Logger.Debugf("match: %v", match)
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

	if len(matchHeaders) > 0 {
		util.Logger.Debugf("Arguments (matches): %s", matchHeaders)
	}

	_, qErr := amqpChannel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), false, false, args)
	if qErr != nil {
		util.Logger.ErrorI("error.queuedeclare", qErr.Error())
	}

	// If the address has subjects, bind to each subject.
	// But if the address has no subjects, bind without a subject. Don't do both.
	if len(source.GetAddress().GetSubjects()) > 0 {
		for _, subject := range source.GetAddress().GetSubjects() {

			bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), true, matchHeaders)
			if bErr != nil {
				util.Logger.ErrorI("error.queuebind", bErr.Error())
			}
		}
	} else {
		bErr := amqpChannel.QueueBind(source.GetName(), "", source.GetAddress().GetName(), true, matchHeaders)
		if bErr != nil {
			util.Logger.ErrorI("error.queuebind", bErr.Error())
		}
	}

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
		util.Logger.ErrorI("error.clientsubscribe", bd.ClientUUID, source.GetName(), err.Error())
		return &pb.Error{Message: err.Error()}
	}

	util.Logger.InfoI("info.clientsubscribe", bd.ClientUUID, source.GetName())

	connErrChan := make(chan *amqp.Error)
	bd.Connection.NotifyClose(connErrChan)

	bd.ActiveStreams++
	defer func() { bd.ActiveStreams-- }()

	for {
		select {
		case chanErr, ok := <-connErrChan:
			if !ok {
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != nil {
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				return nil
			}
		case msg := <-messages:
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
			message := &pb.Message{Uuid: messageUUID, Body: msg.Body, Headers: headers}
			bd.activeMessages.Add(messageUUID, msg)
			// util.Logger.Printf("Delivering %s", messageUUID)
			messageChannel <- message
			bd.consumed++
		}
	}
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx *context.Context) {
	clientUUID, err := util.GetClientUUID(*ctx)
	if err != nil {
		return
	}

	var bd *BrokerDetails
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
		util.Logger.InfoI("info.clientdisconnect", bd.ClientUUID)
		bd.Connection.Close()
	}
	prov.connections.Delete(clientUUID)
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	amqpChannel, err := bd.Connection.Channel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	connErrChan := make(chan *amqp.Error)
	bd.Connection.NotifyClose(connErrChan)

	bd.ActiveStreams++
	defer func() { bd.ActiveStreams-- }()

	for {
		select {
		case chanErr, ok := <-connErrChan:
			if !ok {
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != nil {
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				return nil
			}
		case message := <-messageChannel:
			address := message.GetAddress()
			deliveryMode := 1
			if message.GetPersistent() {
				deliveryMode = 2
			}

			err = prov.declareExchange(message.GetAddress(), bd)
			if err != nil {
				errChan <- &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				continue
			}

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

			// util.Logger.Printf("Sending message to %s:%s", address.GetName(), address.GetSubjects())
			err = amqpChannel.Publish(
				address.GetName(),        // exchange
				address.GetSubjects()[0], // routing key
				false,                    // mandatory
				false,                    // immediate
				amqpMessage)

			if err != nil {
				switch err {
				case *amqp.ErrClosed:
					util.Logger.Debugf("amqp closed: %s", err)
				default:
					util.Logger.Debugf("default: %s", err)
				}
				util.Logger.ErrorI("error.publish", err.Error())

				errMsg := &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				errChan <- errMsg
			} else {
				util.Logger.DebugI("debug.clientpublished", bd.ClientUUID)
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

	for start := time.Now(); time.Since(start) < CONNECT_TIMEOUT*time.Second; {
		if bd.state == CONNECTED {
			util.Logger.InfoI("client.connected", bd.ClientUUID)
			return true
		}
		bd, err = prov.getBrokerDetails(*ctx)
		if err != nil {
			util.Logger.InfoI("info.clientdetailsgone", bd.ClientUUID)
			return false
		}

		if bd.state == CLOSED {
			bd.connect()
		}

		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// connectionWatcher Called at the end of BrokerDetails.connect(), we monitor the bd.ErrorChannel and try to reconnect
// if we get an error on the channel. Receiving nil on the channel means we've closed because of the client
func (bd *BrokerDetails) connectionWatcher() {
	err := <-bd.ErrorChannel
	bd.Lock()
	if err != nil {
		bd.state = DISCONNECTED
		bd.Unlock()
		bd.connect()
		return
	}
	bd.Unlock()
}

func (bd *BrokerDetails) connect() (bool, error) {

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
	var conn *amqp.Connection
	var err error

	cf := bd.connectionConfig

	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	util.Logger.InfoI("info.clientconnect", bd.ClientUUID, cf.GetHost())

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
		util.Logger.Debugf("%s connecting with TLS enabled but verification off: %s:%d", bd.ClientUUID, cf.GetHost(), cf.GetPort())
		tlsConfig.InsecureSkipVerify = true

	} else if tlsEnabled && string(cf.GetCaCertificate()) != "" { // force verification if CA certificate is sent
		util.Logger.Debugf("%s connecting with TLS and provided certificate: %s:%d", bd.ClientUUID, cf.GetHost(), cf.GetPort())
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(cf.GetCaCertificate())

	} else if tlsEnabled { // Regular TLS with cert verification against system certs
		util.Logger.Debugf("%s connecting with TLS using system certs: %s:%d", bd.ClientUUID, cf.GetHost(), cf.GetPort())

	} else { // no tls
		util.Logger.Debugf("%s connecting without TLS: %s:%d", bd.ClientUUID, cf.GetHost(), cf.GetPort())
	}

	connStr = fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, cf.GetCredentials().GetUsername(),
		cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)
	// amqp.DialTLS will use non-TLS if the scheme of the URI is 'amqp'
	conn, err = amqp.DialTLS(connStr, tlsConfig)

	if err != nil {
		util.Logger.ErrorI("error.brokerconnect", err.Error())
		bd.state = CLOSED
		return false, err
	}

	bd.Connection = conn
	bd.ErrorChannel = make(chan *amqp.Error)
	conn.NotifyClose(bd.ErrorChannel)
	go bd.connectionWatcher()
	bd.state = CONNECTED
	bd.knownExchanges = util.NewConcurrentMap()

	util.Logger.InfoI("info.clientconnected", bd.ClientUUID)

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
		clientStat.ID = conn.ClientUUID
		clientStat.ActiveMessages = conn.activeMessages.Length()
		clientStat.Streams = conn.ActiveStreams
		clientStat.Produced = conn.produced
		clientStat.Consumed = conn.consumed
		stats.Clients = append(stats.Clients, clientStat)

	}
	return stats
}
