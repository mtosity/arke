package amqp091

import (
	"crypto/tls"
	"time"

	"github.com/streadway/amqp"
)

// amqp091ConnectionShim Shim so we can do unit testing
type amqp091ConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	NewChannel() (amqp091ChannelShim, error)
	NotifyClose(chan amqp091Error) chan amqp091Error
}

// amqp091ChannelShim Shim so we can do unit testing
type amqp091ChannelShim interface {
	Close() error
	Publish(string, string, amqp091Message) error
	ExchangeDeclare(string, string, bool, bool) error
	ExchangeBind(string, string, string) error
	SetPrefetch(int) error
	QueueDeclare(string, bool, bool, bool, amqp091Table) error
	QueueBind(string, string, string, amqp091Table) error
	Consume(string, bool, bool) (<-chan amqp091Message, error)
	NotifyCancel(chan string) chan string
}

// amqp091Connection A connection to the broker
type amqp091Connection struct {
	amqp091ConnectionShim
	connection       *amqp.Connection
	connStr          string
	tlsCfg           *tls.Config
	clientIdentifier string
}

// amqp091Channel A channel
type amqp091Channel struct {
	amqp091ChannelShim
	channel *amqp.Channel
}

// amqp091Error Error
type amqp091Error struct {
	error amqp.Error
}

// amqp091Message Strucutre of a message
type amqp091Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         amqp091Table
	DeliveryTag     uint64
	ClientSentTime  time.Time
}

// amqp091Table Simple map
type amqp091Table map[string]interface{}

// NewAmqp091Connection Create a new Amqp091Connection object with a connection string and tls config
func NewAmqp091Connection(connStr string, clientIdentifier string, tlsCfg *tls.Config) amqp091ConnectionShim {
	return &amqp091Connection{connStr: connStr, tlsCfg: tlsCfg, clientIdentifier: clientIdentifier}
}

// Connect Connect to the broker
func (ac *amqp091Connection) Connect() error {
	properties := make(amqp.Table)
	properties["connection_name"] = ac.clientIdentifier
	cfg := amqp.Config{
		TLSClientConfig: ac.tlsCfg,
		Heartbeat:       10 * time.Second,
		Locale:          "en_US",
		Properties:      properties,
	}
	conn, err := amqp.DialConfig(ac.connStr, cfg)
	if err != nil {
		return err
	}
	ac.connection = conn
	return nil
}

// NewChannel Create a new channel on the connection
func (ac *amqp091Connection) NewChannel() (amqp091ChannelShim, error) {
	ch, err := ac.connection.Channel()
	if err != nil {
		return nil, err
	}
	ach := &amqp091Channel{channel: ch}
	return ach, nil
}

// NotifyClose Channel to receive connection close notifications on
func (ac *amqp091Connection) NotifyClose(rec chan amqp091Error) chan amqp091Error {
	amqpErrors := ac.connection.NotifyClose(make(chan *amqp.Error, cap(rec)))

	go func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		for {
			select {
			case amqpErr := <-amqpErrors:
				var err amqp091Error
				if amqpErr != nil {
					err = amqp091Error{*amqpErr}
				}
				select {
				case rec <- err:
					continue
				default:
					return
				}
			case <-rec:
				// this should theoretically happen only if the subscribe function
				// sends a message on the rec channel while we are waiting
				// for actual errors from amqpErrors
				return
			}
		}
	}()
	return rec
}

// Close Close the connection to the broker
func (ac *amqp091Connection) Close() error {
	return ac.connection.Close()
}

// IsClosed Is the connection to the broker still open
func (ac *amqp091Connection) IsClosed() bool {
	return ac.connection.IsClosed()
}

// Close Close the channel
func (ch *amqp091Channel) Close() error {
	return ch.channel.Close()
}

// ExchangeDeclare Declare a new exchange
func (ch *amqp091Channel) ExchangeDeclare(addressName, exchangeType string, durable, autoDelete bool) error {

	return ch.channel.ExchangeDeclare(addressName, exchangeType, durable, autoDelete, false, false, nil)
}

// ExchangeBind Bind an exchange to another exchange
func (ch *amqp091Channel) ExchangeBind(addressName, subject, parentName string) error {
	return ch.channel.ExchangeBind(addressName, subject, parentName, false, nil)
}

// QueueDeclare Create a queue
func (ch *amqp091Channel) QueueDeclare(name string, durable, autoDelete, exclusive bool, args amqp091Table) error {
	_, err := ch.channel.QueueDeclare(name, durable, autoDelete, exclusive, false, toAmqpTable(args))
	return err
}

// SetPrefetch Sets quality of service on the channel
func (ch *amqp091Channel) SetPrefetch(prefetchCount int) error {
	if prefetchCount > 0 {
		return ch.channel.Qos(prefetchCount, 0, false)
	}
	return nil
}

// QueueBind Binds an queue to an exchange with subject/arguments
func (ch *amqp091Channel) QueueBind(name, subject, destination string, args amqp091Table) error {
	// true
	return ch.channel.QueueBind(name, subject, destination, true, toAmqpTable(args))
}

// Consume Consume messages from a queue
func (ch *amqp091Channel) Consume(subject string, autoAck, exclusive bool) (<-chan amqp091Message, error) {
	delChan, err := ch.channel.Consume(subject, "", autoAck, exclusive, false, false, nil)

	if err != nil {
		return nil, err
	}

	msgChan := make(chan amqp091Message)

	go func() {
		for del := range delChan {
			msgChan <- fromAmqpMessage(del)
		}
	}()
	return msgChan, nil
}

// Publish Publish a message to an exchange
func (ch *amqp091Channel) Publish(addressName, subject string, msg amqp091Message) error {
	return ch.channel.Publish(addressName, subject, false, false, toAmqpMessage(&msg))
}

// NotifyCancel be notified of deleted queues
func (ch *amqp091Channel) NotifyCancel(rec chan string) chan string {
	amqpErrors := ch.channel.NotifyCancel(make(chan string, cap(rec)))

	go func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		for {
			select {
			case amqpErr := <-amqpErrors:
				select {
				case rec <- amqpErr:
					continue
				default:
					return
				}
			case <-rec:
				// this should theoretically happen only if the subscribe function
				// sends a message on the rec channel while we are waiting
				return
			}
		}
	}()
	return rec
}

func toAmqpTable(at amqp091Table) amqp.Table {
	table := make(amqp.Table)
	for key, val := range at {
		table[key] = val
	}
	return table
}

func fromAmqpTable(tab amqp.Table) amqp091Table {
	table := make(amqp091Table)
	for key, val := range tab {
		table[key] = val
	}
	return table
}

func toAmqpMessage(msg *amqp091Message) amqp.Publishing {
	pub := amqp.Publishing{}
	pub.Body = msg.Body
	pub.DeliveryMode = uint8(msg.DeliveryMode)
	pub.Headers = toAmqpTable(msg.Headers)
	pub.ContentType = msg.ContentType
	pub.ContentEncoding = msg.ContentEncoding
	pub.Timestamp = time.Now()
	return pub
}

func fromAmqpMessage(del amqp.Delivery) amqp091Message {
	msg := amqp091Message{}
	msg.SetDelivery(del)
	msg.Body = del.Body
	msg.DeliveryMode = int(del.DeliveryMode)
	msg.Headers = fromAmqpTable(del.Headers)
	msg.ContentType = del.ContentType
	msg.ContentEncoding = del.ContentEncoding
	msg.DeliveryTag = del.DeliveryTag
	return msg
}

// Ack Ack a message
func (msg *amqp091Message) Ack() error {
	// For unit testing
	switch msg.delivery.(type) {
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Ack(false)
	}
	return nil
}

// Nack Nack a message
func (msg *amqp091Message) Nack(requeue bool) error {
	// For unit testing
	switch msg.delivery.(type) {
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Nack(false, requeue)
	}
	return nil
}

// Error Error
func (e *amqp091Error) Error() string {
	return e.error.Error()
}

// Code Error code from amqp.Error
func (e *amqp091Error) Code() int {
	return e.error.Code
}

// newAmqp091Error New error
func newAmqp091Error(e string, code int) amqp091Error {
	err := amqp091Error{}
	err.error = amqp.Error{Reason: e, Code: code}
	return err
}

// SetDelivery convenience method for unit tests
func (msg *amqp091Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
