package connectors

import (
	"crypto/tls"

	"github.com/streadway/amqp"
)

// Amqp091ConnectionShim Shim so we can do unit testing
type Amqp091ConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	NewChannel() (Amqp091ChannelShim, error)
	NotifyClose(chan Amqp091Error) chan Amqp091Error
}

// Amqp091ChannelShim Shim so we can do unit testing
type Amqp091ChannelShim interface {
	Close() error
	Publish(string, string, Amqp091Message) error
	ExchangeDeclare(string, string, bool, bool) error
	ExchangeBind(string, string, string) error
	SetPrefetch(int) error
	QueueDeclare(string, bool, bool, bool, Amqp091Table) error
	QueueBind(string, string, string, Amqp091Table) error
	Consume(string, bool, bool) (<-chan Amqp091Message, error)
}

// Amqp091Connection A connection to the broker
type Amqp091Connection struct {
	Amqp091ConnectionShim
	connection *amqp.Connection
	connStr    string
	tlsCfg     *tls.Config
}

// Amqp091Channel A channel
type Amqp091Channel struct {
	Amqp091ChannelShim
	channel *amqp.Channel
}

// Amqp091Error Error
type Amqp091Error struct {
	error amqp.Error
}

// Amqp091Message Strucutre of a message
type Amqp091Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         Amqp091Table
	DeliveryTag     uint64
}

// Amqp091Table Simple map
type Amqp091Table map[string]interface{}

// NewAmqp091Connection Create a new Amqp091Connection object with a connection string and tls config
func NewAmqp091Connection(connStr string, tlsCfg *tls.Config) Amqp091ConnectionShim {
	return &Amqp091Connection{connStr: connStr, tlsCfg: tlsCfg}
}

// Connect Connect to the broker
func (ac *Amqp091Connection) Connect() error {
	conn, err := amqp.DialTLS(ac.connStr, ac.tlsCfg)
	if err != nil {
		return err
	}
	ac.connection = conn
	return nil
}

// NewChannel Create a new channel on the connection
func (ac *Amqp091Connection) NewChannel() (Amqp091ChannelShim, error) {
	ch, err := ac.connection.Channel()
	if err != nil {
		return nil, err
	}
	ach := &Amqp091Channel{channel: ch}
	return ach, nil
}

// NotifyClose Channel to receive connection close notifications on
func (ac *Amqp091Connection) NotifyClose(rec chan Amqp091Error) chan Amqp091Error {
	amqpErrors := ac.connection.NotifyClose(make(chan *amqp.Error, cap(rec)))

	go func() {
		for amqpErr := range amqpErrors {
			var err Amqp091Error
			if amqpErr != nil {
				err = Amqp091Error{*amqpErr}
			} else {
			}
			rec <- err
		}
		close(rec)
	}()
	return rec
}

// Close Close the connection to the broker
func (ac *Amqp091Connection) Close() error {
	return ac.connection.Close()
}

// IsClosed Is the connection to the broker still open
func (ac *Amqp091Connection) IsClosed() bool {
	return ac.connection.IsClosed()
}

// Close Close the channel
func (ch *Amqp091Channel) Close() error {
	return ch.channel.Close()
}

// ExchangeDeclare Declare a new exchange
func (ch *Amqp091Channel) ExchangeDeclare(addressName, exchangeType string, durable, autoDelete bool) error {

	return ch.channel.ExchangeDeclare(addressName, exchangeType, durable, autoDelete, false, false, nil)
}

// ExchangeBind Bind an exchange to another exchange
func (ch *Amqp091Channel) ExchangeBind(addressName, subject, parentName string) error {
	return ch.channel.ExchangeBind(addressName, subject, parentName, false, nil)
}

// QueueDeclare Create a queue
func (ch *Amqp091Channel) QueueDeclare(name string, durable, autoDelete, exclusive bool, args Amqp091Table) error {
	_, err := ch.channel.QueueDeclare(name, durable, autoDelete, exclusive, false, toAmqpTable(args))
	return err
}

// SetPrefetch Sets quality of service on the channel
func (ch *Amqp091Channel) SetPrefetch(prefetchCount int) error {
	if prefetchCount > 0 {
		return ch.channel.Qos(prefetchCount, 0, false)
	}
	return nil
}

// QueueBind Binds an queue to an exchange with subject/arguments
func (ch *Amqp091Channel) QueueBind(name, subject, destination string, args Amqp091Table) error {
	// true
	return ch.channel.QueueBind(name, subject, destination, true, toAmqpTable(args))
}

// Consume Consume messages from a queue
func (ch *Amqp091Channel) Consume(subject string, autoAck, exclusive bool) (<-chan Amqp091Message, error) {
	delChan, err := ch.channel.Consume(subject, "", autoAck, exclusive, false, false, nil)

	if err != nil {
		return nil, err
	}

	msgChan := make(chan Amqp091Message)

	go func() {
		for del := range delChan {
			msgChan <- fromAmqpMessage(del)
		}
	}()
	return msgChan, nil
}

// Publish Publish a message to an exchange
func (ch *Amqp091Channel) Publish(addressName, subject string, msg Amqp091Message) error {
	return ch.channel.Publish(addressName, subject, false, false, toAmqpMessage(&msg))
}

func toAmqpTable(at Amqp091Table) amqp.Table {
	table := make(amqp.Table)
	for key, val := range at {
		table[key] = val
	}
	return table
}

func fromAmqpTable(tab amqp.Table) Amqp091Table {
	table := make(Amqp091Table)
	for key, val := range tab {
		table[key] = val
	}
	return table
}

func toAmqpMessage(msg *Amqp091Message) amqp.Publishing {
	pub := amqp.Publishing{}
	pub.Body = msg.Body
	pub.DeliveryMode = uint8(msg.DeliveryMode)
	pub.Headers = toAmqpTable(msg.Headers)
	pub.ContentType = msg.ContentType
	pub.ContentEncoding = msg.ContentEncoding
	return pub
}

func fromAmqpMessage(del amqp.Delivery) Amqp091Message {
	msg := Amqp091Message{}
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
func (msg *Amqp091Message) Ack() error {
	// For unit testing
	switch msg.delivery.(type) {
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Ack(false)
	}
	return nil
}

// Nack Nack a message
func (msg *Amqp091Message) Nack() error {
	// For unit testing
	switch msg.delivery.(type) {
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Nack(false, true)
	}
	return nil
}

// Error Error
func (e *Amqp091Error) Error() string {
	return e.error.Error()
}

// Code Error code from amqp.Error
func (e *Amqp091Error) Code() int {
	return e.error.Code
}

// NewAmqp091Error New error
func NewAmqp091Error(e string) Amqp091Error {
	err := Amqp091Error{}
	err.error = amqp.Error{Reason: e}
	return err
}

// SetDelivery convenience method for unit tests
func (msg *Amqp091Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
