package amqp091

import (
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	GetPublisher(bool) streamPublisherShim
	PutPublisher(bool, streamPublisherShim)
	NewConsumer(string, string, string, stream.MessagesHandler) (streamConsumerShim, error)
	DeclareStream(string, int64) error
}

type streamConnection struct {
	env              *stream.Environment
	maxProducers     int
	maxConsumers     int
	connStr          string
	tlsCfg           *tls.Config
	clientIdentifier string
	streamName       string
	clientDisconnect atomic.Bool
	publishers       *sync.Pool
	pcPublishers     *sync.Pool
}

type streamPublisherShim interface {
	Publish(streamMessage) error
	GetPCChannel() chan streamMessageResponseShim
}

type publisherWrapper interface {
	Send(message.StreamMessage) error
	Close() error
}

type streamPublisher struct {
	publisher publisherWrapper
	pcChannel chan streamMessageResponseShim
}

type streamConsumerShim interface {
	Close() error
}

type streamConsumer struct {
	consumer     *stream.Consumer
	streamName   string
	consumerName string
}

type streamMessage struct {
	Body            []byte
	ContentType     string
	ContentEncoding string
	Headers         map[string]string
}

type streamMessageResponseShim interface {
	IsConfirmed() bool
	GetPublishingId() int64
	GetError() error
	GetMessage() message.StreamMessage
}

func (sc *streamConnection) Connect() error {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetMaxProducersPerClient(sc.getMaxProducers()).
			SetMaxConsumersPerClient(sc.getMaxConsumers()).
			SetUri(sc.connStr).
			SetTLSConfig(sc.tlsCfg))

	if err != nil {
		return err
	}
	sc.env = env

	sc.publishers = &sync.Pool{
		New: func() any {
			return sc.newPublisher(false)
		},
	}
	sc.pcPublishers = &sync.Pool{
		New: func() any {
			return sc.newPublisher(true)
		},
	}

	return nil
}

func (sc *streamConnection) Close() error {
	sc.clientDisconnect.Store(true)
	if sc.IsClosed() {
		return nil
	}

	// Drain the publisher pool, Get will return
	// nil when it tries to create a new publisher
	for {
		producer := sc.publishers.Get()
		if producer == nil {
			break
		}
		producer.(*streamPublisher).Close()
	}
	// Drain the publish confirms publisher pool
	for {
		producer := sc.pcPublishers.Get()
		if producer == nil {
			break
		}
		producer.(*streamPublisher).Close()
	}
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	return sc.env.IsClosed()
}

func (sc *streamConnection) PutPublisher(confirm bool, pub streamPublisherShim) {
	if confirm {
		sc.pcPublishers.Put(pub)
	} else {
		sc.publishers.Put(pub)
	}
}

func (sc *streamConnection) GetPublisher(confirm bool) streamPublisherShim {
	if confirm {
		return sc.pcPublishers.Get().(streamPublisher)
	}
	return sc.publishers.Get().(streamPublisher)
}

func (sc *streamConnection) newPublisher(confirm bool) streamPublisherShim {
	if sc.clientDisconnect.Load() {
		// not returning an error here because we are likely
		// shutting down this connection
		return nil
	}
	var producer publisherWrapper
	var err error
	var pcChan chan streamMessageResponseShim
	if confirm {
		pcChan = make(chan streamMessageResponseShim, 1)
		producer, err = ha.NewReliableProducer(sc.env, sc.streamName,
			stream.NewProducerOptions().
				SetConfirmationTimeOut(5*time.Second).
				SetClientProvidedName(sc.clientIdentifier),
			func(messageStatus []*stream.ConfirmationStatus) {
				for _, msgStatus := range messageStatus {
					pcChan <- msgStatus
				}
			})
		if err != nil {
			return nil
		}
	} else {
		producer, err = sc.env.NewProducer(sc.streamName,
			stream.NewProducerOptions().
				SetClientProvidedName(sc.clientIdentifier))
		if err != nil {
			return nil
		}
	}

	return streamPublisher{publisher: producer, pcChannel: pcChan}
}

func (sc *streamConnection) NewConsumer(streamName string, consumerName string, offset string, handler stream.MessagesHandler) (streamConsumerShim, error) {
	// QueryOffset returns an error if the consumer has yet to store an
	// offset, so we ignore any errors and use the offset returned which
	// is 0 on error
	lastOffset, _ := sc.env.QueryOffset(consumerName, streamName)
	sOffset, qErr := toStreamOffset(offset, lastOffset)
	if qErr != nil {
		return nil, qErr
	}
	consumer, err := sc.env.NewConsumer(
		streamName,
		handler,
		stream.NewConsumerOptions().
			SetClientProvidedName(sc.clientIdentifier).
			SetConsumerName(consumerName).
			SetOffset(sOffset))
	if err != nil {
		return nil, err
	}
	return &streamConsumer{consumer: consumer, streamName: streamName, consumerName: consumerName}, nil
}

func (sc *streamConnection) DeclareStream(streamName string, ttl int64) error {
	opts := &stream.StreamOptions{}
	if ttl > 0 {
		dTTL := time.Duration(ttl * int64(time.Second))
		opts.SetMaxAge(dTTL)
	}
	return sc.env.DeclareStream(streamName, opts)
}

func (sc *streamConnection) getMaxProducers() int {
	if sc.maxProducers < 1 {
		return 1
	}
	return sc.maxProducers
}

func (sc *streamConnection) getMaxConsumers() int {
	if sc.maxConsumers < 1 {
		return 1
	}
	return sc.maxConsumers
}

func (sp streamPublisher) Publish(msg streamMessage) error {
	return sp.publisher.Send(toStreamMessage(msg))
}

func (sp streamPublisher) Close() error {
	return sp.publisher.Close()
}

func (sp streamPublisher) GetPCChannel() chan streamMessageResponseShim {
	return sp.pcChannel
}

func (scc *streamConsumer) Close() error {
	return scc.consumer.Close()
}
