package amqp091

import (
	"crypto/tls"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	NewPublisher() (streamPublisherShim, error)
	NewConsumer(string, string, string, stream.MessagesHandler) (streamConsumerShim, error)
	DeclareStream(string, int64) error
	ShuttingDown(bool)
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
}

type streamPublisherShim interface {
	Publish(streamMessage) error
	Close() error
}

type streamPublisher struct {
	publisher *ha.ReliableProducer
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
	return nil
}

func (sc *streamConnection) ShuttingDown(val bool) {
	sc.clientDisconnect.Store(val)
}

func (sc *streamConnection) Close() error {
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	return sc.env.IsClosed()
}

func (sc *streamConnection) NewPublisher() (streamPublisherShim, error) {
	if sc.clientDisconnect.Load() {
		// not returning an error here because we are likely
		// shutting down this connection
		return nil, nil
	}
	rProducer, err := ha.NewReliableProducer(sc.env,
		sc.streamName,
		stream.NewProducerOptions().
			SetConfirmationTimeOut(5*time.Second).
			SetClientProvidedName(sc.clientIdentifier),
		func(messageStatus []*stream.ConfirmationStatus) {
			// implement this later
			for _, msgStatus := range messageStatus {
				msgStatus.IsConfirmed()
			}
		})
	if err != nil {
		return nil, err
	}
	return &streamPublisher{publisher: rProducer}, nil
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

func (sp *streamPublisher) Publish(msg streamMessage) error {
	return sp.publisher.Send(toStreamMessage(msg))
}

func (sp *streamPublisher) Close() error {
	return sp.publisher.Close()
}

func (scc *streamConsumer) Close() error {
	return scc.consumer.Close()
}
