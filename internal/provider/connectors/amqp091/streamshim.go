package amqp091

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"sassoftware.io/viya/arke/internal/util"
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	GetPublisher(streamName, publisherName string, confirm bool) streamPublisherShim
	PutPublisher(confirm bool, publisher streamPublisherShim)
	NewConsumer(streamName string, consumerName string, offset string, handler stream.MessagesHandler) (streamConsumerShim, error)
	DeclareStream(streamName string, ttl int64) error
	GetLastOffset(streamName string, consumerName string) int64
}

type streamConnection struct {
	env              *stream.Environment
	envLock          sync.Mutex
	maxProducers     int
	maxConsumers     int
	connStr          string
	tlsCfg           *tls.Config
	clientIdentifier string
	clientDisconnect atomic.Bool
	publishers       *util.ConcurrentMap
	ctx              context.Context
	cancel           context.CancelFunc
}

type streamPublisherShim interface {
	Publish(streamMessage) error
	GetPCChannel() chan streamMessageResponseShim
	GetStreamName() string
	GetPublisherName() string
}

type publisherWrapper interface {
	Send(message.StreamMessage) error
	Close() error
}

type streamPublisher struct {
	streamName    string
	publisherName string
	publisher     publisherWrapper
	pcChannel     chan streamMessageResponseShim
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
	Ack             func()
	Nack            func()
	PublishID       int64
}

type streamMessageResponseShim interface {
	IsConfirmed() bool
	GetPublishingId() int64
	GetError() error
	GetMessage() message.StreamMessage
}

func (sc *streamConnection) Connect() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
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

func (sc *streamConnection) Close() error {
	sc.clientDisconnect.Store(true)
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	sc.cancel()
	if sc.env.IsClosed() {
		return nil
	}
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	return sc.env.IsClosed()
}

func (sc *streamConnection) PutPublisher(confirm bool, pub streamPublisherShim) {
	key := genPublisherKey(pub.GetStreamName(), pub.GetPublisherName(), confirm)
	pool, ok := sc.publishers.Get(key)
	if ok {
		pool.(*util.BlockingPool).Put(pub)
	}
}

func (sc *streamConnection) GetPublisher(streamName, publisherName string, confirm bool) streamPublisherShim {
	key := genPublisherKey(streamName, publisherName, confirm)
	pool, ok := sc.publishers.Get(key)
	if !ok {
		limit := maxPoolProducers
		if publisherName != "" {
			// The broker only supports one publisher per publisherName
			limit = 1
		}
		pool = util.NewBlockingPool(context.WithValue(sc.ctx, CtxKey{name: poolKeyName}, key), limit,
			func() any {
				return sc.newPublisher(streamName, publisherName, confirm)
			},
		)
		sc.publishers.Add(key, pool)
	}
	var pub any
	i := 0
	// Sometimes we fail to get a producer, we will
	// retry to prevent failures
	for pub == nil && i < 10 {
		pub = pool.(*util.BlockingPool).Get()
		i++
		if pub == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if pub == nil {
		return nil
	}
	return pub.(streamPublisher)
}

func genPublisherKey(streamName, publisherName string, confirm bool) string {
	key := fmt.Sprintf("%s-%s", streamName, publisherName)
	if confirm {
		key = fmt.Sprintf("%s-%s", key, "confirm")
	}
	return key
}

func (sc *streamConnection) newPublisher(streamName, publisherName string, confirm bool) streamPublisherShim {
	if sc.clientDisconnect.Load() {
		// not returning an error here because we are likely
		// shutting down this connection
		return nil
	}
	var producer publisherWrapper
	var err error
	var pcChan chan streamMessageResponseShim
	options := stream.NewProducerOptions().SetClientProvidedName(sc.clientIdentifier)
	if publisherName != "" {
		options.SetProducerName(publisherName)
	}
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	if confirm {
		options.SetConfirmationTimeOut(5 * time.Second)
		pcChan = make(chan streamMessageResponseShim, 1)
		producer, err = ha.NewReliableProducer(sc.env, streamName, options,
			func(messageStatus []*stream.ConfirmationStatus) {
				for _, msgStatus := range messageStatus {
					pcChan <- msgStatus
				}
			})
		if err != nil {
			util.Logger.Debugf("Error creating publisher : %v\n", err)
			return nil
		}
	} else {
		producer, err = sc.env.NewProducer(streamName, options)
		if err != nil {
			util.Logger.Debugf("Error creating publisher : %v\n", err)
			return nil
		}
	}

	return streamPublisher{streamName: streamName, publisherName: publisherName, publisher: producer, pcChannel: pcChan}
}

func (sc *streamConnection) NewConsumer(streamName string, consumerName string, offset string, handler stream.MessagesHandler) (streamConsumerShim, error) {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
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
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	return sc.env.DeclareStream(streamName, opts)
}

func (sc *streamConnection) GetLastOffset(streamName string, consumerName string) int64 {
	offset, qErr := sc.env.QueryOffset(consumerName, streamName)
	util.Logger.Debugf("GetLastOffset (%s)(%s) [%v]", consumerName, streamName, qErr)
	return offset
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

func (sp streamPublisher) GetPublisherName() string {
	return sp.publisherName
}

func (sp streamPublisher) GetStreamName() string {
	return sp.streamName
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
