package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/viya/arke/api"
)

type streamConnectionMock struct {
	mock.Mock
	blockConnect time.Duration
}

func (m *streamConnectionMock) Connect() error {
	args := m.Called()
	if m.blockConnect > 0 {
		time.Sleep(m.blockConnect * time.Second)
	}
	return args.Error(0)
}

func (m *streamConnectionMock) ShuttingDown(_ bool) {
}

func (m *streamConnectionMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *streamConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *streamConnectionMock) NewPublisher() (streamPublisherShim, error) {
	args := m.Called()
	ret := args.Get(0)
	if ret == nil {
		return nil, args.Error(1)
	}
	return ret.(streamPublisherShim), args.Error(1)
}

func (m *streamConnectionMock) NewConsumer(streamName string, _ string, _ string, handler stream.MessagesHandler) (streamConsumerShim, error) {
	args := m.Called()
	addr := stockAddress()
	addr.Name = streamName
	cCtx := stream.ConsumerContext{}
	sMsg := stockAmqp10Message(stockMessage(addr))
	handler(cCtx, &sMsg)
	// Wait for handler() to send the message back
	time.Sleep(500 * time.Millisecond)
	ret := args.Get(0)
	if ret == nil {
		return nil, args.Error(1)
	}
	return ret.(*streamConsumerMock), args.Error(1)
}

func (m *streamConnectionMock) DeclareStream(_ string, _ int64) error {
	args := m.Called()
	return args.Error(0)
}

type streamPublisherMock struct {
	mock.Mock
}

func (m *streamPublisherMock) Publish(arg1 streamMessage) error {
	args := m.Called(arg1)
	return args.Error(0)
}

func (m *streamPublisherMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

type streamConsumerMock struct {
	mock.Mock
}

func (m *streamConsumerMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func stockStreamMessage(msg *pb.Message) streamMessage {
	expectedMsg := streamMessage{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.Headers = make(map[string]string)
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]
	expectedMsg.Headers["Additional-Header"] = msg.Headers["Additional-Header"]

	return expectedMsg
}

func stockAmqp10Message(msg *pb.Message) amqp.Message {
	sMsg := amqp.Message{}
	sMsg.Data = [][]byte{msg.GetBody()}
	return sMsg
}

func Test_PublishStream(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)
	expectedMsg := stockStreamMessage(msg)

	pmock := &streamPublisherMock{}
	pmock.On("Publish", expectedMsg).Return(nil)
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)

	smock.On("NewPublisher").Return(pmock, nil)
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.PublishOne(ctx, msg)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishStreamFailedConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)

	pmock := &streamPublisherMock{}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(errors.New("Failed connection"))

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.Equal(t, "failed to create stream connection to broker: Failed connection", suberr.Message)

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
}

func Test_PublishStreamNotConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)

	pmock := &streamPublisherMock{}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(true)

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.Equal(t, "connection to broker is closed", suberr.Message)

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
}

func Test_SubscribeStream(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(nil)
	smock.On("NewPublisher").Return(nil)

	pmock := &streamConsumerMock{}
	pmock.On("Close").Return(nil)
	smock.On("NewConsumer").Return(pmock, nil)
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg = <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)
	assert.NotNil(t, msg.GetAddress())
	assert.Equal(t, msg.GetAddress(), src.GetAddress())
	assert.Equal(t, msg.GetAddress().GetSubjects(), subjects)

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(1000 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamBadOpt(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "BadOpt": "true"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support the following source options: [BadOpt]", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamAutoDeleteOrExclusive(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Exclusive:     false,
		AutoDelete:    true,
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	src.Exclusive = true
	suberr = prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	src.AutoDelete = false
	suberr = prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamInvalidTTL(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("NewPublisher").Return(nil)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "true"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "value for MessageTTL option must be a quoted integer", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamFailedConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(errors.New("Failed Connection"))
	smock.On("IsClosed").Return(nil)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "failed to create stream connection to broker: Failed Connection", suberr.Message)

	prov.Disconnect(ctx)
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamNotConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(true)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "connection to broker is closed", suberr.Message)

	prov.Disconnect(ctx)
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamFailedDeclare(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(errors.New("Failed stream declare"))
	smock.On("NewPublisher").Return(nil)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "failed to declare stream: Failed stream declare", suberr.GetMessage())
	assert.True(t, suberr.GetIsFatal())

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(500 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_StreamRetry(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(nil)
	smock.On("NewPublisher").Return(nil)

	pmock := &streamConsumerMock{}
	pmock.On("Close").Return(nil)
	smock.On("NewConsumer").Return(pmock, nil)
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg = <-mc

	assert.NotNil(t, msg)
	err = prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, err)

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(1000 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}
