package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"sassoftware.io/viya/arke/internal/provider"
	"sassoftware.io/viya/arke/internal/util"

	// "github.com/NeowayLabs/wabbit/amqptest/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/viya/arke/api"
)

var ctx context.Context
var cf *pb.ConnectionConfiguration

func init() {
	// Register the MockProvider with the Provider factory.

	// Setup our tests
	ctx = context.Background()
	cf = &pb.ConnectionConfiguration{}
}

type amqpConnectionMock struct {
	mock.Mock
	amqp091ConnectionShim //nolint:unused
	blockConnect          time.Duration
}

func (m *amqpConnectionMock) Connect() error {
	args := m.Called()
	if m.blockConnect > 0 {
		time.Sleep(m.blockConnect * time.Second)
	}
	return args.Error(0)
}

func (m *amqpConnectionMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *amqpConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *amqpConnectionMock) NewChannel() (amqp091ChannelShim, error) {
	args := m.Called()
	return args.Get(0).(amqp091ChannelShim), args.Error(1)
}

func (m *amqpConnectionMock) NotifyClose(chan amqp091Error) chan amqp091Error {
	args := m.Called()
	return args.Get(0).(chan amqp091Error)
}

type amqpChannelMock struct {
	mock.Mock
	amqp091ChannelShim
}

func (m *amqpChannelMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *amqpChannelMock) Publish(arg1 string, arg2 string, arg3 amqp091Message) error {
	args := m.Called(arg1, arg2, arg3)
	return args.Error(0)
}
func (m *amqpChannelMock) ExchangeDeclare(arg1 string, arg2 string, arg4 bool) error {
	args := m.Called(arg1, arg2, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) ExchangeBind(arg1 string, arg2 string, arg3 string) error {
	args := m.Called(arg1, arg2, arg3)
	return args.Error(0)
}
func (m *amqpChannelMock) SetPrefetch(arg1 int) error {
	args := m.Called(arg1)
	return args.Error(0)
}
func (m *amqpChannelMock) QueueDeclare(arg1 string, arg2 bool, arg3 bool, arg4 amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) QueueBind(arg1 string, arg2 string, arg3 string, arg4 amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) Consume(arg1 string, arg2 bool, arg3 bool) (<-chan amqp091Message, error) {
	args := m.Called(arg1, arg2, arg3)
	mc := args.Get(0).(chan amqp091Message)
	return mc, args.Error(1)
}

func (m *amqpChannelMock) NotifyClose(chan amqp091Error) chan amqp091Error {
	args := m.Called()
	return args.Get(0).(chan amqp091Error)
}

func (m *amqpChannelMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestNewAMQP091Provider(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)
}

func TestConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func TestConnect_Error(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(errors.New("error"))
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.NotNil(t, err)

	amock.AssertExpectations(t)
}

func Test_Connect_NoClient(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("noclient")
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "noclient")
}

func TestConnect_TLS_SkipVerify(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	err := prov.Connect(ctx, cc, true)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func TestConnect_TLS_WithCert(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	cc.CaCertificate = []byte("asdf")
	err := prov.Connect(ctx, cc, false)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
	// TODO: Figure out a good way to get tlsConfig and see if the cert is set
}

func TestConnect_Stats(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	stats := prov.Stats()
	assert.Equal(t, len(stats.Clients), 1)
	client := stats.Clients[0]
	assert.Equal(t, client.Streams, 0)
	assert.Equal(t, client.ActiveMessages, 0)
	assert.Equal(t, client.Produced, 0)
	assert.Equal(t, client.Consumed, 0)
	assert.Equal(t, client.ID, "1234")

	amock.AssertExpectations(t)
}

func Test_Ack_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Ack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Ack_AckErr(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(errors.New("ackErr"))
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Address: &pb.Address{Name: "addressname", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.NotNil(t, err)
	assert.Equal(t, "ackErr", err.GetMessage())

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Nack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Retry_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Retry(ctx, &pb.Source{}, msg.GetUuid(), 10)
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_DeadLetter_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.DeadLetter(ctx, &pb.Source{}, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "message not found in active messages")

	amock.AssertExpectations(t)
}

func Test_Ack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Address: &pb.Address{Name: "addressname", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Address: &pb.Address{Name: "addressname", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack_NackErr(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(errors.New("nackErr"))
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Address: &pb.Address{Name: "addressname", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.NotNil(t, err)
	assert.Equal(t, "nackErr", err.GetMessage())

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Retry(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_RetryFailure(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, true).Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("puberr"))

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_RetryFailure_NoBrokerDetails(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("no client identifier")
	}
	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	retErr := prov.Retry(ctx, nil, "", 1)
	assert.NotNil(t, retErr)
	assert.Contains(t, retErr.GetMessage(), "no client identifier")

}
func Test_RetryFailure_DeclareErrorsStillSuccess(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("err")).Once()
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	opts := make(map[string]string)
	opts["junkheader"] = "junkvalue"
	src := &pb.Source{Address: &pb.Address{Name: "addressname"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	src.Options = opts
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_DLQ(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	delMock := mock.Mock{}
	defer close(msgs)
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(nil)
		mm.SetDelivery(dMock) //nolint

		msgs <- mm
	}(&delMock)

	argsDlq := make(amqp091Table)
	argsDlq["x-queue-type"] = "classic"
	args := make(amqp091Table)
	args["x-dead-letter-exchange"] = "dla"
	args["x-queue-type"] = "quorum"
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", "addressname", "topic", false).Return(nil).Once()
	cmock.On("ExchangeDeclare", "dla", "topic", false).Return(nil).Once()
	cmock.On("QueueDeclare", "queuename.quorum", false, false, args).Return(nil).Once()
	cmock.On("QueueDeclare", "queuename.dlq", false, false, argsDlq).Return(nil).Once()
	cmock.On("QueueBind", "queuename.quorum", "routingkey", "addressname", mock.Anything).Return(nil).Once()
	cmock.On("QueueBind", "queuename.dlq", "queuename.quorum", "dla", mock.Anything).Return(nil).Once()
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "routingkey")
	options := map[string]string{"DeadLetterAddress": "dla"}
	src := &pb.Source{Name: "queuename", Address: &pb.Address{Name: "addressname", Subjects: subjects},
		Options: options}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	dlErr := prov.DeadLetter(ctx, src, msg.GetUuid())
	assert.Nil(t, dlErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	delMock.AssertExpectations(t)
}

func Test_Ack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Nack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}
func Test_Publish_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	mc := make(chan *pb.Message)
	ec := make(chan *pb.Error)
	err := prov.Publish(ctx, mc, ec)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Subscribe_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{Name: "addressName"}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}
func Test_Subscribe_NoAddressName(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_SourceNameNotQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", AutoDelete: true, Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname", src.GetName())
}

func Test_SourceNameQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname.quorum", src.GetName())
}

func Test_SourceNameNotQuorum_AutoDelete(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", Address: address, AutoDelete: true}
	src.Name = sourceName(src)
	assert.Equal(t, "myname", src.GetName())
}

func Test_SourceNameNamedDotQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname.quorum", Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname.quorum", src.GetName())
}

func Test_Subscribe_NoAddress(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	src := &pb.Source{}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_Options(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string)
	options["MessageTTL"] = "100"
	options["Expires"] = "100"
	options["DeadLetterAddress"] = "dla"
	options["DeadLetterSubject"] = "dls"

	expectedQueueArgs := amqp091Table{}
	expectedQueueArgs["x-message-ttl"] = 100
	expectedQueueArgs["x-expires"] = 100
	expectedQueueArgs["x-dead-letter-exchange"] = "dla"
	expectedQueueArgs["x-dead-letter-routing-key"] = "dls"
	expectedQueueArgs["x-queue-type"] = "classic"

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.ContentType = "application/json"
		mm.ContentEncoding = "text"
		mm.Headers = make(amqp091Table)
		mm.Headers["something"] = "somethingelse"
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "addressname", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	matches1 := make([]*pb.Match, 0)
	matches1 = append(matches1, &pb.Match{Name: "key1", Value: "value1"})
	matches1 = append(matches1, &pb.Match{Name: "key2", Value: "value2"})
	matches2 := make([]*pb.Match, 0)
	matches2 = append(matches2, &pb.Match{Name: "key3", Value: "value3"})
	matches2 = append(matches2, &pb.Match{Name: "key4", Value: "value4"})
	filters := make([]*pb.Filter, 0)
	filters = append(filters, &pb.Filter{Matches: matches1, Type: pb.Filter_ANY})
	filters = append(filters, &pb.Filter{Matches: matches2, Type: pb.Filter_ANY})

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       options,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    true,
		PrefetchCount: 4}

	expectedMatchHeaders1 := amqp091Table{}
	expectedMatchHeaders1["key1"] = "value1"
	expectedMatchHeaders1["key2"] = "value2"
	expectedMatchHeaders1["x-match"] = "any"

	expectedMatchHeaders2 := amqp091Table{}
	expectedMatchHeaders2["key3"] = "value3"
	expectedMatchHeaders2["key4"] = "value4"
	expectedMatchHeaders2["x-match"] = "any"

	cmock.On("SetPrefetch", 4).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeDeclare", parent.GetName(), "direct", parent.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeDeclare", "dla", "topic", false).Return(nil).Once()
	cmock.On("ExchangeBind", address.GetName(), subjects[0], parent.GetName()).Return(nil)
	cmock.On("ExchangeBind", address.GetName(), subjects[1], parent.GetName()).Return(nil)
	cmock.On("QueueDeclare", src.GetName(), false, false, expectedQueueArgs).Return(nil)
	cmock.On("QueueDeclare", "srcname.dlq", false, false, amqp091Table{"x-queue-type": "classic"}).Return(nil)
	cmock.On("QueueBind", src.GetName(), "subject1", address.GetName(), expectedMatchHeaders1).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject1", address.GetName(), expectedMatchHeaders2).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject2", address.GetName(), expectedMatchHeaders1).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject2", address.GetName(), expectedMatchHeaders2).Return(nil).Once()
	cmock.On("QueueBind", "srcname.dlq", "dls", "dla", mock.Anything).Return(nil).Once()
	cmock.On("Consume", src.GetName(), false, src.GetExclusive()).Return(msgs, nil)
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

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

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 3)
}

func Test_Subscribe_NoSubjectsNoFilters(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.ContentType = "application/json"
		mm.ContentEncoding = "text"
		mm.Headers = make(amqp091Table)
		mm.Headers["something"] = "somethingelse"
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	subjects := make([]string, 0)
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}
	filters := make([]*pb.Filter, 0)

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    false,
		PrefetchCount: 4}

	expectedQueueArgs := amqp091Table{}
	expectedQueueArgs["x-queue-type"] = "quorum"
	expectedQueueArgs["x-expires"] = 300000

	cmock.On("SetPrefetch", 4).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	cmock.On("QueueDeclare", src.GetName()+".quorum", false, false, expectedQueueArgs).Return(nil)
	cmock.On("Consume", src.GetName()+".quorum", false, src.GetExclusive()).Return(msgs, nil)
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

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

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
	// With not subjects and no filters we should NOT
	// call QueueBind
	cmock.AssertNumberOfCalls(t, "QueueBind", 0)
}

func Test_Subscribe_UnsupportedOptions(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string)
	options["unsupported"] = "100"

	expectedOptions := make(map[string]interface{})
	expectedOptions["x-message-ttl"] = 100

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)

	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}, Options: options}
	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, suberr)
	assert.Contains(t, suberr.GetMessage(), "unsupported is an unsupported source option")

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

// Disconnect does not return anything so there isn't much to test
func Test_Disconnect(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	prov.Disconnect(ctx)

	amock.AssertExpectations(t)
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewAMQP091Provider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
	expected := make(map[string]bool)
	expected["MessageTTL"] = true
	expected["DeadLetterAddress"] = true
	expected["DeadLetterSubject"] = true
	expected["Expires"] = true

	assert.Equal(t, opts, expected)
}

func Test_WaitForConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{blockConnect: 1}
	amock.On("Connect").Return(nil)
	amock.On("Connect").Return(nil)
	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	errs <- newAmqp091Error("chanerr", 1) // simulate an error
	time.Sleep(500 * time.Millisecond)
	connected := prov.WaitForConnect(ctx)
	assert.True(t, connected)

	amock.AssertExpectations(t)

}

func Test_Publish(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	msg := &pb.Message{Address: address, Body: []byte("thebody")}
	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"
	msg.Persistent = true

	expectedMsg := amqp091Message{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.DeliveryMode = 2 // persistent
	expectedMsg.ContentType = msg.Headers["Content-Type"]
	expectedMsg.ContentEncoding = msg.Headers["Content-Encoding"]
	expectedMsg.Headers = amqp091Table{}
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		// close(mc)
		// close(errchan)
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Publish_Error(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], mock.Anything).Return(errors.New("puberr"))
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	msg := &pb.Message{Address: address, Body: []byte("thebody")}

	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "puberr")

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Publish_ErrorDeclareExchange(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	msg := &pb.Message{Address: address, Body: []byte("thebody")}
	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"
	msg.Persistent = true

	expectedMsg := amqp091Message{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.DeliveryMode = 2 // persistent
	expectedMsg.ContentType = msg.Headers["Content-Type"]
	expectedMsg.ContentEncoding = msg.Headers["Content-Encoding"]
	expectedMsg.Headers = amqp091Table{}
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(errors.New("declareerr")).Once()
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.Nil(t, err)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_newAmqp091Error(t *testing.T) {
	e := "myError"
	code := 123
	aErr := newAmqp091Error(e, code)
	assert.Equal(t, code, aErr.Code())
	assert.Equal(t, e, aErr.error.Reason)
}

func Test_fromAmqpMessage(t *testing.T) {
	del := amqp.Delivery{}
	del.Body = []byte("Hello")
	del.DeliveryMode = uint8(2)
	del.Headers = amqp.Table{"h1": "header1"}
	del.ContentType = "text"
	del.ContentEncoding = "plain"
	del.DeliveryTag = 1

	aMsg := fromAmqpMessage(del)
	assert.Equal(t, del.Body, aMsg.Body)
	assert.Equal(t, int(del.DeliveryMode), aMsg.DeliveryMode)
	assert.Equal(t, del.Headers["h1"], aMsg.Headers["h1"])
	assert.Equal(t, del.ContentType, aMsg.ContentType)
	assert.Equal(t, del.ContentEncoding, aMsg.ContentEncoding)
	assert.Equal(t, del.DeliveryTag, aMsg.DeliveryTag)
}

func Test_toAmqpMessage(t *testing.T) {
	aMsg := &amqp091Message{}
	aMsg.Body = []byte("Hello")
	aMsg.DeliveryMode = 2
	aMsg.Headers = amqp091Table{"h1": "header1"}
	aMsg.ContentType = "text"
	aMsg.ContentEncoding = "plain"

	del := toAmqpMessage(aMsg)
	assert.Equal(t, aMsg.Body, del.Body)
	assert.Equal(t, aMsg.DeliveryMode, int(del.DeliveryMode))
	assert.Equal(t, aMsg.Headers["h1"], del.Headers["h1"])
	assert.Equal(t, aMsg.ContentType, del.ContentType)
	assert.Equal(t, aMsg.ContentEncoding, del.ContentEncoding)
}

func Test_NewAmqp091Connection(t *testing.T) {
	c := NewAmqp091Connection("connStr", "identifier", nil).(*amqp091Connection)
	assert.Equal(t, "identifier", c.clientIdentifier)
	assert.Equal(t, "connStr", c.connStr)
	assert.Nil(t, c.tlsCfg)
}

func Test_amqpConfig(t *testing.T) {
	cfg := amqpConfig("connName", nil)
	assert.Equal(t, "connName", cfg.Properties["connection_name"])
	assert.Equal(t, 10*time.Second, cfg.Heartbeat)
	assert.Equal(t, "en_US", cfg.Locale)
	assert.Nil(t, cfg.TLSClientConfig)
}

func Test_SetDelivery(t *testing.T) {
	m := &amqp091Message{}
	m.SetDelivery(1)
	assert.Equal(t, 1, m.delivery)
}

func Test_ClientExists(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("1234")
	assert.True(t, exists)
}

func Test_ClientExists_false(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("4321")
	assert.False(t, exists)
}

func Test_getBrokerDetails_err(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	bd, err := prov.getBrokerDetails(ctx)
	assert.NotNil(t, bd)
	assert.NotNil(t, err)
	assert.Equal(t, "could not retrieve broker details for this connection: 1234", err.Error())

}

func Test_SetupDeadLetter_no_BD(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("nope")
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	opts := make(map[string]string)
	opts["DeadLetterAddress"] = "dla"
	src := &pb.Source{Options: opts}
	err := prov.setupDeadLetter(ctx, src)
	assert.NotNil(t, err)
}

func Test_SetupDeadLetter_channel_error(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("NewChannel").Return(cmock, errors.New("chanerr"))

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	bd := BrokerDetails{}
	bd.Connection = amock
	prov.connections.Add("1234", &bd)

	ctx := context.Background()
	opts := make(map[string]string)
	opts["DeadLetterAddress"] = "dla"
	src := &pb.Source{Options: opts}
	err := prov.setupDeadLetter(ctx, src)
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "chanerr")
	amock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_connect_clientDisconnect(t *testing.T) {
	bd := BrokerDetails{}
	bd.clientDisconnect = true
	ok, err := bd.connect()
	assert.False(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_connected(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false
	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.CONNECTED
	}()
	ok, err := bd.connect()
	assert.True(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_closed(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false
	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.CLOSED
	}()
	ok, err := bd.connect()
	assert.False(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_disconnected(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091

	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.DISCONNECTED
	}()
	ok, err := bd.connect()
	assert.True(t, ok)
	assert.Nil(t, err)
	amock.AssertExpectations(t)
}

func mockManagementRequestServer() *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		var status int
		if r.Method == "GET" && r.URL.Path == "/api/bindings/tenant/e/address/q/queue/" {
			status = http.StatusOK
			body = []byte(`[{"source":"arke.test","vhost":"tenant","destination":"queue","destination_type":"queue","routing_key":"routingkey","arguments":{},"properties_key":"routingkey"}]`)
		}
		if r.Method == "DELETE" && r.URL.Path == "/api/bindings/tenant/e/address/q/queue/routingkey/" {
			status = http.StatusNoContent
		}
		if body != nil {
			w.Write(body) //nolint:errcheck
		}
		w.WriteHeader(status)
	}))
	return server
}

func Test_cleanupBindings(t *testing.T) {

	bd := &BrokerDetails{}
	addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
	src := &pb.Source{Address: addr, Name: "queue"}
	creds := &pb.Credentials{Username: "user", Password: "password"}
	bd.connectionConfig = &pb.ConnectionConfiguration{Credentials: creds}

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, err := url.Parse(msrv.URL)
	assert.Nil(t, err)
	bd.connectionConfig.Host = u.Hostname()
	bd.connectionConfig.Tenant = "tenant"
	i, _ := strconv.Atoi(u.Port())
	bd.connectionConfig.AdminPort = int32(i)

	removed := bd.cleanupBindings(src, []string{"routingkey2"})
	assert.Len(t, removed, 1)
}

func Test_cleanupBindings_none(t *testing.T) {

	bd := &BrokerDetails{}
	addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
	src := &pb.Source{Address: addr, Name: "queue"}
	creds := &pb.Credentials{Username: "user", Password: "password"}
	bd.connectionConfig = &pb.ConnectionConfiguration{Credentials: creds}

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, err := url.Parse(msrv.URL)
	assert.Nil(t, err)
	bd.connectionConfig.Host = u.Hostname()
	bd.connectionConfig.Tenant = "tenant"
	i, _ := strconv.Atoi(u.Port())
	bd.connectionConfig.AdminPort = int32(i)

	removed := bd.cleanupBindings(src, []string{"routingkey"})
	assert.Len(t, removed, 0)
}

func Test_declareQueueAutoDelete(t *testing.T) {

	var autoDeleteTests = []struct {
		autoDelete bool
		exclusive  bool
		expires    int
	}{
		{true, false, 0},
		{true, false, int(time.Duration(5 * time.Minute).Milliseconds())},
		{false, true, 0},
	}

	for _, adt := range autoDeleteTests {
		t.Run(fmt.Sprintf("AutoDeleteTest autoDelete:%t, exclusive: %t, expires:%d",
			adt.autoDelete, adt.exclusive, adt.expires), func(t *testing.T) {

			bd := &BrokerDetails{
				knownQueues: util.NewConcurrentMap(),
			}
			addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
			src := &pb.Source{Address: addr, Name: "queue", AutoDelete: adt.autoDelete, Exclusive: adt.exclusive}

			expectedArgs := make(amqp091Table)
			if adt.expires > 0 {
				expectedArgs["x-expires"] = adt.expires
			} else {
				expectedArgs["x-expires"] = int(time.Duration(5 * time.Minute).Milliseconds())
			}
			if adt.autoDelete {
				expectedArgs["x-queue-type"] = "classic"
			} else {
				expectedArgs["x-queue-type"] = "quorum"
			}

			cmock := &amqpChannelMock{}
			cmock.On("QueueDeclare", src.GetName(), false, false, expectedArgs).Return(nil)

			prov := NewAMQP091Provider().(*amqp091provider)
			err := prov.declareQueue(src, bd, cmock, false)
			assert.Nil(t, err)
			cmock.AssertExpectations(t)
		})
	}
}
