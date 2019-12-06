package connectors

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	// "github.com/NeowayLabs/wabbit/amqptest/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/convoy/arke/api"
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
	Amqp091ConnectionShim
	blockConnect time.Duration
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

func (m *amqpConnectionMock) NewChannel() (Amqp091ChannelShim, error) {
	args := m.Called()
	return args.Get(0).(Amqp091ChannelShim), args.Error(1)
}

func (m *amqpConnectionMock) NotifyClose(chan Amqp091Error) chan Amqp091Error {
	args := m.Called()
	return args.Get(0).(chan Amqp091Error)
}

type amqpChannelMock struct {
	mock.Mock
	Amqp091ChannelShim
}

func (m *amqpChannelMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *amqpChannelMock) Publish(arg1 string, arg2 string, arg3 Amqp091Message) error {
	args := m.Called(arg1, arg2, arg3)
	return args.Error(0)
}
func (m *amqpChannelMock) ExchangeDeclare(arg1 string, arg2 string, arg3 bool, arg4 bool) error {
	args := m.Called(arg1, arg2, arg3, arg4)
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
func (m *amqpChannelMock) QueueDeclare(arg1 string, arg2 bool, arg3 bool, arg4 bool, arg5 Amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4, arg5)
	return args.Error(0)
}
func (m *amqpChannelMock) QueueBind(arg1 string, arg2 string, arg3 string, arg4 Amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) Consume(arg1 string, arg2 bool, arg3 bool) (<-chan Amqp091Message, error) {
	args := m.Called(arg1, arg2, arg3)
	mc := args.Get(0).(chan Amqp091Message)
	return mc, args.Error(1)
}

func TestNewAMQP091Provider(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)
}

func TestConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)

	assert.Nil(t, err)
}

func TestConnect_Error(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(errors.New("error"))
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)

	assert.NotNil(t, err)
}

func Test_Connect_NoClient(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "", errors.New("noclient")
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)

	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "noclient")
}

func TestConnect_TLS_SkipVerify(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	err := prov.Connect(&ctx, cc, true)

	assert.Nil(t, err)
}

func TestConnect_TLS_WithCert(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	cc.CaCertificate = []byte("asdf")
	err := prov.Connect(&ctx, cc, false)

	assert.Nil(t, err)
	// TODO: Figure out a good way to get tlsConfig and see if the cert is set
}

func TestConnect_Stats(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	stats := prov.Stats()
	fmt.Println(stats)
	assert.Equal(t, len(stats.Clients), 1)
	client := stats.Clients[0]
	assert.Equal(t, client.Streams, 0)
	assert.Equal(t, client.ActiveMessages, 0)
	assert.Equal(t, client.Produced, 0)
	assert.Equal(t, client.Consumed, 0)
	assert.Equal(t, client.ID, "1234")
}

func Test_Ack_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Ack(&ctx, &msg)
	assert.Contains(t, err.GetMessage(), "No message with uuid")
}

func Test_Nack_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Nack(&ctx, &msg)
	assert.Contains(t, err.GetMessage(), "No message with uuid")
}

func Test_Ack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan Amqp091Message)
	defer close(msgs)
	go func() {
		mm := Amqp091Message{}
		mm.DeliveryTag = 1
		delMock := mock.Mock{}
		delMock.On("Ack").Return(nil)
		mm.SetDelivery(delMock)

		msgs <- mm
	}()

	cmock.On("SetPrefetch", mock.Anything).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("ExchangeBind", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := &pb.Message{}

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	go func() {
		msg = <-mc
	}()

	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(&ctx, msg)
	assert.NotNil(t, msg)
	assert.Nil(t, err)
}

func Test_Nack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan Amqp091Message)
	defer close(msgs)
	go func() {
		mm := Amqp091Message{}
		mm.DeliveryTag = 1
		delMock := mock.Mock{}
		delMock.On("Nack").Return(nil)
		mm.SetDelivery(delMock)

		msgs <- mm
	}()

	cmock.On("SetPrefetch", mock.Anything).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("ExchangeBind", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := &pb.Message{}

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	go func() {
		msg = <-mc
	}()

	time.Sleep(100 * time.Millisecond)
	err = prov.Nack(&ctx, msg)
	assert.NotNil(t, msg)
	assert.Nil(t, err)
}

func Test_Ack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Ack(&ctx, &msg)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve peer info")
}

func Test_Nack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Nack(&ctx, &msg)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve peer info")
}
func Test_Publish_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	mc := make(chan *pb.Message)
	ec := make(chan *pb.Error)
	err := prov.Publish(&ctx, mc, ec)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve peer info")
}

func Test_Subscribe_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{Name: "addressName"}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)
	err := prov.Subscribe(&ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve peer info")
}
func Test_Subscribe_NoAddressName(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)
	err := prov.Subscribe(&ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_NoAddress(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	src := &pb.Source{}
	mc := make(chan *pb.Message)
	err := prov.Subscribe(&ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_Options(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string, 0)
	options["MessageTTL"] = "100"
	options["Expires"] = "100"
	options["DeadLetterAddress"] = "dla"
	options["DeadLetterSubject"] = "dls"

	expectedQueueArgs := Amqp091Table{}
	expectedQueueArgs["x-message-ttl"] = 100
	expectedQueueArgs["x-expires"] = 100
	expectedQueueArgs["x-dead-letter-exchange"] = "dla"
	expectedQueueArgs["x-dead-letter-routing-key"] = "dls"

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan Amqp091Message)
	defer close(msgs)
	go func() {
		mm := Amqp091Message{}
		mm.ContentType = "application/json"
		mm.ContentEncoding = "text"
		mm.Headers = make(Amqp091Table)
		mm.Headers["something"] = "somethingelse"
		mm.DeliveryTag = 1
		delMock := mock.Mock{}
		delMock.On("Ack").Return(nil)
		mm.SetDelivery(&delMock)

		msgs <- mm
	}()

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "addressname", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "key", Value: "value"})
	filter := &pb.Filter{Matches: matches, Type: pb.Filter_ANY}
	src := &pb.Source{Name: "srcname",
		Address: address,
		Options: options,
		Filter: filter,
		Durable: true,
		Exclusive: true,
		AutoDelete: true,
		PrefetchCount: 4}

	expectedMatchHeaders := Amqp091Table{}
	expectedMatchHeaders["key"] = "value"
	expectedMatchHeaders["x-match"] = "any"

	cmock.On("SetPrefetch", 4).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeDeclare", parent.GetName(), "direct", parent.GetDurable(), parent.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeBind", address.GetName(), subjects[0], parent.GetName()).Return(nil)
	cmock.On("ExchangeBind", address.GetName(), subjects[1], parent.GetName()).Return(nil)
	cmock.On("QueueDeclare", src.GetName(), src.GetDurable(), src.GetAutoDelete(), src.GetExclusive(), expectedQueueArgs).Return(nil)
	cmock.On("QueueBind", src.GetName(), "subject1", address.GetName(), expectedMatchHeaders).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject2", address.GetName(), expectedMatchHeaders).Return(nil).Once()
	cmock.On("Consume", src.GetName(), false, src.GetExclusive()).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	go func() {
		msg = <-mc
	}()

	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(&ctx, msg)
	assert.NotNil(t, msg)
	assert.Nil(t, err)
	assert.NotNil(t, msg.GetAddress())
	assert.Equal(t, msg.GetAddress(), src.GetAddress())
	assert.Equal(t, msg.GetAddress().GetSubjects(), subjects)

	cmock.AssertCalled(t, "SetPrefetch", 4)
	cmock.AssertNumberOfCalls(t, "QueueBind", 2)
	cmock.AssertCalled(t, "QueueBind", src.Name, address.Subjects[0], address.Name, expectedMatchHeaders)
	cmock.AssertCalled(t, "QueueBind", src.Name, address.Subjects[1], address.Name, expectedMatchHeaders)
	cmock.AssertCalled(t, "ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete())
	cmock.AssertCalled(t, "ExchangeDeclare", parent.GetName(), "direct", parent.GetDurable(), parent.GetAutoDelete())
	cmock.AssertCalled(t, "ExchangeBind", address.GetName(), subjects[0], parent.GetName())
	cmock.AssertCalled(t, "ExchangeBind", address.GetName(), subjects[1], parent.GetName())
	cmock.AssertCalled(t, "Consume", src.GetName(), false, src.GetExclusive())
}

func Test_Subscribe_UnsupportedOptions(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string, 0)
	options["unsupported"] = "100"

	expectedOptions := make(map[string]interface{})
	expectedOptions["x-message-ttl"] = 100

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan Amqp091Message)
	defer close(msgs)

	cmock.On("SetPrefetch", mock.Anything).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("ExchangeBind", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}, Options: options}
	mc := make(chan *pb.Message)
	defer close(mc)
	suberr := prov.Subscribe(&ctx, src, mc)
	assert.NotNil(t, suberr)
	assert.Contains(t, suberr.GetMessage(), "unsupported is an unsupported source option")

}

// Disconnect does not return anything so there isn't much to test
func Test_Disconnect(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	prov.Disconnect(&ctx)
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewAMQP091Provider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
}

func Test_WaitForConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{blockConnect: 1}
	amock.On("Connect").Return(nil).Times(2)
	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	errs <- Amqp091Error{}
	time.Sleep(250 * time.Millisecond)
	connected := prov.WaitForConnect(&ctx)
	assert.True(t, connected)
}

func Test_Publish(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
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

	expectedMsg := Amqp091Message{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.DeliveryMode = 2 // persistent
	expectedMsg.ContentType = msg.Headers["Content-Type"]
	expectedMsg.ContentEncoding = msg.Headers["Content-Encoding"]
	expectedMsg.Headers = Amqp091Table{}
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete()).Return(nil).Once()
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
		// close(mc)
		// close(errchan)
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(&ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	cmock.AssertNumberOfCalls(t, "Publish", 1)
	cmock.AssertCalled(t, "ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete())
	cmock.AssertCalled(t, "Publish", address.GetName(), address.GetSubjects()[0], expectedMsg)

}

func Test_Publish_Error(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], mock.Anything).Return(errors.New("puberr"))
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeBind", mock.Anything, mock.Anything).Return(nil)
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
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
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(&ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "puberr")

	cmock.AssertNumberOfCalls(t, "Publish", 1)
	cmock.AssertCalled(t, "ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete())
	cmock.AssertCalled(t, "Publish", address.GetName(), address.GetSubjects()[0], mock.Anything)
}

func Test_Publish_ErrorDeclareExchange(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], mock.Anything).Return(errors.New("puberr"))
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete()).Return(errors.New("declareerr")).Once()
	cmock.On("ExchangeBind", mock.Anything, mock.Anything).Return(nil)
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan Amqp091Error, 0)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel").Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	msg := &pb.Message{Address: address, Body: []byte("thebody"), Persistent: true}

	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientUUID = oldGetClientUUID
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(&ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "declareerr")

	cmock.AssertCalled(t, "ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete())
}

// func Test_Publish_ChannelCloseError(t *testing.T) {
// 	prov := NewAMQP091Provider()

// 	oldGetClientUUID := GetClientUUID
// 	GetClientUUID = func(context.Context) (string, error) {
// 		return "1234", nil
// 	}

// 	subjects := make([]string, 0)
// 	subjects = append(subjects, "subject1")
// 	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

// 	cmock := &amqpChannelMock{}
// 	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], mock.Anything).Return(nil)
// 	cmock.On("Close").Return(nil)
// 	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete()).Return(nil).Once()
// 	cmock.On("ExchangeBind", mock.Anything, mock.Anything, mock.Anything).Return(nil)
// 	amock := &amqpConnectionMock{}
// 	amock.On("Connect").Return(nil)

// 	errs := make(chan Amqp091Error, 0)
// 	amock.On("NotifyClose").Return(errs)
// 	amock.On("NewChannel").Return(cmock, nil)
// 	oldNewAmqpConn091 := NewAmqpConn091
// 	NewAmqpConn091 = func(string, *tls.Config) Amqp091ConnectionShim {
// 		return amock
// 	}

// 	mc := make(chan *pb.Message)
// 	errchan := make(chan *pb.Error)

// 	msg := &pb.Message{Address: address, Body: []byte("thebody")}

// 	msg.Headers = make(map[string]string)
// 	msg.Headers["Content-Type"] = "application/json"
// 	msg.Headers["Content-Encoding"] = "utf8"

// 	defer func() {
// 		GetClientUUID = oldGetClientUUID
// 		NewAmqpConn091 = oldNewAmqpConn091
// 	}()

// 	ctx := context.Background()
// 	cc := &pb.ConnectionConfiguration{}
// 	err := prov.Connect(&ctx, cc, false)
// 	assert.Nil(t, err)

// 	done := make(chan bool)
// 	go func() {
// 		fmt.Println("publishing...")
// 		suberr := prov.Publish(&ctx, mc, errchan)
// 		assert.NotNil(t, suberr)
// 		assert.True(t, false)
// 		done <- true
// 	}()

// 	// go func() {
// 	time.Sleep(100 * time.Millisecond)
// 	fmt.Printf("errs: %v\n", errs)
// 	errs <- NewAmqp091Error("chanerr")
// 	<-done
// 	// }()
// 	// time.Sleep(100 * time.Millisecond)

// 	// cmock.AssertNumberOfCalls(t, "Publish", 1)
// 	// cmock.AssertCalled(t, "ExchangeDeclare", address.GetName(), "headers", address.GetDurable(), address.GetAutoDelete())
// 	// cmock.AssertCalled(t, "Publish", address.GetName(), address.GetSubjects()[0], mock.Anything)
// }
