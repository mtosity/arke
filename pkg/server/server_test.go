package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"

	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	. "sassoftware.io/convoy/arke/pkg/server"
	// mp "sassoftware.io/convoy/arke/pkg/provider/mock"
)

var ctx context.Context
var cf *pb.ConnectionConfiguration
var mockp *MockProvider
var conSrv *ConsumerServer
var proSrv *ProducerServer
var expectedErrorMessage string = "this is my error"
var errMsg pb.Error = pb.Error{Message: expectedErrorMessage}

func init() {
	// Register the MockProvider with the Provider factory.
	provider.Register("mockp", NewMockProvider)

	// Setup our tests
	ctx = context.Background()
	cf = &pb.ConnectionConfiguration{Provider: provName}
	mocky, _ := provider.GetProvider(provName)
	mockp = mocky.(*MockProvider)
	conSrv = &ConsumerServer{}
	proSrv = &ProducerServer{}
}

type MockConsumerConsumeServerStream struct {
	mock.Mock
	pb.Consumer_ConsumeServer
}

type MockPubRecv struct {
	Message *pb.Message
	Error   error
}

type MockProducerPublishServerStream struct {
	mock.Mock
	pb.Producer_PublishServer
	Receives   []*MockPubRecv
	SendErrors []error
}

func (stream *MockConsumerConsumeServerStream) Send(msg *pb.ConsumeResponse) error {
	args := stream.Called(msg)

	errArg := args.Get(0)
	var err error
	if errArg == nil {
		err = nil
	} else {
		err = errArg.(error)
	}
	return err
}

func (stream *MockConsumerConsumeServerStream) Recv() (*pb.Consume, error) {
	args := stream.Called()

	var cnsm *pb.Consume
	c := args.Get(0)
	if c == nil {
		cnsm = nil
	} else {
		cnsm = c.(*pb.Consume)
	}

	errArg := args.Get(1)
	var err error
	if errArg == nil {
		err = nil
	} else {
		err = errArg.(error)
	}
	return cnsm, err
}

func (stream *MockConsumerConsumeServerStream) Context() context.Context {
	ctx := context.Background()
	return ctx
}

func (stream *MockProducerPublishServerStream) Context() context.Context {
	ctx := context.Background()
	return ctx
}

func (stream *MockProducerPublishServerStream) Recv() (*pb.Message, error) {
	responses := stream.Receives
	var resp *MockPubRecv
	if len(stream.Receives) > 0 {
		resp, responses = responses[0], responses[1:]
	} else {
		resp = &MockPubRecv{}
	}
	stream.Receives = responses
	return resp.Message, resp.Error
}

func (stream *MockProducerPublishServerStream) Send(*pb.MessageResponse) error {
	errors := stream.SendErrors
	var err error
	if len(stream.SendErrors) > 0 {
		err, errors = errors[0], errors[1:]
	} else {
		err = nil
	}
	stream.SendErrors = errors

	return err
}

type MockProvider struct {
	mock.Mock
	provider.Provider
	MockMessages []*pb.Message
}

type MockContext struct {
	mock.Mock
	context.Context
}

const provName string = "mockp"

// NewMockProvider creates a new provider
func NewMockProvider() provider.Provider {
	prov := &MockProvider{}
	GetClientUUID = func(context.Context) (string, error) {
		return "123", nil
	}
	return prov
}

// Ack ack a message
func (prov *MockProvider) Ack(ctx *context.Context, msgid string) *pb.Error {
	args := prov.Called(ctx, msgid)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// Nack nack a message
func (prov *MockProvider) Nack(ctx *context.Context, msgid string) *pb.Error {
	args := prov.Called(ctx, msgid)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// Connect connect to broker
func (prov *MockProvider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	args := prov.Called(ctx, cf, tlsSkipVerify)

	err := args.Get(0).(*pb.Error)
	return err
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *MockProvider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message, stopChannel <-chan bool) *pb.Error {
	args := prov.Called(ctx, source, messageChannel)

	for _, msg := range prov.MockMessages {
		messageChannel <- msg
	}

	prov.MockMessages = make([]*pb.Message, 0)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}

	return err
}

// Disconnect disconnect from the broker
func (prov *MockProvider) Disconnect(ctx *context.Context) {
}

// Publish publish a message to the broker
func (prov *MockProvider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {

	args := prov.Called(ctx, cf)

	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}

	for {
		select {
		case _ = <-messageChannel:
			errChan <- nil
		case <-time.After(2 * time.Second):
			return nil
		}
	}
}

func (prov *MockProvider) WaitForConnect(ctx *context.Context) bool {
	args := prov.Called(ctx)

	tf := args.Get(0).(bool)
	return tf
}

func (prov *MockProvider) SupportedSourceOptions() map[string]bool {
	opts := make(map[string]bool)
	opts["option1"] = true
	return opts
}

func (prov *MockProvider) MockConnect() {
	prov.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
}

// TestProducerServerNew creates a new producer server
func TestProducerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &ProducerServer{}

	assert.NotNil(t, srv)
}

// TestProducerServerNew creates a new producer server
func TestConsumerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &ConsumerServer{}

	assert.NotNil(t, srv)
}

func TestConsumerServerConnect_Success(t *testing.T) {
	// We have to clear the ExpectedCalls before each test.
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	(*mockp).On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	connectResp, err := conSrv.Connect(ctx, cf)
	conSrv.Disconnect(ctx, &pb.Empty{})
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{})
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerConnect_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&errMsg)
	connectResp, err := conSrv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestServerConnectBadProvider_Fail(t *testing.T) {
	config := &pb.ConnectionConfiguration{Provider: "unknown"}
	connectResp, err := proSrv.Connect(ctx, config)

	assert.NotNil(t, connectResp)
	assert.Regexp(t, regexp.MustCompile("Invalid provider name"), err.Error())
}

func TestServerConnectTwice_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf)
	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{})

	connTwiceError := "can not call Connect more than once. Call Disconnect and try again"
	assert.NotNil(t, connectResp)
	assert.Equal(t, connTwiceError, err.Error())

	mockp.AssertExpectations(t)
}

func TestProducerServerPublish_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("publish_sucess message body")}

	proSrv.Connect(ctx, cf)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Error: io.EOF})

	mockp.On("Publish", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(nil).Once()
	// mockp.On("Publish", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(&pb.Error{}).Once()
	// mockp.On("WaitForConnect", mock.AnythingOfType("*context.Context").Return(false)

	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, nil)
	err := proSrv.Publish(stream)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishRecv_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("pub recv fail")}

	proSrv.Connect(ctx, cf)
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Error: errors.New("recverror")})
	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, nil)

	mockp.On("Publish", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(nil).Once()

	err := proSrv.Publish(stream)
	assert.NotNil(t, err)
	assert.Equal(t, "recverror", err.Error())

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishSend_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("pub send fail")}

	proSrv.Connect(ctx, cf)
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, errors.New("senderror"))

	mockp.On("Publish", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(nil).Once()

	err := proSrv.Publish(stream)
	assert.NotNil(t, err)
	assert.Equal(t, "senderror", err.Error())

	mockp.AssertExpectations(t)
}

func TestServerDisconnect_SuccessNoUUID(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "", errors.New("Can't get Client UUID")
	}

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	empty := &pb.Empty{}
	conSrv.Connect(ctx, cf)
	connectResp, err := conSrv.Disconnect(ctx, empty)

	GetClientUUID = oldGetClientUUID

	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestServerDisconnect_FailNoMap(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	conSrv.Connect(ctx, cf)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "1234", nil
	}
	connectResp, err := conSrv.Disconnect(ctx, empty)
	GetClientUUID = oldGetClientUUID

	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	conSrv.Connect(ctx, cf)
	connectResp, err := conSrv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf)
	connectResp, err := proSrv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestServerNoConnect_FAIL(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	prodstream := &MockProducerPublishServerStream{}
	err := proSrv.Publish(prodstream)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Failed to find connection information")

	stream := &MockConsumerConsumeServerStream{}
	subErr := conSrv.Consume(stream)
	assert.NotNil(t, subErr)
}

func TestConsumerServerConsume(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1"}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.AnythingOfType("*context.Context"), source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Ack", mock.AnythingOfType("*context.Context"), mock.Anything).Return(nil)
	mockp.On("WaitForConnect", mock.AnythingOfType("*context.Context")).Return(false)
	conSrv.Connect(ctx, cf)
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_Nack(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1", Nack: true}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.AnythingOfType("*context.Context"), source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Nack", mock.AnythingOfType("*context.Context"), mock.Anything).Return(nil)
	mockp.On("WaitForConnect", mock.AnythingOfType("*context.Context")).Return(false)
	conSrv.Connect(ctx, cf)
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_BadOption(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	sourceOptions["badoption"] = "bad"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Once()
	conSrv.Connect(ctx, cf)
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_AckErr(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1"}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Ack", mock.AnythingOfType("*context.Context"), mock.Anything).Return(&pb.Error{Message: "ackerr"})
	mockp.On("WaitForConnect", mock.AnythingOfType("*context.Context")).Return(false)
	conSrv.Connect(ctx, cf)
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_SourceTwice(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("*arke.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions, PrefetchCount: 1}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	stream.On("Send", mock.AnythingOfType("*arke.ConsumeResponse")).Return(nil, nil).Once() // Can't seem to do anything more fine grained than this. Would like to check nested types
	stream.On("Recv").Return(cnsm, nil).Twice()
	stream.On("Recv").Return(nil, errors.New("just breaking the loop")).After(100 * time.Millisecond)
	mockp.On("Subscribe", mock.AnythingOfType("*context.Context"), mock.Anything, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	conSrv.Connect(ctx, cf)
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}
