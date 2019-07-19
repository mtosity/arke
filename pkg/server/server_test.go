package server_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

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

type MockConsumerSubscribeServerStream struct {
	mock.Mock
	pb.Consumer_SubscribeServer
}

func NewMockConsumerSubscribeServerStream() pb.Consumer_SubscribeServer {
	stream := &MockConsumerSubscribeServerStream{}
	return stream
}

func (stream *MockConsumerSubscribeServerStream) Send(msg *pb.Message) error {
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

func (stream *MockConsumerSubscribeServerStream) Context() context.Context {
	ctx := context.Background()
	return ctx
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
func (prov *MockProvider) Ack(ctx *context.Context, msg *pb.Message) *pb.Error {
	args := prov.Called(ctx, msg)

	err := args.Get(0).(*pb.Error)
	return err
}

// Nack nack a message
func (prov *MockProvider) Nack(ctx *context.Context, msg *pb.Message) *pb.Error {
	args := prov.Called(ctx, msg)

	err := args.Get(0).(*pb.Error)
	return err
}

// Connect connect to broker
func (prov *MockProvider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration) *pb.Error {
	args := prov.Called(ctx, cf)

	err := args.Get(0).(*pb.Error)
	return err
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *MockProvider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {
	// prov.MessageChannel
	for _, msg := range prov.MockMessages {
		messageChannel <- msg
	}
	close(messageChannel)
	return &pb.Error{}
}

// Disconnect disconnect from the broker
func (prov *MockProvider) Disconnect(ctx *context.Context) {
}

// Publish publish a message to the broker
func (prov *MockProvider) Publish(ctx *context.Context, message *pb.Message) (bool, *pb.Error) {

	args := prov.Called(ctx, message)
	// err := args.Get(1).(*pb.Error)
	return args.Get(0).(bool), args.Get(1).(*pb.Error)
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
	(*mockp).On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	connectResp, err := conSrv.Connect(ctx, cf)
	conSrv.Disconnect(ctx, &pb.Empty{})
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{})
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerConnect_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&errMsg)
	connectResp, err := conSrv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestServerConnectBadProvider_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	config := &pb.ConnectionConfiguration{Provider: "unknown"}
	connectResp, err := proSrv.Connect(ctx, config)

	assert.NotNil(t, connectResp)
	assert.Regexp(t, regexp.MustCompile("Invalid provider name"), err.Error())
}

func TestServerConnectTwice_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

	proSrv.Connect(ctx, cf)
	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{})

	connTwiceError := "Can not call Connect more than once. Call Disconnect and try again."
	assert.NotNil(t, connectResp)
	assert.Equal(t, connTwiceError, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerAckMessage_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	msg := &pb.Message{Body: []byte("message body")}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	mockp.On("Ack", mock.Anything, mock.Anything).Return(&errMsg)

	conSrv.Connect(ctx, cf)
	resp, err := conSrv.AckMessage(ctx, msg)
	conSrv.Disconnect(ctx, &pb.Empty{})

	assert.NotNil(t, resp)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerNckMessage_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	msg := &pb.Message{Body: []byte("message body")}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	mockp.On("Nack", mock.Anything, mock.Anything).Return(&errMsg)

	conSrv.Connect(ctx, cf)
	resp, err := conSrv.NackMessage(ctx, msg)

	assert.NotNil(t, resp)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerAck_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	msg := &pb.Message{}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	mockp.On("Ack", mock.Anything, mock.Anything).Return(&pb.Error{})

	conSrv.Connect(ctx, cf)
	connectResp, err := conSrv.AckMessage(ctx, msg)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerNack_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	msg := &pb.Message{}
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	mockp.On("Nack", mock.Anything, mock.Anything).Return(&pb.Error{})
	conSrv.Connect(ctx, cf)
	connectResp, err := conSrv.NackMessage(ctx, msg)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerSendMessage_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	//ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{}

	mockp.On("Publish", mock.Anything, mock.Anything).Return(true, &pb.Error{})
	proSrv.Connect(ctx, cf)
	resp, err := proSrv.SendMessage(ctx, msg)
	assert.NotNil(t, resp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerSendMessage_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{}

	mockp.On("Publish", mock.Anything, mock.Anything).Return(false, &errMsg)
	proSrv.Connect(ctx, cf)
	resp, err := proSrv.SendMessage(ctx, msg)
	assert.NotNil(t, resp)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerSubscribe(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	source := &pb.Source{}
	stream := &MockConsumerSubscribeServerStream{}

	mockp.MockMessages = make([]*pb.Message, 0)
	mockp.MockMessages = append(mockp.MockMessages, &pb.Message{})
	mockp.MockMessages = append(mockp.MockMessages, &pb.Message{})

	stream.On("Send", mock.Anything).Return(nil).Once()
	// Have to send an error to stop the loop
	stream.On("Send", mock.Anything).Return(errors.New(expectedErrorMessage)).Once()
	conSrv.Connect(ctx, cf)
	err := conSrv.Subscribe(source, stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestServerDisconnect_SuccessNoUUID(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	oldGetClientUUID := GetClientUUID
	GetClientUUID = func(context.Context) (string, error) {
		return "", errors.New("Can't get Client UUID")
	}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

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

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

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

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

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

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})

	proSrv.Connect(ctx, cf)
	connectResp, err := proSrv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestServerNoConnect_FAIL(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	msg := &pb.Message{}
	sendResp, sendErr := proSrv.SendMessage(ctx, msg)
	assert.NotNil(t, sendErr)
	assert.NotNil(t, sendResp)
	assert.False(t, sendResp.Success)

	ackResp, ackErr := conSrv.AckMessage(ctx, msg)
	assert.NotNil(t, ackErr)
	assert.NotNil(t, ackResp)
	assert.False(t, ackResp.Success)

	nackResp, nackErr := conSrv.NackMessage(ctx, msg)
	assert.NotNil(t, nackErr)
	assert.NotNil(t, nackResp)
	assert.False(t, nackResp.Success)

	source := &pb.Source{}
	stream := &MockConsumerSubscribeServerStream{}
	subErr := conSrv.Subscribe(source, stream)
	assert.NotNil(t, subErr)
}
