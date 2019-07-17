package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"

	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	// mp "sassoftware.io/convoy/arke/pkg/provider/mock"
)

func init() {
	// Register the MockProvider with the Provider factory.
	provider.Register("mockp", NewMockProvider)
	provider.GetProvider("mockp")
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
	MockErrors   []*pb.Error
}

type MockContext struct {
	mock.Mock
	context.Context
}

const provName string = "mockp"

// NewMockProvider creates a new provider
func NewMockProvider() provider.Provider {
	prov := &MockProvider{}
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
	fmt.Printf("Args to connect : %v\n", args)
	if len(prov.MockErrors) > 0 {
		err := prov.MockErrors[0]
		return err
	}

	err := args.Get(0).(*pb.Error)
	return err
}

// Connect connect to broker
func (prov *MockProvider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration) *pb.Error {
	args := prov.Called(ctx, cf)
	fmt.Printf("Args to connect : %v\n", args)
	if len(prov.MockErrors) > 0 {
		err := prov.MockErrors[0]
		return err
	}

	//err := args.Get(0).(*pb.Error)
	return nil
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

	srv := &ProducerServer{Provider: prov}

	assert.NotNil(t, srv)
}

// TestProducerServerNew creates a new producer server
func TestConsumerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &ConsumerServer{Provider: prov}

	assert.NotNil(t, srv)
}

func TestConsumerServerConnect_Success(t *testing.T) {
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{Provider: provName}
	//mockp := &MockProvider{}
	mocky, _ := provider.GetProvider(provName)
	mockp := mocky.(*MockProvider)
	(*mockp).On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	mocky = mockp
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Success(t *testing.T) {
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{Provider: provName}
	//mockp := &MockProvider{}
	mocky, _ := provider.GetProvider(provName)
	mockp := mocky.(*MockProvider)
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	srv := &ProducerServer{Provider: mockp}

	connectResp, err := srv.Connect(ctx, cf)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerConnect_Fail(t *testing.T) {
	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	mocky, _ := provider.GetProvider("mockp")
	mockp := mocky.(*MockProvider)
	//mockp := &MockProvider{}
	errMsg := pb.Error{Message: expectedErrorMessage}
	mockp.MockErrors = make([]*pb.Error, 0)
	mockp.MockErrors = append(mockp.MockErrors, &errMsg)

	//(*mocky).(*MockProvider).On("Connect", mock.Anything, mock.Anything).Return(&errMsg)
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&errMsg)
	mocky = mockp
	cf := &pb.ConnectionConfiguration{Provider: provName}
	//log.Printf("Prov mocky %v\n", mocky)
	//log.Printf("Prov mockp %v\n", mockp)
	//fmt.Printf("Provider mocky in server_test : %v\n", uintptr(unsafe.Pointer(mocky)))
	fmt.Printf("Provider mocky in server_test : %p\n", mocky)
	fmt.Printf("Provider in server_test : %p\n", mockp)
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Fail(t *testing.T) {

	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{Provider: provName}
	//mockp := &MockProvider{}
	mocky, _ := provider.GetProvider("mockp")
	mockp := mocky.(*MockProvider)
	errMsg := pb.Error{Message: expectedErrorMessage}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&errMsg)

	srv := &ProducerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerAckMessage_Fail(t *testing.T) {

	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	mockp := &MockProvider{}

	errMsg := pb.Error{Message: expectedErrorMessage}
	msg := &pb.Message{Body: []byte("message body")}

	mockp.On("Ack", mock.Anything, mock.Anything).Return(&errMsg)

	srv := &ConsumerServer{Provider: mockp}
	resp, err := srv.AckMessage(ctx, msg)

	assert.NotNil(t, resp)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerNckMessage_Fail(t *testing.T) {

	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	mockp := &MockProvider{}

	errMsg := pb.Error{Message: expectedErrorMessage}
	msg := &pb.Message{Body: []byte("message body")}

	mockp.On("Nack", mock.Anything, mock.Anything).Return(&errMsg)

	srv := &ConsumerServer{Provider: mockp}
	resp, err := srv.NackMessage(ctx, msg)

	assert.NotNil(t, resp)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerAck_Success(t *testing.T) {
	ctx := context.Background()
	msg := &pb.Message{}
	mockp := &MockProvider{}
	mockp.On("Ack", mock.Anything, mock.Anything).Return(&pb.Error{})
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.AckMessage(ctx, msg)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerMack_Success(t *testing.T) {
	ctx := context.Background()
	msg := &pb.Message{}
	mockp := &MockProvider{}
	mockp.On("Nack", mock.Anything, mock.Anything).Return(&pb.Error{})
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.NackMessage(ctx, msg)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_SuccessNoUUID(t *testing.T) {
	ctx := context.Background()
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_SuccessNoUUID(t *testing.T) {
	ctx := context.Background()
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	srv := &ProducerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	old := GetClientUUID
	defer func() { GetClientUUID = old }()
	GetClientUUID = func(context.Context) (string, error) {
		return "123", nil
	}
	srv := &ConsumerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	old := GetClientUUID
	defer func() { GetClientUUID = old }()
	GetClientUUID = func(context.Context) (string, error) {
		return "123", nil
	}
	srv := &ProducerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerSendMessage_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	mockp := &MockProvider{}
	msg := &pb.Message{}

	mockp.On("Publish", mock.Anything, mock.Anything).Return(true, &pb.Error{})
	srv := &ProducerServer{Provider: mockp}
	resp, err := srv.SendMessage(ctx, msg)
	assert.NotNil(t, resp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerSendMessage_Fail(t *testing.T) {
	expectedErrorMessage := "this is my error"
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	mockp := &MockProvider{}
	msg := &pb.Message{}

	mockp.On("Publish", mock.Anything, mock.Anything).Return(false, &pb.Error{Message: expectedErrorMessage})
	srv := &ProducerServer{Provider: mockp}
	resp, err := srv.SendMessage(ctx, msg)
	assert.NotNil(t, resp)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestConsumerServerSubscribe(t *testing.T) {
	expectedErrorMessage := "this is my error"
	// ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	mockp := &MockProvider{}
	// msg := &pb.Message{}
	source := &pb.Source{}
	stream := &MockConsumerSubscribeServerStream{}

	mockp.MockMessages = make([]*pb.Message, 0)
	mockp.MockMessages = append(mockp.MockMessages, &pb.Message{})
	mockp.MockMessages = append(mockp.MockMessages, &pb.Message{})

	// mockp.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	stream.On("Send", mock.Anything).Return(nil).Once()
	// Have to send an error to stop the loop
	stream.On("Send", mock.Anything).Return(errors.New(expectedErrorMessage)).Once()
	srv := &ConsumerServer{Provider: mockp}
	err := srv.Subscribe(source, stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}
