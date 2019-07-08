package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"

	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"

	// mp "sassoftware.io/convoy/arke/pkg/provider/mock"
	"sassoftware.io/convoy/arke/pkg/server"
)

type MockProvider struct {
	mock.Mock
	provider.Provider
}

type MockContext struct {
	mock.Mock
	context.Context
}

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

	srv := &server.ProducerServer{Provider: prov}

	assert.NotNil(t, srv)
}

// TestProducerServerNew creates a new producer server
func TestConsumerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &server.ConsumerServer{Provider: prov}

	assert.NotNil(t, srv)
}

func TestConsumerServerConnect_Success(t *testing.T) {
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{}
	mockp := &MockProvider{}
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	srv := &server.ConsumerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Success(t *testing.T) {
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{}
	mockp := &MockProvider{}
	mockp.On("Connect", mock.Anything, mock.Anything).Return(&pb.Error{})
	srv := &server.ProducerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerConnect_Fail(t *testing.T) {

	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{}
	mockp := &MockProvider{}
	errMsg := pb.Error{Message: expectedErrorMessage}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&errMsg)

	srv := &server.ConsumerServer{Provider: mockp}
	connectResp, err := srv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Fail(t *testing.T) {

	expectedErrorMessage := "this is my error"
	ctx := context.Background()
	cf := &pb.ConnectionConfiguration{}
	mockp := &MockProvider{}
	errMsg := pb.Error{Message: expectedErrorMessage}

	mockp.On("Connect", mock.Anything, mock.Anything).Return(&errMsg)

	srv := &server.ProducerServer{Provider: mockp}
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

	srv := &server.ConsumerServer{Provider: mockp}
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

	srv := &server.ConsumerServer{Provider: mockp}
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
	srv := &server.ConsumerServer{Provider: mockp}
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
	srv := &server.ConsumerServer{Provider: mockp}
	connectResp, err := srv.NackMessage(ctx, msg)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_SuccessNoUUID(t *testing.T) {
	ctx := context.Background()
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	srv := &server.ConsumerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_SuccessNoUUID(t *testing.T) {
	ctx := context.Background()
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	srv := &server.ProducerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	old := server.GetClientUUID
	defer func() { server.GetClientUUID = old }()
	server.GetClientUUID = func(context.Context) (string, error) {
		return "123", nil
	}
	srv := &server.ConsumerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}
	mockp := &MockProvider{}
	old := server.GetClientUUID
	defer func() { server.GetClientUUID = old }()
	server.GetClientUUID = func(context.Context) (string, error) {
		return "123", nil
	}
	srv := &server.ProducerServer{Provider: mockp}
	connectResp, err := srv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerSendMessage_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	mockp := &MockProvider{}
	msg := &pb.Message{}
	// old := server.GetClientUUID
	// defer func() { server.GetClientUUID = old }()
	// server.GetClientUUID = func(context.Context) (string, error) {
	// 	return "123", nil
	// }

	mockp.On("Publish", mock.Anything, mock.Anything).Return(true, &pb.Error{})
	srv := &server.ProducerServer{Provider: mockp}
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
	srv := &server.ProducerServer{Provider: mockp}
	resp, err := srv.SendMessage(ctx, msg)
	assert.NotNil(t, resp)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErrorMessage, resp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}
