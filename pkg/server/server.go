package server

import (
	"context"
	"errors"
	"log"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

// GetClientUUID Set function as a variable so we can replace the util.GetClientUUID method in unit tests
var GetClientUUID = util.GetClientUUID

// ProducerServer producer server struct
type ProducerServer struct {
	Provider provider.Provider
}

// ConsumerServer consumer server struct
type ConsumerServer struct {
	Provider provider.Provider
}

// Connect Connect for the producer server
func (s *ProducerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	errMsg := s.Provider.Connect(&ctx, cf)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

// Connect Connect for the consumer server
func (s *ConsumerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	errMsg := s.Provider.Connect(&ctx, cf)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

// AckMessage Ack a message for the consumer
func (s *ConsumerServer) AckMessage(ctx context.Context, msg *pb.Message) (*pb.AckResponse, error) {

	success := true
	errMsg := s.Provider.Ack(&ctx, msg)
	var err error

	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	return &pb.AckResponse{Success: success, Error: errMsg}, err
}

// NackMessage nack a message for the consumer
func (s *ConsumerServer) NackMessage(ctx context.Context, msg *pb.Message) (*pb.NackResponse, error) {
	success := true
	errMsg := s.Provider.Nack(&ctx, msg)
	var err error

	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	return &pb.NackResponse{Success: success, Error: errMsg}, err
}

// Subscribe subscribe to a stream of messages for the consumer
func (s *ConsumerServer) Subscribe(source *pb.Source, stream pb.Consumer_SubscribeServer) error {

	messageChannel := make(chan *pb.Message)
	ctx := stream.Context()
	forever := make(chan bool)
	var returnError error
	go func(mc <-chan *pb.Message, prov provider.Provider, ctx *context.Context) {

		for {
			message := <-mc
			err := stream.Send(message)
			if err != nil {
				log.Printf("Could not send message: %s", err.Error())
				// Could not send the message, so disconnect
				prov.Disconnect(ctx)
				returnError = err
				forever <- false
				break
			}
		}
	}(messageChannel, s.Provider, &ctx)
	s.Provider.Subscribe(&ctx, source, messageChannel)
	<-forever
	return returnError
}

// SendMessage send a message to the server
func (s *ProducerServer) SendMessage(ctx context.Context, msg *pb.Message) (*pb.MessageResponse, error) {
	success, errMsg := s.Provider.Publish(&ctx, msg)
	resp := &pb.MessageResponse{Success: success}
	var err error
	if success != true {
		resp.Error = errMsg
		err = errors.New(errMsg.GetMessage())
	}
	return resp, err
}

// Disconnect disconnect from the consumer server
func (s *ConsumerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	s.Provider.Disconnect(&ctx)
	log.Printf("Client %s disconnected", clientUUID)
	return &pb.Empty{}, nil
}

// Disconnect disconnect from the producer server
func (s *ProducerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	s.Provider.Disconnect(&ctx)
	log.Printf("Client %s disconnected", clientUUID)
	return &pb.Empty{}, nil
}
