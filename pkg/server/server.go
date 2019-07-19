package server

import (
	"context"
	"errors"
	"log"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/util"
)

// GetClientUUID Set function as a variable so we can replace the util.GetClientUUID method in unit tests
var GetClientUUID = util.GetClientUUID

// Map that tracks our connection information
// TODO: This leaks, we need a way to prune this map
// if a client goes away without calling Disconnect().
var connectionMap = util.NewConcurrentMap()

// ProducerServer producer server struct
type ProducerServer struct {
}

// ConsumerServer consumer server struct
type ConsumerServer struct {
}

// Connect Connect for the producer server
func (s *ProducerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf)
	return resp, err
}

// Connect Connect for the consumer server
func (s *ConsumerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf)
	return resp, err
}

// AckMessage Ack a message for the consumer
func (s *ConsumerServer) AckMessage(ctx context.Context, msg *pb.Message) (*pb.AckResponse, error) {
	success := true
	prov, findErr := findProvider(ctx)

	if prov == nil {
		ftlError := errors.New(findErr.Message)
		log.Printf("Ack Message failed: %s.", findErr.Message)
		return &pb.AckResponse{Success: false, Error: findErr}, ftlError
	}

	errMsg := prov.Ack(&ctx, msg)

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
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		log.Printf("Nack Message failed: %s.", findErr.Message)
		return &pb.NackResponse{Success: false, Error: findErr}, ftlError
	}

	errMsg := prov.Nack(&ctx, msg)
	var err error

	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	return &pb.NackResponse{Success: success, Error: errMsg}, err
}

// Subscribe subscribe to a stream of messages for the consumer
func (s *ConsumerServer) Subscribe(source *pb.Source, stream pb.Consumer_SubscribeServer) error {
	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		log.Printf("Subscribe failed: %s.", findErr.Message)
		return ftlError
	}

	messageChannel := make(chan *pb.Message)
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
	}(messageChannel, prov, &ctx)
	prov.Subscribe(&ctx, source, messageChannel)
	<-forever
	return returnError
}

// SendMessage send a message to the server
func (s *ProducerServer) SendMessage(ctx context.Context, msg *pb.Message) (*pb.MessageResponse, error) {
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		log.Printf("Send Message failed: %s.", findErr.Message)
		return &pb.MessageResponse{Success: false, Error: findErr}, ftlError
	}

	success, errMsg := prov.Publish(&ctx, msg)
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
	retVal, _ := brokerDisconnect(ctx, empty)
	return retVal, nil
}

// Disconnect disconnect from the producer server
func (s *ProducerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	retVal, _ := brokerDisconnect(ctx, empty)
	return retVal, nil
}

func brokerConnect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	prov, er := provider.GetProvider(cf.GetProvider())
	if er != nil {
		errMsg := &pb.Error{
			Message: er.Error(),
			IsFatal: true,
		}
		return &pb.ConnectResponse{Success: false, Error: errMsg}, er
	}
	// We don't allow a client to call Connect more than once
	clientUUID, clientErr := GetClientUUID(ctx)
	if clientErr != nil {
		err := errors.New(clientErr.Error())
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}
	_, exists := connectionMap.Get(clientUUID)
	if exists {
		err := errors.New("Can not call Connect more than once. Call Disconnect and try again.")
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}
	errMsg := prov.Connect(&ctx, cf)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	if success {
		connectionMap.Add(clientUUID, cf)
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

func brokerDisconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	cf, found := connectionMap.Get(clientUUID)
	if found == true {
		providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
		prov, _ := provider.GetProvider(providerType)
		prov.Disconnect(&ctx)
		connectionMap.Delete(clientUUID)
		log.Printf("Client %s disconnected", clientUUID)
		return &pb.Empty{}, nil
	}

	log.Printf("Disconnect called for client %s, but no connection information found.", clientUUID)
	return &pb.Empty{}, nil
}

func findProvider(ctx context.Context) (provider.Provider, *pb.Error) {
	clientUUID, _ := GetClientUUID(ctx)
	cf, found := connectionMap.Get(clientUUID)
	if !found {
		errMsg := &pb.Error{
			Message: "Failed to find connection information.",
			IsFatal: true,
		}
		log.Printf("Could not find connection information for %s.", clientUUID)
		return nil, errMsg
	}

	providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
	prov, _ := provider.GetProvider(providerType)
	return prov, nil
}
