package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"sassoftware.io/convoy/arke/pkg/provider/amqp091"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	port = ":50051"
)

type producerServer struct {
	prov provider.Provider
}
type consumerServer struct {
	prov provider.Provider
}

func (s *producerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	errMsg := s.prov.Connect(&ctx, cf)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}

	return &pb.ConnectResponse{Success: success}, err
}

func (s *consumerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	errMsg := s.prov.Connect(&ctx, cf)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}

	return &pb.ConnectResponse{Success: success}, err
}

func (s *consumerServer) AckMessage(ctx context.Context, msg *pb.Message) (*pb.AckResponse, error) {

	success := true
	errMsg := s.prov.Ack(&ctx, msg)
	var err error

	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	return &pb.AckResponse{Success: success, Error: errMsg}, err
}

func (s *consumerServer) NackMessage(ctx context.Context, msg *pb.Message) (*pb.NackResponse, error) {
	// Placeholder for now
	return &pb.NackResponse{Success: true}, nil
}

func (s *consumerServer) Subscribe(source *pb.Source, stream pb.Consumer_SubscribeServer) error {

	messageChannel := make(chan *pb.Message)
	ctx := stream.Context()
	forever := make(chan bool)
	go func(chan<- *pb.Message) {

		for {
			message := <-messageChannel
			err := stream.Send(message)
			if err != nil {
				log.Printf(err.Error())
			}
		}
	}(messageChannel)
	s.prov.Subscribe(&ctx, source, messageChannel)
	<-forever
	return nil
}

func (s *producerServer) SendMessage(ctx context.Context, msg *pb.Message) (*pb.MessageResponse, error) {
	success, errMsg := s.prov.Publish(&ctx, msg)
	resp := &pb.MessageResponse{Success: success}
	var err error
	if success != true {
		resp.Error = errMsg
		err = errors.New(errMsg.GetMessage())
	}
	return resp, err
}

func (s *consumerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := util.GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	s.prov.Disconnect(&ctx)
	log.Printf("Client %s disconnected", clientUUID)
	return &pb.Empty{}, nil
}

func (s *producerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := util.GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	s.prov.Disconnect(&ctx)
	log.Printf("Client %s disconnected", clientUUID)
	return &pb.Empty{}, nil
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	kp := keepalive.ServerParameters{
		Time:    5 * time.Second,
		Timeout: 1 * time.Second,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
	s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kp))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			// sig is a ^C, handle it
			s.Stop()
		}
	}()

	prov := amqp091.NewAMQP091Provider()

	pb.RegisterProducerServer(s, &producerServer{prov: prov})
	pb.RegisterConsumerServer(s, &consumerServer{prov: prov})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
