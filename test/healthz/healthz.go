package main

import (

	// "io"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	address = "localhost:50051"
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp)) //nolint staticcheck
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewHealthzClient(conn)
	ctx := context.Background()
	stream, err := c.Check(ctx)
	if err != nil {
		fmt.Println("err on check", err)
	}

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		os.Exit(1)
	}()

	// receive messages in a goroutine
	go func() {
		for {
			hlth, err := stream.Recv()
			if err != nil {
				fmt.Printf("error on recv %s\n", err)
				return
			}
			if status := hlth.GetStatus(); status != nil {
				fmt.Printf("health for uuid %s is %s\n", status.GetUuid(), status.GetCode())
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	for {
		hlth := &pb.Health{}
		hc := &pb.Health_Check{}
		hc.Check = &pb.HealthCheck{Uuid: util.GenUUID()}

		hlth.Resp = hc
		err := stream.Send(hlth)
		if err != nil {
			fmt.Println("error on send", err)
			return
		}
		<-ticker.C
	}
}
