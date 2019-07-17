package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"sassoftware.io/convoy/arke/pkg/provider"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/server"

	pb "sassoftware.io/convoy/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	port = ":50051"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	// Set up cpu and memory profiling if passed in as args
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

	// FIXME: Create some sort of provider factory
	prov, _ := provider.GetProvider("amqp091")

	pb.RegisterProducerServer(s, &server.ProducerServer{Provider: prov})
	pb.RegisterConsumerServer(s, &server.ConsumerServer{Provider: prov})

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
