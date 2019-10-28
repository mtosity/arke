package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/server"
	"sassoftware.io/convoy/arke/pkg/util"

	pb "sassoftware.io/convoy/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"google.golang.org/grpc/reflection"

	_ "sassoftware.io/convoy/arke/pkg/metrics"
)

const (
	port = ":50051"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Force TLS, but always skip verification")

func main() {
	// Set up cpu and memory profiling if passed in as args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			util.Logger.FatalI("error.cpuprofile", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			util.Logger.FatalI("error.cpuprofile", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			util.Logger.FatalI("error.memprofile", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			util.Logger.FatalI("error.memprofile", err)
		}
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		util.Logger.FatalI("error.netlisten", err)
	}
	kp := keepalive.ServerParameters{
		Time:    5 * time.Second,
		Timeout: 1 * time.Second,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
	s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kp),
		grpc.UnaryInterceptor(server.UnaryInterceptor),
		grpc.StreamInterceptor(server.StreamInterceptor))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			// sig is a ^C, handle it
			s.Stop()
		}
	}()

	util.Logger.Debug("Registering producer and consumer services")
	pb.RegisterProducerServer(s, &server.ProducerServer{TLSSkipVerify: *tlsSkipVerify})
	pb.RegisterConsumerServer(s, &server.ConsumerServer{TLSSkipVerify: *tlsSkipVerify})

	util.Logger.Debug("Registering health check service")
	server.RegisterHealthServer(s)

	util.Logger.Debug("Registering reflection service")
	reflection.Register(s)

	util.Logger.Info("info.starting")
	if err := s.Serve(lis); err != nil {
		util.Logger.FatalI("error.failedserve", err)
	}
}
