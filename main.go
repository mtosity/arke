package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/soheilhy/cmux"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/server"
	"sassoftware.io/convoy/arke/pkg/util"

	pb "sassoftware.io/convoy/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"google.golang.org/grpc/reflection"

	"sassoftware.io/convoy/arke/pkg/metrics"
)

var port = flag.String("port", "50051", "Port to serve gRPC and metrics requests")
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

	portEnv := os.Getenv("PORT")
	if portEnv != "" {
		*port = portEnv
		// var err error
		// *port, err = strconv.Atoi(portEnv)
		// if err != nil {
		// 	util.Logger.FatalI("error.port", err)
		// }
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		util.Logger.FatalI("error.netlisten", err.Error())
	}
	kp := keepalive.ServerParameters{
		Time:    5 * time.Second,
		Timeout: 1 * time.Second,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	certFile := os.Getenv("CERT_FILE")
	certKey := os.Getenv("CERT_KEY")

	serverOptions := make([]grpc.ServerOption, 0)
	serverOptions = append(serverOptions, grpc.KeepaliveEnforcementPolicy(kaep))
	serverOptions = append(serverOptions, grpc.KeepaliveParams(kp))
	serverOptions = append(serverOptions, grpc.UnaryInterceptor(server.UnaryInterceptor))
	serverOptions = append(serverOptions, grpc.StreamInterceptor(server.StreamInterceptor))

	if certFile != "" && certKey != "" {
		creds, err := credentials.NewServerTLSFromFile(certFile, certKey)
		if err != nil {
			panic(fmt.Sprintf("Could not load TLS cert and key: %v", err))
		}
		serverOptions = append(serverOptions, grpc.Creds(creds))
	}

	s := grpc.NewServer(serverOptions...)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
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

	util.Logger.InfoI("info.starting", *port)

	mx := cmux.New(lis)
	httpListener := mx.Match(cmux.HTTP1Fast())
	// Matching on application/grpc Content-Type (as suggested) does not seem to work
	// so if we're not HTTP/1, assume gRPC.
	grpcListener := mx.Match(cmux.Any())

	go s.Serve(grpcListener)
	go metrics.Serve(&httpListener)

	if err := mx.Serve(); err != nil {
		switch err.(type) {
		case *net.OpError:
			return
		}
		util.Logger.FatalI("error.failedserve", err.Error())
	}
}
