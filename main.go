package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/soheilhy/cmux"
	metrics "sassoftware.io/convoy/arke/pkg/metrics/prometheus"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/server"
	prometheus "sassoftware.io/convoy/arke/pkg/server/prometheus"
	"sassoftware.io/convoy/arke/pkg/util"
	"sassoftware.io/convoy/arke/pkg/util/tracing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "sassoftware.io/convoy/arke/api"

	"google.golang.org/grpc/reflection"
)

var port = flag.String("port", "50051", "Port to serve gRPC and metrics requests")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Force TLS, but always skip verification")

func run() {
	tp, err := tracing.InitTracerProvider()
	if err != nil {
		log.Fatal(err)
	}
	if tp != nil {
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				log.Printf("Error shutting down tracer provider: %v", err)
			}
		}()
	}

	// If we have a memory limit, set the runntime
	// soft memory limit to help prevent OOM Kills
	memLimit := util.GetMemoryLimit()
	if memLimit > 0 {
		debug.SetMemoryLimit(memLimit)
	}

	// Set up cpu and memory profiling if passed in as args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(filepath.Clean(*cpuprofile))
		if err != nil {
			util.Logger.FatalI("error.cpuprofile", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			util.Logger.FatalI("error.cpuprofile", err)
		}
		defer pprof.StopCPUProfile()
	}

	defer func() {
		if *memprofile != "" {
			f, err := os.Create(filepath.Clean(*memprofile))
			if err != nil {
				util.Logger.FatalI("error.memprofile", err)
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				util.Logger.FatalI("error.memprofile", err)
			}
		}
	}()

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
		Time:    20 * time.Second,
		Timeout: 60 * time.Second,
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
	// Add two Prometeus server options
	serverOptions = append(serverOptions, grpc.UnaryInterceptor(prometheus.UnaryInterceptor))
	serverOptions = append(serverOptions, grpc.StreamInterceptor(prometheus.StreamInterceptor))

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
	pb.RegisterHealthzServer(s, &server.HealthzServer{})

	util.Logger.Debug("Registering health check service")
	server.RegisterHealthServer(s)

	util.Logger.Debug("Registering reflection service")
	reflection.Register(s)

	util.Logger.InfoI("info.starting", *port)

	if certFile != "" && certKey != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, certKey)
		if err != nil {
			log.Panicf("Could not load TLS cert and key: %v", err)
		}

		config := &tls.Config{
			Certificates: []tls.Certificate{certificate},
			Rand:         rand.Reader,
			NextProtos: []string{
				"h2", "http/1.1",
			},
		}

		lis = tls.NewListener(lis, config)
	}

	mx := cmux.New(lis)
	httpListener := mx.Match(cmux.HTTP1Fast())
	// Matching on application/grpc Content-Type (as suggested) does not seem to work
	// so if we're not HTTP/1, assume gRPC.
	grpcListener := mx.Match(cmux.Any())

	go s.Serve(grpcListener) // nolint errcheck
	// To emit prometeus metrics for arke
	go metrics.Serve(&httpListener)

	if err := mx.Serve(); err != nil {
		switch err.(type) { //nolint gocritic
		case *net.OpError:
			return
		}
		util.Logger.FatalI("error.failedserve", err.Error())
	}
}

func main() {
	run()
}
