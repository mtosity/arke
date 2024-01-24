package arke

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/soheilhy/cmux"
	metrics "sassoftware.io/convoy/arke/pkg/metrics/prometheus"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors" // initializes providers
	"sassoftware.io/convoy/arke/pkg/server"
	prometheus "sassoftware.io/convoy/arke/pkg/server/prometheus"
	"sassoftware.io/convoy/arke/pkg/util"
	"sassoftware.io/convoy/arke/pkg/util/tracing"
	"go.opentelemetry.io/otel/sdk/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "sassoftware.io/convoy/arke/api"

	"google.golang.org/grpc/reflection"
)

type Arke struct {
	port           int
	certFile       string
	certKey        string
	server         *grpc.Server
	tlsSkipVerify  bool
	serverOptions  []grpc.ServerOption
	tracerProvider *trace.TracerProvider
}

func (a Arke) WithTLSSkipVerify(tlsSkipVerify bool) Arke {
	a.tlsSkipVerify = tlsSkipVerify
	return a
}

func (a Arke) WithPort(port int) Arke {
	a.port = port
	return a
}

func (a Arke) WithCertFilePath(path string) Arke {
	a.certFile = path
	return a
}

func (a Arke) WithCertKeyPath(path string) Arke {
	a.certKey = path
	return a
}

func DefaultArkeServer() Arke {

	a := Arke{
		port: 50051,
	}

	tp, err := tracing.InitTracerProvider()
	if err != nil {
		log.Fatal(err)
	}
	a.tracerProvider = tp
	fmt.Println(tp)

	// If we have a memory limit, set the runntime
	// soft memory limit to help prevent OOM Kills
	memLimit := util.GetMemoryLimit()
	if memLimit > 0 {
		debug.SetMemoryLimit(memLimit)
	}

	portEnv := os.Getenv("PORT")
	if portEnv != "" {
		var err error
		a.port, err = strconv.Atoi(portEnv)
		if err != nil {
			util.Logger.FatalI("error.port", err)
		}
	}

	kp := keepalive.ServerParameters{
		Time:    20 * time.Second,
		Timeout: 60 * time.Second,
		// Disconnect clients that have been idle for 5 minutes.
		// Idleness on bidirectional streams only kicks in when there are
		// no open streams.
		MaxConnectionIdle: 5 * time.Minute,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	a.serverOptions = append(a.serverOptions, grpc.KeepaliveEnforcementPolicy(kaep))
	a.serverOptions = append(a.serverOptions, grpc.KeepaliveParams(kp))
	// Add two Prometeus server options
	a.serverOptions = append(a.serverOptions, grpc.UnaryInterceptor(prometheus.UnaryInterceptor))
	a.serverOptions = append(a.serverOptions, grpc.StreamInterceptor(prometheus.StreamInterceptor))

	a.server = grpc.NewServer(a.serverOptions...)
	return a
}

func (a Arke) Serve() error {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			a.server.Stop()
		}
	}()

	if a.tracerProvider != nil {
		defer func() {
			if err := a.tracerProvider.Shutdown(context.Background()); err != nil {
				log.Printf("Error shutting down tracer provider: %v", err)
			}
		}()
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", a.port))

	if err != nil {
		util.Logger.ErrorI("error.netlisten", err.Error())
		return err
	}

	util.Logger.Debug("Registering producer and consumer services")
	pb.RegisterProducerServer(a.server, &server.ProducerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterConsumerServer(a.server, &server.ConsumerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterHealthzServer(a.server, &server.HealthzServer{})

	util.Logger.Debug("Registering health check service")
	server.RegisterHealthServer(a.server)

	util.Logger.Debug("Registering reflection service")
	reflection.Register(a.server)

	util.Logger.InfoI("info.starting", a.port)

	if a.certFile != "" && a.certKey != "" {
		certificate, err := tls.LoadX509KeyPair(a.certFile, a.certKey)
		if err != nil {
			util.Logger.ErrorI("error.tls", err.Error())
			return err
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

	go a.server.Serve(grpcListener) // nolint errcheck
	// To emit prometeus metrics for arke
	go metrics.Serve(&httpListener)

	if err := mx.Serve(); err != nil {
		switch err.(type) { //nolint gocritic
		case *net.OpError:
			return err
		}
		util.Logger.ErrorI("error.failedserve", err.Error())
	}
	return nil
}
