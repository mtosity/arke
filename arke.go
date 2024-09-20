package arke

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/sdk/trace"
	"sassoftware.io/viya/arke/i18n"
	metrics "sassoftware.io/viya/arke/pkg/metrics/prometheus"
	_ "sassoftware.io/viya/arke/pkg/provider/connectors" // initializes providers
	"sassoftware.io/viya/arke/pkg/server"
	prometheus "sassoftware.io/viya/arke/pkg/server/prometheus"
	"sassoftware.io/viya/arke/pkg/util"
	"sassoftware.io/viya/arke/pkg/util/tracing"

	"github.com/KimMachineGun/automemlimit/memlimit"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "sassoftware.io/viya/arke/api"

	"google.golang.org/grpc/reflection"
)

type Arke struct {
	port           int
	certFile       string
	certKey        string
	hpaName        string
	server         *grpc.Server
	tlsSkipVerify  bool
	serverOptions  []grpc.ServerOption
	tracerProvider *trace.TracerProvider
	mux            cmux.CMux
}

func (a *Arke) WithTLSSkipVerify(tlsSkipVerify bool) *Arke {
	a.tlsSkipVerify = tlsSkipVerify
	return a
}

func (a *Arke) WithPort(port int) *Arke {
	a.port = port
	return a
}

func (a *Arke) WithCertFilePath(path string) *Arke {
	a.certFile = path
	return a
}

func (a *Arke) WithCertKeyPath(path string) *Arke {
	a.certKey = path
	return a
}

func (a *Arke) WithHpaName(name string) *Arke {
	a.hpaName = name
	return a
}

func defaultKeepAliveParams() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:    20 * time.Second,
		Timeout: 60 * time.Second,
		// Disconnect clients that have been idle for 5 minutes.
		// Idleness on bidirectional streams only kicks in when there are
		// no open streams.
		MaxConnectionIdle: 5 * time.Minute,
	}
}

func defaultKeepAliveEnforcementPolicy() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
}

func DefaultArkeServer() *Arke {

	a := &Arke{
		port:    50051,
		hpaName: "arke",
	}

	tp, err := tracing.InitTracerProvider()
	if err != nil {
		util.Logger.FatalI(i18n.OTELInitError, err)
	}
	a.tracerProvider = tp

	setGoMemLimit()

	portEnv := os.Getenv("PORT")
	if portEnv != "" {
		var err error
		a.port, err = strconv.Atoi(portEnv)
		if err != nil {
			util.Logger.FatalI(i18n.PortParsingError, err)
		}
	}

	kp := defaultKeepAliveParams()

	kaep := defaultKeepAliveEnforcementPolicy()

	a.serverOptions = append(a.serverOptions, grpc.KeepaliveEnforcementPolicy(kaep))
	a.serverOptions = append(a.serverOptions, grpc.KeepaliveParams(kp))
	// Add two Prometheus server options
	a.serverOptions = append(a.serverOptions, grpc.UnaryInterceptor(prometheus.UnaryInterceptor))
	a.serverOptions = append(a.serverOptions, grpc.StreamInterceptor(prometheus.StreamInterceptor))

	a.server = grpc.NewServer(a.serverOptions...)
	return a
}

func setGoMemLimit() {
	_, _ = memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
	)
}

func (a Arke) listener() (net.Listener, error) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", a.port))

	if err != nil {
		return nil, err
	}

	tlsCfg, err := a.tlsConfig()
	if tlsCfg != nil && err == nil {
		lis = tls.NewListener(lis, tlsCfg)
	}

	return lis, nil
}

func (a Arke) tlsConfig() (*tls.Config, error) {
	if a.certFile != "" && a.certKey != "" {
		certificate, err := tls.LoadX509KeyPair(a.certFile, a.certKey)
		if err != nil {
			return nil, err
		}

		config := &tls.Config{
			Certificates: []tls.Certificate{certificate},
			Rand:         rand.Reader,
			NextProtos: []string{
				"h2", "http/1.1",
			},
		}
		return config, nil
	}
	return nil, nil
}

func (a *Arke) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
				util.Logger.ErrorI(i18n.OTELShutdownError, err)
			}
		}()
	}

	lis, err := a.listener()

	if err != nil {
		util.Logger.ErrorI(i18n.NetListenError, err.Error())
		return err
	}

	util.Logger.Debug("Registering producer and consumer services")
	pb.RegisterProducerServer(a.server, &server.ProducerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterConsumerServer(a.server, &server.ConsumerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterHealthzServer(a.server, &server.HealthzServer{})

	util.Logger.Debug("Registering health check service")
	server.RegisterHealthServer(a.server)

	healthChan := make(chan pb.HealthStatus_Code)
	go util.MonitorHPA(healthChan, "sas-arke")
	go server.MonitorHealthChan(healthChan)

	util.Logger.Debug("Registering reflection service")
	reflection.Register(a.server)

	util.Logger.InfoI(i18n.Starting, a.port)

	a.mux = cmux.New(lis)
	httpListener := a.mux.Match(cmux.HTTP1Fast())
	// Matching on application/grpc Content-Type (as suggested) does not seem to work
	// so if we're not HTTP/1, assume gRPC.
	grpcListener := a.mux.Match(cmux.Any())

	go a.server.Serve(grpcListener) // nolint errcheck
	// To emit prometeus metrics for arke
	go metrics.Serve(ctx, &httpListener)

	serveErrChan := make(chan error)
	go func(as *Arke) {
		if err := as.mux.Serve(); err != nil {
			switch err.(type) { //nolint gocritic
			case *net.OpError:
				serveErrChan <- nil
				return
			}
			serveErrChan <- err
		}
		serveErrChan <- nil
	}(a)

	var serveErr error
	select {
	case <-ctx.Done():
		a.mux.Close()
	case serveErr = <-serveErrChan:
		if serveErr != nil {
			util.Logger.ErrorI(i18n.FailedServeError, serveErr.Error())
		}
	}
	return serveErr
}
