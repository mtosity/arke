package server

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterHealthServer registers a health check server for use by consul and client applications
func RegisterHealthServer(s *grpc.Server) *health.Server {
	svr := health.NewServer()
	svr.SetServingStatus("arke", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, svr)
	return svr
}
