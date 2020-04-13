package prometheus

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"

	"sassoftware.io/convoy/arke/pkg/metrics"
	"sassoftware.io/convoy/arke/pkg/metrics/prometheus"
	"sassoftware.io/convoy/arke/pkg/util"
)

// UnaryInterceptor unary grpc interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	fullMethod := strings.TrimPrefix(info.FullMethod, "/")
	fullMethod = strings.ReplaceAll(fullMethod, "/", ".")

	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", fullMethod)
	status := "ok"

	start := time.Now()

	m, err := handler(ctx, req)
	if err != nil {
		util.Logger.Debugf("RPC failed with error %s", err.Error())
		status = "error"
	}

	elapsed := float32(time.Now().Sub(start).Nanoseconds()) / float32(time.Millisecond)

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.AddSampleWithLabels(metrics.RequestElapsedSummary, elapsed, labelset.Labels)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)
	return m, err
}

type wrappedStream struct {
	grpc.ServerStream
	GRPCMethod string
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", w.GRPCMethod)
	status := "ok"

	err := w.ServerStream.RecvMsg(m)
	if err != nil {
		status = "error"
	}

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RecvMsgCounter, 1, labelset.Labels)

	return err
}

func (w *wrappedStream) SendMsg(m interface{}) error {

	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", w.GRPCMethod)
	status := "ok"

	err := w.ServerStream.SendMsg(m)
	if err != nil {
		status = "error"
	}

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.SendMsgCounter, 1, labelset.Labels)

	return err
}

func newWrappedStream(s grpc.ServerStream, method string) grpc.ServerStream {
	return &wrappedStream{s, method}
}

// StreamInterceptor stream grpc interceptor
func StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	fullMethod := strings.TrimPrefix(info.FullMethod, "/")
	fullMethod = strings.ReplaceAll(fullMethod, "/", ".")

	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", fullMethod)
	status := "ok"

	// increment this before we call the long running handler
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)

	start := time.Now()

	err := handler(srv, newWrappedStream(ss, fullMethod))
	if err != nil {
		util.Logger.Debugf("RPC failed with error %s", err.Error())
		status = "error"
	}

	elapsed := float32(time.Now().Sub(start).Nanoseconds()) / float32(time.Millisecond)
	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.AddSampleWithLabels(metrics.RequestElapsedSummary, elapsed, labelset.Labels)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)

	return err
}
