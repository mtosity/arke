package ratelimiter

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
	"sassoftware.io/viya/zlog"
	"sassoftware.io/viya/arke/api"
	"sassoftware.io/viya/arke/i18n"
	"sassoftware.io/viya/arke/internal/metrics"
	"sassoftware.io/viya/arke/internal/metrics/prometheus"
	"sassoftware.io/viya/arke/internal/util"
)

// ClientLimitManager manages rate limiting for clients.
type ClientLimitManager struct {
	// ConcurrentMap of clientLimiters keyed by client identifier
	clients            *util.ConcurrentMap
	bucketSize         int
	fillInterval       time.Duration
	maxAgeStaleClients time.Duration
	enforced           bool
}

type clientLimiter struct {
	limiter            *rate.Limiter
	lastConnectionTime time.Time
}

// ErrTooManyRequests is returned when a client has made too many requests
// and is rate limited.
var ErrTooManyRequests = fmt.Errorf("client has made too many requests - rate limiting enforced")

// LimitMethods is used by the interceptors to determine if a given method should
// be rate limited.
func LimitMethods(_ context.Context, c interceptors.CallMeta) bool {
	limitMethods := []string{
		api.Producer_Publish_FullMethodName,
		api.Producer_Connect_FullMethodName,
		api.Consumer_Consume_FullMethodName,
		api.Consumer_Connect_FullMethodName,
	}
	isRateLimited := slices.Contains(limitMethods, c.FullMethod())

	// This may be helpful in debugging, but is very verbose
	if os.Getenv("SAS_LOG_DEBUG_LIMIT_METHODS") == "true" && c.FullMethod() != "/grpc.health.v1.Health/Check" {
		util.Logger.Debug(
			"Rate limiter checking method name",
			zlog.P{
				"method":        c.FullMethod(),
				"isRateLimited": isRateLimited,
			},
		)
	}
	return isRateLimited
}

// NewClientLimitManager returns an instance of the clientLimitManager. All
// clients are rate limited based on the bucketSize and refillInterval parameters.
// Clients are stale if there hasn't been a rate limit check in maxAgeStaleClients,
// and are removed from the list of clients.
//
// If enforced=false and the other parameters are valid, the rate limiter will
// only warn in the logs that a rate limit was imposed, but not actually enforce
// the rate limit.
func NewClientLimitManager(bucketSize int, refillInterval time.Duration, maxAgeStaleClients time.Duration, enforced bool) (*ClientLimitManager, error) {
	if bucketSize <= 0 || refillInterval <= time.Duration(0) || maxAgeStaleClients <= time.Duration(0) {
		return nil, fmt.Errorf("invalid rate limit parameters")
	}
	return &ClientLimitManager{
		clients:            util.NewConcurrentMap(),
		bucketSize:         bucketSize,
		fillInterval:       refillInterval,
		maxAgeStaleClients: maxAgeStaleClients,
		enforced:           enforced,
	}, nil
}

// StartClientCull will periodically cull stale clients. This should
// be called as a go routine.
func (clm *ClientLimitManager) StartClientCull(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(clm.maxAgeStaleClients):
			clm.cullStaleClients()
		}
	}
}

// cullStaleClients removes clients that haven't been rate limited in maxAgeStaleClients.
func (clm *ClientLimitManager) cullStaleClients() {
	for _, clientID := range clm.clients.GetList() {
		if cl, ok := clm.clients.Get(clientID); ok {
			clientLimiter := cl.(*clientLimiter)
			lastSeconds := int(time.Since(clientLimiter.lastConnectionTime).Seconds())
			maxSeconds := int(clm.maxAgeStaleClients.Seconds())

			util.Logger.Debug("rate limiter checking for stale clients", zlog.P{
				"clientIdentifier":   clientID,
				"lastConnectionTime": clientLimiter.lastConnectionTime,
				"now":                time.Now(),
				"lastSeconds":        lastSeconds,
				"maxSeconds":         maxSeconds,
			})
			if lastSeconds > maxSeconds {
				util.Logger.DebugI(
					"rate limiter removing stale client",
					zlog.P{
						"clientIdentifier":   clientID,
						"lastConnectionTime": clientLimiter.lastConnectionTime,
					},
				)
				clm.clients.Delete(clientID)
			}
		}
	}
}

// Limit checks if the client is allowed to proceed based on the rate limit.
func (clm *ClientLimitManager) Limit(ctx context.Context) error {
	clientIdentifier, err := util.GetClientIdentifier(ctx)

	if err != nil && clm.enforced {
		// If we can't get the client identifier, we can't rate limit
		// Use a global limiter?
		util.Logger.Warn(i18n.RateLimiterNoClientIdentifier)
		return nil
	}

	var c *clientLimiter
	if lim, ok := clm.clients.Get(clientIdentifier); ok {
		util.Logger.Debug("Rate limiter found client", zlog.P{"clientIdentifier": clientIdentifier, "clientCount": clm.clients.Length()})
		c = lim.(*clientLimiter)
	} else {
		c = &clientLimiter{
			limiter: rate.NewLimiter(rate.Every(clm.fillInterval), clm.bucketSize),
		}
		clm.clients.Add(clientIdentifier, c)
	}
	c.lastConnectionTime = time.Now()
	if !c.limiter.Allow() {
		serviceName := util.ServceNameFromClientAddr(clientIdentifier)
		labelset := metrics.NewLabelSet()
		labelset.AddLabel("service_name", serviceName)
		labelset.AddLabel("client_identifier", clientIdentifier)
		prometheus.Stats.Sink.AddSampleWithLabels(metrics.RateLimitEnforcedSummary, 1, labelset.Labels)

		tracer := otel.GetTracerProvider().Tracer("arke")
		_, span := tracer.Start(
			ctx,
			"rate-limit-exceeded",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes([]attribute.KeyValue{
				{Key: "clientID", Value: attribute.StringValue(clientIdentifier)},
				{Key: "serviceName", Value: attribute.StringValue(serviceName)},
				{Key: "rateLimitEnforced", Value: attribute.BoolValue(clm.enforced)},
			}...),
		)
		defer span.End()

		// Is this needed? Or should I just immediately end the span?
		span.AddEvent("rate limit exceeded")

		util.Logger.Warn(i18n.RateLimitExceeded, zlog.P{
			"clientIdentifier":  clientIdentifier,
			"maxEventRateLimit": c.limiter.Limit(),
			"maxEventRateBurst": c.limiter.Burst(),
			"availableTokens":   c.limiter.Tokens(),
			"rateLimitEnforced": clm.enforced,
		})
		if clm.enforced {
			return ErrTooManyRequests
		}
	}
	return nil
}
