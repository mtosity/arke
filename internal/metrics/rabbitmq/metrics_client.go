package rabbitmq

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"sassoftware.io/viya/arke/internal/metrics"
	"sassoftware.io/viya/arke/internal/util"

	pb "sassoftware.io/viya/arke/api"
)

// See https://www.rabbitmq.com/docs/prometheus for details on RabbitMQ Prometheus metrics
const (

	// Metric family names - there are additional metric families available for
	// connection info, memory, node stats, etc.
	metricFamilyQueueCourseMetrics    = "queue_coarse_metrics"
	metricFamilyQueueConsumerCount    = "queue_consumer_count"
	metricFamilyStreamConsumerMetrics = "stream_consumer_metrics"

	// endpointDetailed returns detailed metrics with filtering support
	// additional endpoints are avaliable, but this is the one that provides
	// queue-specific metrics
	endpointDetailed = "/metrics/detailed"
)

var (
	tlsConfig *tls.Config

	// var instead of const so it can be overridden in tests
	defaultPrometheusPort = 15692
)

// MetricsClient is a REST client for retrieving RabbitMQ Prometheus metrics
type MetricsClient struct {
	baseURL    string
	httpClient *http.Client
	vhost      string
	username   string
	password   string
}

// initializeTLSConfig initializes the TLS configuration for the metrics client
// which will be the same for every request.
func init() {

	if _, ok := os.LookupEnv("CERT_KEY"); !ok {
		util.Logger.Debug("Not using TLS for RabbitMQ Prometheus metrics client")
		return
	}
	trustedCerts := os.Getenv("CA_BUNDLE")
	if trustedCerts == "" { // not using TLS
		util.Logger.Debug("CERT_KEY was set, but CA_BUNDLE is not set; not using TLS for RabbitMQ Prometheus metrics client")
		return
	}

	caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(trustedCerts, "/"))))
	if err != nil {
		util.Logger.Debugf("Failed to read RabbitMQ trusted CA certificates from %s: %v", trustedCerts, err)
		return
	}
	tlsConfig = &tls.Config{}
	tlsConfig.RootCAs = x509.NewCertPool()
	tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
	util.Logger.Debug("Using TLS for RabbitMQ Prometheus metrics client")

}

// NewMetricsClientFromPBConnectionConfig creates a new RabbitMQ metrics client
// from a protobuf ConnectionConfiguration
func NewMetricsClient(cfg *pb.ConnectionConfiguration) *MetricsClient {
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	scheme := "http"
	if tlsConfig != nil {
		scheme = "https"
		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}
	baseURL := fmt.Sprintf("%s://%s:%d", scheme, cfg.GetHost(), defaultPrometheusPort)

	client := MetricsClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		vhost:      cfg.GetTenant(),
	}
	creds := cfg.GetCredentials()
	if creds != nil && creds.Username != "" && creds.Password != "" {
		client.username = creds.Username
		client.password = creds.Password
	}

	return &client
}

// detailedMetricsParams holds parameters for the /metrics/* endpoint
type detailedMetricsParams struct {
	// Families specifies which metric families to return.
	// Examples: "queue_coarse_metrics", "queue_consumer_count", "connection_metrics"
	Families []string
	// VHosts filters queue-related metrics to specific virtual hosts
	VHosts []string
}

// toURLValues converts detailedMetricsParams to url.Values for query parameters
func (d *detailedMetricsParams) toURLValues() url.Values {
	values := make(url.Values)

	for _, family := range d.Families {
		values.Add("family", family)
	}

	for _, vhost := range d.VHosts {
		values.Add("vhost", vhost)
	}

	return values
}

// queueStatsParams returns DetailedMetricsParams configured to retrieve
// queue statistics.
func queueStatsParams(vhosts []string) detailedMetricsParams {
	return detailedMetricsParams{
		Families: []string{
			metricFamilyQueueCourseMetrics,
			metricFamilyQueueConsumerCount,
			metricFamilyStreamConsumerMetrics,
		},
		VHosts: vhosts,
	}
}

// GetQueueStats retrieves queue statistics for the specified vhost and queue.
// Fulfills the metrics.BrokerMetricsClient interface.
func (c *MetricsClient) GetQueueStats(ctx context.Context, queue string) (*metrics.QueueStats, error) {
	params := queueStatsParams([]string{c.vhost})
	data, err := c.getMetricsFromEndpoint(ctx, endpointDetailed, params.toURLValues())
	if err != nil {
		return nil, fmt.Errorf("failed to get detailed metrics: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("No metrics data received from RabbitMQ Prometheus endpoint")
	}

	return prometheusToQueueStats(data, c.vhost, queue)
}

// getMetricsFromEndpoint performs the HTTP request to retrieve metrics
func (c *MetricsClient) getMetricsFromEndpoint(ctx context.Context, endpoint string, queryParams url.Values) ([]byte, error) {
	urlStr := c.baseURL + endpoint

	// Add query parameters if any
	if len(queryParams) > 0 {
		urlStr += "?" + queryParams.Encode()
	}

	util.Logger.Debugf("Fetching rabbitmq prometheus metrics from %s", urlStr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	req.Header.Set("Accept", "text/plain")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

// Close closes the HTTP client's idle connections
func (c *MetricsClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// getVHostAndQueueFromLabels returns the vhost and queue from the labels
// of a Prometheus Metric
func getVHostAndQueueFromLabels(p *dto.Metric) (string, string) {
	var vhost, queue string
	for _, label := range p.GetLabel() {
		switch label.GetName() {
		case "vhost":
			vhost = label.GetValue()
		case "queue":
			queue = label.GetValue()
		}
	}
	return vhost, queue
}

// prometheusToQueueStats converts Prometheus MetricFamily to a QueueStats struct
// containing relevant metrics for the specified vhost and queue.
func prometheusToQueueStats(data []byte, vhost string, queue string) (*metrics.QueueStats, error) {
	reader := bytes.NewReader(data)
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(reader)
	if err != nil {
		return nil, err
	}

	var qs *metrics.QueueStats
	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.GetMetric() {
			metricVHost, metricQueue := getVHostAndQueueFromLabels(metric)
			if metricVHost != vhost || metricQueue != queue {
				continue
			}
			if qs == nil {
				qs = &metrics.QueueStats{VHost: vhost, Queue: queue}
			}
			switch metricFamily.GetName() {
			case "rabbitmq_detailed_queue_consumers":
				qs.ConsumersCount = int(metric.GetGauge().GetValue())
			case "rabbitmq_detailed_queue_messages_ready":
				qs.MessagesReadyCount = int(metric.GetGauge().GetValue())
			case "rabbitmq_detailed_queue_messages_unacked":
				qs.MessagesUnackCount = int(metric.GetGauge().GetValue())
			case "rabbitmq_detailed_queue_messages":
				qs.TotalMessagesCount = int(metric.GetGauge().GetValue())
			case "rabbitmq_detailed_stream_consumer_max_offset_lag":
				qs.ConsumerOffsetLag = int(metric.GetGauge().GetValue())
			}
		}
	}
	if qs == nil {
		return nil, fmt.Errorf("no metrics found for vhost %s and queue %s", vhost, queue)
	}
	return qs, nil
}
