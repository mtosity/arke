package rabbitmq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	pb "sassoftware.io/viya/arke/api"
	"sassoftware.io/viya/arke/internal/metrics"

	"github.com/stretchr/testify/assert"
)

func getHostPort(url string) (string, int) {
	trimmed := strings.TrimPrefix(url, "http://")
	trimmed = strings.TrimPrefix(trimmed, "https://")
	parts := strings.SplitN(trimmed, ":", 2)
	host := parts[0]
	port := 0
	if len(parts) == 2 {
		portStr := parts[1]
		if idx := strings.Index(portStr, "/"); idx != -1 {
			portStr = portStr[:idx]
		}
		p, err := strconv.Atoi(portStr)
		if err == nil {
			port = p
		}
	}
	return host, port
}

func TestGetDetailedMetrics(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics/detailed" {
			t.Errorf("Expected path /metrics/detailed, got %s", r.URL.Path)
		}

		// Check query parameters
		families := r.URL.Query()["family"]
		vhosts := r.URL.Query()["vhost"]

		if len(families) == 0 {
			t.Error("Expected family query parameter")
		}
		if len(vhosts) == 0 {
			t.Error("Expected vhost query parameter")
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`# HELP rabbitmq_detailed_queue_messages Queue depth
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="test-queue",vhost="/"} 100
`))
	}))
	defer ts.Close()
	host, port := getHostPort(ts.URL)
	assert.Greater(t, port, 0, "Failed to parse test server port from URL %s", ts.URL)
	pbConnConfig := pb.ConnectionConfiguration{
		Host:   host,
		Tenant: "/",
	}
	oldDefaultPort := defaultPrometheusPort
	defaultPrometheusPort = port
	defer func() {
		defaultPrometheusPort = oldDefaultPort
	}()
	client := NewMetricsClient(&pbConnConfig)
	defer client.Close()

	ctx := context.Background()
	params := detailedMetricsParams{
		Families: []string{"queue_coarse_metrics", "queue_consumer_count"},
		VHosts:   []string{"/", "test-vhost"},
	}

	metrics, err := client.getMetricsFromEndpoint(ctx, endpointDetailed, params.toURLValues())
	if err != nil {
		t.Fatalf("GetDetailedMetrics() error = %v", err)
	}

	if !containsBytes(metrics, "rabbitmq_detailed_queue_messages") {
		t.Error("GetDetailedMetrics() metrics do not contain expected data")
	}
}

func TestQueueStatsParams(t *testing.T) {
	params := queueStatsParams([]string{"/"})

	expectedFamilies := []string{
		metricFamilyQueueCourseMetrics,
		metricFamilyQueueConsumerCount,
		metricFamilyStreamConsumerMetrics,
	}

	assert.Equal(t, expectedFamilies, params.Families, "Families do not match expected")
	assert.Equal(t, []string{"/"}, params.VHosts, "VHosts do not match expected")
}

func TestDetailedMetricsParamsToURLValues(t *testing.T) {
	tests := []struct {
		name     string
		params   detailedMetricsParams
		expected map[string][]string
	}{
		{
			name: "empty params",
			params: detailedMetricsParams{
				Families: []string{},
				VHosts:   []string{},
			},
			expected: map[string][]string{},
		},
		{
			name: "single family and vhost",
			params: detailedMetricsParams{
				Families: []string{"queue_coarse_metrics"},
				VHosts:   []string{"/"},
			},
			expected: map[string][]string{
				"family": {"queue_coarse_metrics"},
				"vhost":  {"/"},
			},
		},
		{
			name: "multiple families and vhosts",
			params: detailedMetricsParams{
				Families: []string{"queue_coarse_metrics", "queue_consumer_count", "stream_consumer_metrics"},
				VHosts:   []string{"/", "test-vhost", "another-vhost"},
			},
			expected: map[string][]string{
				"family": {"queue_coarse_metrics", "queue_consumer_count", "stream_consumer_metrics"},
				"vhost":  {"/", "test-vhost", "another-vhost"},
			},
		},
		{
			name: "families only",
			params: detailedMetricsParams{
				Families: []string{"connection_metrics"},
				VHosts:   []string{},
			},
			expected: map[string][]string{
				"family": {"connection_metrics"},
			},
		},
		{
			name: "vhosts only",
			params: detailedMetricsParams{
				Families: []string{},
				VHosts:   []string{"vhost1", "vhost2"},
			},
			expected: map[string][]string{
				"vhost": {"vhost1", "vhost2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := tt.params.toURLValues()
			assert.Equal(t, tt.expected, map[string][]string(values))
		})
	}
}

func TestGetVHostAndQueueFromLabels(t *testing.T) {
	tests := []struct {
		name          string
		metric        *dto.Metric
		expectedVHost string
		expectedQueue string
	}{
		{
			name: "both vhost and queue labels present",
			metric: &dto.Metric{
				Label: []*dto.LabelPair{
					{Name: stringPtr("vhost"), Value: stringPtr("/")},
					{Name: stringPtr("queue"), Value: stringPtr("test-queue")},
				},
			},
			expectedVHost: "/",
			expectedQueue: "test-queue",
		},
		{
			name: "only vhost label present",
			metric: &dto.Metric{
				Label: []*dto.LabelPair{
					{Name: stringPtr("vhost"), Value: stringPtr("custom-vhost")},
				},
			},
			expectedVHost: "custom-vhost",
			expectedQueue: "",
		},
		{
			name: "only queue label present",
			metric: &dto.Metric{
				Label: []*dto.LabelPair{
					{Name: stringPtr("queue"), Value: stringPtr("my-queue")},
				},
			},
			expectedVHost: "",
			expectedQueue: "my-queue",
		},
		{
			name: "no vhost or queue labels",
			metric: &dto.Metric{
				Label: []*dto.LabelPair{
					{Name: stringPtr("other"), Value: stringPtr("value")},
				},
			},
			expectedVHost: "",
			expectedQueue: "",
		},
		{
			name:          "no labels",
			metric:        &dto.Metric{},
			expectedVHost: "",
			expectedQueue: "",
		},
		{
			name: "multiple labels including vhost and queue",
			metric: &dto.Metric{
				Label: []*dto.LabelPair{
					{Name: stringPtr("instance"), Value: stringPtr("localhost:15692")},
					{Name: stringPtr("vhost"), Value: stringPtr("/app")},
					{Name: stringPtr("queue"), Value: stringPtr("events-queue")},
					{Name: stringPtr("job"), Value: stringPtr("rabbitmq")},
				},
			},
			expectedVHost: "/app",
			expectedQueue: "events-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vhost, queue := getVHostAndQueueFromLabels(tt.metric)
			assert.Equal(t, tt.expectedVHost, vhost, "vhost mismatch")
			assert.Equal(t, tt.expectedQueue, queue, "queue mismatch")
		})
	}
}

func TestPrometheusToQueueStats(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		vhost       string
		queue       string
		expectError bool
		validate    func(t *testing.T, qs *metrics.QueueStats)
	}{
		{
			name: "all metrics present",
			data: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="test-queue",vhost="/"} 5
# HELP rabbitmq_detailed_queue_messages_ready Messages ready
# TYPE rabbitmq_detailed_queue_messages_ready gauge
rabbitmq_detailed_queue_messages_ready{queue="test-queue",vhost="/"} 10
# HELP rabbitmq_detailed_queue_messages_unacked Messages unacked
# TYPE rabbitmq_detailed_queue_messages_unacked gauge
rabbitmq_detailed_queue_messages_unacked{queue="test-queue",vhost="/"} 3
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="test-queue",vhost="/"} 13
# HELP rabbitmq_detailed_stream_consumer_max_offset_lag Stream consumer lag
# TYPE rabbitmq_detailed_stream_consumer_max_offset_lag gauge
rabbitmq_detailed_stream_consumer_max_offset_lag{queue="test-queue",vhost="/"} 7
`,
			vhost:       "/",
			queue:       "test-queue",
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "/", qs.VHost)
				assert.Equal(t, "test-queue", qs.Queue)
				assert.Equal(t, 5, qs.ConsumersCount)
				assert.Equal(t, 10, qs.MessagesReadyCount)
				assert.Equal(t, 3, qs.MessagesUnackCount)
				assert.Equal(t, 13, qs.TotalMessagesCount)
				assert.Equal(t, 7, qs.ConsumerOffsetLag)
			},
		},
		{
			name: "partial metrics",
			data: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="partial-queue",vhost="/app"} 2
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="partial-queue",vhost="/app"} 42
`,
			vhost:       "/app",
			queue:       "partial-queue",
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "/app", qs.VHost)
				assert.Equal(t, "partial-queue", qs.Queue)
				assert.Equal(t, 2, qs.ConsumersCount)
				assert.Equal(t, 42, qs.TotalMessagesCount)
				assert.Equal(t, 0, qs.MessagesReadyCount)
			},
		},
		{
			name: "no matching queue",
			data: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="other-queue",vhost="/"} 1
`,
			vhost:       "/",
			queue:       "non-existent-queue",
			expectError: true,
		},
		{
			name: "no matching vhost",
			data: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="test-queue",vhost="/other"} 1
`,
			vhost:       "/",
			queue:       "test-queue",
			expectError: true,
		},
		{
			name: "multiple queues, match specific one",
			data: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="queue-1",vhost="/"} 1
rabbitmq_detailed_queue_consumers{queue="queue-2",vhost="/"} 2
rabbitmq_detailed_queue_consumers{queue="queue-3",vhost="/"} 3
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="queue-1",vhost="/"} 100
rabbitmq_detailed_queue_messages{queue="queue-2",vhost="/"} 200
rabbitmq_detailed_queue_messages{queue="queue-3",vhost="/"} 300
`,
			vhost:       "/",
			queue:       "queue-2",
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, 2, qs.ConsumersCount)
				assert.Equal(t, 200, qs.TotalMessagesCount)
			},
		},
		{
			name:        "empty metrics data",
			data:        ``,
			vhost:       "/",
			queue:       "test-queue",
			expectError: true,
		},
		{
			name:        "invalid prometheus format",
			data:        `this is not valid prometheus data`,
			vhost:       "/",
			queue:       "test-queue",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qs, err := prometheusToQueueStats([]byte(tt.data), tt.vhost, tt.queue)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, qs)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, qs)
				if tt.validate != nil {
					tt.validate(t, qs)
				}
			}
		})
	}
}

func TestGetQueueStats(t *testing.T) {
	tests := []struct {
		name        string
		vhost       string
		queue       string
		serverResp  string
		statusCode  int
		expectError bool
		validate    func(t *testing.T, qs *metrics.QueueStats)
	}{
		{
			name:  "successful queue stats retrieval",
			vhost: "/",
			queue: "test-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="test-queue",vhost="/"} 3
# HELP rabbitmq_detailed_queue_messages_ready Messages ready
# TYPE rabbitmq_detailed_queue_messages_ready gauge
rabbitmq_detailed_queue_messages_ready{queue="test-queue",vhost="/"} 15
# HELP rabbitmq_detailed_queue_messages_unacked Messages unacked
# TYPE rabbitmq_detailed_queue_messages_unacked gauge
rabbitmq_detailed_queue_messages_unacked{queue="test-queue",vhost="/"} 2
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="test-queue",vhost="/"} 17
`,
			statusCode:  http.StatusOK,
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "/", qs.VHost)
				assert.Equal(t, "test-queue", qs.Queue)
				assert.Equal(t, 3, qs.ConsumersCount)
				assert.Equal(t, 15, qs.MessagesReadyCount)
				assert.Equal(t, 2, qs.MessagesUnackCount)
				assert.Equal(t, 17, qs.TotalMessagesCount)
			},
		},
		{
			name:  "queue not found",
			vhost: "/",
			queue: "non-existent-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="other-queue",vhost="/"} 1
`,
			statusCode:  http.StatusOK,
			expectError: true,
			validate:    nil,
		},
		{
			name:        "server returns error",
			vhost:       "/",
			queue:       "test-queue",
			serverResp:  "",
			statusCode:  http.StatusInternalServerError,
			expectError: true,
			validate:    nil,
		},
		{
			name:  "custom vhost",
			vhost: "/custom",
			queue: "app-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="app-queue",vhost="/custom"} 5
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="app-queue",vhost="/custom"} 100
`,
			statusCode:  http.StatusOK,
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "/custom", qs.VHost)
				assert.Equal(t, "app-queue", qs.Queue)
				assert.Equal(t, 5, qs.ConsumersCount)
				assert.Equal(t, 100, qs.TotalMessagesCount)
			},
		},
		{
			name:  "with stream consumer metrics",
			vhost: "/",
			queue: "stream-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="stream-queue",vhost="/"} 2
# HELP rabbitmq_detailed_stream_consumer_max_offset_lag Stream consumer lag
# TYPE rabbitmq_detailed_stream_consumer_max_offset_lag gauge
rabbitmq_detailed_stream_consumer_max_offset_lag{queue="stream-queue",vhost="/"} 5
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="stream-queue",vhost="/"} 50
`,
			statusCode:  http.StatusOK,
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "stream-queue", qs.Queue)
				assert.Equal(t, 2, qs.ConsumersCount)
				assert.Equal(t, 5, qs.ConsumerOffsetLag)
				assert.Equal(t, 50, qs.TotalMessagesCount)
			},
		},
		{
			name:        "server returns 404",
			vhost:       "/",
			queue:       "test-queue",
			serverResp:  "Not Found",
			statusCode:  http.StatusNotFound,
			expectError: true,
			validate:    nil,
		},
		{
			name:        "server returns 401 unauthorized",
			vhost:       "/",
			queue:       "test-queue",
			serverResp:  "Unauthorized",
			statusCode:  http.StatusUnauthorized,
			expectError: true,
			validate:    nil,
		},
		{
			name:  "multiple queues in response",
			vhost: "/",
			queue: "queue-2",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="queue-1",vhost="/"} 1
rabbitmq_detailed_queue_consumers{queue="queue-2",vhost="/"} 2
rabbitmq_detailed_queue_consumers{queue="queue-3",vhost="/"} 3
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="queue-1",vhost="/"} 100
rabbitmq_detailed_queue_messages{queue="queue-2",vhost="/"} 200
rabbitmq_detailed_queue_messages{queue="queue-3",vhost="/"} 300
`,
			statusCode:  http.StatusOK,
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "queue-2", qs.Queue)
				assert.Equal(t, 2, qs.ConsumersCount)
				assert.Equal(t, 200, qs.TotalMessagesCount)
			},
		},
		{
			name:  "zero metrics values",
			vhost: "/",
			queue: "empty-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="empty-queue",vhost="/"} 0
# HELP rabbitmq_detailed_queue_messages_ready Messages ready
# TYPE rabbitmq_detailed_queue_messages_ready gauge
rabbitmq_detailed_queue_messages_ready{queue="empty-queue",vhost="/"} 0
# HELP rabbitmq_detailed_queue_messages Total messages
# TYPE rabbitmq_detailed_queue_messages gauge
rabbitmq_detailed_queue_messages{queue="empty-queue",vhost="/"} 0
`,
			statusCode:  http.StatusOK,
			expectError: false,
			validate: func(t *testing.T, qs *metrics.QueueStats) {
				assert.Equal(t, "empty-queue", qs.Queue)
				assert.Equal(t, 0, qs.ConsumersCount)
				assert.Equal(t, 0, qs.MessagesReadyCount)
				assert.Equal(t, 0, qs.TotalMessagesCount)
			},
		},
		{
			name:  "vhost mismatch",
			vhost: "/app",
			queue: "test-queue",
			serverResp: `# HELP rabbitmq_detailed_queue_consumers Queue consumer count
# TYPE rabbitmq_detailed_queue_consumers gauge
rabbitmq_detailed_queue_consumers{queue="test-queue",vhost="/"} 1`,
			statusCode:  http.StatusOK,
			expectError: true,
			validate:    nil,
		},
		{
			name:        "empty response",
			vhost:       "/",
			queue:       "test-queue",
			serverResp:  "",
			statusCode:  http.StatusOK,
			expectError: true,
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// start a test HTTP server that serves prometheus metrics
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain; version=0.0.4")
				w.WriteHeader(tt.statusCode)
				if tt.serverResp != "" {
					_, _ = w.Write([]byte(tt.serverResp))
				}
			}))
			defer ts.Close()

			// Get the host and port of the test http server
			host, port := getHostPort(ts.URL)

			// Get the MetricsClient
			pbConnConfig := pb.ConnectionConfiguration{
				Host:   host,
				Tenant: tt.vhost,
			}
			oldDefaultPort := defaultPrometheusPort
			defaultPrometheusPort = port
			defer func() {
				defaultPrometheusPort = oldDefaultPort
			}()
			client := NewMetricsClient(&pbConnConfig)
			defer client.Close()

			qs, err := client.GetQueueStats(context.Background(), tt.queue)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, qs)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, qs)
				if tt.validate != nil {
					tt.validate(t, qs)
				}
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
func containsBytes(b []byte, substr string) bool {
	if len(b) == 0 || len(substr) == 0 {
		return false
	}
	s := string(b)
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
