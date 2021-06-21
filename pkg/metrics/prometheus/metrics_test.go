package prometheus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"sassoftware.io/convoy/arke/pkg/metrics"
)

func Test_Metrics(t *testing.T) {
	var tests = []struct {
		name       string
		metricName []string
		metricDesc string
		key        string
		expected   bool
	}{
		{
			name:       "sperate key - match",
			metricName: []string{"arke", "gauge", "one"},
			metricDesc: "description for gauge1",
			key:        "arke_gauge_one",
			expected:   true,
		},
		{
			name:       "one key - match",
			metricName: []string{"arke_gauge_two"},
			metricDesc: "description for gauge2",
			key:        "arke_gauge_two",
			expected:   true,
		},
		{
			name:       "key - not match",
			metricName: []string{"arke", "gauge", "three"},
			metricDesc: "description for gauge3",
			key:        "arke_gauge_two",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newArkeGauge(tt.metricName, tt.metricDesc)
			assert.NotNil(t, g)
			key := strings.Join(tt.metricName, "_")
			assert.True(t, strings.Contains(key, "_"))
			matches := strings.Contains(g.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for gauge %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}

			c := newArkeCounter(tt.metricName, tt.metricDesc)
			assert.NotNil(t, c)
			matches = strings.Contains(c.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for counter %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}

			s := newArkeSample(tt.metricName, tt.metricDesc)
			assert.NotNil(t, s)
			matches = strings.Contains(s.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for sample %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}
		})
	}

	assert.Equal(t, metrics.ClientActMessageGauge, []string{"arke", "client", "active", "messages"})
	assert.Equal(t, metrics.ClientStreamsGauge, []string{"arke", "client", "streams"})
	assert.Equal(t, metrics.ClientConsumedGauge, []string{"arke", "client", "consumed"})
	assert.Equal(t, metrics.ClientProducedGauge, []string{"arke", "client", "produced"})
	assert.Equal(t, metrics.RequestElapsedSummary, []string{"arke", "request", "elapsed"})
	assert.Equal(t, metrics.RequestTotalCounter, []string{"arke", "request", "total"})
	assert.Equal(t, metrics.RecvMsgCounter, []string{"arke", "recvmsg", "total"})
	assert.Equal(t, metrics.SendMsgCounter, []string{"arke", "sendmsg", "total"})
}
