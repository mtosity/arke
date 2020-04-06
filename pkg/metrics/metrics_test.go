package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Constants(t *testing.T) {
	assert.Equal(t, ClientActMessageGauge, []string{"arke", "client", "active", "messages"})
	assert.Equal(t, ClientStreamsGauge, []string{"arke", "client", "streams"})
	assert.Equal(t, ClientConsumedCounter, []string{"arke", "client", "consumed", "total"})
	assert.Equal(t, ClientProducedCounter, []string{"arke", "client", "produced", "total"})
	assert.Equal(t, RequestElapsedSummary, []string{"arke", "request", "elapsed"})
	assert.Equal(t, RequestTotalCounter, []string{"arke", "request", "total"})
	assert.Equal(t, RecvMsgCounter, []string{"arke", "recvmsg", "total"})
	assert.Equal(t, SendMsgCounter, []string{"arke", "sendmsg", "total"})
}

func Test_LabelSet(t *testing.T) {
	labelset := NewLabelSet()
	assert.NotNil(t, labelset)
	labelset.AddLabel("ClientUUID", "test")
	assert.Equal(t, len(labelset.Labels), 1)
}
