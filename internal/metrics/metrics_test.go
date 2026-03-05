// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Constants(t *testing.T) {
	assert.Equal(t, ClientActMessageGauge, []string{"arke", "client", "active", "messages"})
	assert.Equal(t, ClientStreamsGauge, []string{"arke", "client", "streams"})
	assert.Equal(t, ClientConsumedGauge, []string{"arke", "client", "consumed"})
	assert.Equal(t, ClientProducedGauge, []string{"arke", "client", "produced"})
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
