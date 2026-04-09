// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_setPublishRateParams(t *testing.T) {
	tests := map[string]struct {
		envInterval      string
		envRange         string
		expectedInterval int
		expectedRange    int
	}{
		"defaults when env vars not set": {
			envInterval:      "",
			envRange:         "",
			expectedInterval: defaultPublishRateSampleInterval,
			expectedRange:    defaultPublishRateSampleRange,
		},
		"custom interval": {
			envInterval:      "10",
			envRange:         "",
			expectedInterval: 10,
			expectedRange:    defaultPublishRateSampleRange,
		},
		"custom range": {
			envInterval:      "",
			envRange:         "300",
			expectedInterval: defaultPublishRateSampleInterval,
			expectedRange:    300,
		},
		"both custom": {
			envInterval:      "15",
			envRange:         "120",
			expectedInterval: 15,
			expectedRange:    120,
		},
		"invalid interval falls back to default": {
			envInterval:      "notanint",
			envRange:         "",
			expectedInterval: defaultPublishRateSampleInterval,
			expectedRange:    defaultPublishRateSampleRange,
		},
		"invalid range falls back to default": {
			envInterval:      "",
			envRange:         "notanint",
			expectedInterval: defaultPublishRateSampleInterval,
			expectedRange:    defaultPublishRateSampleRange,
		},
		"both invalid fall back to defaults": {
			envInterval:      "bad",
			envRange:         "bad",
			expectedInterval: defaultPublishRateSampleInterval,
			expectedRange:    defaultPublishRateSampleRange,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.envInterval != "" {
				t.Setenv(envPublishRateSampleInterval, tc.envInterval)
			} else {
				os.Unsetenv(envPublishRateSampleInterval)
			}
			if tc.envRange != "" {
				t.Setenv(envPublishRateSampleRange, tc.envRange)
			} else {
				os.Unsetenv(envPublishRateSampleRange)
			}

			setPublishRateParams()

			assert.Equal(t, tc.expectedInterval, publishRateSampleInterval)
			assert.Equal(t, tc.expectedRange, publishRateSampleRange)
		})
	}
}
