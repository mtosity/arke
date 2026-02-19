package util

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sassoftware.io/viya/arke/i18n"
)

func TestArkeLogger_InfoT(t *testing.T) {
	tests := []struct {
		name        string
		messageID   string
		args        []interface{}
		expectedMsg string
	}{
		{
			name:        "multiple params",
			messageID:   i18n.Binding,
			args:        []interface{}{"queue-name", "key-name", "exchange-name"},
			expectedMsg: "Binding to Queue/Key/Exchange: queue-name/key-name/exchange-name",
		},
		{
			name:        "params given but not needed",
			messageID:   i18n.RateLimiterInitialized2,
			args:        []interface{}{"arg1", "arg2"},
			expectedMsg: "Rate limiter has been initialized",
		},
	}

	for _, tt := range tests {
		t.Setenv(EnvLogLevel, "info")
		t.Run(tt.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("failed to create pipe: %v", err)
			}
			defer r.Close()
			defer w.Close()

			ResetLogger()
			LogOutputFile = w

			al := NewArkeLogger()

			al.Info(tt.messageID, tt.args...)
			w.Close()

			output := make([]byte, 1024)
			n, _ := r.Read(output)
			r.Close()
			logOutput := string(output[:n])
			t.Logf("Log Output: %s", logOutput)
			assert.Contains(t, logOutput, tt.expectedMsg)
		})
	}
}

func TestArkeLogger_Levels(t *testing.T) {
	t.Setenv(EnvLogLevel, "trace")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	ResetLogger()
	LogOutputFile = w

	al := NewArkeLogger()

	al.Trace("trace_message")
	al.Tracef("tracef_message: %s", "formatted")
	al.Debug("debug_message")
	al.Debugf("debugf_message: %s", "formatted")
	al.Info("info_message")
	al.Warn("warn_message")
	al.Error("error_message")
	w.Close()

	output := make([]byte, 2048)
	n, _ := r.Read(output)
	r.Close()
	logOutput := string(output[:n])
	t.Logf("Log Output: %s", logOutput)
	assert.Contains(t, logOutput, "trace_message")
	assert.Contains(t, logOutput, "tracef_message: formatted")
	assert.Contains(t, logOutput, "debug_message")
	assert.Contains(t, logOutput, "debugf_message: formatted")
	assert.Contains(t, logOutput, "info_message")
	assert.Contains(t, logOutput, "warn_message")
	assert.Contains(t, logOutput, "error_message")
}

func TestArkeLogger_fields(t *testing.T) {
	t.Setenv(EnvLogFormat, "json")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	ResetLogger()
	LogOutputFile = w

	al := NewArkeLogger()

	al.Info("hi")
	expectedLogTime := time.Now()

	output := make([]byte, 2048)
	n, _ := r.Read(output)
	r.Close()
	logOutput := string(output[:n])
	t.Logf("Log Output: %s", logOutput)

	data := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(logOutput), &data))
	assert.Equal(t, "info", data["level"])
	assert.NotNil(t, data["source"])
	assert.Equal(t, 1.0, data["version"])

	if data["timeStamp"] != nil {
		actualLogTime, err := time.Parse(time.RFC3339Nano, data["timeStamp"].(string))
		require.NoError(t, err)
		// ci fails if we don't convert to UTC times
		// compare times down to the 100ms (12.3999 == 12.3000)
		assert.Equal(t, actualLogTime.UTC().Truncate(100*time.Millisecond), expectedLogTime.UTC().Truncate(100*time.Millisecond))
	} else {
		assert.NotNil(t, data["timeStamp"], "missing timeStamp field")
	}

	// make sure zerolog.CallerSkipFrameCount is set properly
	assert.Contains(t, data["caller"], "util/logger_test.go:116")
	assert.Equal(t, "hi", data["message"])
	w.Close()
}
