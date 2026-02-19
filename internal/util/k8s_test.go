package util

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
	pb "sassoftware.io/viya/arke/api"
)

// Test_MonitorHPA currently only tests the logging output for various failure scenarios
func Test_MonitorHPA(t *testing.T) {
	tests := []struct {
		name           string
		expectedMsg    string
		setupFunc      func() func() // returns cleanup function
		expectNoReturn bool          // whether function should return early
	}{
		{
			name:           "no namespace from file or env",
			expectedMsg:    "No kubernetes namespace detected, not monitoring HPA for changes",
			expectNoReturn: true,
			setupFunc: func() func() {
				return func() {
				}
			},
		},
		{
			name:           "namespace from file ErrNotInCluster",
			expectedMsg:    "Could not configure HPA cluster monitoring: stat ",
			expectNoReturn: true,
			setupFunc: func() func() {
				origNamespaceFile := namespaceFile
				tDir := os.TempDir()
				ns, _ := os.CreateTemp(tDir, "namespace")
				ns.WriteString("test-namespace") // nolint:errcheck
				ns.Close()
				namespaceFile = ns.Name()
				// Restore original value on cleanup
				return func() {
					namespaceFile = origNamespaceFile
				}
			},
		},
		{
			name:           "namespace from file InClusterConfig error",
			expectedMsg:    "Could not configure HPA cluster monitoring: file does not exist",
			expectNoReturn: true,
			setupFunc: func() func() {
				origNamespaceFile := namespaceFile
				origInClusterConfig := inClusterConfig
				inClusterConfig = func() (*rest.Config, error) {
					return nil, os.ErrNotExist
				}
				tDir := os.TempDir()
				ns, _ := os.CreateTemp(tDir, "namespace")
				ns.WriteString("test-namespace") // nolint:errcheck
				ns.Close()
				namespaceFile = ns.Name()
				// Restore original value on cleanup
				return func() {
					namespaceFile = origNamespaceFile
					inClusterConfig = origInClusterConfig
				}
			},
		},
		{
			name:           "namespace from env var invalid namespace",
			expectedMsg:    "Could not configure HPA cluster monitoring: stat",
			expectNoReturn: true,
			setupFunc: func() func() {
				os.Setenv("NAMESPACE", "test-namespace")
				return func() {
					os.Unsetenv("NAMESPACE")
				}
			},
		},
		{
			name:           "Could not get HPA watcher",
			expectedMsg:    "Could not get HPA watcher:",
			expectNoReturn: true,
			setupFunc: func() func() {
				origNamespaceFile := namespaceFile
				origInClusterConfig := inClusterConfig
				inClusterConfig = func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}
				tDir := os.TempDir()
				ns, _ := os.CreateTemp(tDir, "namespace")
				ns.WriteString("test-namespace") // nolint:errcheck
				ns.Close()
				namespaceFile = ns.Name()
				return func() {
					namespaceFile = origNamespaceFile
					inClusterConfig = origInClusterConfig
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test logging
			healthChan := make(chan pb.HealthStatus_Code, 1)

			// Recreate slog logger with our pipe writer
			ResetLogger()
			r, w, _ := os.Pipe()
			LogOutputFile = w
			t.Setenv(EnvLogFormat, "json")
			t.Setenv(EnvLogLevel, "DEBUG")
			NewArkeLogger()

			// Setup test conditions
			cleanup := tt.setupFunc()
			defer cleanup()

			// Run the function
			MonitorHPA(healthChan, "test-arke")

			// Close writer and read output
			w.Close()

			outputBuffer := make([]byte, 1024)
			bytesRead, _ := r.Read(outputBuffer)
			logMsg := string(outputBuffer[:bytesRead])
			// Validate the log output
			assert.True(t, strings.Contains(logMsg, tt.expectedMsg))

			pentry := map[string]interface{}{}
			err := json.Unmarshal(outputBuffer[:bytesRead], &pentry)
			assert.NoError(t, err, "Log entry is not valid JSON: %v", err)
			for _, field := range []string{"level", "caller", "version", "timeStamp", "source", "message"} {
				_, ok := pentry[field]
				assert.True(t, ok, "Log entry missing field '%s': %s", field, logMsg)
			}

			// Verify no health status was sent if function returned early
			if tt.expectNoReturn {
				select {
				case status := <-healthChan:
					t.Errorf("Expected no health status, but got: %v", status)
				default:
					// Expected behavior - no status sent
				}
			}
		})
	}
}
