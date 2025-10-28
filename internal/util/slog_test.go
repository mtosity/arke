package util

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestLoadAndParseLevel(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected slog.Level
	}{
		{"TraceLevel", "TRACE", LevelTrace},
		{"DebugLevel", "DEBUG", LevelDebug},
		{"WarnLevel", "WARN", LevelWarn},
		{"ErrorLevel", "ERROR", LevelError},
		{"InfoLevelDefault", "", LevelInfo},
		{"UnknownLevel", "FOO", LevelInfo},
		{"LowercaseDebug", "debug", LevelDebug},
		{"MixedCaseWarn", "wArN", LevelWarn},
	}

	orig := os.Getenv(envLogLevel)
	defer os.Setenv(envLogLevel, orig)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv(envLogLevel, tt.envValue)
			got := loadAndParseLevel()
			if got != tt.expected {
				t.Errorf("loadAndParseLevel() = %v, want %v", got, tt.expected)
			}
		})
	}
}
func TestReplaceAttributes(t *testing.T) {
	tests := []struct {
		name    string
		attr    slog.Attr
		wantKey string
		wantVal any
	}{
		{
			name:    "LevelTraceLowercase",
			attr:    slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(LevelTrace)},
			wantKey: slog.LevelKey,
			wantVal: "trace",
		},
		{
			name:    "LevelDebugLowercase",
			attr:    slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(LevelDebug)},
			wantKey: slog.LevelKey,
			wantVal: "debug",
		},
		{
			name:    "TimeKeyRenamed",
			attr:    slog.Attr{Key: slog.TimeKey, Value: slog.StringValue("2024-01-01T00:00:00Z")},
			wantKey: timeStampKey,
			wantVal: "2024-01-01T00:00:00Z",
		},
		{
			name:    "MessageKeyRenamed",
			attr:    slog.Attr{Key: slog.MessageKey, Value: slog.StringValue("hello")},
			wantKey: messageKey,
			wantVal: "hello",
		},
		{
			name: "SourceKeyGrouped",
			attr: slog.Attr{
				Key:   slog.SourceKey,
				Value: slog.AnyValue(&slog.Source{Function: "main.main", File: "main.go", Line: 42}),
			},
			wantKey: propertiesKey,
			// We can't easily check the value, but we can check the key and type
		},
		{
			name:    "OtherKeyUnchanged",
			attr:    slog.Attr{Key: "foo", Value: slog.StringValue("bar")},
			wantKey: "foo",
			wantVal: "bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceAttributes(nil, tt.attr)
			if got.Key != tt.wantKey {
				t.Errorf("replaceAttributes() Key = %v, want %v", got.Key, tt.wantKey)
			}
			// For SourceKeyGrouped, just check the key and type
			if tt.name == "SourceKeyGrouped" {
				if got.Key != propertiesKey {
					t.Errorf("replaceAttributes() SourceKeyGrouped Key = %v, want %v", got.Key, propertiesKey)
				}
				if got.Value.Kind() != slog.KindGroup {
					t.Errorf("replaceAttributes() SourceKeyGrouped Value.Kind = %v, want KindGroup", got.Value.Kind())
				}
				return
			}
			// For others, check value
			if v, ok := got.Value.Any().(string); ok {
				if v != tt.wantVal {
					t.Errorf("replaceAttributes() Value = %v, want %v", v, tt.wantVal)
				}
			} else if tt.wantVal != nil && got.Value.Any() != tt.wantVal {
				t.Errorf("replaceAttributes() Value = %v, want %v", got.Value.Any(), tt.wantVal)
			}
		})
	}
}
func TestGenerateHandler_JSONFormat(t *testing.T) {
	origFormat := os.Getenv(envLogFormat)
	defer os.Setenv(envLogFormat, origFormat)
	os.Setenv(envLogFormat, "json")

	handler := generateHandler()
	// The handler should be a *slog.JSONHandler (unexported, so check type name)
	typeName := fmt.Sprintf("%T", handler)
	if !strings.Contains(typeName, "JSONHandler") {
		t.Errorf("generateHandler() with LOG_FORMAT=json returned type %s, want jsonHandler", typeName)
	}
}

func TestGenerateHandler_TextFormat(t *testing.T) {
	origFormat := os.Getenv(envLogFormat)
	defer os.Setenv(envLogFormat, origFormat)
	os.Setenv(envLogFormat, "term")

	handler := generateHandler()
	// The handler should be a *slog.TextHandler (unexported, so check type name)
	typeName := fmt.Sprintf("%T", handler)
	if !strings.Contains(typeName, "TextHandler") {
		t.Errorf("generateHandler() with LOG_FORMAT=text returned type %s, want textHandler", typeName)
	}
}

func TestGenerateHandler_DefaultFormat(t *testing.T) {
	origFormat := os.Getenv(envLogFormat)
	defer os.Setenv(envLogFormat, origFormat)
	os.Unsetenv(envLogFormat)

	handler := generateHandler()
	// The handler should default to JSONHandler if LOG_FORMAT is unset
	typeName := fmt.Sprintf("%T", handler)
	if !strings.Contains(typeName, "JSONHandler") {
		t.Errorf("generateHandler() with LOG_FORMAT unset returned type %s, want JSONHandler", typeName)
	}
}
func TestCreateLogger_BasicFields(t *testing.T) {
	// Save and restore environment variables
	origFormat := os.Getenv(envLogFormat)
	defer os.Setenv(envLogFormat, origFormat)
	origLevel := os.Getenv(envLogLevel)
	defer os.Setenv(envLogLevel, origLevel)

	// Set known env for deterministic handler
	os.Setenv(envLogFormat, "json")
	os.Setenv(envLogLevel, "INFO")

	l := createLogger()
	if l == nil {
		t.Fatal("createLogger() returned nil")
	}

	// Check logger has expected attributes by emitting a record to a buffer
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() failed: %v", err)
	}
	defer r.Close()
	defer w.Close()

	// Temporarily override logWriter for this test
	oldWriter := LogWriter
	LogWriter = w
	defer func() { LogWriter = oldWriter }()

	// Re-create logger with our pipe writer
	l = createLogger()
	l.Info("test-message")

	w.Close()
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, `"version":1`) {
		t.Errorf("createLogger() output missing version: %s", output)
	}
	if !strings.Contains(output, `"source":"`) {
		t.Errorf("createLogger() output missing source: %s", output)
	}
	if !strings.Contains(output, `"test-message"`) {
		t.Errorf("createLogger() output missing message: %s", output)
	}
}
