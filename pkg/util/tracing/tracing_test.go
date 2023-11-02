package tracing

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTelemetryEnabled(t *testing.T) {
	// A true value for OTEL_SDK_DISABLED is the only way to disable the SDK according to the docs,
	// even though this env var is not used in the Go SDK
	// https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#general-sdk-configuration

	//    OTEL_SDK_DISABLED | true  | false |  ''  | other
	// getTelementryEnabled | false | true  | true | true
	os.Setenv("OTEL_SDK_DISABLED", "true")
	enabled := getTelemetryEnabled()
	assert.False(t, enabled)

	os.Setenv("OTEL_SDK_DISABLED", "false")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)

	os.Setenv("OTEL_SDK_DISABLED", "")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)

	os.Setenv("OTEL_SDK_DISABLED", "other")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)
}
