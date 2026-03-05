// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestGetTelemetryEnabled(t *testing.T) {
	defer os.Setenv("OTEL_SDK_DISABLED", "true")
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

func Test_InitTracerProvider_disabled(t *testing.T) {
	os.Setenv("OTEL_SDK_DISABLED", "true")
	tp, err := InitTracerProvider()
	assert.Nil(t, tp)
	assert.Nil(t, err)
}

func Test_InitTracerProvider_enabled(t *testing.T) {
	os.Setenv("OTEL_SDK_DISABLED", "false")
	defer os.Setenv("OTEL_SDK_DISABLED", "true")
	tp, err := InitTracerProvider()
	assert.NotNil(t, tp)
	assert.Nil(t, err)
}

func Test_getTelemetryCollectorAddress_unset(t *testing.T) {
	os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	addr := getTelemetryCollectorAddress()
	assert.Equal(t, "localhost:4317", addr)

	os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}

func Test_getTelemetryCollectorAddress_set(t *testing.T) {
	defer os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:12345")
	addr := getTelemetryCollectorAddress()
	assert.Equal(t, "localhost:12345", addr)
}

func Test_SpanFromHeaders(t *testing.T) {
	os.Setenv("OTEL_SDK_DISABLED", "false")
	defer os.Setenv("OTEL_SDK_DISABLED", "true")
	tp, err := InitTracerProvider()
	assert.NotNil(t, tp)
	assert.Nil(t, err)

	ctx := context.Background()
	headers := make(map[string]string)
	headers[HeaderTraceParent] = "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"
	spanName := "testSpan"
	kind := trace.SpanKindConsumer
	var span trace.Span
	_, span = SpanFromHeaders(ctx, headers, spanName, kind)
	defer span.End()

	assert.True(t, span.IsRecording())
}
