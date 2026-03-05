// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"bytes"
	"testing"
)

func TestTestLogger_GetOutput(t *testing.T) {
	tests := []struct {
		name     string
		logFunc  func(*TestLogger)
		contains string
	}{
		{
			name: "captures info log",
			logFunc: func(l *TestLogger) {
				l.Info("test info message")
			},
			contains: "test info message",
		},
		{
			name: "captures error log",
			logFunc: func(l *TestLogger) {
				l.Error("test error message")
			},
			contains: "test error message",
		},
		{
			name: "captures multiple logs",
			logFunc: func(l *TestLogger) {
				l.Info("first message")
				l.Info("second message")
			},
			contains: "first message",
		},
		{
			name: "returns empty when no logs written",
			logFunc: func(_ *TestLogger) {
				// no logging
			},
			contains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, cleanup := GetTestLoggerWithCleanup()
			defer cleanup()

			tt.logFunc(logger)

			output := logger.GetOutput()

			if tt.contains != "" && !bytes.Contains(output, []byte(tt.contains)) {
				t.Errorf("GetOutput() output = %s, should contain %s", output, tt.contains)
			}
			if tt.contains == "" && len(output) != 0 {
				t.Errorf("GetOutput() output = %s, expected empty output", output)
			}
		})
	}
}
