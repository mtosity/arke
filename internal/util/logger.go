// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"sync"

	"github.com/rs/zerolog"
	"sassoftware.io/viya/arke/i18n"
)

var (
	Logger         *ArkeLogger
	loggerSyncOnce = sync.Once{}
)

func init() {
	NewArkeLogger()
}

type ArkeLogger struct {
	logger *zerolog.Logger
	T      func(messageID string, args ...interface{}) string
}

func NewArkeLogger() *ArkeLogger {
	return NewArkeFileLogger(LogOutputFile)
}

func NewArkeFileLogger(file *os.File) *ArkeLogger {
	loggerSyncOnce.Do(func() {
		zl := createFileLogger(file)
		// since we call l.logger.Info() in each of the log methods in this file we need to adjust where the caller is reported from
		// default is 2
		zerolog.CallerSkipFrameCount = 3
		Logger = &ArkeLogger{
			logger: zl,
			T:      i18n.T,
		}
	})
	return Logger
}

func ResetLogger() {
	Logger = nil
	loggerSyncOnce = sync.Once{}
	zlogger = nil
	zloggerSyncOnce = sync.Once{}
}

// Info - Log an info level message with translation and parameter substitution
func (l *ArkeLogger) Info(messageID string, args ...interface{}) {
	l.logger.Info().Msg(l.T(messageID, args...))
}

// Warn - Log a warn level message with translation and parameter substitution
func (l *ArkeLogger) Warn(messageID string, args ...interface{}) {
	l.logger.Warn().Msg(l.T(messageID, args...))
}

// Debug - Log a debug level message with translation and parameter substitution
func (l *ArkeLogger) Debug(messageID string, args ...interface{}) {
	l.logger.Debug().Msg(l.T(messageID, args...))
}

// Debugf - Log a debug level message with format string and parameters
func (l *ArkeLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debug().Msgf(format, args...)
}

// Trace - Log a trace level message with translation and parameter substitution
func (l *ArkeLogger) Trace(messageID string, args ...interface{}) {
	l.logger.Trace().Msg(l.T(messageID, args...))
}

// Tracef - Log a trace level message with format string and parameters
func (l *ArkeLogger) Tracef(format string, args ...interface{}) {
	l.logger.Trace().Msgf(format, args...)
}

// Error - Log an error level message with translation and parameter substitution
func (l *ArkeLogger) Error(messageID string, args ...interface{}) {
	l.logger.Error().Msg(l.T(messageID, args...))
}

// Fatal - Log a fatal level message with translation and parameter substitution
func (l *ArkeLogger) Fatal(messageID string, args ...interface{}) {
	l.logger.Fatal().Msg(l.T(messageID, args...))
}
