package util

import (
	"fmt"
	"os"

	"sassoftware.io/viya/zlog"
	"sassoftware.io/viya/arke/pkg/i18n"
)

// Logger default logger
var Logger *zlog.Logger

var tracePerf bool

func init() {
	messages := i18n.L10n()
	err := zlog.DefaultBundle.Add(messages)
	if err != nil {
		panic(err.Error())
	}

	// Set up our default logger

	logFormat := os.Getenv("LOG_FORMAT")
	if logFormat == "" {
		logFormat = zlog.DefaultFormat
	}

	Logger = zlog.New(os.Stderr, logFormat)

	logLevel, err := zlog.ParseLevel(os.Getenv("LOG_LEVEL"))

	if err == nil {
		Logger.MessageBundleLevel = logLevel
	} else {
		logLevel = zlog.DefaultLevel
	}

	Logger.Level = logLevel

	tracePerf = false
	if tracePerfEnv := os.Getenv("TRACE_PERF"); tracePerfEnv == "1" {
		tracePerf = true
	}
}

func DebugNoFormat(s string, args ...interface{}) {
	if tracePerf {
		fmt.Printf(s, args...)
	}
}
