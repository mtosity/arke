package util

import (
	"fmt"
	"os"

	rlogs "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"sassoftware.io/viya/zlog"
	"sassoftware.io/viya/arke/i18n"
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

	// set rabbit streams log level to debug if our log level is debug
	if logLevel == zlog.Debug {
		stream.SetLevelInfo(rlogs.DEBUG)
	}
}

func DebugNoFormat(s string, args ...interface{}) {
	if tracePerf {
		fmt.Printf(s, args...)
	}
}
