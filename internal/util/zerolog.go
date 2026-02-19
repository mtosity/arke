package util

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var (
	zlogger         *zerolog.Logger
	zloggerSyncOnce = sync.Once{}

	// LogOutputFile is the default output destination for the logger
	LogOutputFile = os.Stderr

	// logWriter is the io.Writer used by the logger
	logWriter io.Writer
)

const EnvLogFormat = "LOG_FORMAT"
const EnvLogLevel = "LOG_LEVEL"
const versionKey = "version"

func init() {
	createLogger()
}

// createLogger - Create and configure the root zerolog logger instance
func createLogger() *zerolog.Logger {
	zloggerSyncOnce.Do(func() {
		level, err := loadAndParseLevel()

		zerolog.CallerMarshalFunc = func(_ uintptr, file string, line int) string {
			return filepath.Join(path.Base(path.Dir(file)), path.Base(file)) + ":" + strconv.Itoa(line)
		}
		outputFormat := os.Getenv("LOG_FORMAT")
		if outputFormat == "term" {
			logWriter = zerolog.ConsoleWriter{Out: LogOutputFile}
		} else {
			logWriter = LogOutputFile
		}

		// default is time.RFC3339
		zerolog.TimeFieldFormat = time.RFC3339Nano
		// default is "time"
		zerolog.TimestampFieldName = "timeStamp"

		// must explicitly set the level
		l := zerolog.New(logWriter).Level(level).With().
			Timestamp().
			Str("source", getSource()).
			Caller().
			Int(versionKey, 1).
			Logger()

		if err != nil {
			l.Warn().Msg(err.Error())
		}
		zlogger = &l
	})
	return zlogger
}

func loadAndParseLevel() (zerolog.Level, error) {
	// If no level is set, zerolog would default to disabled logging
	// so handle an empty value as info level
	envLevel := strings.ToLower(os.Getenv(EnvLogLevel))
	if envLevel == "" {
		return zerolog.InfoLevel, nil
	}
	level, err := zerolog.ParseLevel(envLevel)
	if err != nil {
		return zerolog.InfoLevel, fmt.Errorf("invalid log level, defaulting to INFO: %s", envLevel)
	}
	return level, nil
}

func getSource() string {
	source, _ := os.Executable()
	source = path.Base(source)
	return source
}
