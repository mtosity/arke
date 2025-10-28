package util

import (
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"
)

var logger *slog.Logger
var LogWriter = os.Stderr
var logSource = getSource

const (
	LevelTrace = slog.Level(-8)
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)
const envLogFormat = "LOG_FORMAT"
const envLogLevel = "LOG_LEVEL"
const versionKey = "version"
const sourceKey = "source"
const propertiesKey = "properties"
const messageKey = "message"
const timeStampKey = "timeStamp"

func init() {
	logger = createLogger()
}

func createLogger() *slog.Logger {
	return slog.New(generateHandler()).With(versionKey, 1, sourceKey, logSource())
}

func generateHandler() slog.Handler {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: loadAndParseLevel(),
		ReplaceAttr: replaceAttributes,
		AddSource:   true}
	handler = slog.NewJSONHandler(LogWriter, opts)
	format := os.Getenv(envLogFormat)
	if format == "term" {
		handler = slog.NewTextHandler(LogWriter, opts)
	}
	return handler
}

func replaceAttributes(_ []string, attr slog.Attr) slog.Attr {
	// Customize level attribute representation and lowercase it
	if attr.Key == slog.LevelKey {
		level := attr.Value.Any().(slog.Level)
		var strAttrValue string
		switch level {
		case LevelTrace:
			strAttrValue = "trace"
		default:
			strAttrValue = level.String()

		}
		attr.Value = slog.StringValue(strings.ToLower(strAttrValue))
	}

	// Rename time attribute
	if attr.Key == slog.TimeKey {
		attr.Key = timeStampKey
	}

	// Rename message attribute
	if attr.Key == slog.MessageKey {
		attr.Key = messageKey
	}

	// Group source attribute under properties as caller
	if attr.Key == slog.SourceKey {
		s, ok := attr.Value.Any().(*slog.Source)
		if ok {
			attr.Key = "caller"
			dir, f := path.Split(s.File)
			caller := path.Join(path.Base(dir), f) + ":" + strconv.Itoa(s.Line)
			attr.Value = slog.StringValue(caller)
			attr = slog.GroupAttrs(propertiesKey, attr)
		}
	}

	return attr
}

func loadAndParseLevel() slog.Level {
	level := os.Getenv(envLogLevel)
	switch strings.ToUpper(level) {
	case "TRACE":
		return LevelTrace
	case "DEBUG":
		return LevelDebug
	case "WARN":
		return LevelWarn
	case "ERROR":
		return LevelError
	default:
		return LevelInfo
	}
}

func getSource() string {
	source, _ := os.Executable()
	source = path.Base(source)
	return source
}
