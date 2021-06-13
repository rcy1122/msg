package log

import (
	"context"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

type contextKey int

const (
	loggerKey contextKey = iota
)

// Logger the Blockchain-service logger.
type Logger interface {
	logrus.FieldLogger
	WriterLevel(logrus.Level) *io.PipeWriter
}

var (
	mainLogger Logger
	logFile    *os.File
)

func init() {
	mainLogger = logrus.StandardLogger()
	logrus.SetOutput(os.Stdout)
}

// SetLogger sets the logger.
func SetLogger(l Logger) {
	mainLogger = l
}

// SetOutput sets the standard logger output.
func SetOutput(out ...io.Writer) {
	wr := io.MultiWriter(out...)
	logrus.SetOutput(wr)
}

// SetFormatter sets the standard logger formatter.
func SetFormatter(formatter logrus.Formatter) {
	logrus.SetFormatter(formatter)
}

// SetLevel sets the standard logger level.
func SetLevel(level logrus.Level) {
	logrus.SetLevel(level)
}

// GetLevel returns the standard logger level.
func GetLevel() logrus.Level {
	return logrus.GetLevel()
}

// Str adds a string field.
func Str(key, value string) func(logrus.Fields) {
	return func(fields logrus.Fields) {
		fields[key] = value
	}
}

// With Adds fields.
func With(ctx context.Context, opts ...func(logrus.Fields)) context.Context {
	logger := FromContext(ctx)

	fields := make(logrus.Fields)
	for _, opt := range opts {
		opt(fields)
	}
	logger = logger.WithFields(fields)

	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext Gets the logger from context.
func FromContext(ctx context.Context) Logger {
	if ctx == nil {
		panic("nil context")
	}

	logger, ok := ctx.Value(loggerKey).(Logger)
	if !ok {
		logger = mainLogger
	}

	return logger
}

// WithoutContext Gets the main logger.
func WithoutContext() Logger {
	return mainLogger
}

// OpenFile opens the log file using the specified path.
func OpenFile(path string) (*os.File, error) {
	var err error
	logFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}

// CloseFile closes the log and sets the Output to stdout.
func CloseFile() error {
	logrus.SetOutput(os.Stdout)

	if logFile != nil {
		return logFile.Close()
	}
	return nil
}
