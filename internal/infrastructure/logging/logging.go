// Package logging adapts lib-go-log, Yape's structured logger, to the small
// interface the rest of the service uses.
package logging

import (
	"fmt"
	"io"
	"strings"

	"github.com/yaperos/lib-go-log/pkg/logger"
)

const appContext = "ms-ml-conv-history-bs"

// Logger writes structured log lines.
type Logger struct {
	log *logger.YapeLogger
}

// New returns a logger that writes to w.
func New(w io.Writer) *Logger {
	return &Logger{log: logger.New(w).WithParameters(map[string]string{"app_context": appContext})}
}

// SetLevel sets the process-wide level: debug, info, warn or error.
func SetLevel(name string) error {
	level, ok := logger.YapeLoggerMap[strings.ToUpper(name)]
	if !ok {
		return fmt.Errorf("unknown log level %q", name)
	}
	logger.SetGlobalLevel(level)

	return nil
}

// Debugf logs at debug level.
func (l *Logger) Debugf(msg string, v ...any) { l.log.Debugf(msg, v...) }

// Infof logs at info level.
func (l *Logger) Infof(msg string, v ...any) { l.log.Infof(msg, v...) }

// Warnf logs at warn level.
func (l *Logger) Warnf(msg string, v ...any) { l.log.Warnf(msg, v...) }

// Errorf logs at error level.
func (l *Logger) Errorf(msg string, v ...any) { l.log.Errorf(msg, v...) }
