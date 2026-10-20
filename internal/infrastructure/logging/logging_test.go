package logging

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		wantErr bool
	}{
		{name: "debug", level: "debug"},
		{name: "infoUpperCase", level: "INFO"},
		{name: "warn", level: "warn"},
		{name: "error", level: "error"},
		{name: "unknown", level: "loud", wantErr: true},
		{name: "empty", level: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetLevel(tt.level)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestLoggerWritesEveryLevel(t *testing.T) {
	require.NoError(t, SetLevel("debug"))
	t.Cleanup(func() { _ = SetLevel("info") })

	tests := []struct {
		name  string
		write func(*Logger)
		level string
	}{
		{name: "debug", write: func(l *Logger) { l.Debugf("hello %d", 1) }, level: `"level":"debug"`},
		{name: "info", write: func(l *Logger) { l.Infof("hello %d", 1) }, level: `"level":"info"`},
		{name: "warn", write: func(l *Logger) { l.Warnf("hello %d", 1) }, level: `"level":"warn"`},
		{name: "error", write: func(l *Logger) { l.Errorf("hello %d", 1) }, level: `"level":"error"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer

			tt.write(New(&out))

			assert.Contains(t, out.String(), tt.level)
			assert.Contains(t, out.String(), "hello 1")
			assert.Contains(t, out.String(), appContext)
		})
	}
}
