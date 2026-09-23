package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/psrpc"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/livekit/protocol/logger/testutil"
	"github.com/livekit/protocol/logger/zaputil"
	"github.com/livekit/protocol/utils/must"
)

func zapLoggerCore(l Logger) zapcore.Core {
	return l.(ZapLogger).ToZap().Desugar().Core()
}

func TestLoggerComponent(t *testing.T) {
	t.Run("inheriting parent level", func(t *testing.T) {
		l, err := NewZapLogger(&Config{
			Level: "info",
			ComponentLevels: map[string]string{
				"mycomponent": "warn",
			},
		})
		require.NoError(t, err)

		sub := zapLoggerCore(l.WithComponent("sub"))
		require.True(t, sub.Enabled(zapcore.InfoLevel))
		require.False(t, sub.Enabled(zapcore.DebugLevel))

		compLogger := zapLoggerCore(l.WithComponent("mycomponent").WithComponent("level2"))
		require.True(t, compLogger.Enabled(zapcore.WarnLevel))
		require.False(t, compLogger.Enabled(zapcore.InfoLevel))
	})

	t.Run("obeys component override", func(t *testing.T) {
		l, err := NewZapLogger(&Config{
			Level: "info",
			ComponentLevels: map[string]string{
				"sub":  "debug",
				"sub2": "error",
			},
		})
		require.NoError(t, err)

		sub := zapLoggerCore(l.WithComponent("sub"))
		sub2 := zapLoggerCore(l.WithComponent("sub2"))
		require.True(t, sub.Enabled(zapcore.DebugLevel))
		require.False(t, sub2.Enabled(zapcore.InfoLevel))
	})

	t.Run("updates dynamically", func(t *testing.T) {
		config := &Config{
			Level: "info",
			ComponentLevels: map[string]string{
				"sub":  "debug",
				"sub2": "error",
			},
		}
		l, err := NewZapLogger(config)
		require.NoError(t, err)

		sub := zapLoggerCore(l.WithComponent("sub"))
		sub2 := zapLoggerCore(l.WithComponent("sub2.test"))
		err = config.Update(&Config{
			Level: "debug",
			ComponentLevels: map[string]string{
				"sub": "info",
				// sub2 removed
			},
		})
		require.NoError(t, err)

		require.True(t, zapLoggerCore(l).Enabled(zapcore.DebugLevel))
		require.False(t, sub.Enabled(zapcore.DebugLevel))
		require.True(t, sub.Enabled(zapcore.InfoLevel))
		require.True(t, sub2.Enabled(zapcore.InfoLevel))
	})

	t.Run("log output matches expected values", func(t *testing.T) {
		ws := &testutil.BufferedWriteSyncer{}
		l, err := NewZapLogger(&Config{}, WithTap(zaputil.NewWriteEnabler(ws, zapcore.DebugLevel)))
		require.NoError(t, err)
		l.Debugw("foo", "bar", "baz")

		var log TestLogOutput
		require.NoError(t, ws.Unmarshal(&log))

		require.Equal(t, "debug", log.Level)
		require.NotEqual(t, 0, log.TS)
		require.NotEqual(t, "", log.Caller)
		require.Equal(t, "foo", log.Msg)
		require.Equal(t, "baz", log.Bar)
	})

	t.Run("component enabler for tapped logger returns lowest enabled level", func(t *testing.T) {
		tapLevel := zap.NewAtomicLevel()
		l, err := NewZapLogger(&Config{Level: "info"}, WithTap(zaputil.NewWriteEnabler(&testutil.BufferedWriteSyncer{}, tapLevel)))
		require.NoError(t, err)

		lvl := l.ComponentLeveler().ComponentLevel("foo")

		// check config level
		require.False(t, lvl.Enabled(zapcore.DebugLevel))
		require.True(t, lvl.Enabled(zapcore.InfoLevel))

		// check tap level
		tapLevel.SetLevel(zapcore.DebugLevel)
		require.True(t, lvl.Enabled(zapcore.DebugLevel))
	})
}

type TestLogOutput struct {
	testutil.TestLogOutput
	Bar string
}

type logFunc func(string, ...any)

func testLogCaller(f logFunc) {
	f("test")
}

func TestLoggerCallDepth(t *testing.T) {
	t.Cleanup(func() {
		defaultLogger = LogRLogger(discardLogger)
		pkgLogger = LogRLogger(discardLogger)
	})

	var caller string
	testLogCaller(func(string, ...any) {
		_, file, line, _ := runtime.Caller(1)
		caller = fmt.Sprintf("%s:%d", file, line)
	})

	cases := map[string]func(l Logger) logFunc{
		"NewZapLogger": func(l Logger) logFunc {
			return l.Debugw
		},
		"package logger": func(l Logger) logFunc {
			SetLogger(l, "TEST")
			return Debugw
		},
		"GetLogger": func(l Logger) logFunc {
			SetLogger(l, "TEST")
			return GetLogger().Debugw
		},
		"ToZap": func(l Logger) logFunc {
			return l.(ZapLogger).ToZap().Debugw
		},
		"WithUnlikelyValues": func(l Logger) logFunc {
			return l.WithUnlikelyValues().Debugw
		},
	}
	for label, getLogFunc := range cases {
		t.Run(label, func(t *testing.T) {
			ws := &testutil.BufferedWriteSyncer{}
			l := must.Get(NewZapLogger(&Config{}, WithTap(zaputil.NewWriteEnabler(ws, zapcore.DebugLevel))))

			testLogCaller(getLogFunc(l))

			var log TestLogOutput
			require.NoError(t, ws.Unmarshal(&log))
			require.True(t, strings.HasSuffix(caller, log.Caller), `caller mismatch expected suffix match on "%s" got "%s"`, caller, log.Caller)
		})
	}
}

// chanWriteSyncer hands each write to the test, so a line logged from another goroutine is read
// without racing on a shared buffer.
type chanWriteSyncer chan []byte

func (c chanWriteSyncer) Write(p []byte) (int, error) {
	c <- bytes.Clone(p)
	return len(p), nil
}

func (chanWriteSyncer) Sync() error { return nil }

func TestSetLoggerRoutesPsrpcBusErrors(t *testing.T) {
	t.Cleanup(func() {
		defaultLogger = LogRLogger(discardLogger)
		pkgLogger = LogRLogger(discardLogger)
	})

	lines := make(chanWriteSyncer, 1)
	SetLogger(must.Get(NewZapLogger(&Config{}, WithTap(zaputil.NewWriteEnabler(lines, zapcore.DebugLevel)))), "TEST")

	dialErr := errors.New("redis is unreachable")
	var dials atomic.Int32
	rc := redis.NewClient(&redis.Options{
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			if dials.Add(1) == 1 {
				return nil, dialErr
			}
			// Park every reconnect after the first failure, so the bus logs once and then stays
			// quiet instead of retrying into later tests.
			select {}
		},
	})
	psrpc.NewRedisMessageBus(rc)

	var line map[string]any
	select {
	case b := <-lines:
		require.NoError(t, json.Unmarshal(b, &line))
	case <-time.After(5 * time.Second):
		t.Fatal("the psrpc bus error never reached the configured logger")
	}
	require.Equal(t, "error", line["level"])
	require.Equal(t, "redis receive message failed", line["msg"])
	require.Equal(t, "TEST.psrpc", line["logger"])
	require.Contains(t, fmt.Sprint(line), dialErr.Error())
}
