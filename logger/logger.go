package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"sync"
	"sync/atomic"
)

type myWriteSyncer struct {
	ws atomic.Value
	l  sync.Mutex
}

func (m *myWriteSyncer) getWs() zapcore.WriteSyncer {
	val := m.ws.Load()
	if val == nil {
		return nil
	}
	return val.(zapcore.WriteSyncer)
}

func (m *myWriteSyncer) SetWs(ws zapcore.WriteSyncer) {
	m.l.Lock()
	val := m.ws.Load()
	m.ws.Store(ws)
	m.l.Unlock()
	if val != nil {
		switch cas := val.(type) {
		case *lumberjack.Logger:
			err := cas.Close()
			zap.L().Warn("failed to close old log writer", zap.Error(err))
		case zapcore.WriteSyncer:
			err := cas.Sync()
			zap.L().Warn("failed to sync old log writer", zap.Error(err))
		}
	}
}

func (m *myWriteSyncer) Write(p []byte) (n int, err error) {
	m.l.Lock()
	defer m.l.Unlock()
	ws := m.getWs()
	if ws == nil {
		return os.Stdout.Write(p)
	} else {
		return ws.Write(p)
	}
}

func (m *myWriteSyncer) Sync() error {
	m.l.Lock()
	defer m.l.Unlock()
	ws := m.getWs()
	if ws == nil {
		return os.Stdout.Sync()
	} else {
		return ws.Sync()
	}
}

var localWs *myWriteSyncer
var logLevel zap.AtomicLevel
var logger *zap.Logger

func init() {
	localWs = &myWriteSyncer{}
	logLevel = zap.NewAtomicLevel()
	logLevel.SetLevel(zapcore.DebugLevel)
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), zapcore.Lock(localWs), logLevel)
	logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
}

func SetLogLevel(level string) (err error) {
	l := zapcore.DebugLevel
	err = l.UnmarshalText([]byte(level))
	if err == nil {
		logLevel.SetLevel(l)
	}
	return
}

func SetLogWriteSyncer(ws zapcore.WriteSyncer) {
	localWs.SetWs(ws)
}

func SetLogToFile(logFile string) {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    10,
		MaxBackups: 10,
	}
	localWs.SetWs(zapcore.AddSync(lumberJackLogger))
}

func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	logger.Fatal(msg, fields...)
}
