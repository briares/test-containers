package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewAtLevel(level string) (*zap.Logger, error) {
	logLevel := zapcore.InfoLevel

	if level != "" {
		var err error

		logLevel, err = zapcore.ParseLevel(level)
		if err != nil {
			return nil, err
		}
	}

	logConf := zap.NewProductionConfig()
	logConf.Level = zap.NewAtomicLevelAt(logLevel)

	logger, err := logConf.Build()
	if err != nil {
		return nil, err
	}

	return logger, nil
}
