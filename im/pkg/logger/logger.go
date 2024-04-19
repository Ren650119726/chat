package logger

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
	"time"
)

var (
	defaultEncoderCfg = EncoderCfg{
		TimeKey:       "ts",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		Level:         "capitalColor",
		Time:          "ISO8601",
		Duration:      "seconds",
		Caller:        "short",
		Encoding:      "console",
	}
	defaultRotateCfg = RotateCfg{
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
	}

	logger = &Logger{
		rotate:  defaultRotateCfg,
		encoder: defaultEncoderCfg,
		Level:   zapcore.InfoLevel,
	}

	infoLevelEnabler = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl == zapcore.InfoLevel
	})

	errorLevelEnabler = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})

	warnLevelEnabler = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl == zapcore.WarnLevel
	})

	panicLevelEnabler = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl == zapcore.PanicLevel
	})
)

func init() {
	atom := zap.NewAtomicLevel()
	atom.SetLevel(zapcore.InfoLevel)
	core := zapcore.NewCore(
		logger.getEncoder(logger.encoder.LevelKey),
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)),
		atom,
	)
	zapLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	logger.sugaredLogger = zapLogger.Sugar()
	logger.logger = zapLogger
}

type Logger struct {
	logger        *zap.Logger
	sugaredLogger *zap.SugaredLogger

	infoLogOff  bool
	errorLogOff bool
	warnLogOff  bool

	debug bool

	infoLogPath  string
	errorLogPath string
	warnLogPath  string
	panicLogPath string

	rotate  RotateCfg
	encoder EncoderCfg

	Level      zapcore.Level
	logOptions *LogOptions
}

type EncoderCfg struct {
	TimeKey       string
	LevelKey      string
	NameKey       string
	CallerKey     string
	MessageKey    string
	StacktraceKey string
	Level         string
	Time          string
	Duration      string
	Caller        string
	Encoding      string
}

type RotateCfg struct {
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
}

func (l *Logger) Init() {
	l.infoLogPath = path.Join(l.logOptions.LogDir, "info.log")
	l.errorLogPath = path.Join(l.logOptions.LogDir, "error.log")
	l.warnLogPath = path.Join(l.logOptions.LogDir, "warn.log")
	l.panicLogPath = path.Join(l.logOptions.LogDir, "panic.log")

	zapLogger := zap.New(zapcore.NewTee(
		zapcore.NewCore(l.getEncoder(l.encoder.LevelKey), l.getLogWriter(l.infoLogPath), infoLevelEnabler),
		zapcore.NewCore(l.getEncoder(l.encoder.LevelKey), l.getLogWriter(l.errorLogPath), errorLevelEnabler),
		zapcore.NewCore(l.getEncoder(l.encoder.LevelKey), l.getLogWriter(l.warnLogPath), warnLevelEnabler),
		zapcore.NewCore(l.getEncoder(l.encoder.LevelKey), l.getLogWriter(l.panicLogPath), panicLevelEnabler),
	), zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(errorLevelEnabler))
	l.sugaredLogger = zapLogger.Sugar()
	l.logger = zapLogger
}

func (l *Logger) getEncoder(levelKey string) zapcore.Encoder {

	var (
		timeEncoder     = new(zapcore.TimeEncoder)
		durationEncoder = new(zapcore.DurationEncoder)
		callerEncoder   = new(zapcore.CallerEncoder)
		nameEncoder     = new(zapcore.NameEncoder)
		levelEncoder    = new(zapcore.LevelEncoder)
	)

	_ = timeEncoder.UnmarshalText([]byte(l.encoder.Time))
	_ = durationEncoder.UnmarshalText([]byte(l.encoder.Duration))
	_ = callerEncoder.UnmarshalText([]byte(l.encoder.Caller))
	_ = nameEncoder.UnmarshalText([]byte("full"))
	_ = levelEncoder.UnmarshalText([]byte(l.encoder.Level))

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        l.encoder.TimeKey,
		LevelKey:       levelKey,
		NameKey:        l.encoder.NameKey,
		CallerKey:      l.encoder.CallerKey,
		MessageKey:     l.encoder.MessageKey,
		StacktraceKey:  l.encoder.StacktraceKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    *levelEncoder,
		EncodeDuration: *durationEncoder,
		EncodeCaller:   *callerEncoder,
		EncodeName:     *nameEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05"))
		},
	}
	return filterZapEncoder(l.encoder.Encoding, encoderConfig)
}

func (l *Logger) getLogWriter(path string) zapcore.WriteSyncer {
	if path != "" {
		lumberJackLogger := &lumberjack.Logger{
			Filename:   path,
			MaxSize:    l.rotate.MaxSize,
			MaxBackups: l.rotate.MaxBackups,
			MaxAge:     l.rotate.MaxAge,
			Compress:   l.rotate.Compress,
		}
		if l.debug {
			return zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(lumberJackLogger))
		}
		return zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(lumberJackLogger))
		//return zapcore.AddSync(lumberJackLogger)
	}
	return zapcore.AddSync(os.Stdout)
}

func (l *Logger) SetRotate(cfg RotateCfg) {
	if cfg.MaxSize != 0 && cfg.MaxAge != 0 && cfg.MaxBackups != 0 {
		l.rotate = cfg
	}
}

type LogOptions struct {
	Name     string
	LogLevel string
	LogDir   string
	LineNum  bool
}

func InitWithConfig(options *LogOptions) {
	logger.Level = filterZapAtomicLevelByViper(options.LogLevel)
	logger.logOptions = options
	logger.Init()
}

// Debug print the debug message.
func Debug(info ...interface{}) {
	if !logger.infoLogOff {
		if logger.Level <= zapcore.DebugLevel {
			logger.sugaredLogger.Info(info...)
		}
	}
}

// Debugf print the debug message.
func Debugf(template string, args ...interface{}) {
	if !logger.infoLogOff && logger.Level <= zapcore.DebugLevel {
		logger.sugaredLogger.Infof(template, args...)
	}
}

// Info print the info message.
func Info(info ...interface{}) {
	if !logger.infoLogOff && logger.Level <= zapcore.InfoLevel {
		logger.sugaredLogger.Info(info...)
	}
}

type logFunc func(msg string, fields ...zapcore.Field)

// Infof print the info message.
func Infof(template string, args ...interface{}) {
	if !logger.infoLogOff && logger.Level <= zapcore.InfoLevel {
		logger.sugaredLogger.Infof(template, args...)
	}
}

// Warn print the warning message.
func Warn(info ...interface{}) {
	if !logger.infoLogOff && logger.Level <= zapcore.WarnLevel {
		logger.sugaredLogger.Warn(info...)
	}
}

// Warnf print the warning message.
func Warnf(template string, args ...interface{}) {
	if !logger.infoLogOff && logger.Level <= zapcore.WarnLevel {
		logger.sugaredLogger.Warnf(template, args...)
	}
}

// Error print the error message.
func Error(err ...interface{}) {
	if !logger.errorLogOff && logger.Level <= zapcore.ErrorLevel {
		logger.sugaredLogger.Error(err...)
	}
}

// Errorf print the error message.
func Errorf(template string, args ...interface{}) {
	if !logger.errorLogOff && logger.Level <= zapcore.ErrorLevel {
		logger.sugaredLogger.Errorf(template, args...)
	}
}

// Fatal print the fatal message.
func Fatal(info ...interface{}) {
	if !logger.errorLogOff && logger.Level <= zapcore.ErrorLevel {
		logger.sugaredLogger.Fatal(info...)
	}
}

// Fatalf print the fatal message.
func Fatalf(template string, args ...interface{}) {
	if !logger.errorLogOff && logger.Level <= zapcore.ErrorLevel {
		logger.sugaredLogger.Fatalf(template, args...)
	}
}

// Fatal print the panic message.
func Panic(info ...interface{}) {
	logger.sugaredLogger.Panic(info...)
}

// Panicf print the panic message.
func Panicf(template string, args ...interface{}) {
	logger.sugaredLogger.Panicf(template, args...)
}

func filterZapEncoder(encoding string, encoderConfig zapcore.EncoderConfig) zapcore.Encoder {
	var encoder zapcore.Encoder
	switch encoding {
	default:
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	case "json":
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case "console":
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}
	return encoder
}

func filterZapAtomicLevelByViper(level string) zapcore.Level {
	// 日志级别映射
	leveMapping := map[string]zapcore.Level{
		"debug": zap.DebugLevel,
		"info":  zap.InfoLevel,
		"warn":  zap.WarnLevel,
		"error": zap.ErrorLevel,
		"fatal": zap.FatalLevel,
		"panic": zap.PanicLevel,
	}
	if atomViper, ok := leveMapping[level]; ok {
		return atomViper
	}
	return zap.InfoLevel
}
