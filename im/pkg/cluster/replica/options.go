package replica

type Options struct {
	NodeId              uint64 // 当前节点ID
	Config              *Config
	ElectionOn          bool   // 是否开启选举
	LogPrefix           string // 日志前缀
	ElectionTimeoutTick int    // 选举超时tick次数，超过次tick数则发起选举
	Storage             IStorage
}

type Option func(o *Options)

func NewOptions() *Options {
	return &Options{
		ElectionOn: false,
	}
}

func WithStorage(storage IStorage) Option {
	return func(o *Options) {
		o.Storage = storage
	}
}

func WithElectionOn(electionOn bool) Option {
	return func(o *Options) {
		o.ElectionOn = electionOn
	}
}

func WithElectionTimeoutTick(tick int) Option {
	return func(o *Options) {
		o.ElectionTimeoutTick = tick
	}
}

func WithLogPrefix(prefix string) Option {
	return func(o *Options) {
		o.LogPrefix = prefix
	}
}

func WithConfig(config *Config) Option {
	return func(o *Options) {
		o.Config = config
	}
}
