package server

import (
	"chat/im/version"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"os"
	"os/user"
	"path"
	"reflect"
	"strings"
	"time"
)

type Mode string

const (
	DebugMode   Mode = "debug"   //debug 模式
	ReleaseMode Mode = "release" // 正式模式
	BenchMode   Mode = "bench"   // 压力测试模式
	TestMode    Mode = "test"    //测试
)

type Options struct {
	viper.Viper        // 内部配置对象
	Mode        Mode   // 模式 debug 测试 release 正式 bench 压力测试
	HTTPAddr    string // http api的监听地址 默认为 0.0.0.0:5001
	Addr        string // tcp监听地址 例如：tcp://0.0.0.0:5100
	RootDir     string // 根目录
	DataDir     string `mapstructure:"dataDir"` // 数据目录
	WSAddr      string // websocket 监听地址 例如：ws://0.0.0.0:5200
	Logger      Logger
	Version     string
	Cluster     Cluster
}

type Logger struct {
	Dir     string // 日志存储目录
	Level   string
	LineNum bool // 是否显示代码行数
}

type Role string

const (
	RoleReplica Role = "replica"
	RoleProxy   Role = "proxy"
)

type Cluster struct {
	NodeId                     uint64        // 节点ID
	Addr                       string        // 节点监听地址 例如：tcp://0.0.0.0:11110
	GRPCAddr                   string        // 节点grpc监听地址 例如：0.0.0.0:11111
	ServerAddr                 string        // 节点服务地址 例如 127.0.0.1:11110
	ReqTimeout                 time.Duration // 请求超时时间
	Role                       Role          // 节点角色 replica, proxy
	Seed                       string        // 种子节点
	ReplicaCount               int           // 节点副本数量
	SlotReplicaCount           int           // 每个槽的副本数量
	ChannelReplicaCount        int           // 每个频道的副本数量
	SlotCount                  int           // 槽数量
	Nodes                      []*Node       // 集群节点地址
	PeerRPCMsgTimeout          time.Duration // 节点之间rpc消息超时时间
	PeerRPCTimeoutScanInterval time.Duration // 节点之间rpc消息超时时间扫描间隔
}

type Node struct {
	Id         uint64
	ServerAddr string
}

func GetHomeDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err == nil {
		return homeDir, nil
	}
	u, err := user.Current()
	if err == nil {
		return u.HomeDir, nil
	}

	return "", fmt.Errorf("user home directory not found")
}

func DefaultOptions() *Options {
	homeDir, err := GetHomeDir()
	if err != nil {
		panic(err)
	}
	defaultOpts := &Options{
		Mode:     DebugMode,
		HTTPAddr: "0.0.0.0:5001",
		Addr:     "tcp://0.0.0.0:5100",
		RootDir:  path.Join(homeDir, ".ninja"),
		DataDir:  path.Join(homeDir, ".ninja", "data"),
		WSAddr:   "ws://0.0.0.0:5200",
		Version:  version.Version,
		Logger: Logger{
			Dir:     path.Join(homeDir, ".ninja", "logs"),
			Level:   "debug",
			LineNum: true,
		},
		Cluster: Cluster{
			NodeId:                     1,
			Addr:                       "tcp://0.0.0.0:11110",
			GRPCAddr:                   "0.0.0.0:11111",
			ServerAddr:                 "",
			ReqTimeout:                 time.Second * 10,
			Role:                       RoleReplica,
			SlotCount:                  128,
			ReplicaCount:               3,
			SlotReplicaCount:           3,
			ChannelReplicaCount:        3,
			PeerRPCMsgTimeout:          time.Second * 20,
			PeerRPCTimeoutScanInterval: time.Second * 1,
		},
	}
	return defaultOpts
}

func (o *Options) ConfigureWithViper(vp *viper.Viper) {
	o.Viper = *vp
	fmt.Println(o.DataDir)
	defaultsMap := map[string]interface{}{
		"ninja.rootDir": o.RootDir,
		"ninja.dataDir": o.DataDir,
	}

	for param := range defaultsMap {
		val := o.Get(param)
		if val == nil {
			o.SetDefault(param, defaultsMap[param])
		} else {
			o.SetDefault(param, val)
			o.Set(param, val)
		}
	}
	if err := o.UnmarshalKey("ninja", o); err != nil {
		panic(err)
	}
	fmt.Println(o.DataDir)
}

// UnmarshalKey unmarshal key into v
func (o *Options) UnmarshalKey(key string, rawVal any, opts ...viper.DecoderConfigOption) error {
	delimiter := "."
	prefix := key + delimiter

	i := o.Get(key)
	if i == nil {
		return nil
	}
	if isStringMapInterface(i) {
		val := i.(map[string]interface{})
		keys := o.AllKeys()
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			mk := strings.TrimPrefix(k, prefix)
			mk = strings.Split(mk, delimiter)[0]
			if _, exists := val[mk]; exists {
				continue
			}
			mv := o.Get(key + delimiter + mk)
			if mv == nil {
				continue
			}
			val[mk] = mv
		}
		i = val
	}
	return decode(i, defaultDecoderConfig(rawVal))
}

func isStringMapInterface(val interface{}) bool {
	vt := reflect.TypeOf(val)
	return vt.Kind() == reflect.Map &&
		vt.Key().Kind() == reflect.String &&
		vt.Elem().Kind() == reflect.Interface
}

// A wrapper around mapstructure.Decode that mimics the WeakDecode functionality
func decode(input interface{}, config *mapstructure.DecoderConfig) error {
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}

// defaultDecoderConfig returns default mapstructure.DecoderConfig with support
// of time.Duration values & string slices
func defaultDecoderConfig(output interface{}, opts ...viper.DecoderConfigOption) *mapstructure.DecoderConfig {
	c := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c

}
