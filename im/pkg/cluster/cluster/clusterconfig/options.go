package clusterconfig

import (
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"time"
)

type Options struct {
	NodeId               uint64
	ConfigPath           string        // 集群配置文件路径
	Replicas             []uint64      // 副本列表 (必须包含自己本身的id)
	Transport            ITransport    // 传输层
	ElectionTimeoutTick  int           // 选举超时tick次数
	HeartbeatTimeoutTick int           // 心跳超时tick次数
	ProposeTimeout       time.Duration // 提议超时时间
	MessageSendInterval  time.Duration // 消息发送间隔
	MaxIdleInterval      time.Duration // 最大空闲间隔
	AppliedConfigVersion uint64        // 已应用的配置版本
	SlotCount            uint32        // 槽位数量
	Role                 pb.NodeRole   // 节点角色
}

func NewOptions() *Options {
	return &Options{
		ConfigPath:           "clusterconfig.json",
		ElectionTimeoutTick:  10,
		HeartbeatTimeoutTick: 1,
		ProposeTimeout:       time.Second * 5,
		MessageSendInterval:  time.Millisecond * 150,
		MaxIdleInterval:      time.Second * 1,
	}
}

type Option func(opts *Options)

func WithNodeId(nodeId uint64) Option {
	return func(opts *Options) {
		opts.NodeId = nodeId
	}
}

func WithConfigPath(configPath string) Option {
	return func(opts *Options) {
		opts.ConfigPath = configPath
	}
}

func WithElectionTimeoutTick(electionTimeoutTick int) Option {
	return func(opts *Options) {
		opts.ElectionTimeoutTick = electionTimeoutTick
	}
}

func WithHeartbeatTimeoutTick(heartbeatTimeoutTick int) Option {
	return func(opts *Options) {
		opts.HeartbeatTimeoutTick = heartbeatTimeoutTick
	}
}

func WithReplicas(replicas []uint64) Option {
	return func(opts *Options) {
		opts.Replicas = replicas
	}
}

func WithTransport(transport ITransport) Option {
	return func(opts *Options) {
		opts.Transport = transport
	}
}

func WithSlotCount(count uint32) Option {
	return func(opts *Options) {
		opts.SlotCount = count
	}
}

func WithRole(role pb.NodeRole) Option {
	return func(opts *Options) {
		opts.Role = role
	}
}
