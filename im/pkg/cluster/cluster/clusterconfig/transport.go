package clusterconfig

type ITransport interface {
	Send(m Message) error
	OnMessage(f func(m Message))
}

type Message struct {
	Seq              uint64    // 不参与编码
	version          uint16    // 数据版本
	Type             EventType // 消息类型
	Term             uint32    // 当前任期
	From             uint64    // 源节点
	To               uint64    // 目标节点
	ConfigVersion    uint64    // 配置版本
	CommittedVersion uint64    // 已提交的配置版本
	Config           []byte    // 配置
}
