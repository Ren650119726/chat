package cluster

type Options struct {
	NodeID              uint64
	DataDir             string            // 数据存储目录
	SlotCount           uint32            // 槽数量
	InitNodes           map[uint64]string // 初始化节点列表
	SlotMaxReplicaCount uint32            // 每个槽位最大副本数量
}
