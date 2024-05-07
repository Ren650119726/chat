package cluster

type IShardLogStorage interface {
	//LastIndex 最后一条日志的索引
	LastIndex(shardNo string) (uint64, error)
}
