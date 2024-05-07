package replica

type replicaLog struct {
	lastLogIndex uint64 // 最后一条日志下标
	term         uint32 // 当前任期
}
