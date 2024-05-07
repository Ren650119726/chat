package replica

type messageWait struct {
	messageSendIntervalTickCount int // 消息发送间隔
	replicaMaxCount              int

	syncMaxIntervalTickCount            int // 同步消息最大间隔
	syncIntervalTickCount               int // 同步消息发送间隔
	syncTickCount                       int // 同步消息发送间隔计数
	pingTickCount                       int // ping消息发送间隔计数
	appendLogTickCount                  int // 追加日志到磁盘的间隔计数
	msgLeaderTermStartIndexReqTickCount int
}

func (m *messageWait) immediatelyPing() {
	m.pingTickCount = m.messageSendIntervalTickCount
}

func (m *messageWait) setSyncIntervalTickCount(syncIntervalTickCount int) {
	m.syncIntervalTickCount = syncIntervalTickCount
}

// 立马进行下次同步
func (m *messageWait) immediatelySync() {
	if m.syncMaxIntervalTickCount > m.syncIntervalTickCount {
		m.syncTickCount = m.syncMaxIntervalTickCount
	} else {
		m.syncTickCount = m.syncIntervalTickCount
	}
}

func (m *messageWait) tick() {
	m.syncTickCount++
	m.pingTickCount++
	m.msgLeaderTermStartIndexReqTickCount++
	m.appendLogTickCount++
}
