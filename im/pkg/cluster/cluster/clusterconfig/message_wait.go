package clusterconfig

import "time"

type messageWait struct {
	waitMap             map[EventType]map[uint64]waitInfo
	messageSendInterval time.Duration
}

func newMessageWait(messageSendInterval time.Duration) *messageWait {
	return &messageWait{
		waitMap:             make(map[EventType]map[uint64]waitInfo),
		messageSendInterval: messageSendInterval,
	}
}
func (m *messageWait) next(nodeId uint64, msgType EventType) uint64 {
	nodeWaitMap := m.waitMap[msgType]
	if nodeWaitMap == nil {
		nodeWaitMap = make(map[uint64]waitInfo)
		m.waitMap[msgType] = nodeWaitMap
	}
	seq := nodeWaitMap[nodeId].seq + 1
	nodeWaitMap[nodeId] = waitInfo{
		seq:       seq,
		createdAt: time.Now(),
		wait:      true,
	}
	return seq
}

func (m *messageWait) finish(nodeId uint64, msgType EventType) {
	nodeWaitMap := m.waitMap[msgType]
	if nodeWaitMap == nil {
		return
	}
	if v, ok := nodeWaitMap[nodeId]; ok {
		nodeWaitMap[nodeId] = waitInfo{
			seq:       v.seq,
			createdAt: time.Now(),
			wait:      false,
		}
	}
}
func (m *messageWait) has(nodeId uint64, msgType EventType) bool {
	nodeWaitMap := m.waitMap[msgType]
	if nodeWaitMap == nil {
		return false
	}
	if v, ok := nodeWaitMap[nodeId]; ok {
		if v.wait {
			return !v.createdAt.Add(m.messageSendInterval).Before(time.Now())
		}

		return false
	}
	return false
}

type waitInfo struct {
	seq       uint64
	createdAt time.Time
	wait      bool
}
