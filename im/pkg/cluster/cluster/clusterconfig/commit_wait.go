package clusterconfig

import (
	"chat/im/pkg/logger"
	"go.uber.org/zap"
	"sync"
)

type waitItem struct {
	logIndex uint64
	waitC    chan struct{}
}

type commitWait struct {
	waitList []waitItem
	sync.Mutex
}

func newCommitWait() *commitWait {
	return &commitWait{}
}

func (c *commitWait) addWaitIndex(logIndex uint64) (<-chan struct{}, error) {
	c.Lock()
	defer c.Unlock()
	waitC := make(chan struct{}, 1)
	c.waitList = append(c.waitList, waitItem{logIndex: logIndex, waitC: waitC})
	return waitC, nil
}

func (c *commitWait) commitIndex(logIndex uint64) {
	c.Lock()
	defer c.Unlock()
	maxIndex := 0
	exist := false
	for i, item := range c.waitList {
		if item.logIndex <= logIndex {
			select {
			case item.waitC <- struct{}{}:
			default:
				logger.Warn("commitIndex notify failed", zap.Uint64("logIndex", logIndex))
			}
			maxIndex = i
			exist = true
		}
	}
	if exist {
		c.waitList = c.waitList[maxIndex+1:]
	}
}
