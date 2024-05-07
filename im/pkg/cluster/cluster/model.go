package cluster

import (
	"crypto/rand"
	"math/big"
	"sync"
)

var globalRand = &lockedRand{}

type lockedRand struct {
	mu sync.Mutex
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	r.mu.Unlock()
	return int(v.Int64())
}

type SlotInfo struct {
	SlotId   uint32
	LogIndex uint64
}

type SlotLogInfoResp struct {
	NodeId uint64
	Slots  []SlotInfo
}

type SlotLogInfoReq struct {
	SlotIds []uint32
}
