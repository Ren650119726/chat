package replica

import (
	"chat/im/pkg/logger"
	"fmt"
	"go.uber.org/zap"
)

type Replica struct {
	nodeId   uint64
	opts     *Options
	replicas []uint64 // 副本节点ID集合（不包含本节点）

	leader   uint64
	role     Role
	stepFunc func(m Message) error

	localLeaderLastTerm uint32 // 本地领导任期，本地保存的term和startLogIndex的数据中最大的term，如果没有则为0

	replicaLog     *replicaLog
	disabledToSync bool
	logger.Log
	activeReplicas map[uint64]bool

	// -------------------- election --------------------
	electionElapsed           int // 选举计时器
	heartbeatElapsed          int
	randomizedElectionTimeout int // 随机选举超时时间
	tickFnc                   func()
	voteFor                   uint64          // 投票给谁
	votes                     map[uint64]bool // 投票记录

	messageWait *messageWait

	speedLevel SpeedLevel // 当前速度等级
}

func New(nodeId uint64, optList ...Option) *Replica {
	opts := NewOptions()
	for _, opt := range optList {
		opt(opts)
	}
	opts.NodeId = nodeId

	rc := &Replica{
		nodeId: nodeId,
		opts:   opts,
		Log:    logger.NewLog(fmt.Sprintf("replica[%d:%s]", nodeId, opts.LogPrefix)),
	}
	lastLeaderTerm, err := rc.opts.Storage.LeaderLastTerm()
	if err != nil {
		rc.Panic("get last leader term failed", zap.Error(err))
	}

	for _, replicaID := range rc.opts.Config.Replicas {
		if replicaID == nodeId {
			continue
		}
		rc.replicas = append(rc.replicas, replicaID)
	}
	rc.localLeaderLastTerm = lastLeaderTerm
	if rc.opts.ElectionOn {
		if rc.IsSingleNode() { // 如果是单节点，直接成为领导
			var term uint32 = 1
			if rc.replicaLog.term > 0 {
				term = rc.replicaLog.term
			}
			rc.becomeLeader(term)
		} else {
			rc.becomeFollower(rc.replicaLog.term, None)
		}
	}
	return rc
}

func (r *Replica) IsSingleNode() bool {
	return len(r.replicas) == 0
}

func (r *Replica) becomeLeader(term uint32) {
	r.stepFunc = r.stepLeader
	r.reset(term)
	r.tickFnc = r.tickHeartbeat
	r.replicaLog.term = term
	r.leader = r.nodeId
	r.role = RoleLeader
	r.disabledToSync = false

	r.Info("become leader", zap.Uint32("term", r.replicaLog.term))

	if r.replicaLog.lastLogIndex > 0 {
		//获取最后的日志索引
		lastTerm, err := r.opts.Storage.LeaderLastTerm()
		if err != nil {
			r.Panic("get leader last term failed", zap.Error(err))
		}
		if r.replicaLog.term > lastTerm {
			err = r.opts.Storage.SetLeaderTermStartIndex(r.replicaLog.term, r.replicaLog.lastLogIndex+1) // 保存领导任期和领导任期开始的日志下标
			if err != nil {
				r.Panic("set leader term start index failed", zap.Error(err))
			}
		}
	}
	r.activeReplicas = make(map[uint64]bool)
	r.messageWait.immediatelyPing()
}

func (r *Replica) becomeFollower(term uint32, leaderID uint64) {
	r.stepFunc = r.stepFollower
	r.reset(term)
	r.tickFnc = r.tickElection
	r.replicaLog.term = term
	r.leader = leaderID
	r.role = RoleFollower

	r.Info("become follower", zap.Uint32("term", term), zap.Uint64("leader", leaderID))

	r.messageWait.immediatelySync() // 立马可以同步了

	if r.replicaLog.lastLogIndex > 0 && r.leader != None {
		r.Info("disable to sync resolve log conflicts", zap.Uint64("leader", r.leader))
		r.disabledToSync = true // 禁止去同步领导的日志,等待本地日志冲突解决后，再去同步领导的日志
	}

}

func (r *Replica) Tick() {
	r.messageWait.tick()
	if r.tickFnc != nil {
		r.tickFnc()
	}
}

func (r *Replica) reset(term uint32) {
	if r.replicaLog.term != term {
		r.replicaLog.term = term
	}
	r.voteFor = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.setSpeedLevel(LevelFast)
	r.resetRandomizedElectionTimeout()

}

func (r *Replica) tickHeartbeat() {

}

func (r *Replica) tickElection() {
	if !r.opts.ElectionOn { // 禁止选举
		return
	}
	r.electionElapsed++
	if r.pastElectionTimeout() { // 超时开始进行选举
		r.electionElapsed = 0
		err := r.Step(Message{
			MsgType: MsgHup,
		})
		if err != nil {
			r.Debug("node tick election error", zap.Error(err))
			return
		}
	}
}

func (r *Replica) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Replica) Ready() Ready {

	rd := r.readyWithoutAccept()
	r.acceptReady(rd)
	return rd
}

func (r *Replica) setSpeedLevel(level SpeedLevel) {

	r.speedLevel = level

	switch level {
	case LevelFast:
		r.messageWait.setSyncIntervalTickCount(1)
	case LevelNormal:
		r.messageWait.setSyncIntervalTickCount(2)
	case LevelSlow:
		r.messageWait.setSyncIntervalTickCount(10)
	case LevelSlowest:
		r.messageWait.setSyncIntervalTickCount(50)
	case LevelStop: // 这种情况基本是停止状态，要么等待重新激活，要么等待被销毁
		r.messageWait.setSyncIntervalTickCount(100000)
	}

}

func (r *Replica) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.opts.ElectionTimeoutTick + globalRand.Intn(r.opts.ElectionTimeoutTick)
}

func (r *Replica) readyWithoutAccept() Ready {

	return Ready{}
}

func (r *Replica) acceptReady(rd Ready) {

}

func (r *Replica) becomeLearner(term uint32, from uint64) {

}
