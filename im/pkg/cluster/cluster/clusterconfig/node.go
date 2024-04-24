package clusterconfig

import (
	"chat/im/pkg/logger"
	"crypto/rand"
	"math/big"
	"sync"
	"time"
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

type Node struct {
	opts                      *Options
	state                     State
	votes                     map[uint64]bool // 投票结果
	electionElapsed           int             // 选举计时器
	heartbeatElapsed          int             // 心跳计时器
	randomizedElectionTimeout int             // 随机选举超时时间
	tickFnc                   func()
	stepFnc                   func(m Message) error
	role                      RoleType
	msgs                      []Message

	leaderConfigVersion    uint64 // leader配置版本号
	localConfigVersion     uint64 // 本地配置版本号
	committedConfigVersion uint64 // 已提交的配置版本号
	appliedConfigVersion   uint64 // 已应用的配置版本号

	configData           []byte            // 配置数据
	nodeConfigVersionMap map[uint64]uint64 // 每个节点当前配置的版本号

	activeReplicaMap map[uint64]time.Time // 活跃节点
	messageWait      *messageWait
}

func (n *Node) reset(term uint32) {
	n.state.term = term
	n.state.voteFor = None
	n.votes = make(map[uint64]bool)
	n.electionElapsed = 0
	n.resetRandomizedElectionTimeout() //设置随机选举超时时间
}

// resetRandomizedElectionTimeout 设置随机选举超时时间
func (n *Node) resetRandomizedElectionTimeout() {
	n.randomizedElectionTimeout = n.opts.ElectionTimeoutTick + globalRand.Intn(n.opts.ElectionTimeoutTick)
}

func NewNode(opts *Options) *Node {
	n := &Node{
		opts:                 opts,
		nodeConfigVersionMap: make(map[uint64]uint64),
		activeReplicaMap:     make(map[uint64]time.Time),
		messageWait:          newMessageWait(opts.MessageSendInterval),
	}

	n.appliedConfigVersion = opts.AppliedConfigVersion
	n.committedConfigVersion = n.appliedConfigVersion
	n.localConfigVersion = n.appliedConfigVersion

	if len(opts.Replicas) == 0 {
		n.reset(1)
		n.becomeLeader()
	} else {
		//开始raft选举算法
		n.becomeFollower(n.state.term, None)
	}
	return n
}

// becomeLeader 成为leader
func (n *Node) becomeLeader() {
	n.stepFnc = n.stepLeader
	n.reset(n.state.term)
	n.tickFnc = n.tickHeartbeat
	n.state.leader = n.opts.NodeId
	n.role = RoleLeader
	logger.Debug("become leader term ", n.state.term)
}

// 是否是单机
func (n *Node) isSingleNode() bool {

	return len(n.opts.Replicas) == 0 || (len(n.opts.Replicas) == 1 && n.opts.Replicas[0] == n.opts.NodeId)
}

func (n *Node) tickHeartbeat() {
	if !n.isLeader() {
		logger.Warn("not leader, but call tickHeartbeat")
		return
	}
	n.heartbeatElapsed++
	n.electionElapsed++

	if n.electionElapsed >= n.opts.ElectionTimeoutTick {
		n.electionElapsed = 0
	}
	if n.heartbeatElapsed >= n.opts.HeartbeatTimeoutTick {
		n.heartbeatElapsed = 0
		if err := n.Step(Message{From: n.opts.NodeId, Type: EventBeat}); err != nil {
			logger.Info("node tick heartbeat error ", err)
		}
	}
}

func (n *Node) becomeFollower(term uint32, leader uint64) {
	n.stepFnc = n.stepFollower
	n.reset(term)
	n.tickFnc = n.tickElection
	n.state.leader = leader
	n.role = RoleFollower
	logger.Debugf("become follower term %v leader %v", n.state.term, n.state.leader)
}

func (n *Node) tickElection() {
	n.electionElapsed++
	if n.pastElectionTimeout() { // 超时开始进行选举
		n.electionElapsed = 0
		err := n.Step(Message{
			Type: EventHup,
		})
		if err != nil {
			logger.Debug("node tick election error", err)
			return
		}
	}
}

// pastElectionTimeout 判断选举是否超时
func (n *Node) pastElectionTimeout() bool {
	return n.electionElapsed >= n.randomizedElectionTimeout
}

func (n *Node) becomeCandidate() {
	if n.role == RoleLeader {
		logger.Panic("invalid transition [leader -> candidate]")
	}
	n.stepFnc = n.stepCandidate
	n.reset(n.state.term + 1)
	n.tickFnc = n.tickElection
	n.state.voteFor = n.opts.NodeId
	n.state.leader = None
	n.role = RoleCandidate
	logger.Debug("become candidate term ", n.state.term)
}

func (n *Node) GetConfigData() []byte {
	return n.configData
}

func (n *Node) SetConfigData(data []byte) {
	n.configData = data
}

func (n *Node) send(m Message) {
	n.msgs = append(n.msgs, m)
}

func (n *Node) isLeader() bool {
	return n.role == RoleLeader
}

func (n *Node) Ready() Ready {
	// 检查是否需要进行同步，并根据需要发起同步操作。
	// 如果当前节点需要同步，则计算下一个同步序列号，并将一个新的同步消息添加到消息队列中。
	if n.followNeedSync() {
		seq := n.messageWait.next(n.state.leader, EventSync)
		n.msgs = append(n.msgs, n.newSync(seq))
	}
	if n.isLeader() {
		n.sendPingIfNeed()
	}

	if n.hasUnapplyLogs() {
		seq := n.messageWait.next(n.opts.NodeId, EventApply)
		n.msgs = append(n.msgs, n.newApply(seq))
	}

	rd := Ready{
		Messages: n.msgs,
	}
	return rd
}

// hasUnapplyLogs 是否有未应用的日志
func (n *Node) hasUnapplyLogs() bool {
	if n.messageWait.has(n.opts.NodeId, EventApply) {
		return false
	}
	return n.committedConfigVersion > n.appliedConfigVersion
}

func (n *Node) followNeedSync() bool {
	if n.state.leader == 0 {
		return false
	}
	if n.isLeader() {
		return false
	}
	if n.messageWait.has(n.state.leader, EventSync) {
		return false
	}
	return true
}

func (n *Node) sendPingIfNeed() bool {
	if !n.isLeader() {
		return false
	}
	hasPing := false
	for _, replicaId := range n.opts.Replicas {
		if replicaId == n.opts.NodeId {
			continue
		}
		if n.messageWait.has(replicaId, EventHeartbeat) {
			continue
		}
		activeTime := n.activeReplicaMap[replicaId]
		if activeTime.IsZero() || time.Since(activeTime) > n.opts.MaxIdleInterval {
			_ = n.messageWait.next(replicaId, EventHeartbeat)
			n.send(n.newHeartbeat(replicaId))
			hasPing = true
		}
	}
	return hasPing
}

func (n *Node) AcceptReady(rd Ready) {

	n.msgs = nil
}

func (n *Node) Tick() {
	n.tickFnc()
}

func (n *Node) HasReady() bool {
	if len(n.msgs) > 0 {
		return true
	}

	if n.followNeedSync() {
		return true
	}

	if n.isLeader() {
		if n.hasNeedPing() {
			return true
		}
	}
	if n.hasUnapplyLogs() {
		return true
	}

	return false
}

func (n *Node) ProposeConfigChange(version uint64, configData []byte) error {

	if !n.isLeader() {
		logger.Error("not leader, can not propose config change")
		return nil
	}
	return n.Step(Message{
		Type:          EventPropose,
		Term:          n.state.term,
		ConfigVersion: version,
		Config:        configData,
	})
}

// hasNeedPing 判断当前节点是否需要发送 Ping 消息
func (n *Node) hasNeedPing() bool {
	// 非 leader 节点不需要发送 Ping 消息
	if !n.isLeader() {
		return false
	}
	// 遍历所有副本节点，检查是否需要发送 Ping 消息
	for _, replicaId := range n.opts.Replicas {
		// 排除当前节点自身
		if replicaId == n.opts.NodeId {
			continue
		}
		// 如果节点已经等待了心跳消息，则不需要发送 Ping 消息
		if n.messageWait.has(replicaId, EventHeartbeat) {
			continue
		}
		// 检查副本节点的活跃时间，如果超出最大空闲间隔，则需要发送 Ping 消息
		activeTime := n.activeReplicaMap[replicaId]
		if activeTime.IsZero() || time.Since(activeTime) > n.opts.MaxIdleInterval {
			return true
		}
	}
	return false
}

func (n *Node) HasLeader() bool { return n.state.leader != None }

type State struct {
	leader  uint64
	term    uint32 //任期
	voteFor uint64 //选票
}

func (s State) Leader() uint64 {
	return s.leader
}

func (s State) Term() uint32 {
	return s.term
}

func (s State) VoteFor() uint64 {
	return s.voteFor
}

type Ready struct {
	Messages []Message
}

func IsEmptyReady(rd Ready) bool {
	return len(rd.Messages) == 0
}

var EmptyReady = Ready{}
