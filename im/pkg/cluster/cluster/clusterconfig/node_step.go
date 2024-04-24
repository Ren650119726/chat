package clusterconfig

import (
	"chat/im/pkg/logger"
	"time"
)

func (n *Node) Step(m Message) error {
	switch {
	case m.Term == 0:
	case m.Term > n.state.term: //如果自己的节点的term小于收到的term，则更新term，并becomeFollower
		logger.Warnf("received message with higher term %v from:%v to:%v msgType:%v", m.Term, m.From, m.To, m.Type)
		if m.Type == EventHeartbeat { //如果收到的消息是心跳消息，则更新term，leader节点为请求发送者
			n.becomeFollower(m.Term, m.From)
		} else if m.Type != EventVoteResp && m.Type != EventVote { // 如果收到的消息不是心跳消息，则更新term，leader节点为空
			n.becomeFollower(m.Term, None)
		}
	}

	switch m.Type {
	case EventHup: //开始选举请求
		n.hup()
	case EventVote: //收到投票请求
		//用于判断节点n是否可以对消息m进行投票。具体判断条件如下：
		//节点n当前没有投票给任何节点，或者节点n已经投票给了消息发送者并且当前没有领导者；
		//消息m的配置版本号大于或等于节点n的本地配置版本号；
		//消息m的任期大于或等于节点n当前的任期。
		//如果满足以上条件，则节点n可以对消息m进行投票。
		if n.state.voteFor == None || (n.state.voteFor == m.From && n.state.leader == None) ||
			(m.ConfigVersion >= n.localConfigVersion && m.Term >= n.state.term) {
			n.send(Message{
				Term:          n.state.term,
				Type:          EventVoteResp,
				From:          n.opts.NodeId,
				To:            m.From,
				ConfigVersion: n.localConfigVersion,
			})
			n.electionElapsed = 0 //重置选举计时器
			n.state.voteFor = m.From
			logger.Infof("agree vote voteFor:%v term:%v configVersion:%v", m.From, m.Term, n.localConfigVersion)
		} else {
			if n.state.voteFor != None { //如果节点n已经投票给其他节点，则拒绝投票请求
				logger.Infof("already vote for other voteFor:%v", n.state.voteFor)
			} else if m.ConfigVersion < n.localConfigVersion { //如果消息m的配置版本号小于节点n的本地配置版本号，则拒绝投票请求
				logger.Infof("lower config version, reject vote")
			} else if m.Term < n.state.term { //如果消息m的任期小于节点n当前的任期，则拒绝投票请求
				logger.Infof("lower term, reject vote")
			}
		}
	case EventApplyResp:
		if m.CommittedVersion > n.appliedConfigVersion {
			logger.Infof("received apply response from:%v to:%v term:%v configVersion:%v", m.From, m.To, m.Term, m.ConfigVersion)
			n.appliedConfigVersion = m.CommittedVersion
			n.configData = m.Config
		}
	default:
		err := n.stepFnc(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) stepLeader(m Message) error {
	switch m.Type {
	case EventHeartbeat: // 处理心跳事件，无需额外操作。
		// 根据接收到的来自服务器m的消息，更新当前服务器的状态。
		// 如果消息的term大于当前服务器的term，或者消息的term等于当前服务器的term但配置版本更高，
		// 则服务器会转换为follower状态，并发送心跳响应。
		if m.Term > n.state.term {
			n.becomeFollower(m.Term, m.From)
			n.sendHeartbeatResp(m)
		} else if m.Term == n.state.term && m.ConfigVersion > n.localConfigVersion {
			n.becomeFollower(m.Term, m.From)
			n.sendHeartbeatResp(m)
		}
	case EventPropose: //提议
		if m.ConfigVersion > n.localConfigVersion {
			n.leaderConfigVersion = m.ConfigVersion
			n.localConfigVersion = m.ConfigVersion
			n.configData = m.Config
			if n.isSingleNode() {
				n.updateCommitForLeader()
			}
		}
	case EventBeat:
		n.bcastHeartbeat()
	case EventHeartbeatResp:
		n.activeReplicaMap[m.From] = time.Now()
	case EventSync:
		n.nodeConfigVersionMap[m.From] = m.ConfigVersion
		n.activeReplicaMap[m.From] = time.Now()
		// 检查是否存在本地配置版本，并且是否高于已提交的配置版本，若是，则更新提交的配置版本
		if n.localConfigVersion > 0 && n.localConfigVersion > n.committedConfigVersion {
			n.updateCommitForLeader() // 更新提交的配置版本
		}
		// 发送同步响应消息，包含当前节点的配置数据、本地配置版本、已提交配置版本等信息
		n.send(Message{
			From:             n.opts.NodeId,
			To:               m.From,
			Type:             EventSyncResp,
			Term:             n.state.term,
			Config:           n.configData,
			ConfigVersion:    n.localConfigVersion,
			CommittedVersion: n.committedConfigVersion,
		})
	}
	return nil
}

func (n *Node) stepFollower(m Message) error {
	switch m.Type {
	case EventHeartbeat: // 处理心跳事件，无需额外操作。
		// 重置选举计时器，并更新当前的领导者和任期信息。
		// 如果接收到的消息所携带的配置版本大于本地配置版本，则更新本地领导者配置版本。
		// 最后，发送心跳响应给消息的发送者。
		n.electionElapsed = 0
		n.state.leader = m.From
		n.state.term = m.Term
		if m.ConfigVersion > n.localConfigVersion {
			n.leaderConfigVersion = m.ConfigVersion
		}
		n.sendHeartbeatResp(m)
		return nil
	case EventSyncResp: // 处理同步响应事件，无需额外操作。
		// 检查并更新配置版本和提交版本
		// 如果接收到的配置版本（m.ConfigVersion）大于本地配置版本（n.localConfigVersion），
		// 则更新本地配置版本和领导者配置版本为接收到的版本，并更新配置数据为接收到的配置。
		// 如果接收到的提交版本（m.CommittedVersion）大于已提交的配置版本（n.committedConfigVersion），
		// 则更新已提交的配置版本，并记录日志信息。
		if m.ConfigVersion > n.localConfigVersion {
			n.leaderConfigVersion = m.ConfigVersion
			n.localConfigVersion = m.ConfigVersion
			n.configData = m.Config
		}
		// 更新并记录已提交配置版本
		if m.CommittedVersion > n.committedConfigVersion {
			n.committedConfigVersion = m.CommittedVersion
			logger.Infof("update commit for follower committedConfigVersion:%v leaderConfigVersion:%v", n.committedConfigVersion, n.leaderConfigVersion)
		}
		return nil
	}
	return nil
}

func (n *Node) stepCandidate(m Message) error {
	//处理心跳事件和投票响应事件
	switch m.Type {
	case EventVoteResp: // 处理投票响应事件
		logger.Infof("received vote response from:%v to:%v term:%v configVersion:%v", m.From, m.To, m.Term, m.ConfigVersion)
		n.poll(m) //统计投票结果
	case EventHeartbeat:
		n.becomeFollower(m.Term, m.From)
		//发送心跳响应
		n.sendHeartbeatResp(m)
	}
	return nil
}

// poll 统计投票结果
func (n *Node) poll(m Message) {
	n.votes[m.From] = m.Type == EventVoteResp // 投票结果
	var granted int
	for _, v := range n.votes {
		if v {
			granted++
		}
	}
	if granted >= n.quorum() { // 获得票数超过一半
		n.becomeLeader()
	} else if len(n.votes)-granted >= n.quorum() {
		n.becomeFollower(n.state.term, None)
	}
}

func (n *Node) quorum() int {
	return len(n.opts.Replicas)/2 + 1
}

func (n *Node) hup() {
	n.campaign()
}

// campaign 开始选举
func (n *Node) campaign() {
	n.becomeCandidate()
	for _, nodeID := range n.opts.Replicas {
		if nodeID == n.opts.NodeId {
			// 自己给自己投一票
			n.send(Message{To: nodeID, From: nodeID, Term: n.state.term, Type: EventVoteResp})
			continue
		}
		logger.Infof("sent vote request from %v to %v term %v", n.opts.NodeId, nodeID, n.state.term)
		n.sendRequestVote(nodeID)
	}
}

// updateCommitForLeader 更新领导者的提交版本
func (n *Node) updateCommitForLeader() {
	logger.Infof("update commit for leader leaderConfigVersion:%v localConfigVersion:%v", n.leaderConfigVersion, n.localConfigVersion)
	if n.isSingleNode() { // 单机模式
		logger.Infof("update commit for leader for single localConfigVersion:%v", n.localConfigVersion)
		n.committedConfigVersion = n.leaderConfigVersion
		return
	}
	//统计大于本地配置版本号的版本号数量。如果这个数量加上1大于等于节点的票数的一半，就会将提交配置版本号设置为本地配置版本号。
	successCount := 0
	for _, version := range n.nodeConfigVersionMap {
		if version > n.localConfigVersion {
			successCount++
		}
	}
	if successCount+1 >= n.quorum() { // 获得票数超过一半
		logger.Infof("update commit for leader for quorum localConfigVersion:%v", n.localConfigVersion)
		n.committedConfigVersion = n.localConfigVersion
	}
}

func (n *Node) sendRequestVote(nodeID uint64) {
	n.send(Message{
		From:          n.opts.NodeId,
		To:            nodeID,
		Type:          EventVote,
		Term:          n.state.term,
		ConfigVersion: n.localConfigVersion,
	})
}

func (n *Node) sendHeartbeatResp(m Message) {
	n.send(Message{
		To:   m.From,
		Type: EventHeartbeatResp,
		From: n.opts.NodeId,
		Term: n.state.term,
	})
}

// bcastHeartbeat 广播心跳
func (n *Node) bcastHeartbeat() {
	for _, nodeID := range n.opts.Replicas {
		if nodeID == n.opts.NodeId {
			continue
		}
		n.sendHeartbeat(nodeID)
	}
}

// sendHeartbeat 发送心跳
func (n *Node) sendHeartbeat(nodeID uint64) {
	n.send(Message{
		From:             n.opts.NodeId,
		To:               nodeID,
		Type:             EventHeartbeat,
		Term:             n.state.term,
		ConfigVersion:    n.localConfigVersion,
		CommittedVersion: n.committedConfigVersion,
	})
}

func (n *Node) newSync(seq uint64) Message {

	return Message{
		Seq:           seq,
		From:          n.opts.NodeId,
		To:            n.state.leader,
		Type:          EventSync,
		Term:          n.state.term,
		ConfigVersion: n.localConfigVersion + 1,
	}
}

func (n *Node) newHeartbeat(to uint64) Message {
	return Message{
		From:             n.opts.NodeId,
		To:               to,
		Type:             EventHeartbeat,
		Term:             n.state.term,
		ConfigVersion:    n.localConfigVersion,
		CommittedVersion: n.committedConfigVersion,
	}
}

func (n *Node) newApply(seq uint64) Message {
	return Message{
		Seq:           seq,
		From:          n.opts.NodeId,
		To:            n.opts.NodeId,
		Type:          EventApply,
		Term:          n.state.term,
		ConfigVersion: n.localConfigVersion,
		Config:        n.configData,
	}
}
