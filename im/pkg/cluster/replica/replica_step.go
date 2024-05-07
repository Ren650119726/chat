package replica

import "go.uber.org/zap"

func (r *Replica) Step(m Message) error {
	switch {
	case m.Term == 0: // 本地消息
		// r.Warn("term is zero", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To))
	case m.Term > r.replicaLog.term:
		r.Debug("received message with higher term", zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.String("msgType", m.MsgType.String()))
		// 高任期消息
		if m.MsgType == MsgPing || m.MsgType == MsgLeaderTermStartIndexResp || m.MsgType == MsgSyncResp {
			if r.role == RoleLearner {
				r.becomeLearner(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, m.From)
			}
		}
	}
	return nil
}

func (r *Replica) stepLeader(m Message) error {
	return nil
}

func (r *Replica) stepFollower(m Message) error {
	return nil
}
