package clusterconfig

import (
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"chat/im/pkg/logger"
	"context"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/atomic"
	"time"
)

type Server struct {
	configManager *ConfigManager // 配置管理
	opts          *Options
	node          *Node
	recvc         chan Message
	stopper       *syncutil.Stopper
	commitWait    *commitWait
	leaderId      atomic.Uint64
}

func New(nodeId uint64, optList ...Option) *Server {
	opts := NewOptions()
	for _, opt := range optList {
		opt(opts)
	}
	opts.NodeId = nodeId

	s := &Server{
		opts:          opts,
		stopper:       syncutil.NewStopper(),
		recvc:         make(chan Message, 100),
		configManager: NewConfigManager(opts),
		commitWait:    newCommitWait(),
	}
	opts.AppliedConfigVersion = s.configManager.GetConfig().Version
	s.node = NewNode(opts)
	s.node.SetConfigData(s.configManager.GetConfigData())
	return s
}

func (s *Server) Start() error {
	s.stopper.RunWorker(s.run)
	if s.opts.Transport != nil {
		s.opts.Transport.OnMessage(func(msg Message) {
			select {
			case s.recvc <- msg:
			case <-s.stopper.ShouldStop():
				return
			}
		})
	}
	return nil
}

func (s *Server) Stop() {
	s.stopper.Stop()

}

func (s *Server) run() {
	var rd Ready
	var err error
	tick := time.NewTicker(time.Millisecond * 200)

	for {
		if s.node.HasReady() {
			rd = s.node.Ready()
			s.node.AcceptReady(rd)
		} else {
			rd = EmptyReady
		}
		if s.leaderId.Load() != s.node.state.leader {
			if s.node.HasLeader() {
				if s.leaderId.Load() == None {
					logger.Infof("elected leader term:%v leader:%v", s.node.state.term, s.node.state.leader)
				} else {
					logger.Infof("changed leader term:%v leader:%v", s.node.state.term, s.node.state.leader)
				}
			} else {
				logger.Infof("lost leader term:%v leader:%v", s.node.state.term, s.node.state.leader)
			}
			s.leaderId.Store(s.node.state.leader)
		}
		if !IsEmptyReady(rd) {
			for _, msg := range rd.Messages {
				s.handleMessage(msg)
			}
		}
		select {
		case <-tick.C:
			s.node.Tick()
		case msg := <-s.recvc:
			err = s.node.Step(msg)
			if err != nil {
				logger.Error("node step error", err)
			}
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

func (s *Server) handleMessage(m Message) {
	var err error
	if m.To == s.opts.NodeId { // 处理自己的消息
		if m.Type == EventApply {
			s.handleApplyReq(m)
		} else {
			err = s.node.Step(m)
			if err != nil {
				logger.Error("node step error", err)
			}
		}
		return
	}
	if m.To == None {
		logger.Warnf("message to none from:%v to:%v msgType:%v", m.From, m.To, m.Type.String())
		return
	}
	if s.opts.Transport != nil {
		err = s.opts.Transport.Send(m)
		if err != nil {
			logger.Error("transport send error", err)
		}
	}
}

func (s *Server) handleApplyReq(m Message) {
	if len(m.Config) == 0 {
		logger.Panic("config is empty")
		return
	}
	logger.Infof("receive apply config configVersion:%v committedVersion:%v", m.ConfigVersion, m.CommittedVersion)
	newCfg := &pb.Config{}
	err := s.configManager.UnmarshalConfigData(m.Config, newCfg)
	if err != nil {
		logger.Panic("unmarshal config error", err)
		return
	}
	newCfg.Version = m.ConfigVersion
	err = s.configManager.UpdateConfig(newCfg)
	if err != nil {
		logger.Panic("update config error", err)
		return
	}
	logger.Infof("apply config success confVersion:%v", m.ConfigVersion)
	err = s.node.Step(Message{
		From:          m.To,
		To:            m.From,
		Type:          EventApplyResp,
		Term:          m.Term,
		ConfigVersion: m.ConfigVersion,
		Config:        m.Config,
	})
	if err != nil {
		logger.Error("node step error", err)
		return
	}
	s.commitWait.commitIndex(m.ConfigVersion)
}

func (s *Server) ConfigManager() *ConfigManager {
	return s.configManager
}

func (s *Server) IsLeader() bool {

	return s.leaderId.Load() == s.opts.NodeId
}

func (s *Server) IsSingleNode() bool {
	return s.node.isSingleNode()
}

func (s *Server) SetReplicas(replicas []uint64) {
	s.node.opts.Replicas = replicas
}

func (s *Server) GetLastConfigVersion() uint64 {
	return s.node.localConfigVersion
}

func (s *Server) AddOrUpdateNodes(nodes []*pb.Node) error {
	if !s.node.isLeader() {
		return ErrNotLeader
	}
	newCfg := s.configManager.GetConfig().Clone()
	s.configManager.AddOrUpdateNodes(nodes, newCfg)
	return s.ProposeConfigChange(s.configManager.GetConfigDataByCfg(newCfg))
}

// ProposeConfigChange 提交配置变更
func (s *Server) ProposeConfigChange(cfgData []byte) error {
	// 获取当前配置的版本号并加1，以确保新提议的配置版本是唯一的
	version := s.GetLastConfigVersion() + 1
	// 在commitWait上添加一个新的等待索引，用于同步配置更改的提交
	waitC, err := s.commitWait.addWaitIndex(version)
	if err != nil {
		return err
	}
	// 向节点提议进行配置更改
	err = s.node.ProposeConfigChange(version, cfgData)
	if err != nil {
		return err
	}
	// 设置一个超时上下文，以防止配置更改过程无限期等待
	timeoutCtx, cancel := context.WithTimeout(context.Background(), s.opts.ProposeTimeout)
	defer cancel()
	// 等待超时、配置更改完成或服务停止
	select {
	case <-timeoutCtx.Done():
		return timeoutCtx.Err()
	case <-waitC: // 配置更改完成
		return nil
	case <-s.stopper.ShouldStop():
		return ErrStopped
	}
}

func (s *Server) Step(ctx context.Context, m Message) error {
	select {
	case s.recvc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopper.ShouldStop():
		return ErrStopped
	}
}

func (s *Server) WaitLeader() error {
	tk := time.NewTicker(time.Millisecond * 20)
	for {
		select {
		case <-tk.C:
			if s.node.state.leader != None {
				return nil
			}
		case <-s.stopper.ShouldStop():
			return ErrStopped
		}
	}
}

func (s *Server) MustWaitLeader() {
	err := s.WaitLeader()
	if err != nil {
		logger.Panic("wait leader error", err)
	}
}
