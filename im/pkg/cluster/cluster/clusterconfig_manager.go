package cluster

import (
	"chat/im/pkg/cluster/cluster/clusterconfig"
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"chat/im/pkg/logger"
	"github.com/lni/goutils/syncutil"
	"path"
	"sync"
	"time"
)

type clusterconfigManager struct {
	clusterconfigServer *clusterconfig.Server // 分布式配置服务
	stopper             *syncutil.Stopper
	onMessage           func(m clusterconfig.Message)
	opts                *Options
	mu                  sync.RWMutex
}

// handleMessage 处理集群配置消息
func (c *clusterconfigManager) handleMessage(m clusterconfig.Message) {
	c.mu.Lock()
	onMsg := c.onMessage
	c.mu.Unlock()
	if onMsg != nil {
		onMsg(m)
	}
}

func (c *clusterconfigManager) Send(m clusterconfig.Message) error {
	return nil
}

func (c *clusterconfigManager) OnMessage(f func(m clusterconfig.Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMessage = f
}

func newClusterconfigManager(opts *Options) *clusterconfigManager {
	remoteCfgPath := clusterconfig.WithConfigPath(path.Join(opts.DataDir, "clusterconfig.json"))
	c := &clusterconfigManager{
		opts:    opts,
		stopper: syncutil.NewStopper(),
	}
	//todo replicas应该从opts获取
	replicas := []uint64{1, 2, 3}
	c.clusterconfigServer = clusterconfig.New(opts.NodeID, clusterconfig.WithSlotCount(opts.SlotCount), clusterconfig.WithTransport(c), clusterconfig.WithReplicas(replicas), remoteCfgPath)
	return c
}

func (c *clusterconfigManager) start() error {
	c.stopper.RunWorker(c.loop)
	return c.clusterconfigServer.Start()
}

func (c *clusterconfigManager) loop() {
	tk := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-tk.C:
			if c.clusterconfigServer.IsLeader() { //判断当前节点是否是leader节点
				c.checkClusterConfig() //检查集群配置
			}
		case <-c.stopper.ShouldStop():
			return
		}
	}
}

func (c *clusterconfigManager) checkClusterConfig() {
	clusterConfig := c.clusterconfigServer.ConfigManager().GetConfig()

	newCfg := clusterConfig.Clone()
	var hasChange = false
	// 初始化节点
	changed := c.initNodesIfNeed(newCfg)
	if changed {
		hasChange = true
	}
	_ = hasChange
	//todo
	// 初始化slot
	changed = c.initSlotsIfNeed(newCfg)
	if changed {
		hasChange = true
	}
	// 检查节点在线状态
	// 初始化slot leader
	changed = c.initSlotLeaderIfNeed(newCfg)
	if changed {
		hasChange = true
	}
	// 选举slot leader
	c.electionSlotLeaderIfNeed(newCfg)
	// 处理新加入的节点

	//有改变则提交配置变更
	if hasChange {
		logger.Infof("propose config change version: %v", newCfg.Version)
		err := c.clusterconfigServer.ProposeConfigChange(c.clusterconfigServer.ConfigManager().GetConfigDataByCfg(newCfg))
		if err != nil {
			logger.Error("propose config change error", err)
		}
	}
}

// initNodesIfNeed 初始化节点
func (c *clusterconfigManager) initNodesIfNeed(newCfg *pb.Config) bool {
	//检查配置信息已经存在节点信息
	if len(newCfg.Nodes) > 0 {
		return false
	}
	if c.clusterconfigServer.IsSingleNode() { //是否是单节点
		c.clusterconfigServer.ConfigManager().AddOrUpdateNodes([]*pb.Node{
			{
				Id:        c.opts.NodeID,
				AllowVote: true,
				Online:    true,
				CreatedAt: time.Now().Unix(),
				Status:    pb.NodeStatus_NodeStatusDone,
			},
		}, newCfg)
		return true
	}
	if len(newCfg.Nodes) == 0 && len(c.opts.InitNodes) > 0 {
		newNodes := make([]*pb.Node, 0, len(c.opts.InitNodes))
		for replicaID, clusterAddr := range c.opts.InitNodes {
			newNodes = append(newNodes, &pb.Node{
				Id:          replicaID,
				ClusterAddr: clusterAddr,
				AllowVote:   true,
				Online:      true,
				CreatedAt:   time.Now().Unix(),
				Status:      pb.NodeStatus_NodeStatusDone,
			})
		}
		c.clusterconfigServer.ConfigManager().AddOrUpdateNodes(newNodes, newCfg)
		return true
	}
	return false
}

// initSlotsIfNeed 初始化slot
func (c *clusterconfigManager) initSlotsIfNeed(newCfg *pb.Config) bool {
	if len(newCfg.Slots) == 0 {
		newSlots := make([]*pb.Slot, c.opts.SlotCount)
		for i := uint32(0); i < c.opts.SlotCount; i++ {
			newSlots = append(newSlots, &pb.Slot{
				Id:           i,
				ReplicaCount: c.opts.SlotMaxReplicaCount,
			})
		}
		c.clusterconfigServer.ConfigManager().AddOrUpdateSlots(newSlots, newCfg)
		return true
	}
	return false
}

// initSlotLeaderIfNeed 初始化slot leader
func (c *clusterconfigManager) initSlotLeaderIfNeed(newCfg *pb.Config) bool {
	if len(newCfg.Slots) == 0 || len(newCfg.Nodes) == 0 {
		return false
	}
	replicas := make([]uint64, 0, len(newCfg.Nodes))
	for _, n := range newCfg.Nodes {
		// 不允许投票或状态不是NodeStatusDone的节点
		if !n.AllowVote || n.Status != pb.NodeStatus_NodeStatusDone {
			continue
		}
		replicas = append(replicas, n.Id)
	}
	if len(replicas) == 0 {
		return false
	}
	hasChange := false
	offset := 0
	for _, slot := range newCfg.Slots {
		//给slot按照ReplicaCount分配replicas
		if len(slot.Replicas) == 0 {
			replicaCount := slot.ReplicaCount
			if len(replicas) < int(slot.ReplicaCount) {
				slot.Replicas = replicas
			} else {
				slot.Replicas = make([]uint64, 0, replicaCount)
				for i := uint32(0); i < replicaCount; i++ {
					idx := (offset + int(i)) % len(replicas)
					slot.Replicas = append(slot.Replicas, replicas[idx])
				}
			}
			offset++
		}
		if slot.Leader == 0 { //选举leader,随机选取一个replica作为leader
			randomIndex := globalRand.Intn(len(slot.Replicas))
			slot.Term = 1
			slot.Leader = slot.Replicas[randomIndex]
			hasChange = true
		}
	}

	return hasChange
}

// electionSlotLeaderIfNeed 选举slot leader
func (c *clusterconfigManager) electionSlotLeaderIfNeed(newCfg *pb.Config) bool {
	if len(newCfg.Slots) == 0 || len(newCfg.Nodes) == 0 {
		return false
	}
	hasChange := false
	waitElectionSlots := map[uint64][]uint32{}              // 等待选举的槽
	electionSlotIds := make([]uint32, 0, len(newCfg.Slots)) // 需要进行选举的槽id集合
	for _, slot := range newCfg.Slots {
		if slot.Leader == 0 {
			continue
		}
		if c.nodeIsOnline(slot.Leader) {
			continue
		}
		for _, replicaId := range slot.Replicas {
			if replicaId != c.opts.NodeID && !c.nodeIsOnline(replicaId) {
				continue
			}
			waitElectionSlots[replicaId] = append(waitElectionSlots[replicaId], slot.Id)
		}
		electionSlotIds = append(electionSlotIds, slot.Id)
	}
	if len(waitElectionSlots) == 0 {
		return false
	}
	// 获取槽在各个副本上的日志高度
	slotInfoResps, err := c.requestSlotInfos(waitElectionSlots)
	if err != nil {
		logger.Error("request slot infos error", err)
		return false
	}
	if len(slotInfoResps) == 0 {
		return false
	}
	replicaLastLogInfoMap, err := c.collectSlotLogInfo(slotInfoResps)
	if err != nil {
		logger.Error("collect slot log info error", err)
		return false
	}
	_ = replicaLastLogInfoMap
	return hasChange
}

func (c *clusterconfigManager) nodeIsOnline(id uint64) bool {
	return c.clusterconfigServer.ConfigManager().NodeIsOnline(id)
}

// requestSlotInfos 请求获取槽在各个副本上的日志高度
func (c *clusterconfigManager) requestSlotInfos(slots map[uint64][]uint32) ([]*SlotLogInfoResp, error) {

	return nil, nil
}

// collectSlotLogInfo 收集slot的各个副本的日志高度
func (c *clusterconfigManager) collectSlotLogInfo(slotInfoResps []*SlotLogInfoResp) (map[uint64]map[uint32]uint64, error) {

	return nil, nil
}
