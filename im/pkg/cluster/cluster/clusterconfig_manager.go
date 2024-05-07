package cluster

import (
	"chat/im/pkg/cluster/cluster/clusterconfig"
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"chat/im/pkg/logger"
	"chat/im/pkg/util"
	"context"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	c.clusterconfigServer = clusterconfig.New(opts.NodeID, clusterconfig.WithSlotCount(opts.SlotCount), clusterconfig.WithTransport(c), clusterconfig.WithReplicas(opts.Replicas()), remoteCfgPath)
	return c
}

func (c *clusterconfigManager) start() error {
	c.stopper.RunWorker(c.loop)
	return c.clusterconfigServer.Start()
}

func (c *clusterconfigManager) stop() {
	c.clusterconfigServer.Stop()
	c.stopper.Stop()
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

// 等待配置中的节点数量达到指定数量
func (c *clusterconfigManager) waitConfigNodeCount(count int, timeout time.Duration) error {
	tk := time.NewTicker(time.Millisecond * 20)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-tk.C:
			clusterConfig := c.clusterconfigServer.ConfigManager().GetConfig()
			if len(clusterConfig.Nodes) >= count {
				return nil
			}
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		}
	}
}

func (c *clusterconfigManager) waitConfigSlotCount(count uint32, timeout time.Duration) error {
	tk := time.NewTicker(time.Millisecond * 20)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-tk.C:
			clusterConfig := c.clusterconfigServer.ConfigManager().GetConfig()
			if len(clusterConfig.Slots) >= int(count) {
				return nil
			}
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
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
	// 初始化slot
	changed = c.initSlotsIfNeed(newCfg)
	if changed {
		hasChange = true
	}
	// 检查节点在线状态
	changed = c.nodeOnlineChangeIfNeed(newCfg)
	if changed {
		hasChange = changed
	}
	// 初始化slot leader
	changed = c.initSlotLeaderIfNeed(newCfg)
	if changed {
		hasChange = true
	}
	// 选举slot leader
	c.electionSlotLeaderIfNeed(newCfg)
	// 处理新加入的节点
	changed = c.handleNewJoinNodeIfNeed(newCfg)
	if changed {
		hasChange = true
	}
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

// 节点在线状态变更
func (c *clusterconfigManager) nodeOnlineChangeIfNeed(newCfg *pb.Config) bool {
	if c.opts.nodeOnlineFnc == nil {
		return false
	}
	changed := false
	for _, n := range newCfg.Nodes {
		online, err := c.opts.nodeOnlineFnc(n.Id)
		if err != nil {
			logger.Error("check node online error", zap.Error(err))
			continue
		}
		if online != n.Online {
			logger.Infof("node online change nodeID:%v online %v", n.Id, online)
			c.clusterconfigServer.ConfigManager().SetNodeOnline(n.Id, online, newCfg)
			changed = true
		}
	}
	return changed
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
	//计算每个槽的领导节点
	slotLeaderMap := c.calculateSlotLeader(replicaLastLogInfoMap, electionSlotIds)
	if len(slotLeaderMap) == 0 {
		logger.Warnf("没有选举出任何槽领导者！！！ slotIDs %v", electionSlotIds)
		return false
	}
	getSlot := func(slotId uint32) *pb.Slot {
		for _, slot := range newCfg.Slots {
			if slot.Id == slotId {
				return slot
			}
		}
		return nil
	}
	for slotID, leaderNodeID := range slotLeaderMap {
		slot := getSlot(slotID)
		if slot.Leader != leaderNodeID {
			slot.Leader = leaderNodeID
			slot.Term++
			hasChange = true
		}
	}

	return hasChange
}

func (c *clusterconfigManager) nodeIsOnline(id uint64) bool {
	return c.clusterconfigServer.ConfigManager().NodeIsOnline(id)
}

// requestSlotInfos 请求获取槽在各个副本上的日志高度
func (c *clusterconfigManager) requestSlotInfos(waitElectionSlots map[uint64][]uint32) ([]*SlotLogInfoResp, error) {
	slotInfoResps := make([]*SlotLogInfoResp, 0, len(waitElectionSlots))
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	requestGroup, _ := errgroup.WithContext(timeoutCtx)

	for nodeId, slotIds := range waitElectionSlots {
		if nodeId == c.opts.NodeID { //本节点
			slotInfos, err := c.slotInfos(slotIds)
			if err != nil {
				logger.Error("get slot infos error", err)
				continue
			}
			slotInfoResps = append(slotInfoResps, &SlotLogInfoResp{
				NodeId: nodeId,
				Slots:  slotInfos,
			})
			continue
		} else {
			requestGroup.Go(func(nID uint64, sids []uint32) func() error {
				return func() error {
					resp, err := c.requestSlotLogInfo(nID, sids)
					if err != nil {
						logger.Warn("request slot log info error", err)
						return nil
					}
					slotInfoResps = append(slotInfoResps, resp)
					return nil
				}
			}(nodeId, slotIds))
		}
	}
	_ = requestGroup.Wait()
	return slotInfoResps, nil

}

// collectSlotLogInfo 收集slot的各个副本的日志高度
func (c *clusterconfigManager) collectSlotLogInfo(slotInfoResps []*SlotLogInfoResp) (map[uint64]map[uint32]uint64, error) {
	slotLogInfos := make(map[uint64]map[uint32]uint64, len(slotInfoResps))
	for _, resp := range slotInfoResps {
		slotLogInfos[resp.NodeId] = make(map[uint32]uint64, len(resp.Slots))
		for _, slotInfo := range resp.Slots {
			slotLogInfos[resp.NodeId][slotInfo.SlotId] = slotInfo.LogIndex
		}
	}

	return slotLogInfos, nil
}

func (c *clusterconfigManager) slotInfos(slotIds []uint32) ([]SlotInfo, error) {
	slotInfos := make([]SlotInfo, 0, len(slotIds))
	for _, slotId := range slotIds {
		shardNo := GetSlotShardNo(slotId)
		lastLogIndex, err := c.opts.ShardLogStorage.LastIndex(shardNo)
		if err != nil {
			return nil, err
		}
		slotInfos = append(slotInfos, SlotInfo{
			SlotId:   slotId,
			LogIndex: lastLogIndex,
		})
	}
	return slotInfos, nil
}

func (c *clusterconfigManager) requestSlotLogInfo(nodeId uint64, slotIds []uint32) (*SlotLogInfoResp, error) {
	req := &SlotLogInfoReq{
		SlotIds: slotIds,
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.ReqTimeout)
	defer cancel()
	resp, err := c.opts.requestSlotLogInfo(timeoutCtx, nodeId, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// calculateSlotLeader 计算每个槽的领导节点
func (c *clusterconfigManager) calculateSlotLeader(slotLogInfos map[uint64]map[uint32]uint64, slotIds []uint32) map[uint32]uint64 {
	slotLeaderMap := make(map[uint32]uint64, len(slotIds))
	for _, slotId := range slotIds {
		slotLeaderMap[slotId] = c.calculateSlotLeaderBySlot(slotLogInfos, slotId)
	}
	return slotLeaderMap
}

// calculateSlotLeaderBySlot 计算槽的领导节点
func (c *clusterconfigManager) calculateSlotLeaderBySlot(slotLogInfos map[uint64]map[uint32]uint64, slotId uint32) uint64 {
	var leader uint64
	var maxLogIndex uint64
	for replicaId, logIndexMap := range slotLogInfos {
		if logIndexMap[slotId] > maxLogIndex {
			maxLogIndex = logIndexMap[slotId]
			leader = replicaId
		}
	}
	return leader
}

func (c *clusterconfigManager) handleNewJoinNodeIfNeed(newCfg *pb.Config) bool {
	var newNodes []*pb.Node
	for _, n := range newCfg.Nodes {
		if n.Join && n.Status == pb.NodeStatus_NodeStatusWillJoin && n.Role == pb.NodeRole_NodeRoleReplica {
			newNodes = append(newNodes, n)
			break
		}
	}
	if len(newNodes) == 0 {
		return false
	}
	replicaNodes := c.replicaNodes(newCfg.Nodes)                                                    // 获取可投票的副本节点
	slotCountOfNode := c.slotCount() * c.opts.SlotMaxReplicaCount / uint32(len(replicaNodes))       // 每个节点的槽应该的数量
	remainSlotCount := c.slotCount() * c.opts.SlotMaxReplicaCount % uint32(len(replicaNodes))       // 剩余槽数量
	allExports := make([]*pb.SlotMigrate, 0, slotCountOfNode*uint32(len(newNodes))+remainSlotCount) // 导出的槽

	// 获取需要导出的槽
	for _, replicaNode := range replicaNodes {
		nodeSlotCount := c.getNodeSlotCount(replicaNode.Id)
		if nodeSlotCount <= slotCountOfNode {
			continue
		}
		exportCount := nodeSlotCount - slotCountOfNode
		slots := c.getNodeSlots(replicaNode.Id)
		for i := len(slots) - 1; i >= 0; i-- {
			slot := slots[i]
			if exportCount <= 0 {
				break
			}
			if c.containsSlot(allExports, slot.Id) {
				continue
			}
			mg := &pb.SlotMigrate{
				Slot:   slot.Id,
				From:   replicaNode.Id,
				Status: pb.MigrateStatus_MigrateStatusWill,
			}
			replicaNode.Exports = append(replicaNode.Exports, mg)
			allExports = append(allExports, mg)
			exportCount--
		}
	}
	for _, newNode := range newNodes {
		newNode.Status = pb.NodeStatus_NodeStatusJoining
		if len(allExports) == 0 {
			break
		}
		if len(allExports) <= int(slotCountOfNode) {
			newNode.Imports = append(newNode.Imports, allExports...)
			for _, imp := range newNode.Imports {
				imp.To = newNode.Id
			}
			allExports = nil
		} else {
			newNode.Imports = append(newNode.Imports, allExports[:int(slotCountOfNode)]...)
			for _, imp := range newNode.Imports {
				imp.To = newNode.Id
			}
			allExports = allExports[int(slotCountOfNode):]
		}
	}
	return true
}

func (c *clusterconfigManager) replicaNodes(nodes []*pb.Node) []*pb.Node {
	replicaNodes := make([]*pb.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Role == pb.NodeRole_NodeRoleReplica {
			replicaNodes = append(replicaNodes, n)
		}
	}
	return replicaNodes
}

func (c *clusterconfigManager) slotCount() uint32 {
	return c.clusterconfigServer.ConfigManager().GetConfig().SlotCount
}

func (c *clusterconfigManager) getNodeSlotCount(nodeId uint64) uint32 {
	var count uint32
	for _, slot := range c.clusterconfigServer.ConfigManager().GetConfig().Slots {
		if util.ArrayContainsUint64(slot.Replicas, nodeId) {
			count++
		}
	}
	return count
}

func (c *clusterconfigManager) getNodeSlots(nodeId uint64) []*pb.Slot {
	var slots []*pb.Slot
	for _, slot := range c.clusterconfigServer.ConfigManager().GetConfig().Slots {
		if util.ArrayContainsUint64(slot.Replicas, nodeId) {
			slots = append(slots, slot)
		}
	}
	return slots
}

func (c *clusterconfigManager) containsSlot(mgs []*pb.SlotMigrate, slotID uint32) bool {
	for _, mg := range mgs {
		if mg.Slot == slotID {
			return true
		}
	}
	return false

}
