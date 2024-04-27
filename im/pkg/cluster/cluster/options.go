package cluster

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	NodeID              uint64
	DataDir             string            // 数据存储目录
	SlotCount           uint32            // 槽数量
	InitNodes           map[uint64]string // 初始化节点列表
	Seed                string            // 种子节点 格式：id@ip:port
	SlotMaxReplicaCount uint32            // 每个槽位最大副本数量
	ReqTimeout          time.Duration
	ShardLogStorage     IShardLogStorage
	Transport           ITransport

	nodeOnlineFnc func(nodeID uint64) (bool, error) // 节点是否在线

	requestSlotLogInfo func(ctx context.Context, nodeId uint64, req *SlotLogInfoReq) (*SlotLogInfoResp, error)
}

func NewOptions(optList ...Option) *Options {
	opts := &Options{
		SlotCount:           256,
		SlotMaxReplicaCount: 3,
		ReqTimeout:          time.Second * 10,
	}
	for _, opt := range optList {
		opt(opts)
	}
	return opts
}

func (o *Options) Replicas() []uint64 {
	replicas := make([]uint64, 0, len(o.InitNodes))
	if strings.TrimSpace(o.Seed) != "" {
		seedNodeId, _, _ := SeedNode(o.Seed)
		replicas = append(replicas, seedNodeId)
	} else {
		for nodeID := range o.InitNodes {
			replicas = append(replicas, nodeID)
		}
	}
	return replicas
}

func SeedNode(seed string) (uint64, string, error) {
	seedArray := strings.Split(seed, "@")
	if len(seedArray) < 2 {
		return 0, "", errors.New("seed format error")
	}
	seedNodeIDStr := seedArray[0]
	seedAddr := seedArray[1]
	seedNodeID, err := strconv.ParseUint(seedNodeIDStr, 10, 64)
	if err != nil {
		return 0, "", err
	}
	return seedNodeID, seedAddr, nil
}

type Option func(opts *Options)
