package replica

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"
)

type Role uint8

const (
	None uint64 = 0
	All  uint64 = math.MaxUint64 - 1
)

const (
	RoleFollower  Role = iota // 追随者
	RoleCandidate             // 候选者
	RoleLeader                // 领导
	RoleLearner               // 学习者
)

type MsgType uint16

const (
	MsgUnknown                  MsgType = iota // 未知
	MsgVoteReq                                 // 请求投票
	MsgVoteResp                                // 请求投票响应
	MsgPropose                                 // 提案（领导）
	MsgHup                                     // 开始选举
	MsgSyncReq                                 // 同步日志 （追随者）
	MsgSyncGet                                 // 同步日志获取（领导，本地）
	MsgSyncGetResp                             // 同步日志获取响应（追随者）
	MsgSyncResp                                // 同步日志响应（领导）
	MsgLeaderTermStartIndexReq                 // 领导任期开始偏移量请求 （追随者）
	MsgLeaderTermStartIndexResp                // 领导任期开始偏移量响应（领导）
	MsgStoreAppend                             // 存储追加日志
	MsgStoreAppendResp                         // 存储追加日志响应
	MsgApplyLogs                               // 应用日志请求
	MsgApplyLogsResp                           // 应用日志响应
	MsgBeat                                    // 触发ping
	MsgPing                                    // ping
	MsgPong                                    // pong
	MsgConfigReq                               // 配置请求
	MsgConfigResp                              // 配置响应
	MsgMaxValue
)

func (m MsgType) String() string {
	switch m {
	case MsgUnknown:
		return "MsgUnknown[0]"
	case MsgVoteReq:
		return "MsgVoteReq"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgHup:
		return "MsgHup"
	case MsgPropose:
		return "MsgPropose"
	case MsgSyncReq:
		return "MsgSyncReq"
	case MsgSyncGet:
		return "MsgSyncGet"
	case MsgSyncGetResp:
		return "MsgSyncGetResp"
	case MsgSyncResp:
		return "MsgSyncResp"
	case MsgLeaderTermStartIndexReq:
		return "MsgLeaderTermStartIndexReq"
	case MsgLeaderTermStartIndexResp:
		return "MsgLeaderTermStartIndexResp"
	case MsgApplyLogs:
		return "MsgApplyLogs"
	case MsgApplyLogsResp:
		return "MsgApplyLogsResp"
	case MsgBeat:
		return "MsgBeat"
	case MsgPing:
		return "MsgPing"
	case MsgPong:
		return "MsgPong"
	case MsgStoreAppend:
		return "MsgStoreAppend"
	case MsgStoreAppendResp:
		return "MsgStoreAppendResp"
	case MsgConfigReq:
		return "MsgConfigReq"
	case MsgConfigResp:
		return "MsgConfigResp"
	default:
		return fmt.Sprintf("MsgUnkown[%d]", m)
	}
}

type SpeedLevel uint8

const (
	LevelFast    SpeedLevel = iota // 最快速度
	LevelNormal                    // 正常速度
	LevelMiddle                    // 中等速度
	LevelSlow                      // 慢速度
	LevelSlowest                   // 最慢速度
	LevelStop                      // 停止
)

var EmptyMessage = Message{}

type Message struct {
	MsgType        MsgType // 消息类型
	From           uint64
	To             uint64
	Term           uint32 // 领导任期
	Index          uint64
	CommittedIndex uint64 // 已提交日志下标

	ApplyingIndex uint64 // 应用中的下表
	AppliedIndex  uint64

	SpeedLevel  SpeedLevel // 只有msgSync和ping才编码
	Reject      bool       // 拒绝
	ConfVersion uint64     // 配置版本
	Logs        []Log
}

type Log struct {
	Id    uint64
	Index uint64 // 日志下标
	Term  uint32 // 领导任期
	Data  []byte // 日志数据
}

type Config struct {
	Replicas []uint64 // 副本集合（包含当前节点自己）
	Learners []uint64 // 学习节点集合
	Version  uint64   // 配置版本
}

var EmptyLog = Log{}

func IsEmptyLog(v Log) bool {
	return v.Id == 0 && v.Index == 0 && v.Term == 0 && len(v.Data) == 0
}

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

type HardState struct {
	LeaderId uint64 // 领导ID
	Term     uint32 // 领导任期
}

type Ready struct {
	HardState HardState
	Messages  []Message
}
