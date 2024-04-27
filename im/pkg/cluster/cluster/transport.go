package cluster

import "chat/im/pkg/server/proto"

type ITransport interface {
	// Send 发送消息
	Send(to uint64, m *proto.Message, callback func()) error
	// OnMessage 收取消息
	// OnMessage(f func(from uint64, m *proto.Message))
	// 收到消息
	// RecvMessage(from uint64, m *proto.Message)
}

type MemoryTransport struct {
	nodeMessageListenerMap map[uint64]func(m *proto.Message)
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		nodeMessageListenerMap: make(map[uint64]func(m *proto.Message)),
	}
}

func (t *MemoryTransport) Send(to uint64, m *proto.Message, callback func()) error {
	if f, ok := t.nodeMessageListenerMap[to]; ok {
		go func() {
			f(m) // 模拟网络请求
			callback()
		}()
	}
	return nil
}
