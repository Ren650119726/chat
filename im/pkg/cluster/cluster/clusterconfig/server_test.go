package clusterconfig

import (
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
	"time"
)

func TestServerAddOrUpdateNodes(t *testing.T) {
	s1, s2, s3 := newTestClusterServerThree(t)
	defer s1.Stop()
	defer s2.Stop()
	defer s3.Stop()

	var leaderNode *Server
	if s1.IsLeader() {
		leaderNode = s1
	} else if s2.IsLeader() {
		leaderNode = s2
	} else if s3.IsLeader() {
		leaderNode = s3
	}

	err := leaderNode.AddOrUpdateNodes([]*pb.Node{
		{
			Id:            1,
			ApiServerAddr: "hello",
		},
		{
			Id:            2,
			ApiServerAddr: "hello2",
		},
	})
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 500)

	nodes1 := s1.ConfigManager().GetConfig().GetNodes()
	nodes2 := s2.ConfigManager().GetConfig().GetNodes()
	nodes3 := s3.ConfigManager().GetConfig().GetNodes()

	assert.Equal(t, 2, len(nodes1))
	assert.Equal(t, 2, len(nodes2))
	assert.Equal(t, 2, len(nodes3))

	assert.Equal(t, uint64(1), nodes1[0].Id)
	assert.Equal(t, uint64(2), nodes1[1].Id)

	assert.Equal(t, uint64(1), nodes2[0].Id)
	assert.Equal(t, uint64(2), nodes2[1].Id)

	assert.Equal(t, uint64(1), nodes3[0].Id)
	assert.Equal(t, uint64(2), nodes3[1].Id)

	assert.Equal(t, []byte("hello"), nodes1[0].ApiServerAddr)
	assert.Equal(t, []byte("hello2"), nodes1[1].ApiServerAddr)

	assert.Equal(t, []byte("hello"), nodes2[0].ApiServerAddr)
	assert.Equal(t, []byte("hello2"), nodes2[1].ApiServerAddr)

	assert.Equal(t, []byte("hello"), nodes3[0].ApiServerAddr)
	assert.Equal(t, []byte("hello2"), nodes3[1].ApiServerAddr)
}

func newTestClusterServerThree(t *testing.T) (*Server, *Server, *Server) {
	trans := newTestTransport()

	dataDir := t.TempDir()

	fmt.Println("dataDir---->", dataDir)

	electionTimeout := 5

	s1 := New(1, WithElectionTimeoutTick(electionTimeout), WithTransport(trans), WithConfigPath(path.Join(dataDir, "1", "clusterconfig.json")), WithReplicas([]uint64{1, 2, 3}))
	err := s1.Start()
	assert.NoError(t, err)

	trans.OnNodeMessage(1, func(msg Message) {
		err := s1.Step(context.Background(), msg)
		assert.NoError(t, err)
	})

	s2 := New(2, WithElectionTimeoutTick(electionTimeout), WithTransport(trans), WithConfigPath(path.Join(dataDir, "2", "clusterconfig.json")), WithReplicas([]uint64{1, 2, 3}))
	err = s2.Start()
	assert.NoError(t, err)

	trans.OnNodeMessage(2, func(msg Message) {
		err := s2.Step(context.Background(), msg)
		assert.NoError(t, err)
	})

	s3 := New(3, WithElectionTimeoutTick(electionTimeout), WithTransport(trans), WithConfigPath(path.Join(dataDir, "3", "clusterconfig.json")), WithReplicas([]uint64{1, 2, 3}))
	err = s3.Start()
	assert.NoError(t, err)

	trans.OnNodeMessage(3, func(msg Message) {
		err := s3.Step(context.Background(), msg)
		assert.NoError(t, err)
	})

	s1.MustWaitLeader()
	s2.MustWaitLeader()
	s3.MustWaitLeader()

	return s1, s2, s3
}

type testTransport struct {
	nodeOnMessageMap map[uint64]func(msg Message)
}

func newTestTransport() *testTransport {
	return &testTransport{
		nodeOnMessageMap: make(map[uint64]func(msg Message)),
	}
}

func (t *testTransport) Send(msg Message) error {
	if f, ok := t.nodeOnMessageMap[msg.To]; ok {
		go f(msg)
	}
	return nil
}

func (t *testTransport) OnMessage(f func(msg Message)) {
}

func (t *testTransport) OnNodeMessage(nodeId uint64, f func(msg Message)) {
	t.nodeOnMessageMap[nodeId] = f
}
