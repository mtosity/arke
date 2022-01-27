package util

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"
)

type fakeAddr struct {
	net.Addr
	s       string
	network string
}

func (f fakeAddr) String() string {
	return f.s
}
func (f fakeAddr) Network() string {
	return f.network
}

func TestGenUUID(t *testing.T) {
	uuidStr := GenUUID()
	fmt.Println(uuidStr)
	assert.NotNil(t, uuidStr)
	id, err := uuid.Parse(uuidStr)
	assert.IsType(t, uuid.UUID{}, id)
	assert.Nil(t, err)
}

func Test_GetClientAddr(t *testing.T) {
	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)

	addr, err := GetClientAddr(ctx)
	assert.Nil(t, err)
	assert.Equal(t, addr, "127.0.0.1")
}

func Test_SetClientIdentifier(t *testing.T) {

	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)
	id, err := SetClientIdentifier(ctx, "unitTest")
	assert.Contains(t, id, "unitTest-")
	assert.Nil(t, err)

	getID, err := GetClientIdentifier(ctx)
	assert.Equal(t, id, getID)
	assert.Nil(t, err)

	p.Addr = fakeAddr{}
	ctx = peer.NewContext(context.Background(), p)
	getID, err = GetClientIdentifier(ctx)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "Could not retrieve client-id from context")
	assert.Equal(t, "", getID)

}

func Test_RemoveClientIdentifier(t *testing.T) {

	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)
	id, err := SetClientIdentifier(ctx, "unitTest")
	assert.Contains(t, id, "unitTest-")
	assert.Nil(t, err)

	RemoveClientIdentifier(ctx)

	getID, err := GetClientIdentifier(ctx)
	assert.Equal(t, "", getID)
	assert.NotNil(t, err)
	assert.Equal(t, "Could not find client identifier", err.Error())

}

func Test_TestProcessStats(t *testing.T) {
	go MonitorProcessStats()
	time.Sleep(1 * time.Second)
	ps := GetProcessStats()
	assert.NotNil(t, ps)
	assert.Equal(t, ps.MaxMemory, 0) // 0 because we aren't in k8s
	if runtime.GOOS != "windows" {
		assert.Greater(t, ps.CurrentMemory, 0)
		assert.Greater(t, ps.MemoryAverage, 0.0)
	}
	// might not be able to calculate cpu usage yet
}
