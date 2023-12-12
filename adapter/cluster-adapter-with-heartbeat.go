package adapter

import (
	"time"

	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io/v2/socket"
)

type (
	ClusterAdapterWithHeartbeatBuilder struct {
		socket.AdapterConstructor

		Opts *ClusterAdapterOptions
	}

	clusterAdapterWithHeartbeat struct {
		ClusterAdapter

		_opts *ClusterAdapterOptions

		heartbeatTimer *utils.Timer
		nodesMap       *types.Map[string, int64] // uid => timestamp of last message
	}
)

func (c *ClusterAdapterWithHeartbeatBuilder) New(nsp socket.NamespaceInterface) socket.Adapter {
	return NewClusterAdapterWithHeartbeat(nsp, c.Opts)
}

func MakeClusterAdapterWithHeartbeat() ClusterAdapterWithHeartbeat {
	c := &clusterAdapterWithHeartbeat{
		ClusterAdapter: MakeClusterAdapter(),

		_opts:    DefaultClusterAdapterOptions(),
		nodesMap: &types.Map[string, int64]{},
	}

	c.Prototype(c)

	return c
}

func NewClusterAdapterWithHeartbeat(nsp socket.NamespaceInterface, opts *ClusterAdapterOptions) ClusterAdapterWithHeartbeat {
	c := MakeClusterAdapterWithHeartbeat()

	c.SetOpts(opts)

	c.Construct(nsp)

	return c
}

func (c *clusterAdapterWithHeartbeat) Construct(nsp socket.NamespaceInterface) {
	c.ClusterAdapter.Construct(nsp)
}

func (c *clusterAdapterWithHeartbeat) SetOpts(opts *ClusterAdapterOptions) {
}

func (c *clusterAdapterWithHeartbeat) Init() {
	c.Publish(&ClusterMessage{
		Type: MESSAGE_INITIAL_HEARTBEAT,
	})
}

func (c *clusterAdapterWithHeartbeat) scheduleHeartbeat() {
	if c.heartbeatTimer != nil {
		utils.ClearTimeout(c.heartbeatTimer)
	}
	c.heartbeatTimer = utils.SetTimeout(func() {
		c.Publish(&ClusterMessage{
			Type: MESSAGE_HEARTBEAT,
		})
	}, c._opts.HeartbeatInterval())
}

func (c *clusterAdapterWithHeartbeat) Close() {
	utils.ClearTimeout(c.heartbeatTimer)
}

func (c *clusterAdapterWithHeartbeat) OnMessage(message *ClusterMessage, offset int64) {
	if message.Uid == c.Uid() {
		adapter_log.Debug("ignore message from self")
		return
	}

	if message.Uid != "" && message.Uid != EMITTER_UID {
		// we track the UID of each sender, in order to know how many servers there are in the cluster
		c.nodesMap.Store(message.Uid, time.Now().UnixMilli())
	}

	switch message.Type {
	case MESSAGE_INITIAL_HEARTBEAT:
		c.Publish(&ClusterMessage{
			Type: MESSAGE_HEARTBEAT,
		})
		break
	case MESSAGE_HEARTBEAT:
		// nothing to do
		break
	default:
		c.ClusterAdapter.OnMessage(message, offset)
	}
}

func (c *clusterAdapterWithHeartbeat) ServerCount() int64 {
	now := time.Now().UnixMilli()
	c.nodesMap.Range(func(uid string, lastSeen int64) bool {
		if now-lastSeen > c._opts.HeartbeatTimeout() {
			adapter_log.Debug("node %s seems down", uid)
			c.nodesMap.Delete(uid)
		}
		return true
	})
	return int64(1 + c.nodesMap.Len())
}

func (c *clusterAdapterWithHeartbeat) Publish(message *ClusterMessage) (int64, error) {
	c.scheduleHeartbeat()
	return c.ClusterAdapter.Publish(message)
}
