package adapter

import (
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

type (
	MessageType int

	// ClusterMessage
	ClusterMessageRecord struct {
		RequestId   string
		Packet      any
		ClientCount uint64
		Sockets     any
		Opts        *types.PacketOptions
		Rooms       []socket.Room
		Close       bool
	}
	ClusterMessage struct {
		Uid  string
		Type MessageType
		Data *ClusterMessageRecord
	}

	ClusterRequest struct {
		Current int64

		Type      MessageType
		Resolve   func(any, error)
		Timeout   *utils.Timer
		Expected  int64
		Responses []any
	}

	ClusterAckRequest interface {
		ClientCountCallback(uint64)
		Ack([]any, error)
	}

	clusterAckRequest struct {
		clientCountCallback func(uint64)
		ack                 func([]any, error)
	}

	ClusterAdapter interface {
		socket.Adapter

		Uid() string
		OnMessage(*ClusterMessage, int64)
		OnResponse(*ClusterMessage)
		Publish(*ClusterMessage) (int64, error)
		PublishMessage(*ClusterMessage) (int64, error)
		PublishResponse(string, *ClusterMessage) (int64, error)
	}

	ClusterAdapterWithHeartbeat interface {
		ClusterAdapter

		SetOpts(*ClusterAdapterOptions)
	}
)

const (
	MESSAGE_INITIAL_HEARTBEAT         MessageType = 1
	MESSAGE_HEARTBEAT                 MessageType = 2
	MESSAGE_BROADCAST                 MessageType = 3
	MESSAGE_SOCKETS_JOIN              MessageType = 4
	MESSAGE_SOCKETS_LEAVE             MessageType = 5
	MESSAGE_DISCONNECT_SOCKETS        MessageType = 6
	MESSAGE_FETCH_SOCKETS             MessageType = 7
	MESSAGE_FETCH_SOCKETS_RESPONSE    MessageType = 8
	MESSAGE_SERVER_SIDE_EMIT          MessageType = 9
	MESSAGE_SERVER_SIDE_EMIT_RESPONSE MessageType = 10
	MESSAGE_BROADCAST_CLIENT_COUNT    MessageType = 11
	MESSAGE_BROADCAST_ACK             MessageType = 12
)

func NewClusterAckRequest(clientCountCallback func(uint64), ack func([]any, error)) ClusterAckRequest {
	return &clusterAckRequest{
		clientCountCallback: clientCountCallback,
		ack:                 ack,
	}
}

func (c *clusterAckRequest) ClientCountCallback(count uint64) {
	if c.clientCountCallback != nil {
		c.clientCountCallback(count)
	}
}

func (c *clusterAckRequest) Ack(packet []any, err error) {
	if c.ack != nil {
		c.ack(packet, err)
	}
}
