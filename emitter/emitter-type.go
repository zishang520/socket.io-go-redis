package emitter

import (
	"github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/adapter"
	"github.com/zishang520/socket.io/v2/socket"
)

type (
	BroadcastOptions struct {
		Nsp              string
		BroadcastChannel string
		RequestChannel   string
		Parser           types.Parser
	}

	EmitMessage struct {
		Type  adapter.MessageType    `json:"type,omitempty" msgpack:"type,omitempty"`
		Opts  *adapter.PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty"`
		Close bool                   `json:"close,omitempty" msgpack:"close,omitempty"`
		Rooms []socket.Room          `json:"rooms,omitempty" msgpack:"rooms,omitempty"`
	}

	ServerSideEmitMessage struct {
		Uid  adapter.ServerId    `json:"uid,omitempty" msgpack:"uid,omitempty"`
		Type adapter.MessageType `json:"type,omitempty" msgpack:"type,omitempty"`
		Data []any               `json:"data,omitempty" msgpack:"data,omitempty"`
	}
)
