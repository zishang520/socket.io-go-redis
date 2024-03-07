package emitter

import (
	"github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

const UID = "emitter"

type (
	BroadcastOptions struct {
		Nsp              string
		BroadcastChannel string
		RequestChannel   string
		Parser           types.Parser
	}

	Request struct {
		Type  types.RequestType    `json:"type,omitempty" msgpack:"type,omitempty"`
		Opts  *types.PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty"`
		Close bool                 `json:"close,omitempty" msgpack:"close,omitempty"`
		Rooms []socket.Room        `json:"rooms,omitempty" msgpack:"rooms,omitempty"`
	}

	ServerRequest struct {
		Uid  string            `json:"uid,omitempty" mapstructure:"uid,omitempty" msgpack:"uid,omitempty"`
		Type types.RequestType `json:"type,omitempty" mapstructure:"type,omitempty" msgpack:"type,omitempty"`
		Data []any             `json:"data,omitempty" mapstructure:"data,omitempty" msgpack:"data,omitempty"`
	}
)
