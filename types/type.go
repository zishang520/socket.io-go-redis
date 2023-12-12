package types

import (
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	"github.com/zishang520/socket.io/v2/socket"
)

type (
	PacketOptions struct {
		Rooms  []socket.Room          `json:"rooms,omitempty" mapstructure:"rooms,omitempty" msgpack:"rooms,omitempty"`
		Except []socket.Room          `json:"except,omitempty" mapstructure:"except,omitempty" msgpack:"except,omitempty"`
		Flags  *socket.BroadcastFlags `json:"flags,omitempty" mapstructure:"except,omitempty" msgpack:"flags,omitempty"`
	}

	Packet struct {
		Uid    string         `json:"uid,omitempty" mapstructure:"uid,omitempty" msgpack:"uid,omitempty"`
		Packet *parser.Packet `json:"packet,omitempty" mapstructure:"packet,omitempty" msgpack:"packet,omitempty"`
		Opts   *PacketOptions `json:"opts,omitempty" mapstructure:"opts,omitempty" msgpack:"opts,omitempty"`
	}

	Parser interface {
		Encode(any) ([]byte, error)
		Decode([]byte, any) error
	}
)
