package types

import (
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	"github.com/zishang520/socket.io/v2/adapter"
)

type (
	Packet struct {
		Uid    adapter.ServerId       `json:"uid,omitempty" msgpack:"uid,omitempty"`
		Packet *parser.Packet         `json:"packet,omitempty" msgpack:"packet,omitempty"`
		Opts   *adapter.PacketOptions `json:"opts,omitempty" msgpack:"opts,omitempty"`
	}

	Parser interface {
		Encode(any) ([]byte, error)
		Decode([]byte, any) error
	}
)
