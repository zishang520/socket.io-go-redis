package adapter

import (
	"time"

	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

type (
	AckRequest interface {
		ClientCountCallback(uint64)
		Ack([]any, error)
	}

	ackRequest struct {
		clientCountCallback func(uint64)
		ack                 func([]any, error)
	}
)

func NewAckRequest(clientCountCallback func(uint64), ack func([]any, error)) AckRequest {
	return &ackRequest{
		clientCountCallback: clientCountCallback,
		ack:                 ack,
	}
}

func (a *ackRequest) ClientCountCallback(count uint64) {
	if a.clientCountCallback != nil {
		a.clientCountCallback(count)
	}
}

func (a *ackRequest) Ack(packet []any, err error) {
	if a.ack != nil {
		a.ack(packet, err)
	}
}

type (
	Request struct {
		Type     _types.RequestType `json:"type,omitempty" mapstructure:"type,omitempty" msgpack:"type,omitempty"`
		Resolve  func(any, error)   `json:"-"`
		Timeout  *utils.Timer       `json:"timeout,omitempty" mapstructure:"timeout,omitempty" msgpack:"timeout,omitempty"`
		NumSub   int64              `json:"numSub,omitempty" mapstructure:"numSub,omitempty" msgpack:"numSub,omitempty"`
		MsgCount int64              `json:"msgCount,omitempty" mapstructure:"msgCount,omitempty" msgpack:"msgCount,omitempty"`

		RequestId string                `json:"requestId,omitempty" mapstructure:"requestId,omitempty" msgpack:"requestId,omitempty"`
		Rooms     []socket.Room         `json:"rooms,omitempty" mapstructure:"rooms,omitempty" msgpack:"rooms,omitempty"`
		Opts      *_types.PacketOptions `json:"opts,omitempty" mapstructure:"opts,omitempty" msgpack:"opts,omitempty"`
		Sid       socket.SocketId       `json:"sid,omitempty" mapstructure:"sid,omitempty" msgpack:"sid,omitempty"`
		Room      socket.Room           `json:"room,omitempty" mapstructure:"room,omitempty" msgpack:"room,omitempty"`
		Close     bool                  `json:"close,omitempty" mapstructure:"close,omitempty" msgpack:"close,omitempty"`
		Uid       string                `json:"uid,omitempty" mapstructure:"uid,omitempty" msgpack:"uid,omitempty"`
		Data      []any                 `json:"data,omitempty" mapstructure:"data,omitempty" msgpack:"data,omitempty"`
		Packet    *parser.Packet        `json:"packet,omitempty" mapstructure:"packet,omitempty" msgpack:"packet,omitempty"`
		Sockets   []*ResponseSockets    `json:"sockets,omitempty" mapstructure:"sockets,omitempty" msgpack:"sockets,omitempty"`
		Responses []any                 `json:"responses,omitempty" mapstructure:"responses,omitempty" msgpack:"responses,omitempty"`
	}

	Response struct {
		Type        _types.RequestType `json:"type,omitempty" mapstructure:"type,omitempty" msgpack:"type,omitempty"`
		RequestId   string             `json:"requestId,omitempty" mapstructure:"requestId,omitempty" msgpack:"requestId,omitempty"`
		Rooms       []socket.Room      `json:"rooms,omitempty" mapstructure:"rooms,omitempty" msgpack:"rooms,omitempty"`
		Sockets     []*ResponseSockets `json:"sockets,omitempty" mapstructure:"sockets,omitempty" msgpack:"sockets,omitempty"`
		Data        []any              `json:"data,omitempty" mapstructure:"data,omitempty" msgpack:"data,omitempty"`
		ClientCount uint64             `json:"clientcount,omitempty" mapstructure:"clientcount,omitempty" msgpack:"clientcount,omitempty"`
		Packet      []any              `json:"packet,omitempty" mapstructure:"packet,omitempty" msgpack:"packet,omitempty"`
	}

	RedisAdapter interface {
		socket.Adapter

		SetRedis(*_types.RedisClient)
		SetOpts(*RedisAdapterOptions)

		Uid() string
		RequestsTimeout() time.Duration
		PublishOnSpecificResponseChannel() bool
		Parser() _types.Parser

		AllRooms() func(func(*types.Set[socket.Room], error))
	}
)
