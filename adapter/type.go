package adapter

import (
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io/v2/socket"
)

type ResponseSockets struct {
	SocketId        socket.SocketId   `json:"id,omitempty" mapstructure:"id,omitempty" msgpack:"id,omitempty"`
	SocketHandshake *socket.Handshake `json:"handshake,omitempty" mapstructure:"handshake,omitempty" msgpack:"handshake,omitempty"`
	SocketRooms     []socket.Room     `json:"rooms,omitempty" mapstructure:"rooms,omitempty" msgpack:"rooms,omitempty"`
	SocketData      any               `json:"data,omitempty" mapstructure:"data,omitempty" msgpack:"data,omitempty"`

	data *types.Set[socket.Room]
}

func (r *ResponseSockets) Id() socket.SocketId {
	return r.SocketId
}

func (r *ResponseSockets) Handshake() *socket.Handshake {
	return r.SocketHandshake
}

func (r *ResponseSockets) Rooms() *types.Set[socket.Room] {
	if r.data == nil {
		r.data = types.NewSet(r.SocketRooms...)
	}
	return r.data
}

func (r *ResponseSockets) Data() any {
	return r.SocketData
}
