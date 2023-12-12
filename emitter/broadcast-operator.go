package emitter

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zishang520/engine.io/v2/log"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

var emitter_log = log.NewLog("socket.io-emitter")

var RESERVED_EVENTS = types.NewSet(
	"connect",
	"connect_error",
	"disconnect",
	"disconnecting",
	"newListener",
	"removeListener",
)

type BroadcastOperator struct {
	redis            *_types.RedisClient
	broadcastOptions *BroadcastOptions
	rooms            *types.Set[socket.Room]
	exceptRooms      *types.Set[socket.Room]
	flags            *socket.BroadcastFlags
}

func NewBroadcastOperator(redis *_types.RedisClient, broadcastOptions *BroadcastOptions, rooms *types.Set[socket.Room], exceptRooms *types.Set[socket.Room], flags *socket.BroadcastFlags) *BroadcastOperator {
	b := &BroadcastOperator{}
	b.redis = redis

	if broadcastOptions == nil {
		broadcastOptions = &BroadcastOptions{}
	}
	b.broadcastOptions = broadcastOptions

	if rooms == nil {
		rooms = types.NewSet[socket.Room]()
	}
	b.rooms = rooms

	if exceptRooms == nil {
		exceptRooms = types.NewSet[socket.Room]()
	}
	b.exceptRooms = exceptRooms

	if flags == nil {
		flags = &socket.BroadcastFlags{}
	}
	b.flags = flags

	return b
}

// Targets a room when emitting.
func (b *BroadcastOperator) To(room ...socket.Room) *BroadcastOperator {
	rooms := types.NewSet(b.rooms.Keys()...)
	rooms.Add(room...)
	return NewBroadcastOperator(b.redis, b.broadcastOptions, rooms, b.exceptRooms, b.flags)
}

// Targets a room when emitting.
func (b *BroadcastOperator) In(room ...socket.Room) *BroadcastOperator {
	return b.To(room...)
}

// Excludes a room when emitting.
func (b *BroadcastOperator) Except(room ...socket.Room) *BroadcastOperator {
	exceptRooms := types.NewSet(b.exceptRooms.Keys()...)
	exceptRooms.Add(room...)
	return NewBroadcastOperator(b.redis, b.broadcastOptions, b.rooms, exceptRooms, b.flags)
}

// Sets the compress flag.
func (b *BroadcastOperator) Compress(compress bool) *BroadcastOperator {
	flags := *b.flags
	flags.Compress = compress
	return NewBroadcastOperator(b.redis, b.broadcastOptions, b.rooms, b.exceptRooms, &flags)
}

// Sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to
// receive messages (because of network slowness or other issues, or because they’re connected through long polling
// and is in the middle of a request-response cycle).
func (b *BroadcastOperator) Volatile() *BroadcastOperator {
	flags := *b.flags
	flags.Volatile = true
	return NewBroadcastOperator(b.redis, b.broadcastOptions, b.rooms, b.exceptRooms, &flags)
}

// Emits to all clients.
func (b *BroadcastOperator) Emit(ev string, args ...any) error {
	if RESERVED_EVENTS.Has(ev) {
		return errors.New(fmt.Sprintf(`"%s" is a reserved event name`, ev))
	}

	if b.broadcastOptions.Parser == nil {
		return errors.New(`broadcastOptions.Parser is not set`)
	}

	// set up packet object
	data := append([]any{ev}, args...)

	packet := &parser.Packet{
		Type: parser.EVENT,
		Nsp:  b.broadcastOptions.Nsp,
		Data: data,
	}

	opts := &_types.PacketOptions{
		Rooms:  b.rooms.Keys(),
		Except: b.exceptRooms.Keys(),
		Flags:  b.flags,
	}

	msg, err := b.broadcastOptions.Parser.Encode(&_types.Packet{Uid: UID, Packet: packet, Opts: opts})
	if err != nil {
		return nil
	}

	channel := b.broadcastOptions.BroadcastChannel
	if b.rooms != nil && b.rooms.Len() == 1 {
		channel += string((b.rooms.Keys())[0]) + "#"
	}

	emitter_log.Debug("publishing message to channel %s", channel)

	return b.redis.Publish(b.redis.Context, channel, msg).Err()
}

// Makes the matching socket instances join the specified rooms
func (b *BroadcastOperator) SocketsJoin(room ...socket.Room) {
	request, err := json.Marshal(&Request{
		Type: _types.REQUEST_REMOTE_JOIN,
		Opts: &_types.PacketOptions{
			Rooms:  b.rooms.Keys(),
			Except: b.exceptRooms.Keys(),
		},
		Rooms: room,
	})
	if err != nil {
		return
	}
	b.redis.Publish(b.redis.Context, b.broadcastOptions.RequestChannel, request)
}

// Makes the matching socket instances leave the specified rooms
func (b *BroadcastOperator) SocketsLeave(room ...socket.Room) {
	request, err := json.Marshal(&Request{
		Type: _types.REQUEST_REMOTE_LEAVE,
		Opts: &_types.PacketOptions{
			Rooms:  b.rooms.Keys(),
			Except: b.exceptRooms.Keys(),
		},
		Rooms: room,
	})
	if err != nil {
		return
	}
	b.redis.Publish(b.redis.Context, b.broadcastOptions.RequestChannel, request)
}

// Makes the matching socket instances disconnect
func (b *BroadcastOperator) DisconnectSockets(state bool) {
	request, err := json.Marshal(&Request{
		Type: _types.REQUEST_REMOTE_DISCONNECT,
		Opts: &_types.PacketOptions{
			Rooms:  b.rooms.Keys(),
			Except: b.exceptRooms.Keys(),
		},
		Close: state,
	})
	if err != nil {
		return
	}
	b.redis.Publish(b.redis.Context, b.broadcastOptions.RequestChannel, request)
}
