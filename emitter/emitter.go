package emitter

import (
	"encoding/json"
	"errors"

	"github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

type Emitter struct {
	redis            *types.RedisClient
	opts             *EmitterOptions
	broadcastOptions *BroadcastOptions
}

func NewEmitter(redis *types.RedisClient, opts *EmitterOptions, nsps ...string) *Emitter {
	e := &Emitter{}
	e.redis = redis

	if opts == nil {
		opts = DefaultEmitterOptions()
	}
	e.opts = opts

	nsp := "/"
	if len(nsps) > 0 {
		nsp = nsps[0]
		if len(nsp) == 0 {
			nsp = "/"
		}
	}

	e.broadcastOptions = &BroadcastOptions{
		Nsp:              nsp,
		BroadcastChannel: e.opts.Key() + "#" + nsp + "#",
		RequestChannel:   e.opts.Key() + "-request#" + nsp + "#",
		Parser:           e.opts.Parser(),
	}
	return e
}

// Return a new emitter for the given namespace.
func (e *Emitter) Of(nsp string) *Emitter {
	if len(nsp) > 0 {
		if nsp[0] != '/' {
			nsp = "/" + nsp
		}
	} else {
		nsp = "/"
	}
	return NewEmitter(e.redis, e.opts, nsp)
}

// Emits to all clients.
func (e *Emitter) Emit(ev string, args ...any) error {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).Emit(ev, args...)
}

// Targets a room when emitting.
func (e *Emitter) To(room ...socket.Room) *BroadcastOperator {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).To(room...)
}

// Targets a room when emitting.
func (e *Emitter) In(room ...socket.Room) *BroadcastOperator {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).In(room...)
}

// Excludes a room when emitting.
func (e *Emitter) Except(room ...socket.Room) *BroadcastOperator {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).Except(room...)
}

// Sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to
// receive messages (because of network slowness or other issues, or because they’re connected through long polling
// and is in the middle of a request-response cycle).
func (e *Emitter) Volatile() *BroadcastOperator {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).Volatile()
}

// Sets the compress flag.
//
// compress - if `true`, compresses the sending data
func (e *Emitter) Compress(compress bool) *BroadcastOperator {
	return NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).Compress(compress)
}

// Makes the matching socket instances join the specified rooms
func (e *Emitter) SocketsJoin(room ...socket.Room) {
	NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).SocketsJoin(room...)
}

// Makes the matching socket instances leave the specified rooms
func (e *Emitter) SocketsLeave(room ...socket.Room) {
	NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).SocketsLeave(room...)
}

// Makes the matching socket instances disconnect
func (e *Emitter) DisconnectSockets(state bool) {
	NewBroadcastOperator(e.redis, e.broadcastOptions, nil, nil, nil).DisconnectSockets(state)
}

// Send a packet to the Socket.IO servers in the cluster
//
// args - any number of serializable arguments
func (e *Emitter) ServerSideEmit(ev string, args ...any) error {
	if _, withAck := args[len(args)-1].(func([]any, error)); withAck {
		return errors.New("Acknowledgements are not supported")
	}
	request, err := json.Marshal(&ServerRequest{
		Uid:  UID,
		Type: types.REQUEST_SERVER_SIDE_EMIT,
		Data: append([]any{ev}, args...),
	})
	if err != nil {
		return err
	}
	return e.redis.Client.Publish(e.redis.Context, e.broadcastOptions.RequestChannel, request).Err()
}
