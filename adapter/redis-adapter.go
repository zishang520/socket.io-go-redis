package adapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zishang520/engine.io/v2/log"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

var redis_log = log.NewLog("socket.io-redis")

type RedisAdapterBuilder struct {
	socket.AdapterConstructor

	// a Redis client
	Redis *_types.RedisClient
	// additional options
	Opts *RedisAdapterOptions
}

// Adapter constructor.
func (rb *RedisAdapterBuilder) New(nsp socket.NamespaceInterface) socket.Adapter {
	return NewRedisAdapter(nsp, rb.Redis, rb.Opts)
}

type redisAdapter struct {
	socket.Adapter

	redis *_types.RedisClient
	opts  *RedisAdapterOptions

	uid                              string
	requestsTimeout                  time.Duration
	publishOnSpecificResponseChannel bool
	parser                           _types.Parser

	channel                 string
	requestChannel          string
	responseChannel         string
	specificResponseChannel string

	requests             *types.Map[string, *Request]
	ackRequests          *types.Map[string, AckRequest]
	redisListeners       *types.Map[string, *redis.PubSub]
	friendlyErrorHandler func(...any)
}

func MakeRedisAdapter() RedisAdapter {
	c := &redisAdapter{
		Adapter: socket.MakeAdapter(),

		opts:                 DefaultRedisAdapterOptions(),
		requests:             &types.Map[string, *Request]{},
		ackRequests:          &types.Map[string, AckRequest]{},
		redisListeners:       &types.Map[string, *redis.PubSub]{},
		friendlyErrorHandler: func(...any) {},
	}

	c.Prototype(c)

	return c
}

func NewRedisAdapter(nsp socket.NamespaceInterface, redis *_types.RedisClient, opts *RedisAdapterOptions) RedisAdapter {
	c := MakeRedisAdapter()

	c.SetRedis(redis)
	c.SetOpts(opts)

	c.Construct(nsp)

	return c
}

func (r *redisAdapter) Construct(nsp socket.NamespaceInterface) {
	r.Adapter.Construct(nsp)

	r.uid, _ = Uid2(6)
	r.requestsTimeout = r.opts.RequestsTimeout()
	r.publishOnSpecificResponseChannel = r.opts.PublishOnSpecificResponseChannel()
	r.parser = r.opts.Parser()

	prefix := r.opts.Key()

	r.channel = prefix + "#" + nsp.Name() + "#"
	r.requestChannel = prefix + "-request#" + r.Nsp().Name() + "#"
	r.responseChannel = prefix + "-response#" + r.Nsp().Name() + "#"
	r.specificResponseChannel = r.responseChannel + r.uid + "#"

	r.friendlyErrorHandler = func(...any) {
		if r.redis.ListenerCount("error") == 1 {
			redis_log.Warning("missing 'error' handler on this Redis client")
		}
	}

	r.redis.On("error", r.friendlyErrorHandler)

	psub := r.redis.PSubscribe(r.redis.Context, r.channel+"*")
	r.redisListeners.Store("psub", psub)
	go func() {
		defer psub.Close()

		for {
			select {
			case <-r.redis.Context.Done():
				return
			default:
				msg, err := psub.ReceiveMessage(r.redis.Context)
				if err != nil {
					r.redis.Emit("error", err)
					return
				}
				r.onmessage(msg.Pattern, msg.Channel, []byte(msg.Payload))
			}
		}
	}()

	sub := r.redis.Subscribe(r.redis.Context, r.requestChannel, r.responseChannel, r.specificResponseChannel)
	r.redisListeners.Store("sub", sub)
	go func() {
		defer sub.Close()

		for {
			select {
			case <-r.redis.Context.Done():
				return
			default:
				msg, err := sub.ReceiveMessage(r.redis.Context)
				if err != nil {
					r.redis.Emit("error", err)
					return
				}
				r.onrequest(msg.Channel, []byte(msg.Payload))
			}
		}
	}()
}

func (r *redisAdapter) SetRedis(redis *_types.RedisClient) {
	r.redis = redis
}

func (r *redisAdapter) SetOpts(opts *RedisAdapterOptions) {
	r.opts.Assign(opts)
}

// readonly
func (r *redisAdapter) Uid() string {
	return r.uid
}

// readonly
func (r *redisAdapter) RequestsTimeout() time.Duration {
	return r.requestsTimeout
}

// readonly
func (r *redisAdapter) PublishOnSpecificResponseChannel() bool {
	return r.publishOnSpecificResponseChannel
}

// readonly
func (r *redisAdapter) Parser() _types.Parser {
	return r.parser
}

// Called with a subscription message
func (r *redisAdapter) onmessage(pattern string, channel string, msg []byte) {
	if !strings.HasPrefix(channel, r.channel) {
		redis_log.Debug("ignore different channel")
		return
	}

	if len(r.channel) < len(channel) {
		if !strings.HasSuffix(channel, "#") {
			redis_log.Debug("ignore invalid channel %s", channel)
			return
		}

		room := channel[len(r.channel):len(channel)-1]
		if !r.hasRoom(socket.Room(room)) {
			redis_log.Debug("ignore unknown room %s", room)
			return
		}
	}

	var args *_types.Packet

	if r.parser.Decode(msg, &args) != nil {
		redis_log.Debug("ignore exception message")
		return
	}

	if r.uid == args.Uid {
		redis_log.Debug("ignore same uid")
		return
	}

	if args.Packet == nil {
		redis_log.Debug("ignore different namespace")
		return
	}

	if len(args.Packet.Nsp) == 0 {
		args.Packet.Nsp = "/"
	}

	if args.Packet.Nsp != r.Nsp().Name() {
		redis_log.Debug("ignore different namespace")
		return
	}
	opts := &socket.BroadcastOptions{}
	if args.Opts != nil {
		opts.Rooms = types.NewSet(args.Opts.Rooms...)
		opts.Except = types.NewSet(args.Opts.Except...)
	}

	r.Adapter.Broadcast(args.Packet, opts)
}

func (r *redisAdapter) hasRoom(room socket.Room) bool {
	_, ok := r.Rooms().Load(room)
	return ok
}

// Called on request from another node
func (r *redisAdapter) onrequest(channel string, msg []byte) {
	if strings.HasPrefix(channel, r.responseChannel) {
		r.onresponse(channel, msg)
		return
	} else if !strings.HasPrefix(channel, r.requestChannel) {
		redis_log.Debug("ignore different channel")
		return
	}

	var request *Request
	// if the buffer starts with a "{" character
	if msg[0] == '{' {
		if err := json.Unmarshal(msg, &request); err != nil {
			redis_log.Debug("ignoring malformed request")
			return
		}
	} else {
		if err := r.parser.Decode(msg, &request); err != nil {
			redis_log.Debug("ignoring malformed request")
			return
		}
	}

	redis_log.Debug("received request %v", request)

	switch request.Type {
	case _types.REQUEST_SOCKETS:
		if _, ok := r.requests.Load(request.RequestId); ok {
			return
		}
		sockets := r.Adapter.Sockets(types.NewSet(request.Rooms...))
		response, err := json.Marshal(&Response{
			RequestId: request.RequestId,
			Sockets: _types.Slice[socket.SocketId, *ResponseSockets](sockets.Keys()).Map(func(socketId socket.SocketId) *ResponseSockets {
				return &ResponseSockets{
					SocketId: socketId,
				}
			}),
		})
		if err != nil {
			redis_log.Debug("SOCKETS json.Marshal error: %s", err.Error())
			return
		}

		r.PublishResponse(request, response)
		break

	case _types.REQUEST_ALL_ROOMS:
		if _, ok := r.requests.Load(request.RequestId); ok {
			return
		}

		response, err := json.Marshal(&Response{
			RequestId: request.RequestId,
			Rooms:     r.Rooms().Keys(),
		})
		if err != nil {
			redis_log.Debug("ALL_ROOMS json.Marshal error: %s", err.Error())
			return
		}

		r.PublishResponse(request, response)
		break

	case _types.REQUEST_REMOTE_JOIN:
		if request.Opts != nil {
			r.Adapter.AddSockets(&socket.BroadcastOptions{
				Rooms:  types.NewSet(request.Opts.Rooms...),
				Except: types.NewSet(request.Opts.Except...),
			}, request.Rooms)
			return
		}

		if _socket, ok := r.Nsp().Sockets().Load(request.Sid); ok {
			_socket.Join(request.Room)

			response, err := json.Marshal(&Response{
				RequestId: request.RequestId,
			})
			if err != nil {
				redis_log.Debug("REMOTE_JOIN json.Marshal error: %s", err.Error())
				return
			}
			r.PublishResponse(request, response)
		}
		break

	case _types.REQUEST_REMOTE_LEAVE:
		if request.Opts != nil {
			r.Adapter.DelSockets(&socket.BroadcastOptions{
				Rooms:  types.NewSet(request.Opts.Rooms...),
				Except: types.NewSet(request.Opts.Except...),
			}, request.Rooms)
			return
		}

		if _socket, ok := r.Nsp().Sockets().Load(request.Sid); ok {
			_socket.Leave(request.Room)

			response, err := json.Marshal(&Response{
				RequestId: request.RequestId,
			})
			if err != nil {
				redis_log.Debug("REMOTE_LEAVE json.Marshal error: %s", err.Error())
				return
			}
			r.PublishResponse(request, response)
		}
		break

	case _types.REQUEST_REMOTE_DISCONNECT:
		if request.Opts != nil {
			r.Adapter.DisconnectSockets(&socket.BroadcastOptions{
				Rooms:  types.NewSet(request.Opts.Rooms...),
				Except: types.NewSet(request.Opts.Except...),
			}, request.Close)
			return
		}

		if _socket, ok := r.Nsp().Sockets().Load(request.Sid); ok {
			_socket.Disconnect(request.Close)

			response, err := json.Marshal(&Response{
				RequestId: request.RequestId,
			})
			if err != nil {
				redis_log.Debug("REMOTE_LEAVE json.Marshal error: %s", err.Error())
				return
			}
			r.PublishResponse(request, response)
		}
		break

	case _types.REQUEST_REMOTE_FETCH:
		if _, ok := r.requests.Load(request.RequestId); ok {
			return
		}

		r.Adapter.FetchSockets(&socket.BroadcastOptions{
			Rooms:  types.NewSet(request.Opts.Rooms...),
			Except: types.NewSet(request.Opts.Except...),
		})(func(localSockets []socket.SocketDetails, e error) {
			if e != nil {
				redis_log.Debug("REMOTE_FETCH Adapter.FetchSockets error: %s", e.Error())
				return
			}
			response, err := json.Marshal(&Response{
				RequestId: request.RequestId,
				Sockets: _types.Slice[socket.SocketDetails, *ResponseSockets](localSockets).Map(func(_socket socket.SocketDetails) *ResponseSockets {
					return &ResponseSockets{
						SocketId:        _socket.Id(),
						SocketHandshake: _socket.Handshake(),
						SocketRooms:     _socket.Rooms().Keys(),
						SocketData:      _socket.Data(),
					}
				}),
			})
			if err != nil {
				redis_log.Debug("REMOTE_FETCH json.Marshal error: %s", err.Error())
				return
			}
			r.PublishResponse(request, response)
		})
		break

	case _types.REQUEST_SERVER_SIDE_EMIT:
		if request.Uid == r.uid {
			redis_log.Debug("ignore same uid")
			return
		}
		if request.RequestId == "" {
			r.Nsp().EmitUntyped(request.Data[0].(string), request.Data[1:]...)
			return
		}
		called := int32(0)
		callback := func(args []any, err error) {
			// only one argument is expected
			if atomic.CompareAndSwapInt32(&called, 0, 1) {
				redis_log.Debug("calling acknowledgement with %v", args)
				response, err := json.Marshal(&Response{
					Type:      _types.REQUEST_SERVER_SIDE_EMIT,
					RequestId: request.RequestId,
					Data:      args,
				})
				if err != nil {
					redis_log.Debug("SERVER_SIDE_EMIT acknowledgement json.Marshal error: %s", err.Error())
					return
				}
				if err := r.redis.Publish(r.redis.Context, r.responseChannel, response).Err(); err != nil {
					r.redis.Emit("error", err)
				}
			}
		}
		request.Data = append(request.Data, callback)
		r.Nsp().EmitUntyped(request.Data[0].(string), request.Data[1:]...)
		break

	case _types.REQUEST_BROADCAST:
		if _, ok := r.ackRequests.Load(request.RequestId); ok {
			// ignore self
			return
		}

		opts := &socket.BroadcastOptions{
			Rooms:  types.NewSet(request.Opts.Rooms...),
			Except: types.NewSet(request.Opts.Except...),
		}

		r.Adapter.BroadcastWithAck(
			request.Packet,
			opts,
			func(clientCount uint64) {
				redis_log.Debug("waiting for %d client acknowledgements", clientCount)
				response, err := json.Marshal(&Response{
					Type:        _types.REQUEST_BROADCAST_CLIENT_COUNT,
					RequestId:   request.RequestId,
					ClientCount: clientCount,
				})
				if err != nil {
					redis_log.Debug("BROADCAST waiting json.Marshal error: %s", err.Error())
					return
				}
				r.PublishResponse(request, response)
			},
			func(args []any, _ error) {
				redis_log.Debug("received acknowledgement with value %v", args)
				response, err := r.parser.Encode(&Response{
					Type:      _types.REQUEST_BROADCAST_ACK,
					RequestId: request.RequestId,
					Packet:    args,
				})
				if err != nil {
					redis_log.Debug("BROADCAST received json.Marshal error: %s", err.Error())
					return
				}
				r.PublishResponse(request, response)
			},
		)
		break

	default:
		redis_log.Debug("ignoring unknown request type: %d", request.Type)
	}
}

// Send the response to the requesting node
func (r *redisAdapter) PublishResponse(request *Request, response []byte) {
	responseChannel := r.responseChannel
	if r.publishOnSpecificResponseChannel {
		responseChannel += request.Uid + "#"
	}
	redis_log.Debug("publishing response to channel %s", responseChannel)
	if err := r.redis.Publish(r.redis.Context, responseChannel, response).Err(); err != nil {
		r.redis.Emit("error", err)
	}
}

// Called on response from another node
func (r *redisAdapter) onresponse(channel string, msg []byte) {
	var response *Response

	// if the buffer starts with a "{" character
	if msg[0] == '{' {
		if err := json.Unmarshal(msg, &response); err != nil {
			redis_log.Debug("ignoring malformed response")
			return
		}
	} else {
		if err := r.parser.Decode(msg, &response); err != nil {
			redis_log.Debug("ignoring malformed response")
			return
		}
	}

	requestId := response.RequestId

	if ackRequest, ok := r.ackRequests.Load(requestId); ok {
		switch response.Type {
		case _types.REQUEST_BROADCAST_CLIENT_COUNT:
			ackRequest.ClientCountCallback(response.ClientCount)
			break
		case _types.REQUEST_BROADCAST_ACK:
			ackRequest.Ack(response.Packet, nil)
			break
		}
		return
	}

	if requestId == "" {
		redis_log.Debug("ignoring unknown request")
		return
	} else if request, ok := r.requests.Load(requestId); !ok {
		// I don’t know what this weird logic is for.
		if _, ok := r.ackRequests.Load(requestId); !ok {
			redis_log.Debug("ignoring unknown request")
			return
		}
	} else {
		redis_log.Debug("received response %v", response)
		switch request.Type {
		case _types.REQUEST_SOCKETS, _types.REQUEST_REMOTE_FETCH:
			atomic.AddInt64(&request.MsgCount, 1)

			// ignore if response does not contain 'sockets' key
			if len(response.Sockets) == 0 {
				return
			}

			if request.Type == _types.REQUEST_SOCKETS {
				for _, s := range response.Sockets {
					request.Sockets = append(request.Sockets, s)
				}
			} else {
				for _, s := range response.Sockets {
					request.Sockets = append(request.Sockets, s)
				}
			}

			if request.MsgCount == request.NumSub {
				utils.ClearTimeout(request.Timeout)
				if request.Resolve != nil {
					request.Resolve(request.Sockets, nil)
				}
				r.requests.Delete(requestId)
			}
			break

		case _types.REQUEST_ALL_ROOMS:
			atomic.AddInt64(&request.MsgCount, 1)

			// ignore if response does not contain 'rooms' key
			if response.Rooms == nil {
				return
			}
			request.Rooms = append(request.Rooms, response.Rooms...)

			if request.MsgCount == request.NumSub {
				utils.ClearTimeout(request.Timeout)
				if request.Resolve != nil {
					request.Resolve(request.Rooms, nil)
				}
				r.requests.Delete(requestId)
			}
			break

		case _types.REQUEST_REMOTE_JOIN, _types.REQUEST_REMOTE_LEAVE, _types.REQUEST_REMOTE_DISCONNECT:
			utils.ClearTimeout(request.Timeout)
			if request.Resolve != nil {
				request.Resolve(nil, nil)
			}
			r.requests.Delete(requestId)
			break

		case _types.REQUEST_SERVER_SIDE_EMIT:
			request.Responses = append(request.Responses, response.Data)
			length := len(request.Responses)

			redis_log.Debug("serverSideEmit: got %d responses out of %d", length, request.NumSub)
			if int64(length) == request.NumSub {
				utils.ClearTimeout(request.Timeout)
				if request.Resolve != nil {
					request.Resolve(request.Responses, nil)
				}
				r.requests.Delete(requestId)
			}
			break

		default:
			redis_log.Debug("ignoring unknown request type: %d", request.Type)
		}
	}
}

// Broadcasts a packet.
func (r *redisAdapter) Broadcast(packet *parser.Packet, opts *socket.BroadcastOptions) {
	packet.Nsp = r.Nsp().Name()

	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local

	if !onlyLocal {
		rawOpts := &_types.PacketOptions{
			Rooms:  opts.Rooms.Keys(),
			Except: opts.Except.Keys(),
			Flags:  opts.Flags,
		}
		if msg, err := r.parser.Encode(&_types.Packet{Uid: r.Uid(), Packet: packet, Opts: rawOpts}); err == nil {
			channel := r.channel
			if opts.Rooms != nil && opts.Rooms.Len() == 1 {
				channel += string(opts.Rooms.Keys()[0]) + "#"
			}
			redis_log.Debug("publishing message to channel %s", channel)
			if err := r.redis.Publish(r.redis.Context, channel, msg).Err(); err != nil {
				r.redis.Emit("error", err)
			}
		}
	}
	r.Adapter.Broadcast(packet, opts)
}

func (r *redisAdapter) BroadcastWithAck(packet *parser.Packet, opts *socket.BroadcastOptions, clientCountCallback func(uint64), ack func([]any, error)) {
	packet.Nsp = r.Nsp().Name()

	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local

	if !onlyLocal {
		if requestId, err := Uid2(6); err == nil {
			rawOpts := &_types.PacketOptions{
				Rooms:  opts.Rooms.Keys(),
				Except: opts.Except.Keys(),
				Flags:  opts.Flags,
			}

			if request, err := r.parser.Encode(&Request{
				Uid:       r.uid,
				RequestId: requestId,
				Type:      _types.REQUEST_BROADCAST,
				Packet:    packet,
				Opts:      rawOpts,
			}); err == nil {
				if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
					r.redis.Emit("error", err)
				}

				r.ackRequests.Store(requestId, NewAckRequest(clientCountCallback, ack))

				if opts.Flags != nil && opts.Flags.Timeout != nil {
					// we have no way to know at this level whether the server has received an acknowledgement from each client, so we
					// will simply clean up the ackRequests map after the given delay
					utils.SetTimeout(func() {
						r.ackRequests.Delete(requestId)
					}, *opts.Flags.Timeout)
				}
			}
		}
	}

	r.Adapter.BroadcastWithAck(packet, opts, clientCountCallback, ack)
}

// Gets the list of all rooms (across every node)
func (r *redisAdapter) AllRooms() func(func(*types.Set[socket.Room], error)) {
	return func(callback func(*types.Set[socket.Room], error)) {
		localRooms := types.NewSet(r.Rooms().Keys()...)
		numSub := r.ServerCount()

		redis_log.Debug(`waiting for %d responses to "allRooms" request`, numSub)

		if numSub <= 1 {
			callback(localRooms, nil)
			return
		}

		if requestId, err := Uid2(6); err == nil {
			if request, err := json.Marshal(&Request{
				Type:      _types.REQUEST_ALL_ROOMS,
				Uid:       r.uid,
				RequestId: requestId,
			}); err == nil {
				timeout := utils.SetTimeout(func() {
					if _, ok := r.requests.Load(requestId); ok {
						callback(nil, errors.New("timeout reached while waiting for allRooms response"))
						r.requests.Delete(requestId)
					}
				}, r.requestsTimeout)

				r.requests.Store(requestId, &Request{
					Type:   _types.REQUEST_ALL_ROOMS,
					NumSub: numSub,
					Resolve: func(data any, err error) {
						callback(types.NewSet(data.([]socket.Room)...), err)
					},
					Timeout:  timeout,
					MsgCount: 1,
					Rooms:    localRooms.Keys(),
				})

				if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
					r.redis.Emit("error", err)
				}
			}
		}
	}
}

func (r *redisAdapter) FetchSockets(opts *socket.BroadcastOptions) func(func([]socket.SocketDetails, error)) {
	return func(callback func([]socket.SocketDetails, error)) {
		r.Adapter.FetchSockets(opts)(func(localSockets []socket.SocketDetails, _ error) {
			if opts.Flags != nil && opts.Flags.Local {
				callback(localSockets, nil)
				return
			}

			numSub := r.ServerCount()
			redis_log.Debug(`waiting for %d responses to "fetchSockets" request`, numSub)

			if numSub <= 1 {
				callback(localSockets, nil)
				return
			}

			if requestId, err := Uid2(6); err == nil {
				if request, err := json.Marshal(&Request{
					Type:      _types.REQUEST_REMOTE_FETCH,
					Uid:       r.uid,
					RequestId: requestId,
					Opts: &_types.PacketOptions{
						Rooms:  opts.Rooms.Keys(),
						Except: opts.Except.Keys(),
					},
				}); err == nil {
					timeout := utils.SetTimeout(func() {
						if _, ok := r.requests.Load(requestId); ok {
							callback(nil, errors.New("timeout reached while waiting for fetchSockets response"))
							r.requests.Delete(requestId)
						}
					}, r.requestsTimeout)

					r.requests.Store(requestId, &Request{
						Type:   _types.REQUEST_REMOTE_FETCH,
						NumSub: numSub,
						Resolve: func(data any, err error) {
							callback(_types.Slice[*ResponseSockets, socket.SocketDetails](data.([]*ResponseSockets)).Map(func(value *ResponseSockets) socket.SocketDetails {
								return socket.SocketDetails(value)
							}), err)
						},
						Timeout:  timeout,
						MsgCount: 1,
						Sockets: _types.Slice[socket.SocketDetails, *ResponseSockets](localSockets).Map(func(_socket socket.SocketDetails) *ResponseSockets {
							return &ResponseSockets{
								SocketId:        _socket.Id(),
								SocketHandshake: _socket.Handshake(),
								SocketRooms:     _socket.Rooms().Keys(),
								SocketData:      _socket.Data(),
							}
						}),
					})

					if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
						r.redis.Emit("error", err)
					}
				}
			}
		})
	}
}

func (r *redisAdapter) AddSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	if opts.Flags != nil && opts.Flags.Local {
		r.Adapter.AddSockets(opts, rooms)
		return
	}

	if request, err := json.Marshal(&Request{
		Uid:  r.uid,
		Type: _types.REQUEST_REMOTE_JOIN,
		Opts: &_types.PacketOptions{
			Rooms:  opts.Rooms.Keys(),
			Except: opts.Except.Keys(),
		},
		Rooms: rooms,
	}); err == nil {
		if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
			r.redis.Emit("error", err)
		}
	}
}

func (r *redisAdapter) DelSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	if opts.Flags != nil && opts.Flags.Local {
		r.Adapter.DelSockets(opts, rooms)
		return
	}

	if request, err := json.Marshal(&Request{
		Uid:  r.uid,
		Type: _types.REQUEST_REMOTE_LEAVE,
		Opts: &_types.PacketOptions{
			Rooms:  opts.Rooms.Keys(),
			Except: opts.Except.Keys(),
		},
		Rooms: rooms,
	}); err == nil {
		if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
			r.redis.Emit("error", err)
		}
	}
}

func (r *redisAdapter) DisconnectSockets(opts *socket.BroadcastOptions, state bool) {
	if opts.Flags != nil && opts.Flags.Local {
		r.Adapter.DisconnectSockets(opts, state)
		return
	}

	if request, err := json.Marshal(&Request{
		Uid:  r.uid,
		Type: _types.REQUEST_REMOTE_DISCONNECT,
		Opts: &_types.PacketOptions{
			Rooms:  opts.Rooms.Keys(),
			Except: opts.Except.Keys(),
		},
		Close: state,
	}); err == nil {
		if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
			r.redis.Emit("error", err)
		}
	}
}

func (r *redisAdapter) ServerSideEmit(packet []any) error {
	if _, withAck := packet[len(packet)-1].(func([]any, error)); withAck {
		// ignore errors
		r.serverSideEmitWithAck(packet)
		return nil
	}

	request, err := json.Marshal(&Request{
		Uid:  r.uid,
		Type: _types.REQUEST_SERVER_SIDE_EMIT,
		Data: packet,
	})

	if err != nil {
		return err
	}
	return r.redis.Publish(r.redis.Context, r.requestChannel, request).Err()
}

func (r *redisAdapter) serverSideEmitWithAck(packet []any) {
	ack := packet[len(packet)-1].(func([]any, error))
	numSub := r.ServerCount() - 1 // ignore self

	redis_log.Debug(`waiting for %d responses to "serverSideEmit" request`, numSub)

	if numSub <= 0 {
		ack(nil, nil)
		return
	}

	if requestId, err := Uid2(6); err == nil {
		if request, err := json.Marshal(&Request{
			Uid:       r.uid,
			RequestId: requestId, // the presence of this attribute defines whether an acknowledgement is needed
			Type:      _types.REQUEST_SERVER_SIDE_EMIT,
			Data:      packet,
		}); err == nil {
			timeout := utils.SetTimeout(func() {
				if storedRequest, ok := r.requests.Load(requestId); ok {
					ack(storedRequest.Responses, errors.New(fmt.Sprintf(`timeout reached: only %d responses received out of %d`, len(storedRequest.Responses), storedRequest.NumSub)))
					r.requests.Delete(requestId)
				}
			}, r.requestsTimeout)

			r.requests.Store(requestId, &Request{
				Type:    _types.REQUEST_SERVER_SIDE_EMIT,
				NumSub:  numSub,
				Timeout: timeout,
				Resolve: func(data any, err error) {
					ack(data.([]any), err)
				},
				Responses: []any{},
			})

			if err := r.redis.Publish(r.redis.Context, r.requestChannel, request).Err(); err != nil {
				r.redis.Emit("error", err)
			}
		}
	}
}

func (r *redisAdapter) ServerCount() int64 {
	if result, err := r.redis.PubSubNumSub(r.redis.Context, r.requestChannel).Result(); err != nil {
		r.redis.Emit("error", err)
	} else {
		if r, ok := result[r.requestChannel]; ok {
			return r
		}
	}
	return 0
}

func (r *redisAdapter) Close() {
	if psub, ok := r.redisListeners.Load("psub"); ok {
		if err := psub.PUnsubscribe(r.redis.Context, r.channel+"*"); err != nil {
			r.redis.Emit("error", err)
		}
	}
	if sub, ok := r.redisListeners.Load("sub"); ok {
		if err := sub.Unsubscribe(r.redis.Context, r.requestChannel, r.responseChannel, r.specificResponseChannel); err != nil {
			r.redis.Emit("error", err)
		}
	}
	// Thinking about whether r.redisListeners needs to be cleared?
	r.redis.RemoveListener("error", r.friendlyErrorHandler)
}
