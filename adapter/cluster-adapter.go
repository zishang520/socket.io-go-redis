package adapter

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zishang520/engine.io/v2/log"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

const (
	EMITTER_UID     string        = "emitter"
	DEFAULT_TIMEOUT time.Duration = 5000 * time.Millisecond
)

var adapter_log = log.NewLog("socket.io-adapter")

func encodeOptions(opts *socket.BroadcastOptions) *_types.PacketOptions {
	return &_types.PacketOptions{
		Rooms:  opts.Rooms.Keys(),
		Except: opts.Except.Keys(),
		Flags:  opts.Flags,
	}
}

func decodeOptions(opts *_types.PacketOptions) *socket.BroadcastOptions {
	return &socket.BroadcastOptions{
		Rooms:  types.NewSet(opts.Rooms...),
		Except: types.NewSet(opts.Except...),
		Flags:  opts.Flags,
	}
}

// A cluster-ready adapter. Any extending class must:
//
// - implement [ClusterAdapter#PublishMessage] and [ClusterAdapter#PublishResponse]
// - call [ClusterAdapter#OnMessage] and [ClusterAdapter#OnResponse]
type ClusterAdapterBuilder struct {
	socket.AdapterConstructor
}

func (cb *ClusterAdapterBuilder) New(nsp socket.Namespace) socket.Adapter {
	return NewClusterAdapter(nsp)
}

type clusterAdapter struct {
	socket.Adapter

	uid string

	requests    *types.Map[string, *ClusterRequest]
	ackRequests *types.Map[string, ClusterAckRequest]
}

func MakeClusterAdapter() ClusterAdapter {
	c := &clusterAdapter{
		Adapter: socket.MakeAdapter(),

		requests:    &types.Map[string, *ClusterRequest]{},
		ackRequests: &types.Map[string, ClusterAckRequest]{},
	}

	c.Prototype(c)

	return c
}

func NewClusterAdapter(nsp socket.Namespace) ClusterAdapter {
	c := MakeClusterAdapter()

	c.Construct(nsp)

	return c
}

func (c *clusterAdapter) Construct(nsp socket.Namespace) {
	c.Adapter.Construct(nsp)
	c.uid, _ = RandomId()
}

func (c *clusterAdapter) Uid() string {
	return c.uid
}

// Called when receiving a message from another member of the cluster.
func (c *clusterAdapter) OnMessage(message *ClusterMessage, offset int64) {
	if message.Uid == c.uid {
		adapter_log.Debug("ignore message from self")
		return
	}
	adapter_log.Debug("new event of type %d from %s", message.Type, message.Uid)
	switch message.Type {
	case MESSAGE_BROADCAST:
		if message.Data.RequestId != "" {
			c.Adapter.BroadcastWithAck(
				message.Data.Packet.(*parser.Packet),
				decodeOptions(message.Data.Opts),
				func(clientCount uint64) {
					adapter_log.Debug("waiting for %d client acknowledgements", clientCount)
					c.PublishResponse(message.Uid, &ClusterMessage{
						Type: MESSAGE_BROADCAST_CLIENT_COUNT,
						Data: &ClusterMessageRecord{
							RequestId:   message.Data.RequestId,
							ClientCount: clientCount,
						},
					})
				}, func(arg []any, _ error) {
					adapter_log.Debug("received acknowledgement with value %v", arg)
					c.PublishResponse(message.Uid, &ClusterMessage{
						Type: MESSAGE_BROADCAST_ACK,
						Data: &ClusterMessageRecord{
							RequestId: message.Data.RequestId,
							Packet:    arg,
						},
					})
				})
		} else {
			packet := message.Data.Packet.(*parser.Packet)
			opts := decodeOptions(message.Data.Opts)

			c.addOffsetIfNecessary(packet, opts, offset)

			c.Adapter.Broadcast(packet, opts)
		}
		break

	case MESSAGE_SOCKETS_JOIN:
		c.Adapter.AddSockets(
			decodeOptions(message.Data.Opts),
			message.Data.Rooms,
		)
		break

	case MESSAGE_SOCKETS_LEAVE:
		c.Adapter.DelSockets(
			decodeOptions(message.Data.Opts),
			message.Data.Rooms,
		)
		break

	case MESSAGE_DISCONNECT_SOCKETS:
		c.Adapter.DisconnectSockets(
			decodeOptions(message.Data.Opts),
			message.Data.Close,
		)
		break

	case MESSAGE_FETCH_SOCKETS:
		adapter_log.Debug("calling fetchSockets with opts %v", message.Data.Opts)

		c.Adapter.FetchSockets(
			decodeOptions(message.Data.Opts),
		)(func(localSockets []socket.SocketDetails, err error) {
			if err != nil {
				adapter_log.Debug("FETCH_SOCKETS Adapter.OnMessage error: %s", err.Error())
				return
			}
			c.PublishResponse(message.Uid, &ClusterMessage{
				Type: MESSAGE_FETCH_SOCKETS_RESPONSE,
				Data: &ClusterMessageRecord{
					RequestId: message.Data.RequestId,
					Sockets: _types.Slice[socket.SocketDetails, *ResponseSockets](localSockets).Map(func(_socket socket.SocketDetails) *ResponseSockets {
						return &ResponseSockets{
							SocketId:        _socket.Id(),
							SocketHandshake: _socket.Handshake(),
							SocketRooms:     _socket.Rooms().Keys(),
							SocketData:      _socket.Data(),
						}
					}),
				},
			})
		})
		break

	case MESSAGE_SERVER_SIDE_EMIT:
		packet := message.Data.Packet.([]any)
		if message.Data.RequestId == "" {
			c.Nsp().EmitUntyped(packet[0].(string), packet[1:]...)
			return
		}
		called := int32(0)
		callback := func(arg ...any) {
			// only one argument is expected
			if atomic.CompareAndSwapInt32(&called, 0, 1) {
				adapter_log.Debug("calling acknowledgement with %v", arg)
				c.PublishResponse(message.Uid, &ClusterMessage{
					Type: MESSAGE_SERVER_SIDE_EMIT_RESPONSE,
					Data: &ClusterMessageRecord{
						RequestId: message.Data.RequestId,
						Packet:    arg,
					},
				})
			}
		}

		packet = append(packet, callback)
		c.Nsp().EmitUntyped(packet[0].(string), packet[1:]...)
		break

	default:
		adapter_log.Debug("unknown message type: %v", message.Type)
	}
}

// Called when receiving a response from another member of the cluster.
func (c *clusterAdapter) OnResponse(response *ClusterMessage) {
	requestId := response.Data.RequestId

	adapter_log.Debug("received response %v to request %s", response.Type, requestId)

	switch response.Type {
	case MESSAGE_BROADCAST_CLIENT_COUNT:
		if ackRequest, ok := c.ackRequests.Load(requestId); ok {
			ackRequest.ClientCountCallback(response.Data.ClientCount)
		}
		break

	case MESSAGE_BROADCAST_ACK:
		if ackRequest, ok := c.ackRequests.Load(requestId); ok {
			ackRequest.Ack(response.Data.Packet.([]any), nil)
		}
		break

	case MESSAGE_FETCH_SOCKETS_RESPONSE:
		if request, ok := c.requests.Load(requestId); ok {
			atomic.AddInt64(&request.Current, 1)
			request.Responses = append(request.Responses, response.Data.Sockets.([]any)...)

			if request.Current == request.Expected {
				utils.ClearTimeout(request.Timeout)
				request.Resolve(request.Responses, nil)
				c.requests.Delete(requestId)
			}
		}
		break

	case MESSAGE_SERVER_SIDE_EMIT_RESPONSE:
		if request, ok := c.requests.Load(requestId); ok {
			atomic.AddInt64(&request.Current, 1)
			request.Responses = append(request.Responses, response.Data.Packet.([]any)...)

			if request.Current == request.Expected {
				utils.ClearTimeout(request.Timeout)
				request.Resolve(request.Responses, nil)
				c.requests.Delete(requestId)
			}
		}
		break

	default:
		adapter_log.Debug("unknown response type: %v", response.Type)
	}
}

func (c *clusterAdapter) Broadcast(packet *parser.Packet, opts *socket.BroadcastOptions) {
	onlyLocal := opts.Flags != nil && opts.Flags.Local

	if !onlyLocal {
		offset, err := c.Publish(&ClusterMessage{
			Type: MESSAGE_BROADCAST,
			Data: &ClusterMessageRecord{
				Packet: packet,
				Opts:   encodeOptions(opts),
			},
		})
		if err != nil {
			adapter_log.Debug("error while broadcasting message: %s", err.Error())
			return
		}
		c.addOffsetIfNecessary(packet, opts, offset)
	}

	c.Adapter.Broadcast(packet, opts)
}

// Adds an offset at the end of the data array in order to allow the client to receive any missed packets when it
// reconnects after a temporary disconnection.
func (c *clusterAdapter) addOffsetIfNecessary(packet *parser.Packet, opts *socket.BroadcastOptions, offset int64) {
	if c.Nsp().Server().Opts().GetRawConnectionStateRecovery() != nil {
		return
	}
	isEventPacket := packet.Type == parser.EVENT
	// packets with acknowledgement are not stored because the acknowledgement function cannot be serialized and
	// restored on another server upon reconnection
	withoutAcknowledgement := packet.Id == nil
	notVolatile := opts == nil || opts.Flags == nil || opts.Flags.Volatile == false

	if isEventPacket && withoutAcknowledgement && notVolatile {
		packet.Data = append(packet.Data.([]any), offset)
	}
}

func (c *clusterAdapter) BroadcastWithAck(packet *parser.Packet, opts *socket.BroadcastOptions, clientCountCallback func(uint64), ack func([]any, error)) {
	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local
	if !onlyLocal {
		requestId, _ := RandomId()

		c.Publish(&ClusterMessage{
			Type: MESSAGE_BROADCAST,
			Data: &ClusterMessageRecord{
				RequestId: requestId,
				Packet:    packet,
				Opts:      encodeOptions(opts),
			},
		})

		c.ackRequests.Store(requestId, NewClusterAckRequest(clientCountCallback, ack))

		var timeout time.Duration

		if opts.Flags != nil {
			if time := opts.Flags.Timeout; time != nil {
				timeout = *time
			}
		}
		// we have no way to know at this level whether the server has received an acknowledgement from each client, so we
		// will simply clean up the ackRequests map after the given delay
		utils.SetTimeout(func() {
			c.ackRequests.Delete(requestId)
		}, timeout)
	}

	c.Adapter.BroadcastWithAck(packet, opts, clientCountCallback, ack)
}

func (c *clusterAdapter) AddSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	c.Adapter.AddSockets(opts, rooms)

	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local
	if onlyLocal {
		return
	}

	c.Publish(&ClusterMessage{
		Type: MESSAGE_SOCKETS_JOIN,
		Data: &ClusterMessageRecord{
			Opts:  encodeOptions(opts),
			Rooms: rooms,
		},
	})
}

func (c *clusterAdapter) DelSockets(opts *socket.BroadcastOptions, rooms []socket.Room) {
	c.Adapter.DelSockets(opts, rooms)

	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local
	if onlyLocal {
		return
	}

	c.Publish(&ClusterMessage{
		Type: MESSAGE_SOCKETS_LEAVE,
		Data: &ClusterMessageRecord{
			Opts:  encodeOptions(opts),
			Rooms: rooms,
		},
	})
}

func (c *clusterAdapter) DisconnectSockets(opts *socket.BroadcastOptions, state bool) {
	c.Adapter.DisconnectSockets(opts, state)

	onlyLocal := opts != nil && opts.Flags != nil && opts.Flags.Local
	if onlyLocal {
		return
	}

	c.Publish(&ClusterMessage{
		Type: MESSAGE_DISCONNECT_SOCKETS,
		Data: &ClusterMessageRecord{
			Opts:  encodeOptions(opts),
			Close: state,
		},
	})
}

func (c *clusterAdapter) FetchSockets(opts *socket.BroadcastOptions) func(func([]socket.SocketDetails, error)) {
	return func(callback func([]socket.SocketDetails, error)) {
		c.Adapter.FetchSockets(opts)(func(localSockets []socket.SocketDetails, _ error) {

			serverCount := c.ServerCount()

			expectedResponseCount := serverCount - 1

			if (opts.Flags != nil && opts.Flags.Local) || expectedResponseCount == 0 {
				callback(localSockets, nil)
				return
			}

			requestId, _ := RandomId()

			_timeout := DEFAULT_TIMEOUT
			if t := opts.Flags.Timeout; t != nil {
				_timeout = *t
			}
			timeout := utils.SetTimeout(func() {
				if storedRequest, ok := c.requests.Load(requestId); ok {
					callback(nil, errors.New(fmt.Sprintf("timeout reached: only %d responses received out of %d", storedRequest.Current, storedRequest.Expected)))
					c.requests.Delete(requestId)
				}
			}, _timeout)

			c.requests.Store(requestId, &ClusterRequest{
				Type: MESSAGE_FETCH_SOCKETS,
				Resolve: func(data any, err error) {
					callback(data.([]socket.SocketDetails), err)
				},
				Timeout:  timeout,
				Current:  0,
				Expected: expectedResponseCount,
				Responses: _types.Slice[socket.SocketDetails, any](localSockets).Map(func(item socket.SocketDetails) any {
					return item
				}),
			})

			c.Publish(&ClusterMessage{
				Type: MESSAGE_FETCH_SOCKETS,
				Data: &ClusterMessageRecord{
					Opts:      encodeOptions(opts),
					RequestId: requestId,
				},
			})
		})
	}
}

func (c *clusterAdapter) ServerSideEmit(packet []any) error {
	p_l := len(packet)
	ack, withAck := packet[p_l-1].(func([]any, error))

	if !withAck {
		c.Publish(&ClusterMessage{
			Type: MESSAGE_SERVER_SIDE_EMIT,
			Data: &ClusterMessageRecord{
				Packet: packet,
			},
		})
		return nil
	}

	expectedResponseCount := c.ServerCount() - 1

	adapter_log.Debug(`waiting for %d responses to "serverSideEmit" request`, expectedResponseCount)

	if expectedResponseCount <= 0 {
		ack(nil, nil)
		return nil
	}

	requestId, err := RandomId()

	if err != nil {
		return err
	}

	timeout := utils.SetTimeout(func() {
		if storedRequest, ok := c.requests.Load(requestId); ok {
			ack(storedRequest.Responses, errors.New(fmt.Sprintf(`timeout reached: only %d responses received out of %d`, storedRequest.Current, storedRequest.Expected)))
			c.requests.Delete(requestId)
		}
	}, DEFAULT_TIMEOUT)

	c.requests.Store(requestId, &ClusterRequest{
		Type: MESSAGE_SERVER_SIDE_EMIT,
		Resolve: func(data any, err error) {
			ack(data.([]any), err)
		},
		Timeout:   timeout,
		Current:   0,
		Expected:  expectedResponseCount,
		Responses: []any{},
	})

	c.Publish(&ClusterMessage{
		Type: MESSAGE_SERVER_SIDE_EMIT,
		Data: &ClusterMessageRecord{
			RequestId: requestId, // the presence of this attribute defines whether an acknowledgement is needed
			Packet:    packet[:p_l-1],
		},
	})
	return nil
}

func (c *clusterAdapter) Publish(message *ClusterMessage) (int64, error) {
	message.Uid = c.uid
	return c.PublishMessage(message)
}

// Send a message to the other members of the cluster.
func (c *clusterAdapter) PublishMessage(message *ClusterMessage) (int64, error) {
	return 0, nil
}

// Send a response to the given member of the cluster.
func (c *clusterAdapter) PublishResponse(requesterUid string, response *ClusterMessage) (int64, error) {
	return 0, nil
}
