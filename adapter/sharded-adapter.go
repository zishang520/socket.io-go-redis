package adapter

import (
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/adapter"
	"github.com/zishang520/socket.io/v2/socket"
)

// Create a new Adapter based on Redis sharded Pub/Sub introduced in Redis 7.0.
//
// See: https://redis.io/docs/manual/pubsub/#sharded-pubsub
type ShardedRedisAdapterBuilder struct {
	// the Redis client used to publish/subscribe
	Redis *_types.RedisClient
	// some additional options
	Opts *ShardedRedisAdapterOptions
}

func (sb *ShardedRedisAdapterBuilder) New(nsp socket.Namespace) socket.Adapter {
	return NewShardedRedisAdapter(nsp, sb.Redis, sb.Opts)
}

type shardedRedisAdapter struct {
	adapter.ClusterAdapter

	pubSubClient    *redis.PubSub
	redisClient     *_types.RedisClient
	opts            *ShardedRedisAdapterOptions
	channel         string
	responseChannel string
}

func MakeShardedRedisAdapter() ShardedRedisAdapter {
	c := &shardedRedisAdapter{
		ClusterAdapter: adapter.MakeClusterAdapter(),

		opts: DefaultShardedRedisAdapterOptions(),
	}

	c.Prototype(c)

	return c
}

func NewShardedRedisAdapter(nsp socket.Namespace, redis *_types.RedisClient, opts *ShardedRedisAdapterOptions) ShardedRedisAdapter {
	c := MakeShardedRedisAdapter()

	c.SetRedis(redis)
	c.SetOpts(opts)

	c.Construct(nsp)

	return c
}

func (s *shardedRedisAdapter) SetRedis(redisClient *_types.RedisClient) {
	s.redisClient = redisClient
}

func (s *shardedRedisAdapter) SetOpts(opts *ShardedRedisAdapterOptions) {
	if opts == nil {
		opts = DefaultShardedRedisAdapterOptions()
	}
	s.opts.Assign(opts)
}

func (s *shardedRedisAdapter) Construct(nsp socket.Namespace) {
	s.ClusterAdapter.Construct(nsp)

	s.channel = s.opts.ChannelPrefix() + "#" + nsp.Name() + "#"
	s.responseChannel = s.opts.ChannelPrefix() + "#" + nsp.Name() + "#" + string(s.Uid()) + "#"

	s.pubSubClient = s.redisClient.Client.SSubscribe(s.redisClient.Context, s.channel, s.responseChannel)

	if s.opts.SubscriptionMode() == DynamicSubscriptionMode ||
		s.opts.SubscriptionMode() == DynamicPrivateSubscriptionMode {
		s.On("create-room", func(room ...any) {
			if s.shouldUseASeparateNamespace(room[0].(socket.Room)) {
				if err := s.pubSubClient.SSubscribe(s.redisClient.Context, s.dynamicChannel(room[0].(socket.Room))); err != nil {
					s.redisClient.Emit("error", err)
				}
			}
		})
		s.On("delete-room", func(room ...any) {
			if s.shouldUseASeparateNamespace(room[0].(socket.Room)) {
				if err := s.pubSubClient.SUnsubscribe(s.redisClient.Context, s.dynamicChannel(room[0].(socket.Room))); err != nil {
					s.redisClient.Emit("error", err)
				}
			}
		})
	}

	go func() {
		defer s.pubSubClient.Close()

		for {
			select {
			case <-s.redisClient.Context.Done():
				return
			default:
				msg, err := s.pubSubClient.ReceiveMessage(s.redisClient.Context)
				if err != nil {
					s.redisClient.Emit("error", err)
					if err == redis.ErrClosed {
						return
					}
					continue // retry receiving messages
				}
				s.onRawMessage([]byte(msg.Payload), msg.Channel)
			}
		}
	}()
}

func (s *shardedRedisAdapter) Close() {
	channels := []string{s.channel, s.responseChannel}

	if s.opts.SubscriptionMode() == DynamicSubscriptionMode ||
		s.opts.SubscriptionMode() == DynamicPrivateSubscriptionMode {
		s.Rooms().Range(func(room socket.Room, _sids *types.Set[socket.SocketId]) bool {
			if s.shouldUseASeparateNamespace(room) {
				channels = append(channels, s.dynamicChannel(room))
			}
			return true
		})
	}

	if err := s.pubSubClient.SUnsubscribe(s.redisClient.Context, channels...); err != nil {
		s.redisClient.Emit("error", err)
	}
}

func (s *shardedRedisAdapter) DoPublish(message *adapter.ClusterMessage) (adapter.Offset, error) {
	channel := s.computeChannel(message)
	redis_log.Debug("publishing message of type %v to %s", message.Type, channel)

	msg, err := s.encode(message)
	if err != nil {
		return "", err
	}

	return "", s.redisClient.Client.SPublish(s.redisClient.Context, channel, msg).Err()
}

func (s *shardedRedisAdapter) computeChannel(message *adapter.ClusterMessage) string {
	// broadcast with ack can not use a dynamic channel, because the serverCount() method return the number of all
	// servers, not only the ones where the given room exists
	if message.Type != adapter.BROADCAST {
		return s.channel
	}

	data, ok := message.Data.(*adapter.BroadcastMessage)
	if !ok || data.RequestId != nil {
		return s.channel
	}

	if len(data.Opts.Rooms) == 1 {
		if room := data.Opts.Rooms[0]; (s.opts.SubscriptionMode() == DynamicSubscriptionMode && len(string(room)) != 20) ||
			s.opts.SubscriptionMode() == DynamicPrivateSubscriptionMode {
			return s.dynamicChannel(room)
		}
	}

	return s.channel
}

func (s *shardedRedisAdapter) dynamicChannel(room socket.Room) string {
	return s.channel + string(room) + "#"
}

func (s *shardedRedisAdapter) DoPublishResponse(requesterUid adapter.ServerId, response *adapter.ClusterResponse) error {
	redis_log.Debug("publishing response of type %d to %s", response.Type, requesterUid)

	message, err := s.encode(response)
	if err != nil {
		return err
	}
	return s.redisClient.Client.SPublish(s.redisClient.Context, s.channel+string(requesterUid)+"#", message).Err()
}

func (s *shardedRedisAdapter) encode(message *adapter.ClusterMessage) ([]byte, error) {
	mayContainBinary := message.Type == adapter.BROADCAST ||
		message.Type == adapter.BROADCAST_ACK ||
		message.Type == adapter.FETCH_SOCKETS_RESPONSE ||
		message.Type == adapter.SERVER_SIDE_EMIT ||
		message.Type == adapter.SERVER_SIDE_EMIT_RESPONSE
	if mayContainBinary && parser.HasBinary(message.Data) {
		return utils.MsgPack().Encode(message)
	} else {
		return json.Marshal(message)
	}
}

func (s *shardedRedisAdapter) onRawMessage(rawMessage []byte, channel string) {
	var message *adapter.ClusterResponse

	if rawMessage[0] == 0x7b {
		var rawMsg struct {
			Uid  adapter.ServerId    `json:"uid,omitempty" msgpack:"uid,omitempty"`
			Nsp  string              `json:"nsp,omitempty" msgpack:"nsp,omitempty"`
			Type adapter.MessageType `json:"type,omitempty" msgpack:"type,omitempty"`
			Data json.RawMessage     `json:"data,omitempty" msgpack:"data,omitempty"` // Data will hold the specific message data for different types
		}
		if err := json.Unmarshal(rawMessage, &rawMsg); err != nil {
			redis_log.Debug("invalid JSON format: %s", err.Error())
			return
		}

		message = &adapter.ClusterResponse{
			Uid:  rawMsg.Uid,
			Nsp:  rawMsg.Nsp,
			Type: rawMsg.Type,
		}

		if data, err := s.decodeData(rawMsg.Type, rawMsg.Data); err != nil {
			redis_log.Debug("invalid data format: %s", err.Error())
			return
		} else {
			message.Data = data
		}
	} else {
		var rawMsg struct {
			Uid  adapter.ServerId    `json:"uid,omitempty" msgpack:"uid,omitempty"`
			Nsp  string              `json:"nsp,omitempty" msgpack:"nsp,omitempty"`
			Type adapter.MessageType `json:"type,omitempty" msgpack:"type,omitempty"`
			Data msgpack.RawMessage  `json:"data,omitempty" msgpack:"data,omitempty"` // Data will hold the specific message data for different types
		}
		if err := utils.MsgPack().Decode(rawMessage, &rawMsg); err != nil {
			redis_log.Debug("invalid MessagePack format: %s", err.Error())
			return
		}

		message = &adapter.ClusterResponse{
			Uid:  rawMsg.Uid,
			Nsp:  rawMsg.Nsp,
			Type: rawMsg.Type,
		}

		if data, err := s.decodeData(rawMsg.Type, rawMsg.Data); err != nil {
			redis_log.Debug("invalid data format: %s", err.Error())
			return
		} else {
			message.Data = data
		}
	}

	if channel == s.responseChannel {
		s.OnResponse(message)
	} else {
		s.OnMessage(message, "")
	}
}

func (s *shardedRedisAdapter) decodeData(messageType adapter.MessageType, rawData any) (any, error) {
	var target any

	switch messageType {
	case adapter.INITIAL_HEARTBEAT, adapter.HEARTBEAT, adapter.ADAPTER_CLOSE:
		return nil, nil
	case adapter.BROADCAST:
		target = &adapter.BroadcastMessage{}
	case adapter.SOCKETS_JOIN, adapter.SOCKETS_LEAVE:
		target = &adapter.SocketsJoinLeaveMessage{}
	case adapter.DISCONNECT_SOCKETS:
		target = &adapter.DisconnectSocketsMessage{}
	case adapter.FETCH_SOCKETS:
		target = &adapter.FetchSocketsMessage{}
	case adapter.FETCH_SOCKETS_RESPONSE:
		target = &adapter.FetchSocketsResponse{}
	case adapter.SERVER_SIDE_EMIT:
		target = &adapter.ServerSideEmitMessage{}
	case adapter.SERVER_SIDE_EMIT_RESPONSE:
		target = &adapter.ServerSideEmitResponse{}
	case adapter.BROADCAST_CLIENT_COUNT:
		target = &adapter.BroadcastClientCount{}
	case adapter.BROADCAST_ACK:
		target = &adapter.BroadcastAck{}
	default:
		return nil, fmt.Errorf("unknown message type: %v", messageType)
	}

	switch raw := rawData.(type) {
	case json.RawMessage:
		if err := json.Unmarshal(raw, &target); err != nil {
			return nil, err
		}
	case msgpack.RawMessage:
		if err := utils.MsgPack().Decode(raw, &target); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported data format")
	}

	return target, nil
}

func (s *shardedRedisAdapter) ServerCount() int64 {
	result, err := s.redisClient.Client.PubSubShardNumSub(s.redisClient.Context, s.channel).Result()
	if err != nil {
		s.redisClient.Emit("error", err)
		return 0
	}

	if count, ok := result[s.channel]; ok {
		return count
	}
	return 0
}

func (s *shardedRedisAdapter) shouldUseASeparateNamespace(room socket.Room) bool {
	_, isPrivateRoom := s.Sids().Load(socket.SocketId(room))

	return (s.opts.SubscriptionMode() == DynamicSubscriptionMode && !isPrivateRoom) ||
		s.opts.SubscriptionMode() == DynamicPrivateSubscriptionMode
}
