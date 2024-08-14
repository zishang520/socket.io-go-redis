package adapter

import (
	"encoding/json"

	"github.com/redis/go-redis/v9"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	_types "github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/socket"
)

// Create a new Adapter based on Redis sharded Pub/Sub introduced in Redis 7.0.
//
// See: https://redis.io/docs/manual/pubsub/#sharded-pubsub
type ShardedRedisAdapterBuilder struct {
	socket.AdapterConstructor

	// a Redis client
	Redis *_types.RedisClient
	// additional options
	Opts *ShardedRedisAdapterOptions
}

func (sb *ShardedRedisAdapterBuilder) New(nsp socket.Namespace) socket.Adapter {
	return NewShardedRedisAdapter(nsp, sb.Redis, sb.Opts)
}

type shardedRedisAdapter struct {
	ClusterAdapter

	// a Redis client that will be used to publish messages
	pubsub *redis.PubSub
	// a Redis client
	redis *_types.RedisClient

	opts            *ShardedRedisAdapterOptions
	channel         string
	responseChannel string
}

func MakeShardedRedisAdapter() ShardedRedisAdapter {
	c := &shardedRedisAdapter{
		ClusterAdapter: MakeClusterAdapter(),

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

func (s *shardedRedisAdapter) Construct(nsp socket.Namespace) {
	s.ClusterAdapter.Construct(nsp)

	s.channel = s.opts.ChannelPrefix() + "#" + nsp.Name() + "#"
	s.responseChannel = s.opts.ChannelPrefix() + "#" + nsp.Name() + "#" + s.Uid() + "#"

	s.pubsub = s.redis.SSubscribe(s.redis.Context, s.channel)

	s.pubsub.SSubscribe(s.redis.Context, s.responseChannel)

	if s.opts.SubscriptionMode() == DynamicSubscriptionMode {
		s.On("create-room", func(room ...any) {
			_, isPrivateRoom := s.Sids().Load(socket.SocketId(room[0].(socket.Room)))
			if !isPrivateRoom {
				s.pubsub.SSubscribe(s.redis.Context, s.dynamicChannel(room[0].(socket.Room)))
			}
		})
	}

	s.On("delete-room", func(room ...any) {
		_, isPrivateRoom := s.Sids().Load(socket.SocketId(room[0].(socket.Room)))
		if !isPrivateRoom {
			s.pubsub.SUnsubscribe(s.redis.Context, s.dynamicChannel(room[0].(socket.Room)))
		}
	})

	go func() {
		for {
			select {
			case <-s.redis.Context.Done():
				return
			default:
				msg, err := s.pubsub.ReceiveMessage(s.redis.Context)
				if err != nil {
					return
				}
				s.onRawMessage([]byte(msg.Payload), msg.Channel)
			}
		}
	}()
}

func (s *shardedRedisAdapter) SetRedis(redis *_types.RedisClient) {
	s.redis = redis
}

func (s *shardedRedisAdapter) SetOpts(opts *ShardedRedisAdapterOptions) {
	s.opts.Assign(opts)
}

func (s *shardedRedisAdapter) Close() {
	channels := []string{s.channel, s.responseChannel}

	if s.opts.SubscriptionMode() == DynamicSubscriptionMode {
		s.Rooms().Range(func(room socket.Room, _sids *types.Set[socket.SocketId]) bool {
			_, isPrivateRoom := s.Sids().Load(socket.SocketId(room))
			if !isPrivateRoom {
				channels = append(channels, s.dynamicChannel(room))
			}
			return true
		})
	}

	s.pubsub.SUnsubscribe(s.redis.Context, channels...)
}

func (s *shardedRedisAdapter) PublishMessage(message *ClusterMessage) (int64, error) {
	channel := s.computeChannel(message)
	redis_log.Debug("publishing message of type %v to %s", message.Type, channel)

	msg, err := s.encode(message)
	if err != nil {
		return 0, err
	}

	return s.redis.SPublish(s.redis.Context, channel, msg).Result()
}

func (s *shardedRedisAdapter) computeChannel(message *ClusterMessage) string {
	// broadcast with ack can not use a dynamic channel, because the serverCount() method return the number of all
	// servers, not only the ones where the given room exists
	useDynamicChannel := s.opts.SubscriptionMode() == DynamicSubscriptionMode &&
		message.Type == MESSAGE_BROADCAST &&
		message.Data.RequestId == "" &&
		len(message.Data.Opts.Rooms) == 1
	if useDynamicChannel {
		return s.dynamicChannel(message.Data.Opts.Rooms[0])
	} else {
		return s.channel
	}
}

func (s *shardedRedisAdapter) dynamicChannel(room socket.Room) string {
	return s.channel + string(room) + "#"
}

func (s *shardedRedisAdapter) PublishResponse(requesterUid string, response *ClusterMessage) (int64, error) {
	redis_log.Debug("publishing response of type %v to %s", response.Type, requesterUid)

	message, err := s.encode(response)
	if err != nil {
		return 0, err
	}
	return s.redis.SPublish(s.redis.Context, s.channel+requesterUid+"#", message).Result()
}

func (s *shardedRedisAdapter) encode(message *ClusterMessage) ([]byte, error) {
	mayContainBinary := message.Type == MESSAGE_BROADCAST || message.Type == MESSAGE_BROADCAST_ACK || message.Type == MESSAGE_FETCH_SOCKETS_RESPONSE || message.Type == MESSAGE_SERVER_SIDE_EMIT || message.Type == MESSAGE_SERVER_SIDE_EMIT_RESPONSE
	if mayContainBinary && parser.HasBinary(message.Data) {
		return utils.MsgPack().Encode(message)
	} else {
		return json.Marshal(message)
	}
}

func (s *shardedRedisAdapter) onRawMessage(rawMessage []byte, channel string) {
	var message *ClusterMessage

	if rawMessage[0] == 0x7b {
		if err := json.Unmarshal(rawMessage, &message); err != nil {
			redis_log.Debug("invalid format: %s", err.Error())
			return
		}
	} else {
		if err := utils.MsgPack().Decode(rawMessage, &message); err != nil {
			redis_log.Debug("invalid format: %s", err.Error())
			return
		}
	}

	if channel == s.responseChannel {
		s.OnResponse(message)
	} else {
		s.OnMessage(message, 0)
	}
}

func (s *shardedRedisAdapter) ServerCount() int64 {
	if result, err := s.redis.PubSubShardNumSub(s.redis.Context, s.channel).Result(); err != nil {
		if r, ok := result[s.channel]; ok {
			return r
		}
	}
	return 0
}
