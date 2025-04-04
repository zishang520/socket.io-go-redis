package adapter

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/zishang520/engine.io/v2/log"
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	"github.com/zishang520/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/adapter"
	"github.com/zishang520/socket.io/v2/socket"
)

var (
	redis_streams_log = log.NewLog("socket.io-redis")
	// Precompile the regular expression once
	offsetRegex = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
)

const RESTORE_SESSION_MAX_XRANGE_CALLS = 100

// Returns a function that will create a new adapter instance.
type RedisStreamsAdapterBuilder struct {
	// the Redis client used to publish/subscribe
	Redis *types.RedisClient
	// some additional options
	Opts RedisStreamsAdapterOptionsInterface

	namespaceToAdapters types.Map[string, RedisStreamsAdapter]
	offset              types.String // Default:"$"
	polling             atomic.Bool  // Default:false
	shouldClose         atomic.Bool  // Default:false
}

func (sb *RedisStreamsAdapterBuilder) poll(options RedisStreamsAdapterOptionsInterface) {
	for {
		if sb.shouldClose.Load() || sb.namespaceToAdapters.Len() == 0 {
			sb.polling.Store(false)
			return
		}

		offset := sb.offset.Load()
		if offset == "" {
			offset = "$"
		}
		response, err := sb.Redis.Client.XRead(sb.Redis.Context, &redis.XReadArgs{
			Streams: []string{options.StreamName()},
			ID:      offset,
			Count:   options.ReadCount(),
			Block:   5000 * time.Millisecond,
		}).Result()

		if err != nil {
			redis_streams_log.Debug("something went wrong while consuming the stream: %s", err.Error())
			continue
		}

		if len(response) > 0 {
			for _, entry := range response[0].Messages {
				redis_streams_log.Debug("reading entry %s", entry.ID)

				if message := RawClusterMessage(entry.Values); message.Nsp() != "" {
					if adapter, exists := sb.namespaceToAdapters.Load(message.Nsp()); exists {
						adapter.OnRawMessage(message, entry.ID)
					}
				}

				sb.offset.Store(entry.ID)
			}
		}
	}
}

func (sb *RedisStreamsAdapterBuilder) New(nsp socket.Namespace) socket.Adapter {
	options := DefaultRedisStreamsAdapterOptions().Assign(sb.Opts)

	if options.GetRawStreamName() == nil {
		options.SetStreamName("socket.io")
	}
	if options.GetRawMaxLen() == nil {
		options.SetMaxLen(10_000)
	}
	if options.GetRawReadCount() == nil {
		options.SetReadCount(100)
	}
	if options.GetRawSessionKeyPrefix() == nil {
		options.SetSessionKeyPrefix("sio:session:")
	}
	if options.GetRawHeartbeatInterval() == nil {
		options.SetHeartbeatInterval(5_000)
	}
	if options.GetRawHeartbeatTimeout() == nil {
		options.SetHeartbeatTimeout(10_000)
	}

	adapter := NewRedisStreamsAdapter(nsp, sb.Redis, options)
	sb.namespaceToAdapters.Store(nsp.Name(), adapter)

	if !sb.polling.CompareAndSwap(false, true) {
		sb.shouldClose.Store(false)
		go sb.poll(options)
	}

	adapter.Cleanup(func() {
		sb.namespaceToAdapters.Delete(nsp.Name())
		if sb.namespaceToAdapters.Len() == 0 {
			sb.shouldClose.Store(true)
		}
	})

	return adapter
}

type redisStreamsAdapter struct {
	adapter.ClusterAdapterWithHeartbeat

	redisClient *types.RedisClient
	opts        *RedisStreamsAdapterOptions

	_cleanup types.Callable
}

func MakeRedisStreamsAdapter() RedisStreamsAdapter {
	c := &redisStreamsAdapter{
		ClusterAdapterWithHeartbeat: adapter.MakeClusterAdapterWithHeartbeat(),

		opts: DefaultRedisStreamsAdapterOptions(),

		_cleanup: nil,
	}

	c.Prototype(c)

	return c
}

func NewRedisStreamsAdapter(nsp socket.Namespace, redis *types.RedisClient, opts any) RedisStreamsAdapter {
	c := MakeRedisStreamsAdapter()

	c.SetRedis(redis)
	c.SetOpts(opts)

	c.Construct(nsp)

	return c
}

func (r *redisStreamsAdapter) SetRedis(redisClient *types.RedisClient) {
	r.redisClient = redisClient
}

func (r *redisStreamsAdapter) SetOpts(opts any) {
	r.ClusterAdapterWithHeartbeat.SetOpts(opts)

	if options, ok := opts.(RedisStreamsAdapterOptionsInterface); ok {
		r.opts.Assign(options)
	}
}

func (r *redisStreamsAdapter) Construct(nsp socket.Namespace) {
	r.ClusterAdapterWithHeartbeat.Construct(nsp)

	r.Init()
}

func (r *redisStreamsAdapter) DoPublish(message *adapter.ClusterMessage) (adapter.Offset, error) {
	redis_streams_log.Debug("publishing %+v", message)

	return "", r.redisClient.Client.XAdd(r.redisClient.Context, &redis.XAddArgs{
		Stream: r.opts.StreamName(),
		MaxLen: r.opts.MaxLen(),
		Approx: true, // "~" in Redis is approximated trimming.
		ID:     "*",
		// XAddArgs accepts values in the following formats:
		//   - XAddArgs.Values = []interface{}{"key1", "value1", "key2", "value2"}
		//   - XAddArgs.Values = []string("key1", "value1", "key2", "value2")
		//   - XAddArgs.Values = map[string]interface{}{"key1": "value1", "key2": "value2"}
		Values: map[string]any(r.encode(message)),
	}).Err()
}

func (r *redisStreamsAdapter) DoPublishResponse(requesterUid adapter.ServerId, response *adapter.ClusterResponse) error {
	_, err := r.DoPublish(response)
	return err
}

func (redisStreamsAdapter) encode(message *adapter.ClusterResponse) RawClusterMessage {
	rawMessage := RawClusterMessage{
		"uid":  fmt.Sprintf("%s", message.Uid),
		"nsp":  message.Nsp,
		"type": fmt.Sprintf("%d", message.Type),
	}

	if message.Data != nil {
		mayContainBinary := message.Type == adapter.BROADCAST ||
			message.Type == adapter.FETCH_SOCKETS_RESPONSE ||
			message.Type == adapter.SERVER_SIDE_EMIT ||
			message.Type == adapter.SERVER_SIDE_EMIT_RESPONSE ||
			message.Type == adapter.BROADCAST_ACK

		if mayContainBinary && parser.HasBinary(message.Data) {
			if data, err := utils.MsgPack().Encode(message.Data); err == nil {
				rawMessage["data"] = base64.StdEncoding.EncodeToString(data)
			}
		} else {
			if data, err := json.Marshal(message.Data); err == nil {
				rawMessage["data"] = string(data)
			}
		}
	}

	return rawMessage
}

func (r *redisStreamsAdapter) Cleanup(cleanup func()) {
	r._cleanup = cleanup
}

func (r *redisStreamsAdapter) Close() {
	defer r.ClusterAdapterWithHeartbeat.Close()

	if r._cleanup != nil {
		r._cleanup()
	}
}

func (r *redisStreamsAdapter) OnRawMessage(rawMessage RawClusterMessage, offset string) error {
	message, err := r.decode(rawMessage)
	if err != nil {
		return err
	}
	r.OnMessage(message, adapter.Offset(offset))
	return nil
}

func (r *redisStreamsAdapter) decode(rawMessage RawClusterMessage) (*adapter.ClusterResponse, error) {
	// Parse the message type
	tp, err := strconv.ParseInt(rawMessage.Type(), 10, 0)
	if err != nil {
		return nil, err
	}

	// Initialize the base message
	message := &adapter.ClusterMessage{
		Uid:  adapter.ServerId(rawMessage.Uid()),
		Nsp:  rawMessage.Nsp(),
		Type: adapter.MessageType(tp),
	}

	// No need to process data if it's empty
	data := rawMessage.Data()
	if data == "" {
		return message, nil
	}

	// Determine format and decode data accordingly
	var rawData any
	if data[0] == '{' {
		rawData = json.RawMessage(data)
	} else {
		data, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, err
		}
		rawData = msgpack.RawMessage(data)
	}

	// Decode message data based on the message type
	message.Data, err = r.decodeData(message.Type, rawData)
	if err != nil {
		return nil, err
	}

	return message, nil
}

func (r *redisStreamsAdapter) decodeData(messageType adapter.MessageType, rawData any) (any, error) {
	// Pre-allocate the target based on message type
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

	// Decode data based on the format
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
		return nil, errors.New("unsupported data format")
	}

	return target, nil
}

func (r *redisStreamsAdapter) PersistSession(session *socket.SessionToPersist) {
	redis_streams_log.Debug("persisting session %v", session)
	sessionKey := r.opts.SessionKeyPrefix() + string(session.Pid)
	data, err := utils.MsgPack().Encode(session)
	if err != nil {
		return
	}

	if err := r.redisClient.Client.Set(
		r.redisClient.Context,
		sessionKey,
		base64.StdEncoding.EncodeToString(data),
		time.Duration(r.Nsp().Server().Opts().ConnectionStateRecovery().MaxDisconnectionDuration())*time.Millisecond,
	).Err(); err != nil {
		r.redisClient.Emit("error", err)
	}
}

func (r *redisStreamsAdapter) RestoreSession(pid socket.PrivateSessionId, offset string) (*socket.Session, error) {
	redis_streams_log.Debug("restoring session %s from offset %s", pid, offset)

	// Reuse the precompiled regex
	if !offsetRegex.MatchString(offset) {
		return nil, errors.New("invalid offset")
	}

	sessionKey := r.opts.SessionKeyPrefix() + string(pid)

	rawSession, err := r.redisClient.Client.GetDel(r.redisClient.Context, sessionKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if rawSession == "" {
		return nil, errors.New("session not found")
	}

	offsets, err := r.redisClient.Client.XRange(r.redisClient.Context, r.opts.StreamName(), offset, offset).Result()
	if err != nil {
		return nil, err
	}

	if len(offsets) == 0 {
		return nil, errors.New("offset not found")
	}

	raw, err := base64.StdEncoding.DecodeString(rawSession)
	if err != nil {
		return nil, err
	}

	session := &socket.Session{}

	if err := utils.MsgPack().Decode(raw, &session.SessionToPersist); err != nil {
		return nil, err
	}

	redis_streams_log.Debug("found session %+v", session)

	// Loop over the stream entries and process them
	for i := 0; i < RESTORE_SESSION_MAX_XRANGE_CALLS; i++ {
		entries, err := r.redisClient.Client.XRange(r.redisClient.Context, r.opts.StreamName(), r.nextOffset(offset), "+").Result()
		if err != nil || len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			if rawClusterMessage := RawClusterMessage(entry.Values); rawClusterMessage.Nsp() == r.Nsp().Name() && rawClusterMessage.Type() == fmt.Sprintf("%d", adapter.BROADCAST) {
				if message, err := r.decode(rawClusterMessage); err == nil {

					if data, ok := message.Data.(*adapter.BroadcastMessage); ok && r.shouldIncludePacket(session.Rooms, data.Opts) {
						session.MissedPackets = append(session.MissedPackets, data.Packet)
					}
				}
			}
			offset = entry.ID
		}
	}

	return session, nil
}

func (redisStreamsAdapter) nextOffset(offset string) string {
	dashPos := strings.LastIndex(offset, "-")
	if dashPos == -1 {
		return offset
	}

	timestamp := offset[:dashPos]
	sequence := offset[dashPos+1:]

	if seqNum, err := strconv.ParseUint(sequence, 10, 64); err == nil {
		return timestamp + "-" + strconv.FormatUint(seqNum+1, 10)
	}

	return offset
}

func (redisStreamsAdapter) shouldIncludePacket(sessionRooms *types.Set[socket.Room], opts *adapter.PacketOptions) bool {
	included := len(opts.Rooms) == 0
	if !included {
		for _, room := range opts.Rooms {
			if sessionRooms.Has(room) {
				included = true
				break
			}
		}
	}

	notExcluded := true
	for _, room := range opts.Except {
		if sessionRooms.Has(room) {
			notExcluded = false
			break
		}
	}

	return included && notExcluded
}
