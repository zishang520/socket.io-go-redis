package adapter

import (
	"time"

	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-redis/types"
)

type (
	RedisAdapterOptionsInterface interface {
		SetKey(string)
		GetRawKey() *string
		Key() string

		SetRequestsTimeout(time.Duration)
		GetRawRequestsTimeout() *time.Duration
		RequestsTimeout() time.Duration

		SetPublishOnSpecificResponseChannel(bool)
		GetRawPublishOnSpecificResponseChannel() *bool
		PublishOnSpecificResponseChannel() bool

		SetParser(types.Parser)
		GetRawParser() types.Parser
		Parser() types.Parser
	}

	RedisAdapterOptions struct {

		// the name of the key to pub/sub events on as prefix
		// Default: "socket.io"
		key *string

		// after this timeout the adapter will stop waiting from responses to request
		// Default: 5000
		requestsTimeout *time.Duration

		// Whether to publish a response to the channel specific to the requesting node.
		//
		// - if true, the response will be published to `${key}-request#${nsp}#${uid}#`
		// - if false, the response will be published to `${key}-request#${nsp}#`
		//
		// This option currently defaults to false for backward compatibility, but will be set to true in the next major
		// release.
		//
		// Default: false
		publishOnSpecificResponseChannel *bool

		// The parser to use for encoding and decoding messages sent to Redis.
		// This option defaults to using `msgpack`, a MessagePack implementation.
		parser types.Parser
	}
)

func DefaultRedisAdapterOptions() *RedisAdapterOptions {
	return &RedisAdapterOptions{}
}

func (s *RedisAdapterOptions) Assign(data RedisAdapterOptionsInterface) (RedisAdapterOptionsInterface, error) {
	if data == nil {
		return s, nil
	}
	if s.GetRawKey() == nil {
		s.SetKey(data.Key())
	}
	if s.GetRawRequestsTimeout() == nil {
		s.SetRequestsTimeout(data.RequestsTimeout())
	}
	if s.GetRawPublishOnSpecificResponseChannel() == nil {
		s.SetPublishOnSpecificResponseChannel(data.PublishOnSpecificResponseChannel())
	}
	if s.GetRawParser() == nil {
		s.SetParser(data.Parser())
	}

	return s, nil
}

func (s *RedisAdapterOptions) SetKey(key string) {
	s.key = &key
}
func (s *RedisAdapterOptions) GetRawKey() *string {
	return s.key
}
func (s *RedisAdapterOptions) Key() string {
	if s.key == nil {
		return "socket.io"
	}

	return *s.key
}

func (s *RedisAdapterOptions) SetRequestsTimeout(requestsTimeout time.Duration) {
	s.requestsTimeout = &requestsTimeout
}
func (s *RedisAdapterOptions) GetRawRequestsTimeout() *time.Duration {
	return s.requestsTimeout
}
func (s *RedisAdapterOptions) RequestsTimeout() time.Duration {
	if s.requestsTimeout == nil {
		return time.Duration(5000 * time.Millisecond)
	}

	return *s.requestsTimeout
}

func (s *RedisAdapterOptions) SetPublishOnSpecificResponseChannel(publishOnSpecificResponseChannel bool) {
	s.publishOnSpecificResponseChannel = &publishOnSpecificResponseChannel
}
func (s *RedisAdapterOptions) GetRawPublishOnSpecificResponseChannel() *bool {
	return s.publishOnSpecificResponseChannel
}
func (s *RedisAdapterOptions) PublishOnSpecificResponseChannel() bool {
	if s.publishOnSpecificResponseChannel == nil {
		return false
	}

	return *s.publishOnSpecificResponseChannel
}

func (s *RedisAdapterOptions) SetParser(parser types.Parser) {
	s.parser = parser
}
func (s *RedisAdapterOptions) GetRawParser() types.Parser {
	return s.parser
}
func (s *RedisAdapterOptions) Parser() types.Parser {
	if s.parser == nil {
		return utils.MsgPack()
	}

	return s.parser
}
