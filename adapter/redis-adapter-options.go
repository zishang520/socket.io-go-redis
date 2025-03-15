package adapter

import (
	"time"

	"github.com/zishang520/socket.io-go-redis/emitter"
)

type (
	RedisAdapterOptionsInterface interface {
		emitter.EmitterOptionsInterface

		SetRequestsTimeout(time.Duration)
		GetRawRequestsTimeout() *time.Duration
		RequestsTimeout() time.Duration

		SetPublishOnSpecificResponseChannel(bool)
		GetRawPublishOnSpecificResponseChannel() *bool
		PublishOnSpecificResponseChannel() bool
	}

	RedisAdapterOptions struct {
		emitter.EmitterOptions

		// after this timeout the adapter will stop waiting from responses to request
		// Default: 5000 * time.Millisecond
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
	}
)

func DefaultRedisAdapterOptions() *RedisAdapterOptions {
	return &RedisAdapterOptions{}
}

func (s *RedisAdapterOptions) Assign(data RedisAdapterOptionsInterface) (RedisAdapterOptionsInterface, error) {
	if data == nil {
		return s, nil
	}
	if data.GetRawKey() != nil {
		s.SetKey(data.Key())
	}
	if data.GetRawRequestsTimeout() != nil {
		s.SetRequestsTimeout(data.RequestsTimeout())
	}
	if data.GetRawPublishOnSpecificResponseChannel() != nil {
		s.SetPublishOnSpecificResponseChannel(data.PublishOnSpecificResponseChannel())
	}
	if data.GetRawParser() != nil {
		s.SetParser(data.Parser())
	}

	return s, nil
}

func (s *RedisAdapterOptions) SetRequestsTimeout(requestsTimeout time.Duration) {
	s.requestsTimeout = &requestsTimeout
}
func (s *RedisAdapterOptions) GetRawRequestsTimeout() *time.Duration {
	return s.requestsTimeout
}
func (s *RedisAdapterOptions) RequestsTimeout() time.Duration {
	if s.requestsTimeout == nil {
		return 5000 * time.Millisecond
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
