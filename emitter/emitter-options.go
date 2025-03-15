package emitter

import (
	"github.com/zishang520/engine.io/v2/utils"
	"github.com/zishang520/socket.io-go-redis/types"
)

type (
	EmitterOptionsInterface interface {
		SetKey(string)
		GetRawKey() *string
		Key() string

		SetParser(types.Parser)
		GetRawParser() types.Parser
		Parser() types.Parser
	}

	EmitterOptions struct {
		// Default: "socket.io"
		key *string

		// The parser to use for encoding messages sent to Redis.
		// Defaults to msgpack, a MessagePack implementation.
		parser types.Parser
	}
)

func DefaultEmitterOptions() *EmitterOptions {
	return &EmitterOptions{}
}

func (s *EmitterOptions) Assign(data EmitterOptionsInterface) (EmitterOptionsInterface, error) {
	if data == nil {
		return s, nil
	}

	if data.GetRawKey() != nil {
		s.SetKey(data.Key())
	}
	if data.GetRawParser() != nil {
		s.SetParser(data.Parser())
	}

	return s, nil
}

func (s *EmitterOptions) SetKey(key string) {
	s.key = &key
}
func (s *EmitterOptions) GetRawKey() *string {
	return s.key
}

// Default: "socket.io"
func (s *EmitterOptions) Key() string {
	if s.key == nil {
		return "socket.io"
	}

	return *s.key
}

func (s *EmitterOptions) SetParser(parser types.Parser) {
	s.parser = parser
}
func (s *EmitterOptions) GetRawParser() types.Parser {
	return s.parser
}

// The parser to use for encoding messages sent to Redis.
// Defaults to msgpack, a MessagePack implementation.
func (s *EmitterOptions) Parser() types.Parser {
	if s.parser == nil {
		return utils.MsgPack()
	}

	return s.parser
}
