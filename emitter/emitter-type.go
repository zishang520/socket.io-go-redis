package emitter

import (
	"github.com/wuchieh/socket.io-go-redis/types"
)

type (
	BroadcastOptions struct {
		Nsp              string
		BroadcastChannel string
		RequestChannel   string
		Parser           types.Parser
	}

	Packet = types.RedisPacket

	Request = types.RedisRequest

	Response = types.RedisResponse
)
