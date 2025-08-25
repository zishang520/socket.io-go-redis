package adapter

import (
	"github.com/wuchieh/socket.io-go-redis/types"
	"github.com/zishang520/socket.io/v2/adapter"
)

type ShardedRedisAdapter interface {
	adapter.ClusterAdapter

	SetRedis(*types.RedisClient)
	SetOpts(any)
}
