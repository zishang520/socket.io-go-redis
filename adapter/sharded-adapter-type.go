package adapter

import "github.com/zishang520/socket.io-go-redis/types"

type ShardedRedisAdapter interface {
	ClusterAdapter

	SetRedis(*types.RedisClient)
	SetOpts(*ShardedRedisAdapterOptions)
}
