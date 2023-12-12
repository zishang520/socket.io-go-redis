package adapter

type (
	subMode struct {
		name string
	}

	ShardedRedisAdapterOptionsInterface interface {
		SetChannelPrefix(string)
		GetRawChannelPrefix() *string
		ChannelPrefix() string

		SetSubscriptionMode(subMode)
		GetRawSubscriptionMode() *subMode
		SubscriptionMode() subMode
	}

	ShardedRedisAdapterOptions struct {
		// The prefix for the Redis Pub/Sub channels.
		//
		// Default: "socket.io"
		channelPrefix *string

		// The subscription mode impacts the number of Redis Pub/Sub channels:
		//
		// - StaticSubscriptionMode: 2 channels per namespace
		//
		// Useful when used with dynamic namespaces.
		//
		// - DynamicSubscriptionMode: (2 + 1 per public room) channels per namespace
		//
		// The default value, useful when some rooms have a low number of clients (so only a few Socket.IO servers are notified).
		//
		// Only public rooms (i.e. not related to a particular Socket ID) are taken in account, because:
		//
		// - a lot of connected clients would mean a lot of subscription/unsubscription
		// - the Socket ID attribute is ephemeral
		//
		// Default: DynamicSubscriptionMode
		subscriptionMode *subMode
	}
)

var (
	StaticSubscriptionMode  subMode = subMode{"static"}
	DynamicSubscriptionMode subMode = subMode{"dynamic"}
)

func DefaultShardedRedisAdapterOptions() *ShardedRedisAdapterOptions {
	return &ShardedRedisAdapterOptions{}
}

func (s *ShardedRedisAdapterOptions) Assign(data ShardedRedisAdapterOptionsInterface) (ShardedRedisAdapterOptionsInterface, error) {
	if data == nil {
		return s, nil
	}
	if s.GetRawChannelPrefix() == nil {
		s.SetChannelPrefix(data.ChannelPrefix())
	}

	if s.GetRawSubscriptionMode() == nil {
		s.SetSubscriptionMode(data.SubscriptionMode())
	}

	return s, nil
}

func (s *ShardedRedisAdapterOptions) SetChannelPrefix(channelPrefix string) {
	s.channelPrefix = &channelPrefix
}
func (s *ShardedRedisAdapterOptions) GetRawChannelPrefix() *string {
	return s.channelPrefix
}
func (s *ShardedRedisAdapterOptions) ChannelPrefix() string {
	if s.channelPrefix == nil {
		return "socket.io"
	}

	return *s.channelPrefix
}

func (s *ShardedRedisAdapterOptions) SetSubscriptionMode(subMode subMode) {
	s.subscriptionMode = &subMode
}
func (s *ShardedRedisAdapterOptions) GetRawSubscriptionMode() *subMode {
	return s.subscriptionMode
}
func (s *ShardedRedisAdapterOptions) SubscriptionMode() subMode {
	if s.subscriptionMode == nil {
		return DynamicSubscriptionMode
	}

	return *s.subscriptionMode
}
