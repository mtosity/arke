package metrics

import "context"

type QueueStats struct {
	// Name of the vhost the queue belongs to
	VHost string
	// Name of the queue
	Queue string
	// Number of consumers subscribed to the queue
	ConsumersCount int
	// Sum of ready and unacknowledged messages - total queue depth
	TotalMessagesCount int
	// Number of messages delivered to consumers but not yet acknowledged
	MessagesUnackCount int
	// Number of messages ready to be delivered to consumers
	MessagesReadyCount int
	// Highest stream consumer offset lag (how slow is the slowest stream consumer)
	ConsumerOffsetLag int
}

type BrokerMetricsProvider interface {
	// Returns a *QueueStats for the specified queue
	GetQueueStats(ctx context.Context, queue string) (*QueueStats, error)
}
