// Package kafka is a thin wrapper over franz-go's producer and consumer
// clients (design-plan.md sections 4.1, 12), reused across services.
package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducer returns a franz-go client configured per design-plan.md 4.1:
// acks=all, ~5ms linger batching, zstd compression (falling back to lz4,
// then none, if the broker does not support it).
func NewProducer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
	)
}

// NewConsumer returns a franz-go client consuming topics as consumer group
// groupID, for reuse by the processor/gateway/recorder. extra opts are
// appended last so callers can override defaults (e.g. the processor
// disables auto-commit for its at-least-once produce-then-commit loop, see
// design-plan.md section 4.2).
func NewConsumer(brokers []string, groupID string, topics []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
	}
	return kgo.NewClient(append(base, opts...)...)
}

// Produce produces one record without blocking the caller; onDone (optional)
// receives the eventual error, if any. Design-plan.md 4.1: never block the
// WS read loop on Kafka.
func Produce(ctx context.Context, cl *kgo.Client, topic string, key, value []byte, onDone func(error)) {
	cl.Produce(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}, func(_ *kgo.Record, err error) {
		if onDone != nil {
			onDone(err)
		}
	})
}
