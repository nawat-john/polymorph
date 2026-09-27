// Package kafka is a thin wrapper over franz-go's producer and consumer
// clients (design-plan.md sections 4.1, 12), reused across services.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// pollTimeout bounds each individual PollFetches call in ConsumeUpToEnd.
const pollTimeout = 500 * time.Millisecond

// ConsumeUpToEnd polls cl (already subscribed to topic via ConsumeTopics)
// and calls onRecord for every fetched record, stopping once every
// partition has been consumed up to the high watermark recorded at the
// start of the call (a partition with no records at all is skipped
// immediately). Used to bootstrap caches from compacted topics.
//
// It stops at an explicit end-offset snapshot rather than after "N idle
// polls": pm.snapshots is written continuously, and an idle heuristic kept
// getting reset by that live trickle - taking on the order of two minutes
// once the topic had a few minutes of uncompacted history.
//
// cl must not be shared with any other goroutine. cl stays open afterwards
// (the caller owns it), so no adm.Close() here: that would close cl too.
func ConsumeUpToEnd(ctx context.Context, cl *kgo.Client, topic string, onRecord func(*kgo.Record)) error {
	adm := kadm.NewClient(cl)

	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("list end offsets for %s: %w", topic, err)
	}

	target := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			target[o.Partition] = o.Offset // high watermark: next offset to be written
		}
	})

	reached := make(map[int32]bool, len(target))
	for len(reached) < len(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pctx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := cl.PollFetches(pctx)
		cancel()

		fetches.EachRecord(func(r *kgo.Record) {
			onRecord(r)
			if want, ok := target[r.Partition]; ok && r.Offset+1 >= want {
				reached[r.Partition] = true
			}
		})
	}
	return nil
}

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
