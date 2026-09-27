package main

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchesFor(partitions map[int32]struct {
	hwm       int64
	lastOff   int64
	hasRecord bool
}) kgo.Fetches {
	parts := make([]kgo.FetchPartition, 0, len(partitions))
	for p, v := range partitions {
		fp := kgo.FetchPartition{Partition: p, HighWatermark: v.hwm}
		if v.hasRecord {
			fp.Records = []*kgo.Record{{Offset: v.lastOff}}
		}
		parts = append(parts, fp)
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "pm.raw", Partitions: parts}}}}
}

// TestPartitionLagAccumulatesAcrossPolls proves the fix for the bug where
// processor_consume_lag was overwritten from only the partitions present in
// one poll, silently discarding whatever the other partitions had reported.
// A poll that only touches partition 0 must not erase partition 1's
// previously-observed backlog from the reported total.
func TestPartitionLagAccumulatesAcrossPolls(t *testing.T) {
	pl := newPartitionLag()

	// Poll 1: partition 0 is 100 records behind, partition 1 is caught up.
	pl.update(fetchesFor(map[int32]struct {
		hwm       int64
		lastOff   int64
		hasRecord bool
	}{
		0: {hwm: 150, lastOff: 49, hasRecord: true}, // lag = 150 - 50 = 100
		1: {hwm: 10, lastOff: 9, hasRecord: true},   // lag = 10 - 10 = 0
	}))
	if got := sumLag(pl); got != 100 {
		t.Fatalf("lag after poll 1 = %d, want 100", got)
	}

	// Poll 2: only partition 1 has new records this round (partition 0 sits
	// idle, e.g. its consumer is still catching up / momentarily quiet). The
	// old buggy version reset the gauge to just partition 1's lag (0),
	// making a real backlog on partition 0 disappear. The fix must keep
	// reporting partition 0's last known 100.
	pl.update(fetchesFor(map[int32]struct {
		hwm       int64
		lastOff   int64
		hasRecord bool
	}{
		1: {hwm: 20, lastOff: 19, hasRecord: true}, // still caught up
	}))
	if got := sumLag(pl); got != 100 {
		t.Fatalf("lag after poll 2 (partition 0 absent this round) = %d, want 100 (must not drop to 0)", got)
	}

	// Poll 3: partition 0 catches up.
	pl.update(fetchesFor(map[int32]struct {
		hwm       int64
		lastOff   int64
		hasRecord bool
	}{
		0: {hwm: 150, lastOff: 149, hasRecord: true}, // lag = 0
	}))
	if got := sumLag(pl); got != 0 {
		t.Fatalf("lag after poll 3 = %d, want 0", got)
	}
}

func sumLag(pl *partitionLag) int64 {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	var total int64
	for _, l := range pl.lag {
		total += l
	}
	return total
}
