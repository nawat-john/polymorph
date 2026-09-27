package hub

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is an in-memory hub.Conn for tests: no network, no coder/websocket
// dependency. If block is non-nil, Write hangs until unblock() is called (or
// its context is done) - used to simulate a stuck/slow client for the
// eviction test.
type fakeConn struct {
	mu     sync.Mutex
	frames [][]byte

	closed atomic.Bool
	code   atomic.Int32
	reason atomic.Value // string

	block    chan struct{}
	writeErr error // if set, Write always fails with this error instead of succeeding
}

func newFakeConn() *fakeConn { return &fakeConn{} }

func newBlockingConn() *fakeConn { return &fakeConn{block: make(chan struct{})} }

// newFailingConn simulates a connection whose write fails immediately (e.g.
// a timed-out or broken socket), for testing the write-error eviction path
// (as opposed to newBlockingConn's queue-full path).
func newFailingConn(err error) *fakeConn { return &fakeConn{writeErr: err} }

func (f *fakeConn) unblock() {
	if f.block == nil {
		return
	}
	select {
	case <-f.block:
	default:
		close(f.block)
	}
}

func (f *fakeConn) Write(ctx context.Context, data []byte) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	cp := append([]byte(nil), data...)
	f.mu.Lock()
	f.frames = append(f.frames, cp)
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) Close(code int, reason string) error {
	f.closed.Store(true)
	f.code.Store(int32(code))
	f.reason.Store(reason)
	return nil
}

func (f *fakeConn) framesSnapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.frames))
	copy(out, f.frames)
	return out
}

// waitFrames polls until at least n frames have been written, or fails the
// test after timeout. A client's outbound channel is drained by its own
// writer goroutine asynchronously, so tests that just called Subscribe/
// Publish*/flushAll need to give it a moment to actually perform the write.
func (f *fakeConn) waitFrames(t *testing.T, n int, timeout time.Duration) [][]byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := f.framesSnapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d frames, got %d", n, len(got))
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeMetrics is a no-op-by-default hub.Metrics that records the counters
// tests care about.
type fakeMetrics struct {
	clients       atomic.Int64
	subscriptions atomic.Int64
	messagesOut   atomic.Int64
	bytesOut      atomic.Int64
	evictions     atomic.Int64
	queueDepth    atomic.Int64
}

func (m *fakeMetrics) SetClients(n int)         { m.clients.Store(int64(n)) }
func (m *fakeMetrics) SetSubscriptions(n int64) { m.subscriptions.Store(n) }
func (m *fakeMetrics) AddMessagesOut(n int)     { m.messagesOut.Add(int64(n)) }
func (m *fakeMetrics) AddBytesOut(n int)        { m.bytesOut.Add(int64(n)) }
func (m *fakeMetrics) IncSlowClientEvictions()  { m.evictions.Add(1) }
func (m *fakeMetrics) SetQueueDepth(n int)      { m.queueDepth.Store(int64(n)) }
