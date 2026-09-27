// Command loadgen-clients is a WebSocket fan-out load generator
// (design-plan.md section 4.5): it opens many connections to a gateway,
// subscribes each to a handful of assets (discovered from the gateway's
// GET /markets), and measures client-perceived end-to-end latency from each
// Tick's rts (RecvTS) field. It reports real, measured numbers on exit or
// SIGINT - never estimates.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/shutdown"
	"github.com/nawat-john/oddspulse/internal/wsproto"
)

func main() {
	wsURL := flag.String("url", "ws://localhost:8080/ws", "gateway WebSocket URL")
	conns := flag.Int("conns", 100, "number of concurrent WebSocket connections")
	subsPerConn := flag.Int("subs-per-conn", 20, "asset subscriptions per connection")
	duration := flag.Duration("duration", 30*time.Second, "how long to run before reporting and exiting")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "loadgen-clients")

	sigCtx, stop := shutdown.NewContext()
	defer stop()
	ctx, cancel := context.WithTimeout(sigCtx, *duration)
	defer cancel()

	assetIDs, err := fetchAssetIDs(*wsURL)
	if err != nil {
		log.Warn("fetch /markets failed, running with top/alerts/sys subscriptions only", "error", err)
	} else {
		log.Info("discovered assets", "count", len(assetIDs))
	}

	results := make([]connResult, *conns)
	var wg sync.WaitGroup
	var connected atomic.Int64
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = runConn(ctx, *wsURL, pickAssets(assetIDs, *subsPerConn, i), &connected)
		}(i)
	}

	log.Info("running", "conns", *conns, "subs_per_conn", *subsPerConn, "duration", *duration, "url", *wsURL)
	wg.Wait()

	report(*conns, *duration, results)
}

// connResult is what one connection measured over its lifetime.
type connResult struct {
	connected   bool
	err         error
	msgs        int64
	latenciesMs []float64
}

func runConn(ctx context.Context, wsURL string, assets []string, connected *atomic.Int64) connResult {
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	conn, resp, err := websocket.Dial(dialCtx, wsURL, nil)
	dialCancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return connResult{err: err}
	}
	defer func() { _ = conn.CloseNow() }()
	connected.Add(1)

	// Drain the "hello" greeting (best-effort - a slow/unavailable server
	// just means this read times out and we move on).
	helloCtx, helloCancel := context.WithTimeout(ctx, 5*time.Second)
	_, _, _ = conn.Read(helloCtx)
	helloCancel()

	subs := []wsproto.ClientMsg{
		{Op: wsproto.OpSub, Ch: wsproto.ChTop},
		{Op: wsproto.OpSub, Ch: wsproto.ChAlerts},
	}
	if len(assets) > 0 {
		subs = append(subs, wsproto.ClientMsg{Op: wsproto.OpSub, Ch: wsproto.ChAsset, IDs: assets})
	}
	for _, s := range subs {
		b, _ := json.Marshal(s)
		writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
		err := conn.Write(writeCtx, websocket.MessageText, b)
		writeCancel()
		if err != nil {
			return connResult{connected: true, err: err}
		}
	}

	res := connResult{connected: true}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break // ctx done (duration elapsed / SIGINT) or the connection dropped
		}
		var m wsproto.ServerMsg
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		if m.T != wsproto.TypeSnap && m.T != wsproto.TypeTicks {
			continue
		}
		var ticks []model.Tick
		if json.Unmarshal(m.D, &ticks) != nil {
			continue
		}
		now := time.Now().UnixMilli()
		for _, t := range ticks {
			res.msgs++
			// Only "ticks" (a fresh live push) measures pipeline e2e latency.
			// "snap" replays whatever price was last cached for that asset,
			// which can be arbitrarily old (design-plan.md section 4.3's
			// snapshot-on-subscribe) - including it here would conflate
			// "how stale is this asset's last update" with "how fast does
			// the pipeline deliver a fresh event", so it is excluded.
			if m.T == wsproto.TypeTicks && t.RecvTS > 0 {
				if lat := float64(now - t.RecvTS); lat >= 0 {
					res.latenciesMs = append(res.latenciesMs, lat)
				}
			}
		}
	}
	return res
}

// pickAssets returns n asset ids from pool for connection index i, wrapping
// around (and overlapping across connections) if pool is smaller than
// conns*n - deliberately: many clients sharing an asset also exercises
// design-plan.md section 9's "hot market" fan-out scenario.
func pickAssets(pool []string, n, i int) []string {
	if len(pool) == 0 || n <= 0 {
		return nil
	}
	out := make([]string, n)
	for j := 0; j < n; j++ {
		out[j] = pool[(i*n+j)%len(pool)]
	}
	return out
}

func fetchAssetIDs(wsURL string) ([]string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	}
	u.Path = "/markets"

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var markets []model.Market
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		return nil, err
	}
	var ids []string
	for _, m := range markets {
		ids = append(ids, m.ClobTokenIDs...)
	}
	return ids, nil
}

func report(attempted int, duration time.Duration, results []connResult) {
	var connectedN, failedN int
	var totalMsgs int64
	var allLatencies []float64
	var firstErr error
	for _, r := range results {
		if r.connected {
			connectedN++
		} else {
			failedN++
			if firstErr == nil && r.err != nil {
				firstErr = r.err
			}
		}
		totalMsgs += r.msgs
		allLatencies = append(allLatencies, r.latenciesMs...)
	}

	sort.Float64s(allLatencies)
	p50 := percentile(allLatencies, 0.50)
	p99 := percentile(allLatencies, 0.99)
	msgsPerSec := float64(totalMsgs) / duration.Seconds()

	fmt.Printf("\n=== loadgen-clients report ===\n")
	fmt.Printf("connections attempted:  %d\n", attempted)
	fmt.Printf("connections established: %d (%.1f%%)\n", connectedN, 100*float64(connectedN)/float64(attempted))
	fmt.Printf("connections failed:     %d\n", failedN)
	if firstErr != nil {
		fmt.Printf("first error:            %v\n", firstErr)
	}
	fmt.Printf("duration:               %s\n", duration)
	fmt.Printf("messages received:      %d\n", totalMsgs)
	fmt.Printf("latency samples:        %d\n", len(allLatencies))
	fmt.Printf("throughput:             %.1f msgs/s\n", msgsPerSec)
	fmt.Printf("latency p50:            %.1f ms\n", p50)
	fmt.Printf("latency p99:            %.1f ms\n", p99)
}

// percentile returns the nearest-rank percentile p (0..1) of sorted, or 0 if
// sorted is empty.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted)) * p)
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
