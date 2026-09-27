// Command loadgen-producer is a synthetic RawEvent generator
// (design-plan.md section 4.5): it simulates --assets independent
// per-asset random walks and produces them to pm.raw at a target --rate
// events/sec, so the ingest pipeline's throughput can be proven without
// depending on real Polymarket volume (design-plan.md section 9, scenario
// S1). --surge-prob optionally injects large price jumps to exercise the
// processor's surge/alert path under load.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nawat-john/oddspulse/internal/kafka"
	"github.com/nawat-john/oddspulse/internal/model"
	"github.com/nawat-john/oddspulse/internal/shutdown"
)

const topicRaw = "pm.raw"

// ticksPerSec is the internal scheduling resolution each worker uses to
// pace its share of the target rate (fixed-point accumulation smooths out
// rates that are not an exact multiple of this). Not exposed as a flag - an
// implementation detail of the rate limiter, not a tunable.
const ticksPerSec = 200

func main() {
	brokers := flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
	rate := flag.Int("rate", 10000, "target events/sec produced to pm.raw")
	assets := flag.Int("assets", 1000, "number of synthetic assets to simulate")
	surgeProb := flag.Float64("surge-prob", 0, "probability (0..1) that any given event is an injected surge (large price jump) rather than a normal random-walk step")
	workers := flag.Int("workers", 4, "number of concurrent producer goroutines sharing the target rate")
	duration := flag.Duration("duration", 0, "how long to run before exiting (0 = run until SIGINT/SIGTERM)")
	realAssetsURL := flag.String("real-assets-url", "", "optional gateway /markets URL (e.g. http://localhost:8081/markets); when set, ticks are generated for these REAL, client-visible asset ids (up to --assets of them) instead of synthetic loadgen-N ids - lets S3/S4-style load tests generate volume that loadgen-clients' real-asset subscriptions actually see. Overwrites those assets' real prices with synthetic noise for the test's duration; a benchmark-only knob, never for production use")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "loadgen-producer")

	ctx, stop := shutdown.NewContext()
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	producer, err := kafka.NewProducer(strings.Split(*brokers, ","))
	if err != nil {
		log.Error("kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	var states assetStates
	if *realAssetsURL != "" {
		ids, err := fetchRealAssetIDs(*realAssetsURL, *assets)
		if err != nil {
			log.Error("fetch real asset ids", "error", err)
			os.Exit(1)
		}
		states = newAssetStatesFromIDs(ids)
		log.Info("using real asset ids", "count", len(states), "source", *realAssetsURL)
	} else {
		states = newAssetStates(*assets)
	}

	var produced, errs atomic.Int64
	perWorker := *workers
	if perWorker < 1 {
		perWorker = 1
	}
	ratePerWorker := *rate / perWorker
	for w := 0; w < perWorker; w++ {
		go runWorker(ctx, producer, states.shard(w, perWorker), ratePerWorker, *surgeProb, &produced, &errs)
	}

	log.Info("running", "rate", *rate, "assets", *assets, "workers", perWorker, "surge_prob", *surgeProb, "brokers", *brokers)
	reportLoop(ctx, log, &produced, &errs)

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = producer.Flush(flushCtx)
	flushCancel()
	log.Info("shutting down", "produced_total", produced.Load(), "errors_total", errs.Load())
}

// assetState is one simulated asset's random-walk price. price is only ever
// touched by the single worker goroutine that owns it (assetStates.shard
// partitions assets disjointly across workers), so it needs no lock.
type assetState struct {
	assetID  string
	marketID string
	price    float64
}

type assetStates []*assetState

// newAssetStates creates n synthetic assets, each starting at a random price
// in [0.05, 0.95] with its own synthetic market (one outcome per market -
// good enough for load testing the pipeline; real markets have 2+ outcomes
// but the processor/gateway do not care).
func newAssetStates(n int) assetStates {
	out := make(assetStates, n)
	for i := range out {
		out[i] = &assetState{
			assetID:  "loadgen-" + strconv.Itoa(i),
			marketID: "loadgen-market-" + strconv.Itoa(i),
			price:    0.05 + rand.Float64()*0.9,
		}
	}
	return out
}

// newAssetStatesFromIDs creates one asset state per real asset id (see
// -real-assets-url), each starting at a random price in [0.05, 0.95] - the
// real market's actual current price is not read; this is synthetic load
// against a real, client-visible id, not a replay of real data.
func newAssetStatesFromIDs(ids []string) assetStates {
	out := make(assetStates, len(ids))
	for i, id := range ids {
		out[i] = &assetState{assetID: id, marketID: id, price: 0.05 + rand.Float64()*0.9}
	}
	return out
}

// fetchRealAssetIDs GETs a gateway's /markets endpoint (the same shape
// cmd/loadgen-clients discovers assets from) and returns up to max asset
// ids.
func fetchRealAssetIDs(url string, max int) ([]string, error) {
	resp, err := http.Get(url) //nolint:gosec // url is an operator-supplied benchmark flag, not untrusted input
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
		if max > 0 && len(ids) >= max {
			return ids[:max], nil
		}
	}
	return ids, nil
}

// shard returns the subset of states owned by worker index w of numWorkers
// (round-robin split; disjoint and covers every asset).
func (a assetStates) shard(w, numWorkers int) assetStates {
	var out assetStates
	for i, s := range a {
		if i%numWorkers == w {
			out = append(out, s)
		}
	}
	return out
}

// step advances the asset's price by one random-walk tick and returns the
// RawEvent to produce. surgeProb chance of a large (5-20 percentage point)
// jump instead of the normal small step, sized to reliably cross the
// processor's default surge threshold (design-plan.md 4.2: >=5pp within the
// surge window).
func (a *assetState) step(surgeProb float64) model.RawEvent {
	delta := (rand.Float64()*2 - 1) * 0.002
	if surgeProb > 0 && rand.Float64() < surgeProb {
		sign := 1.0
		if rand.Float64() < 0.5 {
			sign = -1
		}
		delta = sign * (0.05 + rand.Float64()*0.15)
	}
	a.price = clamp(a.price+delta, 0.01, 0.99)

	const spread = 0.004
	now := time.Now().UnixMilli()
	return model.RawEvent{
		V:        model.RawEventVersion,
		Src:      "loadgen",
		Kind:     model.KindQuote,
		AssetID:  a.assetID,
		MarketID: a.marketID,
		Price:    a.price,
		BestBid:  clamp(a.price-spread/2, 0, 1),
		BestAsk:  clamp(a.price+spread/2, 0, 1),
		Size:     100,
		Side:     "BUY",
		SrcTS:    now,
		RecvTS:   now, // loadgen-producer stands in for the ingestor: recv_ts is "now"
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// runWorker paces itself to ratePerSec events/sec (fixed-point accumulation
// against a ticksPerSec-Hz ticker so integer division does not lose the
// remainder), cycling through its assigned assets round-robin, and produces
// each stepped event to pm.raw keyed by asset_id (design-plan.md 4.1: same
// key -> same partition -> preserves per-asset ordering).
func runWorker(ctx context.Context, p *kgo.Client, assets assetStates, ratePerSec int, surgeProb float64, produced, errs *atomic.Int64) {
	if ratePerSec <= 0 || len(assets) == 0 {
		<-ctx.Done()
		return
	}

	interval := time.Second / ticksPerSec
	t := time.NewTicker(interval)
	defer t.Stop()

	perTick := float64(ratePerSec) / float64(ticksPerSec)
	acc := 0.0
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			acc += perTick
			n := int(acc)
			acc -= float64(n)
			for i := 0; i < n; i++ {
				a := assets[idx%len(assets)]
				idx++
				ev := a.step(surgeProb)
				value, err := json.Marshal(ev)
				if err != nil {
					continue
				}
				kafka.Produce(ctx, p, topicRaw, []byte(ev.AssetID), value, func(err error) {
					if err != nil {
						errs.Add(1)
					} else {
						produced.Add(1)
					}
				})
			}
		}
	}
}

// reportLoop logs cumulative produced/error counts every 5s until ctx is
// done, so a ramp (S1) can be watched live instead of only reported at exit.
func reportLoop(ctx context.Context, log *slog.Logger, produced, errs *atomic.Int64) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	lastProduced := int64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := produced.Load()
			log.Info("progress", "produced_total", cur, "eps", (cur-lastProduced)/5, "errors_total", errs.Load())
			lastProduced = cur
		}
	}
}
