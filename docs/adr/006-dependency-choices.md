# ADR-006: Two dependency choices formalized (coder/websocket, caarlos0/env)

Design-plan.md §12 explicitly left both of these as "either works, pick one and
write an ADR". Both were decided in Phase 1's implementation commit; this formalizes
the reasoning that commit already gave in brief.

## coder/websocket over gorilla/websocket

**Context:** the gateway (§4.3) and every WS-related test double (`internal/hub`'s
`Conn` interface) need a server-side WebSocket implementation; the whole codebase
already threads `context.Context` through every blocking call for cancellation and
graceful shutdown (design-plan.md §4.3's "graceful shutdown", `internal/shutdown`).

**Decision:** use `github.com/coder/websocket`.

**Reasoning:** `coder/websocket`'s `Read`/`Write`/`Ping` take a `context.Context`
directly and respect its cancellation/deadline, which matches this codebase's
context-first idioms exactly (`cmd/gateway/ws.go`'s per-connection `connCtx`,
`internal/hub/client.go`'s per-write `context.WithTimeout(ctx, writeTimeout)`).
`gorilla/websocket` predates widespread `context.Context` use in its API and instead
exposes `SetReadDeadline`/`SetWriteDeadline` on the underlying `net.Conn` - workable,
but a second, older idiom to bridge against everywhere this project otherwise uses
contexts. `coder/websocket` is also the smaller, actively-maintained library (no
transitive dependencies beyond `golang.org/x/net` for extensions), which matched
picking one focused library per concern (go.mod's original commit message) rather
than pulling in `gorilla/websocket`'s wider, longer-lived API surface for the same
job.

## caarlos0/env over koanf

**Context:** every service reads its configuration from environment variables only
(design-plan.md §13's `GW_*`/`PM_*`/`KAFKA_BROKERS` table) - a single, flat,
12-factor-style source, with no file, remote, or layered-provider config in scope
anywhere in this design.

**Decision:** use `github.com/caarlos0/env/v11`.

**Reasoning:** `caarlos0/env` does exactly one thing - populate a struct from env
vars via struct tags (`env:"KAFKA_BROKERS" envDefault:"localhost:19092"
envSeparator:","`, as used in every `cmd/*/main.go`'s `Config`) - with essentially no
API to learn beyond that. `koanf` is a general multi-provider configuration library
(file, env, remote, flags, layered merging) built for services that need more than
one config source or format; none of that is needed here, and pulling it in would
mean carrying a bigger dependency and a wider API surface to reach the same
single-provider outcome `caarlos0/env` already gives in one `env.ParseAs[Config]()`
call per service.
