# OddsPulse

Realtime prediction-market tracker: Polymarket price streams flow through Redpanda (Kafka API) into a Go WebSocket gateway that fans them out to browsers.

> Status: Phase 0 (scaffolding). Nothing is functional yet. See [docs/design-plan.md](docs/design-plan.md) for the full plan.

## Quickstart

_Placeholder — filled in as phases land._ Currently only the broker and its topics can be started:

```sh
make up      # start Redpanda and create topics
make topics  # re-run topic creation
make down
```

## Documentation

- [Design plan](docs/design-plan.md)
- [Polymarket API notes](docs/polymarket-notes.md)
- [Architecture decision records](docs/adr/README.md)

## Disclaimer

OddsPulse is a personal engineering project. It is not investment advice, and it is not affiliated with, endorsed by, or sponsored by Polymarket.
