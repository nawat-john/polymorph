SERVICES := ingestor processor gateway recorder loadgen-producer loadgen-clients
COMPOSE  := docker compose -f deploy/docker-compose.yml

.PHONY: build test vet lint up down topics logs clean

build:
	@mkdir -p bin
	@for s in $(SERVICES); do go build -o bin/$$s ./cmd/$$s || exit 1; done

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# Re-run the topic init job (idempotent) against a running broker.
topics:
	$(COMPOSE) run --rm redpanda-init

logs:
	$(COMPOSE) logs -f

clean:
	rm -rf bin data
