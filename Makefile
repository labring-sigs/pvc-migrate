BINARY := bin/pvc-migrate
VERSION ?= dev
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test test-race vet lint check e2e clean

all: check build

build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/pvc-migrate

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

check: test vet lint

e2e:
	PVC_MIGRATE_E2E=1 go test -tags=e2e -v -count=1 -timeout=30m ./test/e2e

clean:
	rm -f $(BINARY) coverage.out
