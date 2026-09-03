BINARY := bin/pvc-migrate
VERSION ?= dev
TOOL_IMAGE_REPOSITORY ?= ghcr.io/labring-sigs/pvc-migrate
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.toolImageRepository=$(TOOL_IMAGE_REPOSITORY)

LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_GEN_VERSION ?= v0.19.0
PVC_MIGRATE_E2E_MODE ?= session
E2E_TIMEOUT ?= 90m

.PHONY: all build test test-race vet lint check e2e manifests clean

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

manifests: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object paths=./api/... output:dir=api/v1alpha1
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) crd paths=./api/... output:stdout > deploy/crd.yaml

$(LOCALBIN)/controller-gen:
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

e2e:
	PVC_MIGRATE_E2E=1 PVC_MIGRATE_E2E_MODE=$(PVC_MIGRATE_E2E_MODE) \
		go test -tags=e2e -v -count=1 -timeout=$(E2E_TIMEOUT) ./test/e2e

clean:
	rm -f $(BINARY) coverage.out
