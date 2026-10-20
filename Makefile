# ms-ml-conv-history-bs: two binaries (cmd/api, cmd/consumer).
# Mocks come from //go:generate directives next to each port; `make mocks` only runs them.

GO ?= go
MIN_COVERAGE ?= 96
# The tables are created by the testing repo, which keeps every conversation
# migration (migrations/history/<flow>/ for this service). These targets run it from here.
MIGRATIONS_REPO ?= ../ml-genai-agents-bs-testing
# OpenAPI, generated from the swag annotations in cmd/api/main.go and the handlers,
# as in cpgs-clg-core. Regenerate with `make openapi` in the same change as any
# contract change, and `make openapi/check` fails when the committed spec is stale.
SWAG ?= $(shell go env GOPATH)/bin/swag
OPENAPI_FILE = docs/ms-ml-conv-history-bs-openapi.yml
SWAG_INIT = $(SWAG) init -g cmd/api/main.go --instanceName api --outputTypes yaml --parseInternal --parseDependency --parseDepth 2 --parseDependencyLevel 2 --packagePrefix github.com/yaperos/ms-ml-conv-history-bs

# Packages that hold logic. Mains, mocks and test helpers are left out of the coverage bar.
PACKAGES = $$($(GO) list ./internal/... | grep -v -e /mocks -e '/testsupport$$')

## help: list the targets
.PHONY: help
help:
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## build: build both binaries into ./bin
.PHONY: build
build:
	$(GO) build -o bin/api ./cmd/api
	$(GO) build -o bin/consumer ./cmd/consumer

## test: clean the test cache and run every package suite
.PHONY: test
test:
	$(GO) clean -testcache
	$(GO) test -v $(PACKAGES)

## test-race: the same suites with the race detector (the concurrency guards fail here, not in make test)
.PHONY: test-race
test-race:
	$(GO) clean -testcache
	$(GO) test -race $(PACKAGES)

## cover: fail when any package is under MIN_COVERAGE percent
.PHONY: cover
cover:
	@$(GO) clean -testcache
	@$(GO) test -cover $(PACKAGES) | tee coverage.out
	@awk -v min=$(MIN_COVERAGE) '/coverage:/ { gsub("%","",$$5); if ($$5+0 < min) { print "FAIL " $$2 " is at " $$5 "%, under " min "%"; bad=1 } } END { exit bad }' coverage.out

## it: integration tests against a throwaway Postgres (needs Docker)
.PHONY: it
it:
	$(GO) clean -testcache
	$(GO) test -v ./it/...

## pipeline: end-to-end tests against the local cluster (Kafka, Debezium, Postgres); fails if the stack is down
.PHONY: pipeline
pipeline:
	$(GO) clean -testcache
	PIPELINE_REQUIRE_STACK=1 $(GO) test -v -run TestPipeline ./it/history

## openapi: regenerate docs/ms-ml-conv-history-bs-openapi.yml from the swag annotations
.PHONY: openapi
openapi:
	@command -v $(SWAG) >/dev/null || { echo "Error: swag not found: go install github.com/swaggo/swag/cmd/swag@latest"; exit 1; }
	$(SWAG_INIT) --output docs/api
	mv docs/api/api_swagger.yaml $(OPENAPI_FILE)
	rm -rf docs/api

## openapi/check: fail when the committed OpenAPI spec differs from the annotations
.PHONY: openapi/check
openapi/check:
	@command -v $(SWAG) >/dev/null || { echo "Error: swag not found: go install github.com/swaggo/swag/cmd/swag@latest"; exit 1; }
	@tmp=$$(mktemp -d) && $(SWAG_INIT) --output $$tmp >/dev/null && \
		diff -u $(OPENAPI_FILE) $$tmp/api_swagger.yaml > $$tmp/diff || { cat $$tmp/diff; rm -rf $$tmp; echo "The OpenAPI spec is stale: run make openapi"; exit 1; }; \
		rm -rf $$tmp; echo "OpenAPI spec is up to date"

## check-conventions: no goroutine in production code outside async, no time.Sleep in tests
.PHONY: check-conventions
check-conventions:
	@./scripts/check-conventions.sh

## check: vet, conventions and the OpenAPI spec, the checks a change must pass before it is pushed
.PHONY: check
check: check-conventions openapi/check
	$(GO) vet ./...

## mocks: run the go:generate directives of the ports
.PHONY: mocks
mocks:
	$(GO) generate ./internal/...

## migrations/up: create the history tables of every flow, through the testing repo
.PHONY: migrations/up
migrations/up:
	$(MAKE) -C $(MIGRATIONS_REPO) migrations/up SERVICE=history

## migrations/down: drop the history tables of every flow with all their rows; needs CONFIRM=yes
.PHONY: migrations/down
migrations/down:
	$(MAKE) -C $(MIGRATIONS_REPO) migrations/down SERVICE=history CONFIRM=$(CONFIRM)
