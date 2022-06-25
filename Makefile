SHELL=bash

test:
	go test -count=1 -race -cover ./...
.PHONY: test

audit:
	set -o pipefail; go list -json -m all | nancy sleuth
.PHONY: audit

build:
	go build ./...
.PHONY: build

lint:
	exit
.PHONY: lint
