test:
	go test -race -cover ./...
.PHONY: test

audit:
	go list -m all | nancy sleuth
.PHONY: audit

build:
	go build ./...
.PHONY: build