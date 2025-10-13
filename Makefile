test:
	go test -count=1 -race -cover ./...
.PHONY: test

audit:
	dis-vulncheck
.PHONY: audit

build:
	go build ./...
.PHONY: build

lint:
	golangci-lint run ./...
