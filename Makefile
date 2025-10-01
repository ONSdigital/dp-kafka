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
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45.2
	golangci-lint run ./...
