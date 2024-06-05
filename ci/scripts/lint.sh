#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-kafka
  go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45.2
  make lint
popd
