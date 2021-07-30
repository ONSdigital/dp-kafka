#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-kafka
  make lint
popd
