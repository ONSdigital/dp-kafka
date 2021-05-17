#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-kafka
  make audit
popd