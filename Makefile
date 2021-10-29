# these env vars are all for `drain`, below

APP?=consumer-batch
TOPIC?=observation-extracted
GROUP?=dp-observation-importer
CERT_APP?=$(GROUP)
DP_CONFIGS?=../dp-configs
ENV?=develop

host_num?=publishing 3
host_bin=bin-$(APP)

GOOS?=$(shell go env GOOS)
GOARCH?=$(shell go env GOARCH)

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BUILD_SCRIPT=$(BUILD_ARCH)/$(APP).sh

SECRETS_JSON=$(DP_CONFIGS)/secrets/$(ENV)/$(CERT_APP).json

########################################

test:
	go test -race -cover ./...
.PHONY: test

audit:
	go list -json -m all | nancy sleuth
.PHONY: audit

build:
	go build ./...
.PHONY: build

lint:
	exit
.PHONY: lint

########################################
# below runs a consumer to drain a topic in an env

drain:
	GOOS=linux make deploy clean || echo "Please run (with same vars): make clean ENV=$(ENV)"

secrets:
	:

env-vars: secrets
	@jq -r ' . as $$o |  keys | .[] | "export " + . + "=" + ($$o[.] | tojson)' $(SECRETS_JSON) | grep '^export KAFKA_' | sed 's/\\n/\n/g'
	@echo export SNOOZE=false KAFKA_PARALLEL_MESSAGES=12 KAFKA_BATCH_SIZE=100 LOG_QUIET=1
	@echo export KAFKA_CONSUMED_TOPIC="$(TOPIC)" KAFKA_CONSUMED_GROUP="$(GROUP)"

pre-build-app:
	mkdir -p $(BUILD_ARCH)

build-app: pre-build-app
	GOOS=$(GOOS) go build -o $(BUILD_ARCH)/$(APP) examples/$(APP)/main.go

script: pre-build-app
	make env-vars > $(BUILD_SCRIPT)

deploy: build-app script clean-deploy
	dp scp $(ENV) $(host_num) -r -- $(BUILD_ARCH)/. $(host_bin)
	dp ssh $(ENV) $(host_num) -- 'bash -c "cd $(host_bin) && source ./$(APP).sh && ./$(APP)"'

clean: clean-deploy
	-rm -r $(BUILD)

clean-deploy:
	dp ssh $(ENV) $(host_num) -- 'bash -c "[[ ! -d $(host_bin) ]] || rm -r $(host_bin)"'

.PHONY: drain pre-build-app build-app script env-vars deploy clean clean-deploy
