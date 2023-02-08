SHELL := /bin/bash

OUT_FILE=arke
GOPKGS:=$(shell go list ./... | grep -v api | tr '\n' ',')

HAVE_PROTOC:=$(shell which protoc 2>/dev/null)
PROTOC=:
ifneq ("$(HAVE_PROTOC)","")
    PROTOC=protoc
else
    $(info No protoc command found, skipping generate task.)
endif

HAVE_PROTOC_DOC:=$(shell which protoc-gen-doc 2>/dev/null)
PROTOCDOC=:
ifneq ("$(HAVE_PROTOC_DOC)","")
    PROTOCDOC=protoc
else
    $(info No protoc-gen-doc command found, skipping generate task.)
endif

HAVE_PROTOC_JAVA:=$(shell which protoc-gen-grpc-java.exe 2>/dev/null)
PROTOCJAVA=:
ifneq ("$(HAVE_PROTOC_JAVA)","")
    PROTOCJAVA=protoc
else
    $(info No protoc-gen-grpc-java.exe command found, skipping generate task.)
endif

UNAME_S := $(shell uname -s | tr A-Z a-z)
ifeq ($(UNAME_S),linux)
	proto_libs = /usr/include
endif
ifeq ($(UNAME_S),darwin)
	proto_libs = /opt/homebrew/include
endif

RABBIT_CONTAINER_RUNNING:=$(shell docker ps -aq -f status=running -f name=integration_rabbitmq_1 2>/dev/null)
RUN_COMPOSE=:
ifneq ("$(RABBIT_CONTAINER_RUNNING)", "")
	RUN_COMPOSE=true
else
	RUN_COMPOSE=false
endif

# used for integration test builds
UNAME_ARCH := $(shell uname -m)
arch=:arm64
ifeq ($(UNAME_S),x86_64)
	arch = amd64
endif

ci: clean linux-nogen

all: clean setup generate osx # osx windows ## Cleans, installs dependencies, generates I18n resource bundle and builds all binaries
.PHONY: all

clean: ## Deletes go build output
	rm -rf build

setup: ## Makes build directories
	mkdir -p build/linux
	mkdir -p build/darwin
	mkdir -p build/windows
	ln -nsf darwin build/osx

generate: generate-proto generate-doc ## Generates Go protobuf files and protobuf docs

generate-proto: ## Generates protobufs
	$(PROTOC) -I$(proto_libs) --proto_path=api/protobuf-spec --go_out=api --go_opt=paths=source_relative --go-grpc_out=api --go-grpc_opt=paths=source_relative api/protobuf-spec/arke.proto

generate-proto-java: ## Generates protobufs for java
	$(PROTOCJAVA) -I$(proto_libs) --plugin=protoc-gen-grpc-java=$(HAVE_PROTOC_JAVA) --proto_path=api/protobuf-spec --grpc-java_out=api/java api/protobuf-spec/*.proto
	$(PROTOCJAVA) -I$(proto_libs) --proto_path=api/protobuf-spec --java_out=api/java api/protobuf-spec/*.proto

generate-doc: ## Generates protobuf docs
	$(PROTOCDOC) -I$(proto_libs) --proto_path=api/protobuf-spec --doc_out=./doc --doc_opt=markdown,arke_protocol.md api/protobuf-spec/*.proto

test-clients: ## Builds test clients
	$(MAKE) -C test/test_producer $(UNAME_S)
	$(MAKE) -C test/test_consumer  $(UNAME_S)
	$(MAKE) -C test/simple_consumer  $(UNAME_S)
	$(MAKE) -C test/simple_producer  $(UNAME_S)
	$(MAKE) -C test/healthz  $(UNAME_S)

linux: linux-nogen generate ## Builds binary for linux_amd64 (lax)

linux-nogen: setup ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -o build/linux/${OUT_FILE}

osx: darwin ## Builds binary for darwin_amd64 (osx)

darwin: setup generate ## Builds binary for darwin_amd64 (osx)
	${BUILD_ENV} GOARCH=arm64 GOOS=darwin go build -o build/darwin/${OUT_FILE}

windows: setup generate ## Builds binary for windows_amd64 (wx6)
	${BUILD_ENV} GOARCH=amd64 GOOS=windows go build -o build/windows/${OUT_FILE}

build: $(UNAME_S) ## Builds binary for current platform

run:
	./build/$(UNAME_S)/${OUT_FILE} &
stop:
	pkill -2 -f build/$(UNAME_S)/${OUT_FILE}

test: generate lint test-nogen ## Executes unit tests

test-nogen: ## Executes unit tests without protoc generation
	LOG_FORMAT=term go test -timeout 30s --coverprofile coverage.out ./pkg/... -cover -v
	go tool cover -html=coverage.out -o coverage.html

# integration test related

build_test_c:
	${BUILD_ENV} go test -c ./ -cover -covermode=count -coverpkg=./... -o build/$(UNAME_S)/${OUT_FILE}.test

pre_stop_test_c:
	killall -2 arke.test || true
	sleep 2

stop_test_c:
	killall -2 arke.test || true
	sleep 2

run_test_c:
	LOG_FORMAT=term ./build/$(UNAME_S)/arke.test -test.coverprofile integration-coverage.out ./pkg/... -cover -v &

integration_coverage_report:
	go tool cover -html=integration-coverage.out -o integration-coverage.html

integration_coverage: pre_stop_test_c compose build_test_c run_test_c integration_rabbitmq integration_azure stop_test_c integration_coverage_report ## Builds arke test binary and runs targets for gathering code coverage from integration tests

integration_rabbitmq: ## Runs integration tests for RabbitMQ
	source env-rabbitmq && go test -count=1 -v -tags=integration ./tests/integration/

integration_azure: ## Runs integration tests for Azure Service bus
	source env-azure && go test -count=1 -v -tags=integration ./tests/integration/

lint: ## Run golangci-lint tool
	golangci-lint run --timeout=30m --disable-all --max-issues-per-linter 0 --max-same-issues 0 --enable=errcheck --enable=gosimple --enable=govet --enable=ineffassign --enable=staticcheck --enable=typecheck --enable=unused --enable=revive --enable=gocritic  --allow-parallel-runners

compose: #linux ## Builds and runs docker image(s) for integration tests
	$(RUN_COMPOSE) || (cd tests/integration && \
		docker-compose -f docker-compose-certs.yml down && \
		docker-compose -f docker-compose-certs.yml build && \
		docker-compose -f docker-compose-certs.yml up --remove-orphans && \
		docker-compose -f docker-compose.yml build && \
		docker-compose -f docker-compose.yml down && \
		docker-compose -f docker-compose.yml up --remove-orphans -d rabbitmq && \
		sleep 10)

compose_down: ## Removes integration tests Docker resources
	cd tests/integration ; \
		docker-compose down

integration_test: ## Runs integration tests
	echo -e "\033[0;36mNo providerTLS\033[0m"
	go test -count=1 -v -tags=integration ./tests/integration/

integration_test_tls: ## Runs integration tests with TLS enabled
	echo -e "\033[0;31mProvider TLS enabled\033[0m"
	PROVIDER_TLS=true SAS_BROKER_PORT=5671 go test -count=1 -v -tags=integration ./tests/integration/

integration_test_tls_send_ca: ## Runs integraiton tests with TLS enabled by sending TLS certs
	echo "\033[0;31mProvider TLS enabled (sending CA cert)\033[0m"
	PROVIDER_TLS=sendCA SAS_BROKER_PORT=5671 go test -count=1 -v -tags=integration ./tests/integration/

integration: compose integration_test ## Runs compose and integration_test

integration_all: integration integration_test_tls integration_test_tls_send_ca ## Runs all integration_test* targets.

help: ## Lists the makefile's targets
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
