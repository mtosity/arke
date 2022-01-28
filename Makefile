
OUT_FILE=arke
HAVE_PROTOC:=$(shell which protoc 2>/dev/null)
HAVE_PROTOC_DOC:=$(shell which protoc-gen-doc 2>/dev/null)
HAVE_PROTOC_JAVA:=$(shell which protoc-gen-grpc-java.exe 2>/dev/null)
GOPKGS:=$(shell go list ./... | grep -v api | tr '\n' ',')

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	proto_libs = /usr/include
endif
ifeq ($(UNAME_S),Darwin)
	proto_libs = /opt/homebrew/include
endif

all: clean setup generate linux # osx windows ## Cleans, installs dependencies, generates I18n resource bundle and builds all binaries
.PHONY: all

clean: ## Deletes go build output, i18n resources and resource_windows.syso file
	rm -rf build

setup: ## Makes build directories and installs vendor dependencies
	mkdir -p build/linux
	mkdir -p build/osx
	mkdir -p build/windows

generate: generate-proto generate-doc

generate-proto: ## Generates protobufs
    ifneq ("$(HAVE_PROTOC)","")
	protoc -I$(proto_libs) --proto_path=api/protobuf-spec --go_out=api --go_opt=paths=source_relative --go-grpc_out=api --go-grpc_opt=paths=source_relative api/protobuf-spec/arke.proto
    else
        $(info No protoc command found, skipping generate task.)
    endif

generate-proto-java: ## Generates protobufs for java
    ifneq ("$(HAVE_PROTOC_JAVA)","")
	protoc -I$(proto_libs) --plugin=protoc-gen-grpc-java=$(HAVE_PROTOC_JAVA) --proto_path=api/protobuf-spec --grpc-java_out=api/java api/protobuf-spec/*.proto
	protoc -I$(proto_libs) --proto_path=api/protobuf-spec --java_out=api/java api/protobuf-spec/*.proto
    else
        $(info No protoc-gen-grpc-java command found, skipping generate task.)
    endif

generate-doc: ## Generates protobuf docs
    ifneq ("$(HAVE_PROTOC_DOC)","")
	protoc -I$(proto_libs) --proto_path=api/protobuf-spec --doc_out=./doc --doc_opt=markdown,arke_protocol.md api/protobuf-spec/*.proto
    else
        $(info No protoc doc command found, skipping generate doc task.)
    endif

linux: setup generate ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -o build/linux/${OUT_FILE}
	$(MAKE) -C test/test_producer linux
	$(MAKE) -C test/test_consumer linux
	$(MAKE) -C test/simple_consumer linux
	$(MAKE) -C test/simple_producer linux
	$(MAKE) -C test/healthz linux

osx: setup generate ## Builds binary for darwin_amd64 (osx)
	${BUILD_ENV} GOARCH=amd64 GOOS=darwin go build -o build/osx/${OUT_FILE}
	$(MAKE) -C test/test_producer osx
	$(MAKE) -C test/test_consumer osx
	$(MAKE) -C test/simple_consumer osx
	$(MAKE) -C test/simple_producer osx
	$(MAKE) -C test/healthz osx

windows: setup generate ## Builds binary for windows_amd64 (wx6)
	${BUILD_ENV} GOARCH=amd64 GOOS=windows go build -o build/windows/${OUT_FILE}
	$(MAKE) -C test/test_producer windows
	$(MAKE) -C test/test_consumer windows
	$(MAKE) -C test/simple_consumer windows
	$(MAKE) -C test/simple_producer windows
	$(MAKE) -C test/healthz windows

test: generate ## Executes unit tests
	LOG_FORMAT=term go test -timeout 30s --coverprofile coverage.out ./pkg/... -cover -v
	go tool cover -html=coverage.out -o coverage.html

compose: linux ## Builds and runs docker image(s) for integration tests
	cp build/linux/arke tests/integration/
	cd tests/integration; \
		docker-compose -f docker-compose-certs.yml build; \
		docker-compose -f docker-compose-certs.yml up; \
		docker-compose -f docker-compose.yml build; \
		docker-compose -f docker-compose.yml down; \
		docker-compose -f docker-compose.yml up -d; \
		sleep 10

compose_down:
	cd tests/integration ; \
		docker-compose down

integration_test:
	echo "\033[0;36mNo providerTLS\033[0m"
	go test -count=1 -v -tags=integration ./tests/integration/

integration_test_tls:
	echo "\033[0;31mProvider TLS enabled\033[0m"
	PROVIDER_TLS=true SAS_BROKER_PORT=5671 go test -count=1 -v -tags=integration ./tests/integration/

integration_test_tls_send_ca:
	echo "\033[0;31mProvider TLS enabled (sending CA cert)\033[0m"
	PROVIDER_TLS=sendCA SAS_BROKER_PORT=5671 go test -count=1 -v -tags=integration ./tests/integration/

integration: compose integration_test

integration_all: integration integration_test_tls integration_test_tls_send_ca

help: ## Lists the makefile's targets
	@grep -E '^[-a-zA-Z0-9]+:.*?#{2} .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
