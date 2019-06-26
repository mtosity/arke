
OUT_FILE=arke

all: clean setup generate linux windows# osx windows ## Cleans, installs dependencies, generates I18n resource bundle and builds all binaries

clean: ## Deletes go build output, i18n resources and resource_windows.syso file
	rm -rf build

.PHONY: dep-version
dep-version: ## Shows the dep version information
	dep version

setup: install ## Makes build directories and installs vendor dependencies
	mkdir -p build/linux
	mkdir -p build/osx
	mkdir -p build/windows

install: dep-version ## Installs dependencies specified in the Gopkg.lock
	dep ensure -vendor-only -v

update: dep-version ## Updates the Gopkg.lock using dependencies specified in the Gopkg.toml
	dep ensure -update -v

generate: ## Generates protobufs
	protoc --proto_path=api/protobuf-spec --go_out=plugins=grpc:api/ api/protobuf-spec/*.proto

linux: setup generate ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -o build/linux/${OUT_FILE}
	$(MAKE) -C test/test_producer linux
	$(MAKE) -C test/test_consumer linux

osx: setup generate ## Builds binary for darwin_amd64 (osx)
	${BUILD_ENV} GOARCH=amd64 GOOS=darwin go build -o build/osx/${OUT_FILE}
	$(MAKE) -C test/test_producer osx
	$(MAKE) -C test/test_consumer osx

windows: setup generate ## Builds binary for windows_amd64 (wx6)
	${BUILD_ENV} GOARCH=amd64 GOOS=windows go build -o build/windows/${OUT_FILE}
	$(MAKE) -C test/test_producer windows
	$(MAKE) -C test/test_consumer windows

test: generate ## Executes unit tests
	go test ./tests/... -cover -v

help: ## Lists the makefile's targets
	@grep -E '^[-a-zA-Z0-9]+:.*?#{2} .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
