
OUT_FILE=healthz

all: linux osx windows

.PHONY: dep-version
dep-version: ## Shows the dep version information
	dep version

linux: ## Builds binary for linux_amd64 (lax)
	${BUILD_ENV} GOARCH=amd64 GOOS=linux go build -o ../../build/linux/${OUT_FILE}

osx: ## Builds binary for darwin_amd64 (osx)
	${BUILD_ENV} GOARCH=amd64 GOOS=darwin go build -o ../../build/osx/${OUT_FILE}

windows: ## Builds binary for windows_amd64 (wx6)
	${BUILD_ENV} GOARCH=amd64 GOOS=windows go build -o ../../build/windows/${OUT_FILE}

help: ## Lists the makefile's targets
	@grep -E '^[-a-zA-Z0-9]+:.*?#{2} .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
