include .bingo/Variables.mk

FILES_TO_FMT ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)

.PHONY: test-local
test-local:
	THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS,BOS $(MAKE) test

.PHONY: test
test:
	go test ./...

.PHONY: test-e2e
test-e2e: docker $(GOTESPLIT)
	@echo ">> cleaning docker environment."
	@docker system prune -f --volumes
	@echo ">> cleaning e2e test garbage."
	@rm -rf ./test/e2e/e2e_*
	@echo ">> running /test/e2e tests."
	# NOTE(bwplotka):
	# * If you see errors on CI (timeouts), but not locally, try to add -parallel 1 (Wiard note: to the GOTEST_OPTS arg) to limit to single CPU to reproduce small 1CPU machine.
	@$(GOTESPLIT) -total ${GH_PARALLEL} -index ${GH_INDEX} ./test/e2e/... -- ${GOTEST_OPTS}


.PHONY: deps
deps: ## Ensures fresh go.mod and go.sum.
	@go mod tidy -compat=1.17
	@go mod verify

.PHONY: format
format: $(GOIMPORTS)
	@echo ">> formatting go code"
	@gofmt -s -w $(FILES_TO_FMT)
	@$(GOIMPORTS) -w $(FILES_TO_FMT)

.PHONY:lint
lint: deps $(GOLANGCI_LINT) $(FAILLINT)
	$(call require_clean_work_tree,'detected not clean work tree before running lint, previous job changed something?')
	@echo ">> verifying modules being imported"
	@# TODO(bwplotka): Add, Printf, DefaultRegisterer, NewGaugeFunc and MustRegister once exception are accepted. Add fmt.{Errorf}=github.com/pkg/errors.{Errorf} once https://github.com/fatih/faillint/issues/10 is addressed.
	@$(FAILLINT) -paths "errors=github.com/pkg/errors,\
github.com/prometheus/tsdb=github.com/prometheus/prometheus/tsdb,\
github.com/prometheus/prometheus/pkg/testutils=github.com/thanos-io/thanos/pkg/testutil,\
github.com/prometheus/client_golang/prometheus.{DefaultGatherer,DefBuckets,NewUntypedFunc,UntypedFunc},\
github.com/prometheus/client_golang/prometheus.{NewCounter,NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,\
NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec}=github.com/prometheus/client_golang/prometheus/promauto.{NewCounter,\
NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec},\
sync/atomic=go.uber.org/atomic" ./...
	@$(FAILLINT) -paths "fmt.{Print,Println,Sprint}" -ignore-tests ./...
	@echo ">> linting all of the Go files GOGC=${GOGC}"
	@$(GOLANGCI_LINT) run
