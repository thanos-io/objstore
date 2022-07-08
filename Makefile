
.PHONY: test-local
test-local:
	THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS,BOS $(MAKE) test

.PHONY: test
test:
	go test ./...

.PHONY: deps
deps: ## Ensures fresh go.mod and go.sum.
	@go mod tidy -compat=1.17
	@go mod verify
