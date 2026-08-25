.PHONY: test integration

test:
	go test ./...

integration:
	./scripts/run-e2e-integration.sh

test.lint:
	golangci-lint run --config .golangci.yml --verbose ./...

test.unit:
	go test -coverprofile=coverage_unit.out -tags=unit ./...

test.integration:
	go test -coverprofile=coverage_integration.out -tags=integration ./...
