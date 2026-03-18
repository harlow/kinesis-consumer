.PHONY: test integration

test:
	go test ./...

integration:
	./scripts/run-e2e-integration.sh
