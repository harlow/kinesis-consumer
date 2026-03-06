.PHONY: test integration

test:
	go test ./...

integration:
	./scripts/run-example-integration.sh
