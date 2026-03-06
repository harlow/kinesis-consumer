# consumer-group-ddb example

This example runs multiple consumer processes that share shard leases using DynamoDB.

## Run local dependencies

From this directory:

```bash
cp .env.example .env # optional: adjust ports if you have conflicts
docker compose up -d  # or: docker-compose up -d
```

Services:
- Kinesis: `http://localhost:${KINESIS_PORT:-4567}`
- DynamoDB Local: `http://localhost:${DDB_PORT:-8000}`
- DynamoDB Admin UI: `http://localhost:${DDB_ADMIN_PORT:-8001}`

If you prefer manual startup, these are equivalent:

```bash
docker run --rm -d --name kc-kinesis -p 4567:4567 instructure/kinesalite
docker run --rm -d --name kc-ddb -p 8000:8000 amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb
```

Optional DynamoDB UI:

```bash
docker run --rm -d --name kc-ddb-admin -p 8001:8001 -e DYNAMO_ENDPOINT=http://host.docker.internal:8000 aaronshaf/dynamodb-admin
```

Open [http://localhost:8001](http://localhost:8001).

## Produce test data

```bash
cat examples/producer/users.txt | go run examples/producer/main.go --stream my-stream --endpoint http://localhost:4567
```

## Run consumer workers

Start worker A:

```bash
go run examples/consumer-group-ddb/main.go \
  --group demo-app \
  --stream my-stream \
  --worker-id worker-a \
  --lease-table consumer_leases \
  --checkpoint-table consumer_checkpoints \
  --ksis-endpoint http://localhost:4567 \
  --ddb-endpoint http://localhost:8000
```

Start worker B in another terminal:

```bash
go run examples/consumer-group-ddb/main.go \
  --group demo-app \
  --stream my-stream \
  --worker-id worker-b \
  --lease-table consumer_leases \
  --checkpoint-table consumer_checkpoints \
  --ksis-endpoint http://localhost:4567 \
  --ddb-endpoint http://localhost:8000
```

You should see records split across workers over time. If one worker exits, the remaining worker should take over all leases after lease expiry.
The example also attempts to enable DynamoDB TTL on `ttl` for stale worker row cleanup.
`--worker-id` is optional; if omitted, one is auto-generated.
`--group` is preferred; `--app` remains as a backward-compatible alias.

## Inspect lease ownership from CLI

```bash
aws dynamodb query \
  --table-name consumer_leases \
  --key-condition-expression "namespace = :ns AND begins_with(shard_id, :lease)" \
  --expression-attribute-values '{":ns":{"S":"demo-app#my-stream"},":lease":{"S":"LEASE#"}}' \
  --endpoint-url http://localhost:8000
```

## Cleanup

```bash
docker compose down  # or: docker-compose down
```
