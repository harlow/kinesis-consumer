# Consumer

Read records from the Kinesis stream

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream and Redis for checkpoint:

```
export AWS_PROFILE=
export AWS_REGION=
export REDIS_URL=
```

### Run the consumer

    $  go run main.go --app appName --stream streamName
