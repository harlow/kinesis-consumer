# Consumer

Read records from the Kinesis stream

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```
export AWS_PROFILE=
export AWS_REGION=
```

### Run the consumer

    $ go run main.go --app appName --stream streamName --table tableName
