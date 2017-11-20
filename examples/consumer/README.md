# Consumer

Read records from the Kinesis stream

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```
export AWS_ACCESS_KEY=
export AWS_REGION_NAME=
export AWS_SECRET_KEY=
```

### Run the consumer

    $ go run main.go -a appName -s streamName
