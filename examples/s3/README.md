# S3 Pipeline

The S3 Connector Pipeline performs the following steps:

1. Pull records from Kinesis and buffer them untill the desired threshold is met.
2. Upload the batch of records to an S3 bucket.
3. Set the current Shard checkpoint in Redis.

The pipleline config vars are loaded done with [gcfg].

[gcfg]: https://code.google.com/p/gcfg/

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```
export AWS_ACCESS_KEY=
export AWS_REGION_NAME=
export AWS_SECRET_KEY=
```

### Running the code

    $ go run main.go -a appName -s streamName
