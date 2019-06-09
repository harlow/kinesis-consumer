# Producer

A prepopulated file with JSON users is available on S3 for seeing the stream.

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```
export AWS_PROFILE=
export AWS_REGION=
```

### Running the code

    $ go run main.go --stream streamName
