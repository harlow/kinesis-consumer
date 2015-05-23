# Seed the Stream

A prepopulated file with JSON users is available on S3 for seeing the stream:

https://s3.amazonaws.com/kinesis.test/users.txt

### Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```
export AWS_ACCESS_KEY=
export AWS_REGION_NAME=
export AWS_SECRET_KEY=
```

### Running the code

    $ go run main.go
