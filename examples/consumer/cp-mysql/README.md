# Consumer with mysl checkpoint

Read records from the Kinesis stream using mysql as checkpoint

## Environment Variables

Export the required environment vars for connecting to the Kinesis stream:

```shell
export AWS_PROFILE=
export AWS_REGION=
```

## Run the consumer

     go run main.go --app <appName> --stream <streamName> --table <tableName> --connection <connectionString>

Connection string should look something like

     user:password@/dbname
