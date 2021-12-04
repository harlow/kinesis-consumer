# Consumer with postgres checkpoint

Read records from the Kinesis stream using postgres as checkpoint

## Run the consumer

     go run main.go --app appName --stream streamName --table tableName --connection connectionString
