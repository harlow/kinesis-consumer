# Consumer with mysl checkpoint

Read records from the Kinesis stream using mysql as checkpoint

## Run the consumer

     go run main.go --app <appName> --stream <streamName> --table <tableName> --connection <connectionString>

Connection string should look something like

     user:password@/dbname
