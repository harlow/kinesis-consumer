module github.com/harlow/kinesis-consumer

require (
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/apex/log v1.6.0
	github.com/aws/aws-sdk-go-v2 v1.11.2
	github.com/aws/aws-sdk-go-v2/config v1.6.1
	github.com/aws/aws-sdk-go-v2/credentials v1.3.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.2.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.5.0
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.6.0
	github.com/awslabs/kinesis-aggregation/go/v2 v2.0.0-20220623125934-28468a6701b5
	github.com/go-redis/redis/v8 v8.0.0-beta.6
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/lib/pq v1.7.0
	github.com/pkg/errors v0.9.1
	github.com/yuin/gopher-lua v0.0.0-20200603152657-dc2b0ca8b37e // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

go 1.13
