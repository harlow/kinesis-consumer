package connector

import (
	"testing"
)

func TestCopyStatement(t *testing.T) {
	e := RedshiftEmitter{
		Delimiter: ",",
		S3Bucket:  "test_bucket",
		TableName: "test_table",
	}
	f := e.copyStatement("/test.txt")

	copyStatement := "COPY test_table FROM 's3://test_bucket/test.txt' CREDENTIALS 'aws_access_key_id=;aws_secret_access_key=' DELIMITER ',';"

	if f != copyStatement {
		t.Errorf("copyStatement() = %s want %s", f, copyStatement)
	}
}
