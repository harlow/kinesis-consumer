package connector

import "testing"

func TestInsertStmt(t *testing.T) {
	e := RedshiftManifestEmitter{FileTable: "funz"}
	s := []string{"file1", "file2"}

	expected := "INSERT INTO funz VALUES ('file1'),('file2');"
	result := e.fileInsertStmt(s)

	if result != expected {
		t.Errorf("fileInsertStmt() = %v want %v", result, expected)
	}
}

func TestManifestName(t *testing.T) {
	e := RedshiftManifestEmitter{}
	s := []string{"2014/01/01/a-b", "2014/01/01/c-d"}

	expected := "2000/01/01/_manifest/a-b_c-d"
	result := e.getManifestName("2000/01/01", s)

	if result != expected {
		t.Errorf("getManifestName() = %v want %v", result, expected)
	}
}

func TestGenerateManifestFile(t *testing.T) {
	e := RedshiftManifestEmitter{S3Bucket: "bucket_name", CopyMandatory: true}
	s := []string{"file1"}

	expected := "{\"entries\":[{\"url\":\"s3://bucket_name/file1\",\"mandatory\":true}]}"
	result := string(e.generateManifestFile(s))

	if result != expected {
		t.Errorf("generateManifestFile() = %v want %v", result, expected)
	}
}
