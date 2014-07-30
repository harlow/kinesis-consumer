package emitters

import (
	"fmt"
)

type RedshiftEmitter struct {
}

func (e RedshiftEmitter) Emit(path string, data []byte) {
	// first call S3 bucket
	// pg.query("COPY file_path TO table_name")
	// pg.query("INSERT INTO imported_files VALUE file_path")
	fmt.Printf("debug: emitting %v to Redshift\n", path)
	fmt.Println(string(data))
}
