package consumer

import (
	"reflect"
	"testing"
)

func TestKinesis_ListAllShards(t *testing.T) {
	type fields struct {
		client     KinesisClient
		streamName string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := Kinesis{
				client:     tt.fields.client,
				streamName: tt.fields.streamName,
			}
			got, err := k.ListAllShards()
			if (err != nil) != tt.wantErr {
				t.Errorf("Kinesis.ListAllShards() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Kinesis.ListAllShards() = %v, want %v", got, tt.want)
			}
		})
	}
}
