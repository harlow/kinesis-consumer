package consumergroup

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/mock"

	consumer "github.com/harlow/kinesis-consumer"
)

var testLease1 = consumer.Lease{
	LeaseKey:       "000001",
	Checkpoint:     "1234345",
	LeaseCounter:   0,
	LeaseOwner:     "1",
	HeartbeatID:    "12345",
	LastUpdateTime: time.Time{},
}

type MockDynamo struct {
	mock.Mock
}

func (m *MockDynamo) PutItem(item *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	ret := m.Called(item)          //get return args
	if e := ret.Get(0); e != nil { //get first return args if not nil
		return e.(*dynamodb.PutItemOutput), ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (m *MockDynamo) UpdateItem(item *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	ret := m.Called(item)          //get return args
	if e := ret.Get(0); e != nil { //get first return args if not nil
		return e.(*dynamodb.UpdateItemOutput), ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (m *MockDynamo) GetItem(item *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	ret := m.Called(item)          //get return args
	if e := ret.Get(0); e != nil { //get first return args if not nil
		return e.(*dynamodb.GetItemOutput), ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (m *MockDynamo) Scan(item *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	ret := m.Called(item)          //get return args
	if e := ret.Get(0); e != nil { //get first return args if not nil
		return e.(*dynamodb.ScanOutput), ret.Error(1)
	}
	return nil, ret.Error(1)
}

func TestDynamoStorage_CreateLease(t *testing.T) {

	tests := []struct {
		name        string
		tableName   string
		lease       consumer.Lease
		dynamoErr   error
		expectedErr error
	}{
		{
			name:        "CreateLease_ExpectSuccess",
			tableName:   "test",
			lease:       testLease1,
			dynamoErr:   nil,
			expectedErr: nil,
		},
		{
			name:        "CreateLease_Expect_Error_StorageCouldNotUpdateOrCreateLease",
			tableName:   "test",
			lease:       testLease1,
			dynamoErr:   awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "", nil),
			expectedErr: consumer.StorageCouldNotUpdateOrCreateLease,
		},
		{
			name:        "CreateLease_Expect_Error_ErrCodeInternalServerError",
			tableName:   "test",
			lease:       testLease1,
			dynamoErr:   awserr.New(dynamodb.ErrCodeInternalServerError, "", nil),
			expectedErr: awserr.New(dynamodb.ErrCodeInternalServerError, "", nil),
		},
	}
	for _, tt := range tests {
		mockDynamo := MockDynamo{}
		mockDynamo.On("PutItem", mock.Anything).Return(nil, tt.dynamoErr)
		t.Run(tt.name, func(t *testing.T) {
			dynamoClient := DynamoStorage{
				Db:        &mockDynamo,
				tableName: tt.tableName,
			}
			err := dynamoClient.CreateLease(tt.lease)
			if err != nil {
				if err.Error() != tt.expectedErr.Error() {
					t.Errorf("DynamoStorage.CreateLease() error = %v, expectedErr %v,", err, tt.expectedErr)
				}
			}
		})
	}
}

func TestDynamoStorage_UpdateLease(t *testing.T) {
	type fields struct {
		Db        DynamoDb
		tableName string
	}
	type args struct {
		originalLease consumer.Lease
		updatedLease  consumer.Lease
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDynamo := MockDynamo{}
			mockDynamo.On("PutItem", mock.Anything).Return(nil, nil)
			dynamoClient := DynamoStorage{
				Db:        tt.fields.Db,
				tableName: tt.fields.tableName,
			}
			if err := dynamoClient.UpdateLease(tt.args.originalLease, tt.args.updatedLease); (err != nil) != tt.wantErr {
				t.Errorf("DynamoStorage.UpdateLease() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_mapLeaseToLeaseUpdate(t *testing.T) {
	type args struct {
		lease consumer.Lease
	}
	tests := []struct {
		name string
		args args
		want LeaseUpdate
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapLeaseToLeaseUpdate(tt.args.lease); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapLeaseToLeaseUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamoStorage_GetLease(t *testing.T) {
	stringCounter := strconv.Itoa(testLease1.LeaseCounter)
	dynamoOutput := dynamodb.GetItemOutput{
		ConsumedCapacity: nil,
		Item: map[string]*dynamodb.AttributeValue{
			"leaseKey":     {S: &testLease1.LeaseKey},
			"checkpoint":   {S: &testLease1.Checkpoint},
			"leaseCounter": {N: &stringCounter},
			"leaseOwner":   {S: &testLease1.LeaseOwner},
			"heartbeatID":  {S: &testLease1.HeartbeatID},
		},
	}

	tests := []struct {
		name      string
		tableName string
		leaseKey  string
		want      *consumer.Lease
		dynamoOut *dynamodb.GetItemOutput
		dynamoErr error
		wantErr   bool
	}{
		{
			name:      "GetLease_Expect_Success",
			tableName: "",
			leaseKey:  testLease1.LeaseKey,
			want:      &testLease1,
			dynamoOut: &dynamoOutput,
			dynamoErr: nil,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		mockDynamo := MockDynamo{}
		mockDynamo.On("GetItem", mock.Anything).Return(tt.dynamoOut, tt.dynamoErr)
		t.Run(tt.name, func(t *testing.T) {
			dynamoClient := DynamoStorage{
				Db:        &mockDynamo,
				tableName: tt.tableName,
			}
			got, err := dynamoClient.GetLease(tt.leaseKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("DynamoStorage.GetLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DynamoStorage.GetLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapLeaseKeyToDdbKey(t *testing.T) {
	type args struct {
		leaseKey string
	}
	tests := []struct {
		name string
		args args
		want map[string]*dynamodb.AttributeValue
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapLeaseKeyToDdbKey(tt.args.leaseKey); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapLeaseKeyToDdbKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamoStorage_GetAllLeases(t *testing.T) {
	type fields struct {
		Db        DynamoDb
		tableName string
	}
	tests := []struct {
		name    string
		fields  fields
		want    map[string]consumer.Lease
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dynamoClient := DynamoStorage{
				Db:        tt.fields.Db,
				tableName: tt.fields.tableName,
			}
			got, err := dynamoClient.GetAllLeases()
			if (err != nil) != tt.wantErr {
				t.Errorf("DynamoStorage.GetAllLeases() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DynamoStorage.GetAllLeases() = %v, want %v", got, tt.want)
			}
		})
	}
}
