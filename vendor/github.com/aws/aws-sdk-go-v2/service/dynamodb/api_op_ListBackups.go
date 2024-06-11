// Code generated by smithy-go-codegen DO NOT EDIT.

package dynamodb

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	internalEndpointDiscovery "github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"time"
)

// List DynamoDB backups that are associated with an Amazon Web Services account
// and weren't made with Amazon Web Services Backup. To list these backups for a
// given table, specify TableName . ListBackups returns a paginated list of
// results with at most 1 MB worth of items in a page. You can also specify a
// maximum number of entries to be returned in a page.
//
// In the request, start time is inclusive, but end time is exclusive. Note that
// these boundaries are for the time at which the original backup was requested.
//
// You can call ListBackups a maximum of five times per second.
//
// If you want to retrieve the complete list of backups made with Amazon Web
// Services Backup, use the [Amazon Web Services Backup list API.]
//
// [Amazon Web Services Backup list API.]: https://docs.aws.amazon.com/aws-backup/latest/devguide/API_ListBackupJobs.html
func (c *Client) ListBackups(ctx context.Context, params *ListBackupsInput, optFns ...func(*Options)) (*ListBackupsOutput, error) {
	if params == nil {
		params = &ListBackupsInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "ListBackups", params, optFns, c.addOperationListBackupsMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*ListBackupsOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type ListBackupsInput struct {

	// The backups from the table specified by BackupType are listed.
	//
	// Where BackupType can be:
	//
	//   - USER - On-demand backup created by you. (The default setting if no other
	//   backup types are specified.)
	//
	//   - SYSTEM - On-demand backup automatically created by DynamoDB.
	//
	//   - ALL - All types of on-demand backups (USER and SYSTEM).
	BackupType types.BackupTypeFilter

	// LastEvaluatedBackupArn is the Amazon Resource Name (ARN) of the backup last
	// evaluated when the current page of results was returned, inclusive of the
	// current page of results. This value may be specified as the
	// ExclusiveStartBackupArn of a new ListBackups operation in order to fetch the
	// next page of results.
	ExclusiveStartBackupArn *string

	// Maximum number of backups to return at once.
	Limit *int32

	// Lists the backups from the table specified in TableName . You can also provide
	// the Amazon Resource Name (ARN) of the table in this parameter.
	TableName *string

	// Only backups created after this time are listed. TimeRangeLowerBound is
	// inclusive.
	TimeRangeLowerBound *time.Time

	// Only backups created before this time are listed. TimeRangeUpperBound is
	// exclusive.
	TimeRangeUpperBound *time.Time

	noSmithyDocumentSerde
}

type ListBackupsOutput struct {

	// List of BackupSummary objects.
	BackupSummaries []types.BackupSummary

	//  The ARN of the backup last evaluated when the current page of results was
	// returned, inclusive of the current page of results. This value may be specified
	// as the ExclusiveStartBackupArn of a new ListBackups operation in order to fetch
	// the next page of results.
	//
	// If LastEvaluatedBackupArn is empty, then the last page of results has been
	// processed and there are no more results to be retrieved.
	//
	// If LastEvaluatedBackupArn is not empty, this may or may not indicate that there
	// is more data to be returned. All results are guaranteed to have been returned if
	// and only if no value for LastEvaluatedBackupArn is returned.
	LastEvaluatedBackupArn *string

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationListBackupsMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsAwsjson10_serializeOpListBackups{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsAwsjson10_deserializeOpListBackups{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "ListBackups"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addOpListBackupsDiscoverEndpointMiddleware(stack, options, c); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opListBackups(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addValidateResponseChecksum(stack, options); err != nil {
		return err
	}
	if err = addAcceptEncodingGzip(stack, options); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	return nil
}

func addOpListBackupsDiscoverEndpointMiddleware(stack *middleware.Stack, o Options, c *Client) error {
	return stack.Finalize.Insert(&internalEndpointDiscovery.DiscoverEndpoint{
		Options: []func(*internalEndpointDiscovery.DiscoverEndpointOptions){
			func(opt *internalEndpointDiscovery.DiscoverEndpointOptions) {
				opt.DisableHTTPS = o.EndpointOptions.DisableHTTPS
				opt.Logger = o.Logger
			},
		},
		DiscoverOperation:            c.fetchOpListBackupsDiscoverEndpoint,
		EndpointDiscoveryEnableState: o.EndpointDiscovery.EnableEndpointDiscovery,
		EndpointDiscoveryRequired:    false,
		Region:                       o.Region,
	}, "ResolveEndpointV2", middleware.After)
}

func (c *Client) fetchOpListBackupsDiscoverEndpoint(ctx context.Context, region string, optFns ...func(*internalEndpointDiscovery.DiscoverEndpointOptions)) (internalEndpointDiscovery.WeightedAddress, error) {
	input := getOperationInput(ctx)
	in, ok := input.(*ListBackupsInput)
	if !ok {
		return internalEndpointDiscovery.WeightedAddress{}, fmt.Errorf("unknown input type %T", input)
	}
	_ = in

	identifierMap := make(map[string]string, 0)
	identifierMap["sdk#Region"] = region

	key := fmt.Sprintf("DynamoDB.%v", identifierMap)

	if v, ok := c.endpointCache.Get(key); ok {
		return v, nil
	}

	discoveryOperationInput := &DescribeEndpointsInput{}

	opt := internalEndpointDiscovery.DiscoverEndpointOptions{}
	for _, fn := range optFns {
		fn(&opt)
	}

	go c.handleEndpointDiscoveryFromService(ctx, discoveryOperationInput, region, key, opt)
	return internalEndpointDiscovery.WeightedAddress{}, nil
}

func newServiceMetadataMiddleware_opListBackups(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "ListBackups",
	}
}
