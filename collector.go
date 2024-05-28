package consumer

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	labelStreamName = "stream_name"
	labelShardID    = "shard_id"
)

var (
	collectorMillisBehindLatest = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "net",
		Subsystem: "kinesis",
		Name:      "milliseconds_behind_latest",
		Help:      "The number of milliseconds the GetRecords response is from the tip of the stream, indicating how far behind current time the consumer is. A value of zero indicates that record processing is caught up, and there are no new records to process at this moment.",
	}, []string{
		labelStreamName,
		labelShardID,
	})

	counterEventsConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "net",
		Subsystem: "kinesis",
		Name:      "events_consumed_count_total",
		Help:      "Number of events consumed from the shard belonging to the stream.",
	}, []string{
		labelStreamName,
		labelShardID,
	})

	counterCheckpointsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "net",
		Subsystem: "kinesis",
		Name:      "checkpoints_written_count_total",
		Help:      "Number of checkpoints that have been written for the shard belonging to the stream. Note that those checkpoints might not have been flushed yet, as this happens independently at a fixed interval.",
	}, []string{
		labelStreamName,
		labelShardID,
	})
)
