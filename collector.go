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

	gaugeBatchSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   "net",
		Subsystem:   "kinesis",
		Name:        "get_records_result_size",
		Help:        "number of records received from a call to get results",
		ConstLabels: nil,
	}, []string{
		labelStreamName,
		labelShardID,
	})

	histogramBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "net",
		Subsystem: "kinesis",
		Name:      "records_processing_duration",
		Help:      "time in seconds it takes to process all of the records that were returned from a get records call",
		Buckets:   []float64{0.1, 0.5, 1, 3, 5, 10, 30, 60},
	}, []string{
		labelStreamName,
		labelShardID,
	})

	histogramAverageRecordDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "net",
		Subsystem: "kinesis",
		Name:      "average_record_processing_duration",
		Help:      "average time in seconds it takes to process a single record in a batch",
		Buckets:   []float64{0.003, 0.005, 0.01, 0.025, 0.05, 0.1, 1, 3},
	}, []string{
		labelStreamName,
		labelShardID,
	})
)
