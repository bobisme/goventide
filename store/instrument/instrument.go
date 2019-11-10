package instrument

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"github.com/bobisme/goventide/store"
)

// stats
var (
	// The latency in milliseconds
	MLatencyMs = stats.Float64("store/latency", "latency in milliseconds", "ms")
)

// tags
var (
	KeyMethod, _      = tag.NewKey("method")
	KeyError, _       = tag.NewKey("error")
	KeyStatus, _      = tag.NewKey("status")
	KeyStreamName, _  = tag.NewKey("stream_name")
	KeyMessageType, _ = tag.NewKey("message_type")
)

// stats views
var (
	latencyView = &view.View{
		Name:        "demo/latency",
		Measure:     MLatencyMs,
		Description: "The distribution of the latencies",

		// Latency in buckets:
		// [>=0ms, >=25ms, >=50ms, >=75ms, >=100ms, >=200ms, >=400ms, >=600ms,
		//  >=800ms, >=1s, >=2s, >=4s, >=6s]
		Aggregation: view.Distribution(
			0, 5, 10, 25, 50, 75, 100, 200, 400, 600, 800, 1000, 2000, 4000, 6000),
		TagKeys: []tag.Key{KeyMethod}}

	latencyCountView = &view.View{
		Name:        "demo/calls",
		Measure:     MLatencyMs,
		Description: "The number of calls",
		Aggregation: view.Count(),
	}
)

func init() {
	if err := view.Register(latencyView, latencyCountView); err != nil {
		log.Panic().Err(err).Msg("Failed to register views")
	}
}

func sinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

type InstrumentedStore struct {
	store store.Store
}

func InstrumentStore(s store.Store) *InstrumentedStore {
	return &InstrumentedStore{s}
}

func (s *InstrumentedStore) Category(
	ctx context.Context, streamName string,
) (cat string, finalErr error) {
	ctx, err := tag.New(
		ctx, tag.Insert(KeyMethod, "category"),
		tag.Insert(KeyStatus, "OK"),
		tag.Insert(KeyStreamName, streamName),
	)
	if err != nil {
		return cat, err
	}
	startTime := time.Now()
	defer func() {
		if finalErr != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(KeyStatus, "ERROR"),
				tag.Upsert(KeyError, finalErr.Error()))
		}
		stats.Record(ctx, MLatencyMs.M(sinceInMilliseconds(startTime)))
	}()

	return s.store.Category(ctx, streamName)
}

func (s *InstrumentedStore) StreamVersion(
	ctx context.Context, streamName string,
) (v int, ok bool, finalErr error) {
	ctx, err := tag.New(
		ctx, tag.Insert(KeyMethod, "stream_version"),
		tag.Insert(KeyStatus, "OK"),
		tag.Insert(KeyStreamName, streamName),
	)
	if err != nil {
		return v, ok, err
	}
	startTime := time.Now()
	defer func() {
		if finalErr != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(KeyStatus, "ERROR"),
				tag.Upsert(KeyError, finalErr.Error()))
		}
		stats.Record(ctx, MLatencyMs.M(sinceInMilliseconds(startTime)))
	}()

	return s.store.StreamVersion(ctx, streamName)
}

func (s *InstrumentedStore) WriteMessage(
	ctx context.Context, id string, streamName string, msgType string,
	data interface{}, opts ...store.WriteMessageOpt,
) (v int, finalErr error) {
	ctx, err := tag.New(
		ctx, tag.Insert(KeyMethod, "write_message"),
		tag.Insert(KeyStatus, "OK"),
		tag.Insert(KeyStreamName, streamName),
		tag.Insert(KeyMessageType, msgType),
	)
	if err != nil {
		return v, err
	}
	startTime := time.Now()
	defer func() {
		if finalErr != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(KeyStatus, "ERROR"),
				tag.Upsert(KeyError, finalErr.Error()))
		}
		stats.Record(ctx, MLatencyMs.M(sinceInMilliseconds(startTime)))
	}()

	return s.store.WriteMessage(ctx, id, streamName, msgType, data, opts...)
}
