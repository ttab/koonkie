package koonkie

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
)

type FollowerOptions struct {
	Metrics      FollowerMetrics
	DocType      string
	StartAfter   int64
	CaughtUp     bool
	WaitDuration time.Duration
}

// NewLogFollower creates a new follower
func NewLogFollower(
	docs repository.Documents,
	opts FollowerOptions,
) *LogFollower {
	return &LogFollower{
		docs:     docs,
		docType:  opts.DocType,
		position: opts.StartAfter,
		caughtUp: opts.CaughtUp,
		wait: int32(min( //nolint: gosec
			opts.WaitDuration.Milliseconds(),
			math.MaxInt32,
		)),
	}
}

type LogFollower struct {
	docs     repository.Documents
	docType  string
	position int64
	caughtUp bool
	wait     int32

	metrics FollowerMetrics

	m              sync.Mutex
	endCompactRead bool
	endCompact     int64
}

const (
	eventlogBatchSize  = 100
	compactedBlockSize = 500
)

// GetState returns the current log position and a boolean that is we have
// caught up with the log and have switched from the compacted log to streaming
// the eventlog.
func (lf *LogFollower) GetState() (int64, bool) {
	return lf.position, lf.caughtUp
}

func (lf *LogFollower) GetNext(
	ctx context.Context,
) ([]*repository.EventlogItem, error) {
	var items []*repository.EventlogItem

	err := lf.checkLogEnd(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: The ergonomics of Eventlog/CompactedEventlog are a bit bad,
	// both should be able to filter by doc type and event type, and they
	// should return a "lastEvaluatedID" for pagination purposes. Compacted
	// eventlog becomes inscrutable otherwise as it only will return empty
	// results if nothing matches the filter in the compacted window.

	if lf.caughtUp {
		items, err = lf.pollEventlog(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		items, err = lf.pollCompactedEventlog(ctx)
		if err != nil {
			return nil, err
		}
	}

	state := "compact"
	if lf.caughtUp {
		state = "tail"
	}

	if lf.metrics != nil {
		lf.metrics.SetPosition(state, lf.position)
	}

	return items, nil
}

func (lf *LogFollower) pollEventlog(
	ctx context.Context,
) ([]*repository.EventlogItem, error) {
	res, err := lf.docs.Eventlog(ctx,
		&repository.GetEventlogRequest{
			After:     lf.position,
			BatchSize: eventlogBatchSize,
			WaitMs:    lf.wait,
		})
	if err != nil {
		return nil, fmt.Errorf("poll eventlog: %w", err)
	}

	var items []*repository.EventlogItem

	for _, item := range res.Items {
		if lf.docType != "" && item.Type != lf.docType {
			continue
		}

		items = append(items, item)
	}

	if len(res.Items) > 0 {
		lf.position = res.Items[len(res.Items)-1].Id
	}

	return items, nil
}

func (lf *LogFollower) pollCompactedEventlog(
	ctx context.Context,
) ([]*repository.EventlogItem, error) {
	until := min(lf.endCompact, lf.position+compactedBlockSize)

	res, err := lf.docs.CompactedEventlog(ctx,
		&repository.GetCompactedEventlogRequest{
			After: lf.position,
			Until: until,
			Type:  lf.docType,
		})
	if err != nil {
		return nil, fmt.Errorf("poll compacted eventlog: %w", err)
	}

	lf.position = until

	if until >= lf.endCompact {
		lf.caughtUp = true
	}

	return res.Items, nil
}

func (lf *LogFollower) checkLogEnd(ctx context.Context) error {
	lf.m.Lock()
	defer lf.m.Unlock()

	if lf.endCompactRead {
		return nil
	}

	res, err := lf.docs.Eventlog(ctx,
		&repository.GetEventlogRequest{
			After: -1,
		})
	if err != nil {
		return fmt.Errorf(
			"read last event in eventlog: %w", err)
	}

	if len(res.Items) > 0 {
		lf.endCompact = res.Items[0].Id
	}

	lf.endCompactRead = true

	return nil
}

type FollowerMetrics interface {
	SetPosition(state string, position int64)
}

func NewPrometheusFollowerMetrics(
	reg prometheus.Registerer, followerName string,
) (*PrometheusFollowerMetrics, error) {
	logPosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "eventlog_follower_position",
			Help: "Current log position for eventloglog follower",
		},
		[]string{"follower", "state"},
	)
	if err := reg.Register(logPosition); err != nil {
		return nil, fmt.Errorf("failed to register log position metric: %w", err)
	}

	return &PrometheusFollowerMetrics{
		logPosition: logPosition,
	}, nil
}

var _ FollowerMetrics = &PrometheusFollowerMetrics{}

type PrometheusFollowerMetrics struct {
	name        string
	logPosition *prometheus.GaugeVec
}

// SetPosition implements FollowerMetrics.
func (p *PrometheusFollowerMetrics) SetPosition(state string, position int64) {
	p.logPosition.WithLabelValues(p.name, state).Set(float64(position))
}

// WithName creates a separate instance with another follower name.
func (p *PrometheusFollowerMetrics) WithName(followerName string) *PrometheusFollowerMetrics {
	return &PrometheusFollowerMetrics{
		name:        followerName,
		logPosition: p.logPosition,
	}
}
