package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

// Logger is what the consumer needs to report. Messages carry record
// coordinates and counts, never record content.
type Logger interface {
	Debugf(msg string, v ...any)
	Infof(msg string, v ...any)
	Warnf(msg string, v ...any)
	Errorf(msg string, v ...any)
}

// Backoff bounds the wait between attempts to store one event. The wait doubles
// from Initial up to Max.
type Backoff struct {
	Initial time.Duration
	Max     time.Duration
}

// DefaultBackoff waits 1 s, then 2 s, up to 30 s between attempts.
var DefaultBackoff = Backoff{Initial: time.Second, Max: 30 * time.Second}

// Stats counts what the consumer did since it started.
type Stats struct {
	Stored     atomic.Int64 // new events written to the inbox
	Projected  atomic.Int64 // of those, events that also added a chat message
	Duplicates atomic.Int64 // events already in the inbox, written again by a redelivery
	Invalid    atomic.Int64 // events stored without a message because they lacked their text
	Tombstones atomic.Int64 // records with no value
	Malformed  atomic.Int64 // records that could not be decoded, skipped
	Rejected   atomic.Int64 // events the store refused for good, skipped
	Retries    atomic.Int64 // attempts repeated after a store error
}

// StatsSnapshot is a plain copy of Stats.
type StatsSnapshot struct {
	Stored, Projected, Duplicates, Invalid, Tombstones, Malformed, Rejected, Retries int64
}

// Snapshot copies the counters.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Stored: s.Stored.Load(), Projected: s.Projected.Load(), Duplicates: s.Duplicates.Load(), Invalid: s.Invalid.Load(),
		Tombstones: s.Tombstones.Load(), Malformed: s.Malformed.Load(), Rejected: s.Rejected.Load(), Retries: s.Retries.Load(),
	}
}

// Consumer reads the conversation events topic, stores each event and commits its offset only
// after the store succeeded.
//
//   - A record it cannot decode, or an event the store refuses for good, is
//     logged, counted and committed, so one bad record cannot block a partition.
//   - A store error that a retry can fix (the database is down) is retried in place
//     with a capped backoff until it works or ctx ends; the offset moves only then.
//   - Delivery is at least once. A redelivered event hits the inbox's primary key
//     and writes nothing.
type Consumer struct {
	source  resource.RecordSource
	ingest  service.Projection
	log     Logger
	backoff Backoff
	sleep   func(ctx context.Context, d time.Duration) error
	grace   time.Duration
	stats   Stats
}

// ConsumerOption tunes a Consumer.
type ConsumerOption func(*Consumer)

// WithBackoff sets the wait between attempts to store one event.
func WithBackoff(b Backoff) ConsumerOption {
	return func(c *Consumer) { c.backoff = b }
}

// DefaultShutdownGrace is how long Run waits for the shutdown signal to reach its
// context after the source has ended.
const DefaultShutdownGrace = time.Second

// WithShutdownGrace sets how long Run waits for ctx to be cancelled when the
// source ends without it. The library and the process's own signal handler get the
// same signal and either can run first.
func WithShutdownGrace(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.grace = d }
}

// NewConsumer returns a consumer over source that stores through ingest.
func NewConsumer(source resource.RecordSource, ingest service.Projection, log Logger, opts ...ConsumerOption) *Consumer {
	c := &Consumer{source: source, ingest: ingest, log: log, backoff: DefaultBackoff, sleep: sleepContext, grace: DefaultShutdownGrace}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Stats returns the counters.
func (c *Consumer) Stats() StatsSnapshot {
	return c.stats.Snapshot()
}

// Run consumes until the source stops, then logs the totals. ctx is cancelled by
// the shutdown signal. The Kafka library ends its loop quietly, with no error,
// both on that signal and on a fatal broker error; only the first is a clean
// stop, so a source that ends with ctx still live is reported as a failure and
// the process exits non-zero.
func (c *Consumer) Run(ctx context.Context) error {
	err := c.source.Consume(ctx, func(record resource.Record) error { return c.handle(ctx, record) })
	if err == nil && !c.stoppedBySignal(ctx) {
		err = errors.New("kafka source stopped without a shutdown signal")
	}

	s := c.stats.Snapshot()
	c.log.Infof("consumer stopped: stored=%d projected=%d duplicates=%d invalid=%d tombstones=%d malformed=%d rejected=%d retries=%d",
		s.Stored, s.Projected, s.Duplicates, s.Invalid, s.Tombstones, s.Malformed, s.Rejected, s.Retries)

	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}

// stoppedBySignal reports whether ctx is cancelled, waiting up to the grace for it.
func (c *Consumer) stoppedBySignal(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}

	timer := time.NewTimer(c.grace)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

// handle deals with one record. It returns an error only when the record was left
// uncommitted, so the broker will deliver it again.
//
// Once ctx is cancelled it handles nothing. The library logs a callback error and
// may still hand out one more record before its own signal stops the loop; a
// malformed record or a tombstone would be committed without touching the store,
// and that commit would cover the offset of an event left unstored just before.
func (c *Consumer) handle(ctx context.Context, record resource.Record) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stopped before record %s/%d/%d was handled: %w", record.Topic, record.Partition, record.Offset, err)
	}

	event, err := Decode(record)

	var decodeErr *DecodeError
	switch {
	case errors.As(err, &decodeErr):
		c.stats.Malformed.Add(1)
		c.log.Warnf("skipping a record that cannot be decoded: %s", decodeErr)

		return c.commit(record)
	case event == nil:
		c.stats.Tombstones.Add(1)

		return c.commit(record)
	}

	result, err := c.store(ctx, *event)
	switch {
	case errors.Is(err, domain.ErrRejected):
		c.stats.Rejected.Add(1)
		c.log.Errorf("skipping event, the store refuses it for good: %v", err)

		return c.commit(record)
	case err != nil:
		return err
	}

	c.count(*event, result)

	return c.commit(record)
}

// store writes the event, retrying in place while the store keeps failing for a
// reason a retry can fix.
func (c *Consumer) store(ctx context.Context, event domain.InboxEvent) (domain.ApplyResult, error) {
	delay := c.backoff.Initial

	for {
		result, err := c.ingest.Handle(ctx, event)
		if err == nil || errors.Is(err, domain.ErrRejected) {
			return result, err
		}
		if ctx.Err() != nil {
			return domain.ApplyResult{}, fmt.Errorf("stopped before event %s was stored: %w", event.Ref(), ctx.Err())
		}

		c.stats.Retries.Add(1)
		c.log.Errorf("event %s was not stored, trying again in %s: %v", event.Ref(), delay, err)

		if err := c.sleep(ctx, delay); err != nil {
			return domain.ApplyResult{}, fmt.Errorf("stopped before event %s was stored: %w", event.Ref(), err)
		}
		delay = min(delay*2, c.backoff.Max)
	}
}

func (c *Consumer) count(event domain.InboxEvent, result domain.ApplyResult) {
	if !result.Applied {
		c.stats.Duplicates.Add(1)
		c.log.Debugf("event %s was already stored", event.Ref())

		return
	}

	c.stats.Stored.Add(1)
	if result.Projected {
		c.stats.Projected.Add(1)
	}
	if result.Invalid {
		c.stats.Invalid.Add(1)
		c.log.Warnf("event %s (%s) should have added a chat message but lacks its text; stored without one", event.Ref(), event.EventName)
	}
}

func (c *Consumer) commit(record resource.Record) error {
	if err := c.source.Commit(record); err != nil {
		// The library logs the returned error. A later commit covers this offset; if
		// none comes, the broker redelivers the record and the inbox ignores it.
		return fmt.Errorf("commit %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
	}

	return nil
}

// sleepContext waits d, or returns early with the context's error.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
