package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

// fakeSource plays the broker: it delivers the messages in order and records the
// commits and the order of every step in journal.
type fakeSource struct {
	messages    []resource.Message
	consumeErr  error
	commitErr   error
	journal     *journal
	handleError []error
}

func (f *fakeSource) Consume(_ context.Context, handle func(resource.Message) error) error {
	for _, m := range f.messages {
		f.handleError = append(f.handleError, handle(m))
	}

	return f.consumeErr
}

func (f *fakeSource) Commit(msg resource.Message) error {
	f.journal.add(fmt.Sprintf("commit %d", msg.Offset))

	return f.commitErr
}

type journal struct {
	mu    sync.Mutex
	steps []string
}

func (j *journal) add(step string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.steps = append(j.steps, step)
}

type recordingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLog) add(level, msg string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+" "+fmt.Sprintf(msg, v...))
}

func (l *recordingLog) Debugf(msg string, v ...any) { l.add("debug", msg, v...) }
func (l *recordingLog) Infof(msg string, v ...any)  { l.add("info", msg, v...) }
func (l *recordingLog) Warnf(msg string, v ...any)  { l.add("warn", msg, v...) }
func (l *recordingLog) Errorf(msg string, v ...any) { l.add("error", msg, v...) }

func (l *recordingLog) text() string { return strings.Join(l.lines, "\n") }

func message(offset int64, value string) resource.Message {
	return resource.Message{Topic: topic, Partition: 1, Offset: offset, Value: []byte(value)}
}

func TestConsumerHandle(t *testing.T) {
	const secret = "card-4111-1111"

	storeDown := errors.New("connection reset")
	rejected := fmt.Errorf("insert inbox event 7: %w: constraint %q (23505)", domain.ErrRejected, "conversation_inbox_stream_key")
	badValue := `{"id": "` + secret + `"}`
	backoff := Backoff{Initial: time.Second, Max: 3 * time.Second}

	// handleResult is what Handle returns on its nth call.
	type handleResult struct {
		result domain.ApplyResult
		err    error
	}

	tests := []struct {
		name         string
		value        string
		results      []handleResult
		commitErr    error
		cancelled    bool
		wantHandle   int
		wantSleeps   []time.Duration
		wantJournal  []string
		wantErr      string
		wantStats    StatsSnapshot
		wantLogHas   []string
		wantLogLacks []string
	}{
		{
			name:        "newEventIsStoredThenCommitted",
			value:       validBody,
			results:     []handleResult{{result: domain.ApplyResult{Applied: true, Projected: true}}},
			wantHandle:  1,
			wantJournal: []string{"handle 7", "commit 41"},
			wantStats:   StatsSnapshot{Stored: 1, Projected: 1},
		},
		{
			name:        "eventWithoutAMessageIsStoredAndCommitted",
			value:       validBody,
			results:     []handleResult{{result: domain.ApplyResult{Applied: true}}},
			wantHandle:  1,
			wantJournal: []string{"handle 7", "commit 41"},
			wantStats:   StatsSnapshot{Stored: 1},
		},
		{
			name:        "redeliveryIsCountedAndCommitted",
			value:       validBody,
			results:     []handleResult{{result: domain.ApplyResult{}}},
			wantHandle:  1,
			wantJournal: []string{"handle 7", "commit 41"},
			wantStats:   StatsSnapshot{Duplicates: 1},
		},
		{
			name:         "eventMissingItsTextIsFlaggedNotBlocked",
			value:        validBody,
			results:      []handleResult{{result: domain.ApplyResult{Applied: true, Invalid: true}}},
			wantHandle:   1,
			wantJournal:  []string{"handle 7", "commit 41"},
			wantStats:    StatsSnapshot{Stored: 1, Invalid: 1},
			wantLogHas:   []string{"warn event 7 (UserMessageSent) should have added a chat message"},
			wantLogLacks: []string{"hola"},
		},
		{
			name:         "malformedRecordIsSkippedAndCommitted",
			value:        badValue,
			wantHandle:   0,
			wantJournal:  []string{"commit 41"},
			wantStats:    StatsSnapshot{Malformed: 1},
			wantLogHas:   []string{"warn skipping a record that cannot be decoded: decode record " + topic + "/1/41", "field has the wrong type (id)"},
			wantLogLacks: []string{secret},
		},
		{
			name:        "tombstoneIsCommitted",
			value:       "",
			wantHandle:  0,
			wantJournal: []string{"commit 41"},
			wantStats:   StatsSnapshot{Tombstones: 1},
		},
		{
			name:         "eventTheStoreRefusesIsSkippedAndCommitted",
			value:        validBody,
			results:      []handleResult{{err: rejected}},
			wantHandle:   1,
			wantJournal:  []string{"handle 7", "commit 41"},
			wantStats:    StatsSnapshot{Rejected: 1},
			wantLogHas:   []string{"error skipping event 7", `constraint "conversation_inbox_stream_key" (23505)`},
			wantLogLacks: []string{"hola"},
		},
		{
			name:        "storeErrorIsRetriedInPlaceBeforeCommitting",
			value:       validBody,
			results:     []handleResult{{err: storeDown}, {err: storeDown}, {result: domain.ApplyResult{Applied: true, Projected: true}}},
			wantHandle:  3,
			wantSleeps:  []time.Duration{time.Second, 2 * time.Second},
			wantJournal: []string{"handle 7", "sleep 1s", "handle 7", "sleep 2s", "handle 7", "commit 41"},
			wantStats:   StatsSnapshot{Stored: 1, Projected: 1, Retries: 2},
			wantLogHas:  []string{"error event 7 was not stored, trying again in 1s: connection reset"},
		},
		{
			name:  "waitGrowsToTheCapAndStaysThere",
			value: validBody,
			results: []handleResult{
				{err: storeDown}, {err: storeDown}, {err: storeDown}, {err: storeDown}, {err: storeDown},
				{result: domain.ApplyResult{Applied: true}},
			},
			wantHandle:  6,
			wantSleeps:  []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second, 3 * time.Second},
			wantJournal: nil, // checked through wantSleeps
			wantStats:   StatsSnapshot{Stored: 1, Retries: 5},
		},
		{
			name:        "stoppingDuringTheWaitLeavesTheRecordUncommitted",
			value:       validBody,
			results:     []handleResult{{err: storeDown}},
			cancelled:   false,
			wantHandle:  1,
			wantSleeps:  []time.Duration{time.Second},
			wantJournal: []string{"handle 7", "sleep 1s"},
			wantErr:     "stopped before event 7 was stored: context canceled",
			wantStats:   StatsSnapshot{Retries: 1},
		},
		{
			name:        "alreadyStoppedDoesNotRetry",
			value:       validBody,
			results:     []handleResult{{err: storeDown}},
			cancelled:   true,
			wantHandle:  1,
			wantJournal: []string{"handle 7"},
			wantErr:     "stopped before event 7 was stored: context canceled",
		},
		{
			name:        "commitFailureIsReportedAndLeavesTheOffsetBehind",
			value:       validBody,
			results:     []handleResult{{result: domain.ApplyResult{Applied: true}}},
			commitErr:   errors.New("broker not available"),
			wantHandle:  1,
			wantJournal: []string{"handle 7", "commit 41"},
			wantErr:     "commit " + topic + "/1/41: broker not available",
			wantStats:   StatsSnapshot{Stored: 1},
			wantLogHas:  []string{"warn offset of " + topic + "/1/41 was not committed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &journal{}
			log := &recordingLog{}
			src := &fakeSource{messages: []resource.Message{message(41, tt.value)}, commitErr: tt.commitErr, journal: j}

			ingest := mocks.NewProjection(t)
			calls := 0
			ingest.On("Handle", mock.Anything, mock.Anything).Return(
				func(_ context.Context, e domain.InboxEvent) domain.ApplyResult {
					r := tt.results[calls]
					return r.result
				},
				func(_ context.Context, e domain.InboxEvent) error {
					r := tt.results[calls]
					j.add(fmt.Sprintf("handle %d", e.OutboxID))
					calls++
					return r.err
				},
			).Maybe()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelled {
				cancel()
			}

			c := NewConsumer(src, ingest, log, WithBackoff(backoff))
			var sleeps []time.Duration
			c.sleep = func(ctx context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				j.add("sleep " + d.String())
				if tt.name == "stoppingDuringTheWaitLeavesTheRecordUncommitted" {
					cancel()
					return ctx.Err()
				}

				return nil
			}

			err := c.Run(ctx)

			require.NoError(t, err)
			require.Len(t, src.handleError, 1)
			if tt.wantErr != "" {
				require.Error(t, src.handleError[0])
				assert.Contains(t, src.handleError[0].Error(), tt.wantErr)
			} else {
				assert.NoError(t, src.handleError[0])
			}
			assert.Equal(t, tt.wantHandle, calls)
			assert.Equal(t, tt.wantSleeps, sleeps)
			if tt.wantJournal != nil {
				assert.Equal(t, tt.wantJournal, j.steps)
			}
			assert.Equal(t, tt.wantStats, c.Stats())
			for _, want := range tt.wantLogHas {
				assert.Contains(t, log.text(), want)
			}
			for _, lacks := range tt.wantLogLacks {
				assert.NotContains(t, log.text(), lacks)
			}
		})
	}
}

func TestConsumerRun(t *testing.T) {
	tests := []struct {
		name       string
		consumeErr error
		wantErr    string
	}{
		{name: "sourceEndsCleanly"},
		{name: "sourceFails", consumeErr: errors.New("broker down"), wantErr: "consume broker down"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &recordingLog{}
			src := &fakeSource{
				messages:   []resource.Message{message(1, validBody), message(2, ""), message(3, "{broken")},
				consumeErr: tt.consumeErr, journal: &journal{},
			}
			ingest := mocks.NewProjection(t)
			ingest.On("Handle", mock.Anything, mock.Anything).Return(domain.ApplyResult{Applied: true, Projected: true}, nil).Once()

			err := NewConsumer(src, ingest, log).Run(context.Background())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{"commit 1", "commit 2", "commit 3"}, src.journal.steps, "one commit per record, in order")
			assert.Contains(t, log.text(), "info consumer stopped: stored=1 projected=1 duplicates=0 invalid=0 tombstones=1 malformed=1 rejected=0 retries=0")
		})
	}
}

func TestSleepContext(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		d       time.Duration
		wantErr error
	}{
		{name: "waitsForTheDuration", ctx: context.Background(), d: time.Millisecond},
		{name: "returnsEarlyWhenCancelled", ctx: cancelled, d: time.Hour, wantErr: context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()

			err := sleepContext(tt.ctx, tt.d)

			assert.ErrorIs(t, err, tt.wantErr)
			assert.Less(t, time.Since(start), time.Minute)
		})
	}
}

func TestConsumerDefaults(t *testing.T) {
	tests := []struct {
		name string
		opts []ConsumerOption
		want Backoff
	}{
		{name: "defaultBackoff", want: DefaultBackoff},
		{name: "customBackoff", opts: []ConsumerOption{WithBackoff(Backoff{Initial: time.Millisecond, Max: time.Second})}, want: Backoff{Initial: time.Millisecond, Max: time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewConsumer(&fakeSource{}, mocks.NewProjection(t), &recordingLog{}, tt.opts...)

			assert.Equal(t, tt.want, c.backoff)
		})
	}
}
