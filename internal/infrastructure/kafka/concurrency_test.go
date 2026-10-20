package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// Decode keeps no state: the same records decoded at once give what they give one
// at a time. Run with -race.
func TestDecodeGivesTheSameWhenRecordsAreDecodedAtOnce(t *testing.T) {
	values := []string{validBody, "", "{broken", rangeRecord("0", "1", "c", "E"), "\x00\x00\x00\x00\x09" + validBody}

	type decoded struct {
		Event *domain.InboxEvent
		Err   string
	}
	scenario := func(exec concurrent.Executor) []decoded {
		return concurrent.Collect(exec, 100, func(i int) decoded {
			event, err := Decode(record(values[i%len(values)]))
			out := decoded{Event: event}
			if err != nil {
				out.Err = err.Error()
			}

			return out
		})
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	for i, executor := range concurrent.All {
		assert.Equal(t, outcomes[0], outcomes[i], "%s: the one-at-a-time results", executor.Name)
	}
}

// Stats can be read from another goroutine (a metrics endpoint, a shutdown hook)
// while the consumer is counting. Readers must see counters that only grow, and
// the totals must be exact. Run with -race: a plain int would be flagged.
func TestConsumerCountersCanBeReadWhileItConsumes(t *testing.T) {
	const records, readers, reads = 200, 8, 300

	scenario := func(exec concurrent.Executor) []int64 {
		messages := concurrent.Collect(concurrent.Sequential, records, func(i int) resource.Message { return message(int64(i), validBody) })
		src := &fakeSource{messages: messages, journal: &journal{}}
		ingest := mocks.NewProjection(t)
		ingest.On("Handle", mock.Anything, mock.Anything).Return(domain.ApplyResult{Applied: true, Projected: true}, nil)
		consumer := NewConsumer(src, ingest, &recordingLog{})

		// Task 0 consumes; the others read the counters while it does.
		seen := concurrent.Collect(exec, 1+readers, func(i int) []int64 {
			if i == 0 {
				ctx, stop := context.WithCancel(context.Background())
				defer stop()
				src.onDone = stop // the shutdown signal arrives after the last record
				require.NoError(t, consumer.Run(ctx))

				return nil
			}

			return concurrent.Collect(concurrent.Sequential, reads, func(int) int64 { return consumer.Stats().Stored })
		})

		for _, series := range seen[1:] {
			assert.True(t, nonDecreasing(series), "a counter never goes back")
		}
		stats := consumer.Stats()

		return []int64{stats.Stored, stats.Projected, stats.Duplicates, stats.Retries}
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	for i, executor := range concurrent.All {
		assert.Equal(t, []int64{records, records, 0, 0}, outcomes[i], "%s: exact totals", executor.Name)
	}
}

func nonDecreasing(values []int64) bool {
	for i := 1; i < len(values); i++ {
		if values[i] < values[i-1] {
			return false
		}
	}

	return true
}
