package async_test

// This file is the reference for using the async seam. The types below are
// illustrations, not production code; they have the shape production code
// should have. Copy the pattern that matches the work you need.
//
// Rules the examples follow:
//   - Production code never writes `go`. It calls async.Execute, and
//     scripts/check-conventions.sh fails on a bare goroutine.
//   - A one-shot task (it ends by itself) can run under any executor, so a test
//     can run it synchronously, in a chosen order, or all at once.
//   - A long-running loop is bound to a context, reports when it has stopped, and
//     is only ever started with the real executor: a loop run synchronously
//     would never return.
//   - Tasks do not share variables. Each writes into a slot of its own, or into a
//     type that guards itself, so a test needs no mutex.
//   - Tests swap the executor with `defer async.SetExecutor(async.SetExecutor(x))`
//     and never sleep to wait for a task.

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/async"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// within bounds how long a test waits for a task it started, so a task that never
// runs fails the test instead of hanging it.
const within = 10 * time.Second

// ---------------------------------------------------------------------------
// Pattern 1: fire and forget. One-shot work that must not slow the caller.
// ---------------------------------------------------------------------------

// entrySink receives audit entries. It guards itself, so the task that writes and
// the test that reads need no lock of their own.
type entrySink struct {
	mu      sync.Mutex
	entries []string
	written chan struct{}
}

func newEntrySink() *entrySink { return &entrySink{written: make(chan struct{}, 1024)} }

func (s *entrySink) Write(entry string) {
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	s.mu.Unlock()
	s.written <- struct{}{}
}

func (s *entrySink) Entries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.entries)
}

// waitFor blocks until n entries were written.
func (s *entrySink) waitFor(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-s.written:
		case <-time.After(within):
			t.Fatal("an entry was never written")
		}
	}
}

// auditTrail is the shape production code takes: it is handed what it needs and
// does its background work through async.Execute.
type auditTrail struct{ sink *entrySink }

func (a auditTrail) Record(entry string) {
	async.Execute(func() { a.sink.Write(entry) })
}

func TestReferenceFireAndForget(t *testing.T) {
	tests := []struct {
		name     string
		executor async.ExecutorFunc
		// settle is what a test does to be sure the work is finished.
		settle func(t *testing.T, s *entrySink)
	}{
		{
			name:     "productionExecutorRunsItInAnotherGoroutine",
			executor: async.ExecuteAsync,
			settle:   func(t *testing.T, s *entrySink) { s.waitFor(t, 1) },
		},
		{
			name:     "synchronousExecutorHasFinishedWhenRecordReturns",
			executor: async.ExecuteSync,
			settle:   func(*testing.T, *entrySink) {}, // nothing to wait for
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer async.SetExecutor(async.SetExecutor(tt.executor))
			sink := newEntrySink()

			auditTrail{sink: sink}.Record("transfer approved")
			tt.settle(t, sink)

			assert.Equal(t, []string{"transfer approved"}, sink.Entries())
		})
	}
}

// ---------------------------------------------------------------------------
// Pattern 2: a deferred executor. The test decides when, and in what order,
// the tasks run.
// ---------------------------------------------------------------------------

// queuedExecutor holds tasks instead of running them.
type queuedExecutor struct {
	mu    sync.Mutex
	tasks []func()
}

func (q *queuedExecutor) Execute(fn func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, fn)
}

func (q *queuedExecutor) Pending() []func() {
	q.mu.Lock()
	defer q.mu.Unlock()

	return slices.Clone(q.tasks)
}

func TestReferenceDeferredExecutorControlsTheOrder(t *testing.T) {
	entries := []string{"a", "b", "c", "d", "e"}
	names := []string{"ascending", "descending", "evensThenOdds", "rotatedByAThird", "rotatedByTwoThirds"}

	for i, order := range concurrent.Orders(len(entries)) {
		t.Run(names[i], func(t *testing.T) {
			queue := &queuedExecutor{}
			defer async.SetExecutor(async.SetExecutor(queue.Execute))
			sink := newEntrySink()
			trail := auditTrail{sink: sink}

			for _, e := range entries {
				trail.Record(e)
			}
			assert.Empty(t, sink.Entries(), "nothing runs until the test says so")
			assert.Len(t, queue.Pending(), len(entries))

			concurrent.Sequential(concurrent.Reorder(queue.Pending(), order))

			assert.Equal(t, concurrent.Reorder(entries, order), sink.Entries(), "the tasks ran in the order the test chose")
		})
	}
}

// ---------------------------------------------------------------------------
// Pattern 3: all at once. The deferred executor hands its tasks to the
// concurrent helpers, which release them together. The outcome must equal the
// one-at-a-time outcome; run with -race.
// ---------------------------------------------------------------------------

func TestReferenceTheSameTasksAllAtOnceGiveTheSameOutcome(t *testing.T) {
	entries := concurrent.Map(make([]struct{}, 64), func(struct{}) string { return "" })
	for i := range entries {
		entries[i] = "entry-" + string(rune('A'+i%26)) + string(rune('a'+i/26))
	}

	scenario := func(exec concurrent.Executor) []string {
		queue := &queuedExecutor{}
		defer async.SetExecutor(async.SetExecutor(queue.Execute))
		sink := newEntrySink()
		trail := auditTrail{sink: sink}

		for _, e := range entries {
			trail.Record(e)
		}
		exec(queue.Pending())

		written := sink.Entries()
		sort.Strings(written) // all at once has no order; the set must be the same

		return written
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	expected := slices.Clone(entries)
	sort.Strings(expected)
	for i, executor := range concurrent.All {
		assert.Equal(t, expected, outcomes[i], "%s: every entry was written exactly once", executor.Name)
	}
}

// ---------------------------------------------------------------------------
// Pattern 4: a long-running worker. It is bound to a context, reports when it
// has stopped, and is started with the real executor.
// ---------------------------------------------------------------------------

// startWorker runs work for every tick until ctx is cancelled. The returned
// channel closes when the worker has stopped, which is what a test (or a
// graceful shutdown) waits on.
func startWorker(ctx context.Context, ticks <-chan struct{}, work func()) <-chan struct{} {
	stopped := make(chan struct{})
	async.Execute(func() {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks:
				work()
			}
		}
	})

	return stopped
}

func TestReferenceWorkerIsBoundToItsContext(t *testing.T) {
	tests := []struct {
		name  string
		ticks int
	}{
		{name: "stopsWhileIdle", ticks: 0},
		{name: "doesTheWorkOfEveryTickBeforeItStops", ticks: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A loop must never run under the synchronous executor: it would not return.
			defer async.SetExecutor(async.SetExecutor(async.ExecuteAsync))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ticks := make(chan struct{})
			done := make(chan struct{}, tt.ticks+1)

			stopped := startWorker(ctx, ticks, func() { done <- struct{}{} })
			for range tt.ticks {
				ticks <- struct{}{}
			}
			for range tt.ticks {
				select {
				case <-done:
				case <-time.After(within):
					t.Fatal("a tick was never worked")
				}
			}
			cancel()

			select {
			case <-stopped:
			case <-time.After(within):
				t.Fatal("the worker did not stop when its context was cancelled")
			}
			assert.Len(t, done, 0, "no work ran after the last tick")
		})
	}
}

// ---------------------------------------------------------------------------
// Pattern 5: one worker per partition. This is how the consumer would run
// partitions in parallel: a finite task per partition, in order within it, each
// writing only its own slot. It runs under every executor, so the synchronous
// one gives the reference outcome.
// ---------------------------------------------------------------------------

// runPartitions handles every value of every partition, partitions in parallel
// and values in order within a partition, and returns what each partition saw.
func runPartitions(partitions [][]int) [][]int {
	seen := make([][]int, len(partitions)) // slot p is written only by the task of partition p
	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for p := range partitions {
		async.Execute(func() {
			defer wg.Done()
			for _, v := range partitions[p] {
				seen[p] = append(seen[p], v)
			}
		})
	}
	wg.Wait()

	return seen
}

func TestReferenceOneWorkerPerPartitionKeepsEachPartitionInOrder(t *testing.T) {
	partitions := [][]int{{1, 2, 3, 4, 5}, {10, 20, 30}, {}, {100, 200, 300, 400, 500, 600, 700}}

	tests := []struct {
		name     string
		executor async.ExecutorFunc
	}{
		{name: "synchronousIsTheReference", executor: async.ExecuteSync},
		{name: "productionExecutorRunsThePartitionsInParallel", executor: async.ExecuteAsync},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer async.SetExecutor(async.SetExecutor(tt.executor))

			got := runPartitions(partitions)

			assert.Len(t, got, len(partitions))
			for p, want := range partitions {
				assert.Equal(t, want, append([]int{}, got[p]...), "partition %d keeps its own order", p)
			}
		})
	}
}
