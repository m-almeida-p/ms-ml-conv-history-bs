package async

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

func TestExecuteRunsWithTheCurrentExecutor(t *testing.T) {
	tests := []struct {
		name     string
		executor ExecutorFunc
		wantSync bool
	}{
		{name: "synchronousExecutorRunsBeforeExecuteReturns", executor: ExecuteSync, wantSync: true},
		{name: "customExecutorIsUsed", executor: func(fn func()) { fn() }, wantSync: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer SetExecutor(SetExecutor(tt.executor))
			ran := false

			Execute(func() { ran = true })

			assert.Equal(t, tt.wantSync, ran)
		})
	}
}

func TestExecuteAsyncRunsInAnotherGoroutine(t *testing.T) {
	var wg sync.WaitGroup
	var ran atomic.Bool
	wg.Add(1)

	ExecuteAsync(func() {
		defer wg.Done()
		ran.Store(true)
	})
	wg.Wait()

	assert.True(t, ran.Load())
}

func TestTheDefaultExecutorIsAsynchronous(t *testing.T) {
	var wg sync.WaitGroup
	var ran atomic.Bool
	wg.Add(1)

	Execute(func() {
		defer wg.Done()
		ran.Store(true)
	})
	wg.Wait()

	assert.True(t, ran.Load())
}

func TestSetExecutorReturnsThePreviousOneToRestore(t *testing.T) {
	defaultExecutor := SetExecutor(ExecuteSync)
	synchronous := SetExecutor(defaultExecutor) // puts the default back, hands back the synchronous one

	ranBeforeReturning := false
	synchronous(func() { ranBeforeReturning = true })
	assert.True(t, ranBeforeReturning, "what SetExecutor returned is the executor that was replaced")

	var wg sync.WaitGroup
	var ran atomic.Bool
	wg.Add(1)
	Execute(func() {
		defer wg.Done()
		ran.Store(true)
	})
	wg.Wait()
	assert.True(t, ran.Load(), "the default executor is back, and it still runs the work")
}

// Swapping the executor while many callers use it must not be a data race. Run
// with -race: the atomic pointer is what keeps it quiet.
func TestSwappingTheExecutorWhileItIsInUseIsSafe(t *testing.T) {
	defer SetExecutor(SetExecutor(ExecuteSync))
	var calls atomic.Int64

	tasks := make([]func(), 0, 200)
	for i := range 200 {
		if i%10 == 0 {
			tasks = append(tasks, func() { SetExecutor(ExecuteSync) })
			continue
		}
		tasks = append(tasks, func() { Execute(func() { calls.Add(1) }) })
	}
	concurrent.Simultaneous(tasks)

	assert.Equal(t, int64(180), calls.Load(), "every call ran, with whichever executor was current")
}
