package concurrent

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSequentialRunsTasksInOrder(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "noTasks", count: 0},
		{name: "oneTask", count: 1},
		{name: "manyTasks", count: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var order []int
			tasks := Map(make([]struct{}, tt.count), func(struct{}) func() { return nil })
			for i := range tt.count {
				tasks[i] = func() { order = append(order, i) }
			}

			Sequential(tasks)

			want := make([]int, tt.count)
			for i := range want {
				want[i] = i
			}
			assert.Equal(t, want, append([]int{}, order...))
		})
	}
}

func TestSimultaneousRunsEveryTaskAndWaits(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "noTasks", count: 0},
		{name: "oneTask", count: 1},
		{name: "manyTasks", count: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ran atomic.Int64
			tasks := make([]func(), tt.count)
			for i := range tasks {
				tasks[i] = func() { ran.Add(1) }
			}

			Simultaneous(tasks)

			assert.Equal(t, int64(tt.count), ran.Load(), "it returns only after every task finished")
		})
	}
}

// The tasks wait for each other at a barrier, so the batch can only finish if all
// of them run at the same time. One at a time it would never finish.
func TestSimultaneousReallyRunsTasksAtTheSameTime(t *testing.T) {
	const count = 32
	var barrier sync.WaitGroup
	barrier.Add(count)

	tasks := make([]func(), count)
	for i := range tasks {
		tasks[i] = func() {
			barrier.Done()
			barrier.Wait()
		}
	}

	finished := make(chan struct{})
	go func() {
		Simultaneous(tasks)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the tasks did not run at the same time")
	}
}

func TestCollect(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "none", n: 0},
		{name: "one", n: 1},
		{name: "many", n: 100},
	}

	for _, executor := range All {
		for _, tt := range tests {
			t.Run(executor.Name+"/"+tt.name, func(t *testing.T) {
				got := Collect(executor.Run, tt.n, func(i int) int { return i * i })

				want := make([]int, tt.n)
				for i := range want {
					want[i] = i * i
				}
				assert.Equal(t, want, got, "results come back in index order")
			})
		}
	}
}

func TestOutcomes(t *testing.T) {
	var executorsUsed []string
	scenario := func(exec Executor) int {
		executorsUsed = append(executorsUsed, "run")
		return Count(Collect(exec, 10, func(i int) bool { return i%2 == 0 }), func(b bool) bool { return b })
	}

	got := Outcomes(scenario, All)

	assert.Equal(t, []int{5, 5}, got)
	assert.Len(t, executorsUsed, len(All))
}

func TestCountAndMap(t *testing.T) {
	assert.Equal(t, 0, Count([]int(nil), func(int) bool { return true }))
	assert.Equal(t, 2, Count([]int{1, 2, 3, 4}, func(n int) bool { return n%2 == 0 }))
	assert.Equal(t, []string{}, Map([]int(nil), func(n int) string { return "x" }))
	assert.Equal(t, []string{"1", "2"}, Map([]int{1, 2}, func(n int) string { return string(rune('0' + n)) }))
}

func TestOrders(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "empty", n: 0},
		{name: "one", n: 1},
		{name: "two", n: 2},
		{name: "six", n: 6},
		{name: "sixty", n: 60},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orders := Orders(tt.n)

			assert.Len(t, orders, 5)
			for _, order := range orders {
				sorted := slices.Clone(order)
				slices.Sort(sorted)
				want := make([]int, tt.n)
				for i := range want {
					want[i] = i
				}
				assert.Equal(t, want, append([]int{}, sorted...), "every order is a permutation of 0..n-1")
			}
			assert.Equal(t, Orders(tt.n), orders, "the same n gives the same orders, so a failure reproduces")
		})
	}
}

func TestOrdersShapes(t *testing.T) {
	orders := Orders(6)

	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, orders[0])
	assert.Equal(t, []int{5, 4, 3, 2, 1, 0}, orders[1])
	assert.Equal(t, []int{0, 2, 4, 1, 3, 5}, orders[2])
	assert.Equal(t, []int{2, 3, 4, 5, 0, 1}, orders[3])
	assert.Equal(t, []int{4, 5, 0, 1, 2, 3}, orders[4])
}

func TestReorder(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		order  []int
		want   []string
	}{
		{name: "identity", values: []string{"a", "b", "c"}, order: []int{0, 1, 2}, want: []string{"a", "b", "c"}},
		{name: "reversed", values: []string{"a", "b", "c"}, order: []int{2, 1, 0}, want: []string{"c", "b", "a"}},
		{name: "aSubset", values: []string{"a", "b", "c"}, order: []int{1}, want: []string{"b"}},
		{name: "empty", values: nil, order: nil, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Reorder(tt.values, tt.order))
		})
	}
}
