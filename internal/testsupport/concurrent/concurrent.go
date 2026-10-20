// Package concurrent holds the helpers of the concurrency tests. They follow the
// style of cpgs-clg-core's async executor: how a batch of work runs is a function
// value, so the same scenario runs one task at a time to give the reference
// outcome and all at once to try to break it. Run the tests with -race.
//
// The helpers are functions over values: a task returns its result into its own
// slot and the caller folds the slots, so a test body needs no mutex, no counter
// shared between goroutines and no sleep.
package concurrent

import "sync"

// Executor decides how a batch of tasks runs, and returns when all are done.
type Executor func(tasks []func())

// Named pairs an executor with the name a subtest reports.
type Named struct {
	Name string
	Run  Executor
}

// All is every executor, the reference first.
var All = []Named{
	{Name: "oneAtATime", Run: Sequential},
	{Name: "allAtOnce", Run: Simultaneous},
}

// Sequential runs the tasks in order on the calling goroutine. Its outcome is the
// reference every other executor is compared with.
func Sequential(tasks []func()) {
	for _, task := range tasks {
		task()
	}
}

// Simultaneous starts one goroutine per task, parks them all at a gate, releases
// them together and waits for them. Starting together, instead of as the
// scheduler pleases, is what makes a race likely enough to fail a test.
func Simultaneous(tasks []func()) {
	var parked, finished sync.WaitGroup
	gate := make(chan struct{})

	parked.Add(len(tasks))
	finished.Add(len(tasks))
	for _, task := range tasks {
		go func() {
			defer finished.Done()
			parked.Done()
			<-gate
			task()
		}()
	}

	parked.Wait()
	close(gate)
	finished.Wait()
}

// Collect runs fn for 0..n-1 with exec and returns the results in index order.
// Each task writes only its own slot, and exec returns after every task is done,
// so reading the slots afterwards is race free.
func Collect[T any](exec Executor, n int, fn func(i int) T) []T {
	results := make([]T, n)
	tasks := make([]func(), n)
	for i := range n {
		tasks[i] = func() { results[i] = fn(i) }
	}
	exec(tasks)

	return results
}

// Outcomes runs a scenario once per executor and returns the outcomes in the same
// order. A test compares them to the first: concurrency must not change the result.
func Outcomes[R any](scenario func(exec Executor) R, executors []Named) []R {
	outcomes := make([]R, 0, len(executors))
	for _, e := range executors {
		outcomes = append(outcomes, scenario(e.Run))
	}

	return outcomes
}

// Count returns how many values satisfy pred.
func Count[T any](values []T, pred func(T) bool) int {
	n := 0
	for _, v := range values {
		if pred(v) {
			n++
		}
	}

	return n
}

// Map applies f to every value.
func Map[T, U any](values []T, f func(T) U) []U {
	out := make([]U, 0, len(values))
	for _, v := range values {
		out = append(out, f(v))
	}

	return out
}

// Orders returns fixed, repeatable orders in which to feed n items: ascending,
// descending, evens then odds, and the ascending order rotated by a third and by
// two thirds. They replace a random shuffle, so a failure reproduces.
func Orders(n int) [][]int {
	ascending := make([]int, n)
	for i := range ascending {
		ascending[i] = i
	}

	descending := make([]int, n)
	for i := range descending {
		descending[i] = n - 1 - i
	}

	evensThenOdds := make([]int, 0, n)
	for parity := range 2 {
		for i := parity; i < n; i += 2 {
			evensThenOdds = append(evensThenOdds, i)
		}
	}

	rotate := func(by int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = (i + by) % n
		}

		return out
	}

	if n == 0 {
		return [][]int{ascending, descending, evensThenOdds, ascending, ascending}
	}

	return [][]int{ascending, descending, evensThenOdds, rotate(n / 3), rotate(2 * n / 3)}
}

// Reorder returns values in the given order. Each index of order must be a valid
// index of values.
func Reorder[T any](values []T, order []int) []T {
	out := make([]T, 0, len(order))
	for _, i := range order {
		out = append(out, values[i])
	}

	return out
}
