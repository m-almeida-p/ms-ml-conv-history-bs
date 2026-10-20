// Package async is the one place that starts goroutines. Production code that
// needs background work calls Execute, and a test swaps the executor to run it
// one task at a time or under its own control, as cpgs-clg-core does.
// scripts/check-conventions.sh fails on a goroutine started anywhere else.
//
// usage_test.go is the reference for using it: fire and forget, a deferred
// executor that lets a test choose the order, all tasks at once under -race, a
// worker bound to a context with a verified shutdown, and one worker per
// partition. Read it before starting any background work.
package async

import "sync/atomic"

// ExecutorFunc runs fn either asynchronously or synchronously.
type ExecutorFunc func(fn func())

// current holds the executor. It is an atomic pointer, so swapping it while code
// is calling Execute is not a data race.
var current atomic.Pointer[ExecutorFunc]

func init() {
	executor := ExecutorFunc(ExecuteAsync)
	current.Store(&executor)
}

// Execute runs fn with the current executor, a goroutine unless a test changed it.
func Execute(fn func()) {
	(*current.Load())(fn)
}

// SetExecutor replaces the executor and returns the previous one, to restore it:
//
//	defer async.SetExecutor(async.SetExecutor(async.ExecuteSync))
func SetExecutor(executor ExecutorFunc) ExecutorFunc {
	return *current.Swap(&executor)
}

// ExecuteSync runs fn on the calling goroutine. Tests use it to see the result at once.
func ExecuteSync(fn func()) {
	fn()
}

// ExecuteAsync runs fn in a new goroutine. It is the production behavior.
func ExecuteAsync(fn func()) {
	go fn()
}
