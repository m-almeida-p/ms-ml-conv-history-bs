// Package resource holds the ports for outside resources other than storage.
package resource

import "context"

//go:generate mockery --name MessageSource --filename message_source.go

// Message is one record read from the broker.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// MessageSource is a stream of broker records with manual offset commits.
type MessageSource interface {
	// Consume delivers each record to handle, one at a time and in order within a
	// partition, until the source stops: the process is told to stop, the broker
	// fails for good, or ctx ends. An error from handle means the record was not
	// processed; the source logs it and goes on, so handle must not return before
	// it has dealt with the record.
	Consume(ctx context.Context, handle func(Message) error) error

	// Commit records that msg, and everything before it in its partition, is done.
	Commit(msg Message) error
}
