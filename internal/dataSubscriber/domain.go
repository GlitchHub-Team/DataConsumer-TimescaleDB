package datasubscriber

import "github.com/nats-io/nats.go/jetstream"

type BatchProcessor interface {
	ProcessBatch(batch jetstream.MessageBatch)
}
