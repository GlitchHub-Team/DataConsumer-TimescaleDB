package datasubscriber

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type BatchSize int

type NatsDataSubscriberController struct {
	processor BatchProcessor
	consumer  jetstream.Consumer
	batchSize int
	logger    *zap.Logger
	ctx       context.Context
}

func (c *NatsDataSubscriberController) Listen() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msgs, err := c.consumer.Fetch(c.batchSize, jetstream.FetchContext(c.ctx))
			if err != nil {
				c.logger.Error("Errore durante il fetch dei messaggi", zap.Error(err))
				continue
			}
			c.processor.ProcessBatch(msgs)
		}
	}
}

func NewNatsDataSubscriberController(
	consumer jetstream.Consumer,
	batchProcessor BatchProcessor,
	batchSize BatchSize,
	logger *zap.Logger,
	ctx context.Context,
) *NatsDataSubscriberController {
	return &NatsDataSubscriberController{
		consumer:  consumer,
		batchSize: int(batchSize),
		logger:    logger,
		ctx:       ctx,
		processor: batchProcessor,
	}
}
