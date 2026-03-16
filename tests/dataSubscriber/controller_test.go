package datasubscriber_test

import (
	"context"
	"os"
	"testing"
	"time"

	natssever "DataConsumer/cmd/external/natsServer"
	datasubscriber "DataConsumer/internal/dataSubscriber"
	natsutil "DataConsumer/internal/natsutil"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type recordingBatchProcessor struct {
	called chan int
}

func (p *recordingBatchProcessor) ProcessBatch(batch jetstream.MessageBatch) {
	processed := 0
	for msg := range batch.Messages() {
		processed++
		_ = msg.Ack()
	}
	p.called <- processed
}

func TestNatsDataSubscriberController_Listen_ProcessaMessaggiPubblicati(t *testing.T) {
	if err := os.RemoveAll("natsStorage"); err != nil {
		t.Fatalf("RemoveAll(natsStorage) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll("natsStorage")
	})

	port := 4223
	nc := natssever.NewNATSMockConnection(
		natsutil.NatsAddress("127.0.0.1"),
		natsutil.NatsPort(port),
		natsutil.NatsToken(""),
		natsutil.NatsSeed(""),
	)
	t.Cleanup(func() {
		_ = nc.Drain()
		nc.Close()
	})

	js := natssever.NewJetStreamContext(nc)
	consumerCtx, cancelConsumer := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelConsumer()

	consumer, err := natssever.NewJetStreamConsumer(js, consumerCtx, zap.NewNop(), natssever.NatsSubject("sensor.*.*.*"))
	if err != nil {
		t.Fatalf("NewJetStreamConsumer() unexpected error: %v", err)
	}

	processor := &recordingBatchProcessor{called: make(chan int, 1)}
	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	controller := datasubscriber.NewNatsDataSubscriberController(
		consumer,
		processor,
		datasubscriber.BatchSize(1),
		zap.NewNop(),
		listenCtx,
	)

	done := make(chan struct{})
	go func() {
		controller.Listen()
		close(done)
	}()

	if err := nc.Publish("sensor.a.b.c", []byte(`{"k":"v"}`)); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush() unexpected error: %v", err)
	}

	select {
	case got := <-processor.called:
		if want := 1; got != want {
			t.Errorf("processed messages got %d, want %d", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting ProcessBatch() call")
	}

	cancelListen()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Listen() did not stop after context cancellation")
	}
}

func TestNatsDataSubscriberController_Listen_FetchErrorThenStop(t *testing.T) {
	if err := os.RemoveAll("natsStorage"); err != nil {
		t.Fatalf("RemoveAll(natsStorage) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll("natsStorage")
	})

	port := 4224
	nc := natssever.NewNATSMockConnection(
		natsutil.NatsAddress("127.0.0.1"),
		natsutil.NatsPort(port),
		natsutil.NatsToken(""),
		natsutil.NatsSeed(""),
	)

	js := natssever.NewJetStreamContext(nc)
	consumerCtx, cancelConsumer := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelConsumer()

	consumer, err := natssever.NewJetStreamConsumer(js, consumerCtx, zap.NewNop(), natssever.NatsSubject("sensor.*.*.*"))
	if err != nil {
		t.Fatalf("NewJetStreamConsumer() unexpected error: %v", err)
	}

	// Force fetch failures by closing the client connection before Listen starts.
	nc.Close()

	processor := &recordingBatchProcessor{called: make(chan int, 1)}
	listenCtx, cancelListen := context.WithCancel(context.Background())

	controller := datasubscriber.NewNatsDataSubscriberController(
		consumer,
		processor,
		datasubscriber.BatchSize(1),
		zap.NewNop(),
		listenCtx,
	)

	done := make(chan struct{})
	go func() {
		controller.Listen()
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancelListen()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Listen() did not stop after context cancellation")
	}

	select {
	case got := <-processor.called:
		t.Fatalf("ProcessBatch() should not be called on fetch error path, got %d processed", got)
	default:
	}
}
