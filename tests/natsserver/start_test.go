package natssever_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	natsserver "DataConsumer/cmd/external/natsServer"
)

func startTestServer(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready for connections")
	}

	url := fmt.Sprintf("nats://%s", s.Addr().String())
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Connect(%s) unexpected error: %v", url, err)
	}

	t.Cleanup(func() {
		_ = nc.Drain()
		nc.Close()
		s.Shutdown()
		s.WaitForShutdown()
	})

	return s, nc
}

func TestCreateStream_ClosedConnectionDoesNotPanic(t *testing.T) {
	_, nc := startTestServer(t)
	nc.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("createStream(closed-conn) panicked: %v", r)
		}
	}()

	natsserver.CreateStream(nc)
}

func TestNewJetStreamConsumer_ReturnsErrorWhenStreamMissing(t *testing.T) {
	_, nc := startTestServer(t)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	consumer, err := natsserver.NewJetStreamConsumer(js, ctx, zap.NewNop(), natsserver.NatsSubject("sensor.*.*.*"))
	if err == nil {
		t.Fatal("NewJetStreamConsumer() expected error when stream is missing, got nil")
	}
	if consumer != nil {
		t.Fatal("NewJetStreamConsumer() expected nil consumer on error")
	}
}
