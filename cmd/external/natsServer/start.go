package natssever

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"DataConsumer/internal/natsutil"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	streamName = "SENSOR_DATA_STREAM"
)

type NatsSubject string

func NewNATSConnection(address natsutil.NatsAddress, port natsutil.NatsPort, token natsutil.NatsToken, seed natsutil.NatsSeed) *nats.Conn {
	opts := &server.Options{
		Host:      string(address),
		Port:      int(port),
		JetStream: true,
		StoreDir:  "natsStorage",
	}
	s, err := server.NewServer(opts)
	if err != nil {
		log.Fatalf("Impossible to create a new NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		log.Fatalf("NATS server hasn't started on time")
	}
	log.Printf("NATS server is running on %s:%d", address, port)

	opt := natsutil.JWTAuth(string(token), string(seed))

	nc, err := nats.Connect("nats://"+string(address)+":"+strconv.Itoa(int(port)), opt)
	if err != nil {
		log.Fatalf("Error while connecting to NATS server: %v", err)
	}

	CreateStream(nc)

	return nc
}

func CreateStream(nc *nats.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Printf("Errore inizializzazione JetStream: %v", err)
		return
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"sensor.*.*.*"},
		Retention: jetstream.LimitsPolicy,
	})

	if err != nil {
		log.Printf("Errore creazione stream %s: %v", streamName, err)
	} else {
		log.Printf("Stream %s configurata e pronta", streamName)
	}
}

func NewJetStreamContext(nc *nats.Conn) jetstream.JetStream {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Error while creating JetStream context: %v", err)
	}
	return js
}

func NewJetStreamConsumer(js jetstream.JetStream, ctx context.Context, logger *zap.Logger, subject NatsSubject) (jetstream.Consumer, error) {
	consumerConfig := jetstream.ConsumerConfig{
		Durable:       "data-subscriber",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: string(subject),
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, streamName, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("errore nella creazione del JetStream Consumer: %w", err)
	}

	logger.Info("Consumer JetStream creato correttamente", zap.String("durable", consumerConfig.Durable))
	return consumer, nil
}
