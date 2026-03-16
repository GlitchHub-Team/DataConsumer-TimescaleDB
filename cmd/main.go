package main

import (
	"context"
	"database/sql"
	"flag"
	"os"
	"strconv"
	"time"

	natssever "DataConsumer/cmd/external/natsServer"
	"DataConsumer/cmd/external/timescale"
	"DataConsumer/cmd/logger"
	datastorer "DataConsumer/internal/dataStorer"
	datasubscriber "DataConsumer/internal/dataSubscriber"
	natsutil "DataConsumer/internal/natsutil"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	subject := flag.String("subject", "sensor.*.*.*", "Subject NATS che ascolterà il data consumer")
	batchSize := flag.Int("batch-size", 100, "Dimensione del batch per l'elaborazione dei messaggi NATS, il data consumer richiederà a NATS N messaggi alla volta, dove N è la dimensione del batch.")
	flag.Parse()

	if *batchSize <= 0 {
		*batchSize = 100
	}

	fx.New(
		fx.Provide(logger.NewLogger),
		fx.WithLogger(logger.GetFxLogger),

		fx.Supply(natsutil.NatsAddress(os.Getenv("NATS_HOST"))),
		fx.Supply(natsutil.NatsPort(envInt("NATS_PORT", 4222))),
		fx.Supply(natsutil.NatsToken(os.Getenv("NATS_TOKEN"))),
		fx.Supply(natsutil.NatsSeed(os.Getenv("NATS_SEED"))),
		fx.Supply(natssever.NatsSubject(*subject)),
		fx.Supply(datasubscriber.BatchSize(*batchSize)),
		fx.Supply(jetstream.FetchMaxWait(30*time.Second)),

		fx.Provide(natssever.NewNATSConnection),
		fx.Provide(natssever.NewJetStreamContext),
		fx.Provide(natssever.NewJetStreamConsumer),
		fx.Provide(datasubscriber.NewNatsDataSubscriberController),

		fx.Supply(timescale.TimescaleAddress(os.Getenv("POSTGRES_HOST"))),
		fx.Supply(timescale.TimescalePort(envInt("POSTGRES_PORT", 5432))),
		fx.Supply(timescale.TimescaleUsername(os.Getenv("POSTGRES_USER"))),
		fx.Supply(timescale.TimescalePassword(os.Getenv("POSTGRES_PASSWORD"))),
		fx.Supply(timescale.TimescaleDBName(os.Getenv("POSTGRES_DB"))),

		fx.Provide(timescale.NewTimescaleDBConnection),

		fx.Provide(
			fx.Annotate(
				datasubscriber.NewNatsBatchProcessor,
				fx.As(new(datasubscriber.BatchProcessor)),
			),
		),

		fx.Provide(
			fx.Annotate(
				datastorer.NewStoreDataService,
				fx.As(new(datastorer.StoreDataUseCase)),
			),
		),

		fx.Provide(
			fx.Annotate(
				datastorer.NewTimescaleWriteDataRepository,
				fx.As(new(datastorer.WriteDataPort)),
			),
		),

		fx.Provide(func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}),

		fx.Invoke(Init),
	).Run()
}

func Init(lc fx.Lifecycle, controller *datasubscriber.NatsDataSubscriberController, db timescale.TimescaleDBConnection, ctx context.Context, cancel context.CancelFunc, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Avvio dell'servizio DataConsumer")
			go controller.Listen()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := (*sql.DB)(db).Close(); err != nil {
				logger.Error("Errore durante la chiusura del database Timescale", zap.Error(err))
			}
			cancel()
			return nil
		},
	})
}
