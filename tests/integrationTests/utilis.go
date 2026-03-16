package integrationtests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	natssever "DataConsumer/cmd/external/natsServer"
	"DataConsumer/cmd/external/timescale"
	datastorer "DataConsumer/internal/dataStorer"
	datasubscriber "DataConsumer/internal/dataSubscriber"
	natsutil "DataConsumer/internal/natsutil"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type integrationPipeline struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	db       *sql.DB
	cancel   context.CancelFunc
}

func setupRealPipeline(t *testing.T, subject string) *integrationPipeline {
	t.Helper()

	natsHost := getEnvOrDefault("NATS_HOST", "nats")
	natsPort := envIntOrDefault("NATS_PORT", 4222)
	timescaleHost := getEnvOrDefault("POSTGRES_HOST", "timescale")
	timescalePort := envIntOrDefault("POSTGRES_PORT", 5432)
	timescaleUser := getEnvOrDefault("POSTGRES_USER", "admin")
	timescalePass := getEnvOrDefault("POSTGRES_PASSWORD", "admin")
	timescaleDB := getEnvOrDefault("POSTGRES_DB", "sensor_db")

	nc, err := connectNATS(natsHost, natsPort)
	if err != nil {
		t.Skipf("NATS non raggiungibile (%v), integrazione saltata", err)
	}

	natssever.CreateStream(nc)
	js := natssever.NewJetStreamContext(nc)

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = js.DeleteConsumer(cleanupCtx, "SENSOR_DATA_STREAM", "data-subscriber")
	cleanupCancel()

	consumerCtx, cancelConsumer := context.WithTimeout(context.Background(), 5*time.Second)
	consumer, err := natssever.NewJetStreamConsumer(js, consumerCtx, zap.NewNop(), natssever.NatsSubject(subject))
	cancelConsumer()
	if err != nil {
		_ = nc.Drain()
		nc.Close()
		t.Fatalf("NewJetStreamConsumer() unexpected error: %v", err)
	}

	conn, err := timescale.NewTimescaleDBConnection(
		timescale.TimescaleAddress(timescaleHost),
		timescale.TimescalePort(timescalePort),
		timescale.TimescaleUsername(timescaleUser),
		timescale.TimescalePassword(timescalePass),
		timescale.TimescaleDBName(timescaleDB),
	)
	if err != nil {
		_ = nc.Drain()
		nc.Close()
		t.Skipf("Timescale non raggiungibile (%v), integrazione saltata", err)
	}
	db := (*sql.DB)(conn)

	pipelineCtx, cancelPipeline := context.WithCancel(context.Background())
	repo := datastorer.NewTimescaleWriteDataRepository(conn, pipelineCtx)
	service := datastorer.NewStoreDataService(repo)
	processor := datasubscriber.NewNatsBatchProcessor(service, zap.NewNop())
	controller := datasubscriber.NewNatsDataSubscriberController(
		consumer,
		processor,
		datasubscriber.BatchSize(1),
		zap.NewNop(),
		pipelineCtx,
	)

	go func() {
		controller.Listen()
	}()

	p := &integrationPipeline{nc: nc, js: js, consumer: consumer, db: db, cancel: cancelPipeline}
	t.Cleanup(func() {
		p.cancel()
		_ = p.nc.Drain()
		p.nc.Close()
		_ = p.db.Close()
	})

	return p
}

func connectNATS(host string, port int) (*nats.Conn, error) {
	url := fmt.Sprintf("nats://%s:%d", host, port)
	token := os.Getenv("NATS_TOKEN")
	seed := os.Getenv("NATS_SEED")

	opts := []nats.Option{nats.Timeout(2 * time.Second)}
	if token != "" && seed != "" {
		opts = append(opts, natsutil.JWTAuth(token, seed))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}

	if !nc.IsConnected() {
		nc.Close()
		return nil, fmt.Errorf("connessione NATS non stabilita")
	}

	return nc, nil
}

func buildSubject(tenantID, gatewayID, sensorID uuid.UUID) string {
	return fmt.Sprintf("sensor.%s.%s.%s", tenantID.String(), gatewayID.String(), sensorID.String())
}

func mustBuildValidBody(t *testing.T, sensorID, gatewayID, tenantID uuid.UUID, profile string, ts time.Time, data json.RawMessage) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"sensorId":  sensorID,
		"gatewayId": gatewayID,
		"tenantId":  tenantID,
		"profile":   profile,
		"timestamp": ts,
		"data":      data,
	})
	if err != nil {
		t.Fatalf("json.Marshal() unexpected error: %v", err)
	}
	return body
}

func publishAndFlush(t *testing.T, js jetstream.JetStream, subject string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, body); err != nil {
		t.Fatalf("jetstream.Publish() unexpected error: %v", err)
	}
}

func waitStoredData(db *sql.DB, tenantID, sensorID, gatewayID uuid.UUID, ts time.Time, timeout time.Duration) (string, []byte, error) {
	deadline := time.Now().Add(timeout)
	query := fmt.Sprintf(`SELECT profile, data FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantID.String())

	for time.Now().Before(deadline) {
		var profile string
		var payload []byte
		err := db.QueryRow(query, sensorID, gatewayID, ts).Scan(&profile, &payload)
		if err == nil {
			return profile, payload, nil
		}
		if err != sql.ErrNoRows {
			return "", nil, err
		}
		time.Sleep(200 * time.Millisecond)
	}

	return "", nil, fmt.Errorf("row not found before timeout")
}

func waitRowCount(db *sql.DB, tenantID, sensorID, gatewayID uuid.UUID, ts time.Time, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	query := fmt.Sprintf(`SELECT count(*) FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantID.String())

	for time.Now().Before(deadline) {
		count, err := rowCount(db, query, sensorID, gatewayID, ts)
		if err != nil {
			return err
		}
		if count == expected {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("unexpected row count for tenant %s", tenantID.String())
}

func rowCount(db *sql.DB, query string, sensorID, gatewayID uuid.UUID, ts time.Time) (int, error) {
	var count int
	err := db.QueryRow(query, sensorID, gatewayID, ts).Scan(&count)
	return count, err
}

func cleanupInsertedRow(t *testing.T, db *sql.DB, tenantID, sensorID, gatewayID uuid.UUID, ts time.Time) {
	t.Helper()
	query := fmt.Sprintf(`DELETE FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantID.String())
	if _, err := db.Exec(query, sensorID, gatewayID, ts); err != nil {
		t.Fatalf("cleanup delete unexpected error: %v", err)
	}
}

func waitConsumerAckPending(consumer jetstream.Consumer, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		info, err := consumer.Info(ctx)
		cancel()
		if err != nil {
			return err
		}
		if info.NumAckPending == expected {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("unexpected consumer ack pending")
}

func randomNonExistingTenantID() uuid.UUID {
	for {
		candidate := uuid.New()
		found := false
		for _, known := range timescale.MockTenantSchemas {
			if candidate == known {
				found = true
				break
			}
		}
		if !found {
			return candidate
		}
	}
}

func jsonDiff(a, b []byte) bool {
	var av any
	var bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return true
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return true
	}
	encodedA, _ := json.Marshal(av)
	encodedB, _ := json.Marshal(bv)
	return string(encodedA) != string(encodedB)
}

func getEnvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}
