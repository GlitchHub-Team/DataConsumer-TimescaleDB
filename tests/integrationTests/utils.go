package integrationtests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	natsserver "DataConsumer/cmd/external/natsServer"
	"DataConsumer/cmd/external/timescale"
	datastorer "DataConsumer/internal/dataStorer"
	datasubscriber "DataConsumer/internal/dataSubscriber"
	"DataConsumer/internal/natsutil"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type integrationPipeline struct {
	nc       *nats.Conn
	ncTest   *nats.Conn
	js       jetstream.JetStream
	jsTest   jetstream.JetStream
	consumer jetstream.Consumer
	db       *sql.DB
	cancel   context.CancelFunc
}

type integrationNATSConfig struct {
	Address       string
	Port          int
	CAPemPath     string
	CredsPath     string
	TestCredsPath string
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func setupRealPipeline(t *testing.T, subject string) *integrationPipeline {
	t.Helper()

	pathToCmd := "../../cmd/"

	natsAddress := natsutil.NatsAddress(os.Getenv("NATS_HOST"))
	natsPort := natsutil.NatsPort(envInt("NATS_PORT", 4222))
	natsCAPemPath := natsutil.NatsCAPemPath(pathToCmd + os.Getenv("DATA_CONSUMER_CA_PEM_PATH"))
	natsCredsPath := natsutil.NatsCredsPath(pathToCmd + os.Getenv("DATA_CONSUMER_CREDS_PATH"))
	natsTestCredsPath := natsutil.NatsCredsPath(pathToCmd + os.Getenv("DATA_CONSUMER_TEST_CREDS_PATH"))

	timescaleHost := getEnvOrDefault("POSTGRES_HOST", "timescale")
	timescalePort := envIntOrDefault("POSTGRES_PORT", 5432)
	timescaleUser := getEnvOrDefault("POSTGRES_USER", "admin")
	timescalePass := getEnvOrDefault("POSTGRES_PASSWORD", "admin")
	timescaleDB := getEnvOrDefault("POSTGRES_DB", "sensor_db")

	nc := natsserver.NewNATSConnection(natsAddress, natsPort, natsCredsPath, natsCAPemPath)
	ncTest := natsserver.NewNATSConnection(natsAddress, natsPort, natsTestCredsPath, natsCAPemPath)

	js := natsserver.NewJetStreamContext(nc)
	jsTest := natsserver.NewJetStreamContext(ncTest)

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = js.DeleteConsumer(cleanupCtx, "SENSOR_DATA_STREAM", "data-subscriber")
	cleanupCancel()

	consumerCtx, cancelConsumer := context.WithTimeout(context.Background(), 5*time.Second)
	consumer, err := natsserver.NewJetStreamConsumer(js, consumerCtx, zap.NewNop(), natsserver.NatsSubject(subject))
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

	p := &integrationPipeline{nc: nc, js: js, ncTest: ncTest, jsTest: jsTest, consumer: consumer, db: db, cancel: cancelPipeline}
	t.Cleanup(func() {
		p.cancel()
		_ = p.nc.Drain()
		p.nc.Close()
		_ = p.db.Close()
	})

	return p
}

func loadIntegrationNATSConfig() integrationNATSConfig {
	pathToCmd := "../../cmd/"
	return integrationNATSConfig{
		Address:       string(natsutil.NatsAddress(os.Getenv("NATS_HOST"))),
		Port:          int(natsutil.NatsPort(envInt("NATS_PORT", 4222))),
		CAPemPath:     string(natsutil.NatsCAPemPath(pathToCmd + os.Getenv("DATA_CONSUMER_CA_PEM_PATH"))),
		CredsPath:     string(natsutil.NatsCredsPath(pathToCmd + os.Getenv("DATA_CONSUMER_CREDS_PATH"))),
		TestCredsPath: string(natsutil.NatsCredsPath(pathToCmd + os.Getenv("DATA_CONSUMER_TEST_CREDS_PATH"))),
	}
}

func ensureNATSReachable(cfg integrationNATSConfig) error {
	options := []nats.Option{
		natsutil.CredsFileAuth(cfg.CredsPath),
		natsutil.CAPemAuth(cfg.CAPemPath),
		nats.Timeout(3 * time.Second),
		nats.MaxReconnects(0),
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", cfg.Address, cfg.Port), options...)
	if err != nil {
		return err
	}
	defer nc.Close()

	return nil
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
	query := fmt.Sprintf(`SELECT profile, data FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantSchemaName(tenantID))

	for time.Now().Before(deadline) {
		var profile string
		var payload []byte
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := db.QueryRowContext(ctx, query, sensorID, gatewayID, ts).Scan(&profile, &payload)
		cancel()
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
	query := fmt.Sprintf(`SELECT count(*) FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantSchemaName(tenantID))

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := db.QueryRowContext(ctx, query, sensorID, gatewayID, ts).Scan(&count)
	return count, err
}

func cleanupInsertedRow(t *testing.T, db *sql.DB, tenantID, sensorID, gatewayID uuid.UUID, ts time.Time) {
	t.Helper()
	query := fmt.Sprintf(`DELETE FROM "%s".sensor_data WHERE sensor_id=$1 AND gateway_id=$2 AND timestamp=$3`, tenantSchemaName(tenantID))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, query, sensorID, gatewayID, ts); err != nil {
		t.Fatalf("cleanup delete unexpected error: %v", err)
	}
}

func tenantSchemaName(tenantID uuid.UUID) string {
	return "tenant_" + tenantID.String()
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
