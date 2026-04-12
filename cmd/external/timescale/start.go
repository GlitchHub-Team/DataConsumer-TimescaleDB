package timescale

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type TimescaleDBConnection *sql.DB

type MockPostgres struct {
	DB   *sql.DB
	Mock sqlmock.Sqlmock
}

type (
	TimescaleAddress  string
	TimescalePort     int
	TimescaleUsername string
	TimescalePassword string
	TimescaleDBName   string
)

const SensorDataTableName = "sensor_data"

func NewTimescaleDBConnection(addr TimescaleAddress, port TimescalePort, user TimescaleUsername, pass TimescalePassword, dbname TimescaleDBName) (TimescaleDBConnection, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		addr, port, user, pass, dbname,
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("impossibile aprire connessione Postgres: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("impossibile raggiungere Postgres: %w", err)
	}
	return db, nil
}

var MockTenantSchemas = []uuid.UUID{
	mustParseUUID("11111111-1111-1111-1111-111111111111"),
	mustParseUUID("22222222-2222-2222-2222-222222222222"),
}

func NewMockPostgres() (*MockPostgres, error) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		return nil, err
	}

	return &MockPostgres{
		DB:   db,
		Mock: mock,
	}, nil
}

func NewTimescaleMockDBConnection(mock *MockPostgres) TimescaleDBConnection {
	return mock.DB
}

func BuildTenantSchemaDDL(tenantID uuid.UUID) []string {
	schemaName := tenantSchemaName(tenantID)

	return []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, schemaName),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s".%s (
						sensor_id UUID NOT NULL,
						gateway_id UUID NOT NULL,
						timestamp TIMESTAMPTZ NOT NULL,
						tenant_id UUID NOT NULL,
						profile VARCHAR(255) NOT NULL,
						data JSONB NOT NULL,
						PRIMARY KEY (sensor_id, gateway_id, timestamp)
					)`, schemaName, SensorDataTableName),
	}
}

func tenantSchemaName(tenantID uuid.UUID) string {
	return "tenant_" + tenantID.String()
}

func BuildMockTenantSchemaDDL() []string {
	statements := make([]string, 0, len(MockTenantSchemas)*2)
	for _, tenantID := range MockTenantSchemas {
		statements = append(statements, BuildTenantSchemaDDL(tenantID)...)
	}
	return statements
}

func (m *MockPostgres) Close() error {
	if m == nil || m.DB == nil {
		return nil
	}
	return m.DB.Close()
}

func mustParseUUID(value string) uuid.UUID {
	id, err := uuid.Parse(value)
	if err != nil {
		panic(err)
	}
	return id
}
