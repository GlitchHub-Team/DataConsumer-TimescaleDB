package timescale

import (
	"database/sql"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
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
	schemaName := tenantID.String()

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
