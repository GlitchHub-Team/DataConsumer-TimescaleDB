package timescale_test

import (
	"strings"
	"testing"

	"DataConsumer/cmd/external/timescale"
)

func TestNewMockPostgres(t *testing.T) {
	mp, err := timescale.NewMockPostgres()
	if err != nil {
		t.Fatalf("NewMockPostgres() unexpected error: %v", err)
	}
	if mp == nil {
		t.Fatal("NewMockPostgres() returned nil mock")
	}
	if mp.DB == nil {
		t.Fatal("mock DB should not be nil")
	}
	mp.Mock.ExpectClose()

	if err := mp.Close(); err != nil {
		t.Fatalf("Close() unexpected error: %v", err)
	}
	if err := mp.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet() unexpected error: %v", err)
	}
}

func TestMockTenantSchemas_ReturnsTwoSchemas(t *testing.T) {
	schemas := timescale.MockTenantSchemas
	if got, want := len(schemas), 2; got != want {
		t.Fatalf("MockTenantSchemas() got %d schemas, want %d", got, want)
	}
	if schemas[0] == schemas[1] {
		t.Fatal("mock tenant schemas should be distinct")
	}
}

func TestBuildTenantSchemaDDL_ReturnsSchemaAndTableStatements(t *testing.T) {
	tenantID := timescale.MockTenantSchemas[0]
	ddl := timescale.BuildTenantSchemaDDL(tenantID)
	schemaName := "tenant_" + tenantID.String()

	if got, want := len(ddl), 2; got != want {
		t.Fatalf("BuildTenantSchemaDDL() got %d statements, want %d", got, want)
	}

	if !strings.Contains(ddl[0], `CREATE SCHEMA IF NOT EXISTS "`+schemaName+`"`) {
		t.Fatalf("schema DDL does not contain tenant schema name: %s", ddl[0])
	}
	if !strings.Contains(ddl[1], `CREATE TABLE IF NOT EXISTS "`+schemaName+`".sensor_data`) {
		t.Fatalf("table DDL does not contain target table: %s", ddl[1])
	}
	if !strings.Contains(ddl[1], `data JSONB NOT NULL`) {
		t.Fatalf("table DDL does not define JSONB payload column: %s", ddl[1])
	}
}

func TestBuildMockTenantSchemaDDL_ReturnsDDLForBothSchemas(t *testing.T) {
	ddl := timescale.BuildMockTenantSchemaDDL()
	if got, want := len(ddl), 4; got != want {
		t.Fatalf("BuildMockTenantSchemaDDL() got %d statements, want %d", got, want)
	}
}
