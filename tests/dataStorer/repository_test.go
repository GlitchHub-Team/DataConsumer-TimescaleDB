package datastorer_test

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"DataConsumer/cmd/external/timescale"
	datastorer "DataConsumer/internal/dataStorer"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestTimescaleWriteDataRepository_WriteData_EmptyBatchReturnsNil(t *testing.T) {
	repo, mockPg := newRepositoryWithMock(t)

	if err := repo.WriteData([]*datastorer.SensorData{}, uuid.New()); err != nil {
		t.Fatalf("WriteData(nil) unexpected error: %v", err)
	}

	if err := mockPg.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL calls for empty batch: %v", err)
	}
}

func TestTimescaleWriteDataRepository_WriteData_ReturnsErrorOnMissingSchema(t *testing.T) {
	repo, mockPg := newRepositoryWithMock(t)
	tenantID := uuid.New()
	data := singleDataForTenant(tenantID)

	query := `INSERT INTO "` + tenantID.String() + `".sensor_data (sensor_id, gateway_id, tenant_id, profile, timestamp, data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (sensor_id, gateway_id, timestamp) DO NOTHING`
	mockPg.Mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(data[0].SensorId, data[0].GatewayId, data[0].TenantId, data[0].Profile, data[0].Timestamp, data[0].Data).
		WillReturnError(errors.New(`pq: schema "` + tenantID.String() + `" does not exist`))

	err := repo.WriteData(data, tenantID)
	if err == nil {
		t.Fatal("WriteData() expected error for missing schema, got nil")
	}

	if err := mockPg.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestTimescaleWriteDataRepository_WriteData_SucceedsOnDefinedMockSchemas(t *testing.T) {
	repo, mockPg := newRepositoryWithMock(t)
	schemas := timescale.MockTenantSchemas

	for _, tenantID := range schemas {
		data := singleDataForTenant(tenantID)
		query := `INSERT INTO "` + tenantID.String() + `".sensor_data (sensor_id, gateway_id, tenant_id, profile, timestamp, data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (sensor_id, gateway_id, timestamp) DO NOTHING`

		mockPg.Mock.ExpectExec(regexp.QuoteMeta(query)).
			WithArgs(data[0].SensorId, data[0].GatewayId, data[0].TenantId, data[0].Profile, data[0].Timestamp, data[0].Data).
			WillReturnResult(sqlmock.NewResult(0, 1))

		if err := repo.WriteData(data, tenantID); err != nil {
			t.Fatalf("WriteData() unexpected error for schema %s: %v", tenantID.String(), err)
		}
	}

	if err := mockPg.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestTimescaleWriteDataRepository_WriteData_ReturnsErrorOnInvalidJSONBytes(t *testing.T) {
	repo, mockPg := newRepositoryWithMock(t)
	tenantID := timescale.MockTenantSchemas[0]

	invalidJSONData := []*datastorer.SensorData{
		{
			SensorId:  uuid.New(),
			GatewayId: uuid.New(),
			TenantId:  tenantID,
			Profile:   datastorer.HeartRate,
			Timestamp: time.Unix(2000, 0).UTC(),
			Data:      []byte(`{"value":`),
		},
	}

	query := `INSERT INTO "` + tenantID.String() + `".sensor_data (sensor_id, gateway_id, tenant_id, profile, timestamp, data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (sensor_id, gateway_id, timestamp) DO NOTHING`
	mockPg.Mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(
			invalidJSONData[0].SensorId,
			invalidJSONData[0].GatewayId,
			invalidJSONData[0].TenantId,
			invalidJSONData[0].Profile,
			invalidJSONData[0].Timestamp,
			invalidJSONData[0].Data,
		).
		WillReturnError(errors.New(`pq: invalid input syntax for type json`))

	err := repo.WriteData(invalidJSONData, tenantID)
	if err == nil {
		t.Fatal("WriteData() expected error for invalid JSON bytes, got nil")
	}

	if err := mockPg.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestTimescaleWriteDataRepository_WriteData_IgnoresDuplicatePrimaryKey(t *testing.T) {
	repo, mockPg := newRepositoryWithMock(t)
	tenantID := timescale.MockTenantSchemas[0]

	duplicateRecord := []*datastorer.SensorData{
		{
			SensorId:  uuid.New(),
			GatewayId: uuid.New(),
			TenantId:  tenantID,
			Profile:   datastorer.HeartRate,
			Timestamp: time.Unix(3000, 0).UTC(),
			Data:      []byte(`{"v":99}`),
		},
	}

	query := `INSERT INTO "` + tenantID.String() + `".sensor_data (sensor_id, gateway_id, tenant_id, profile, timestamp, data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (sensor_id, gateway_id, timestamp) DO NOTHING`
	mockPg.Mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(
			duplicateRecord[0].SensorId,
			duplicateRecord[0].GatewayId,
			duplicateRecord[0].TenantId,
			duplicateRecord[0].Profile,
			duplicateRecord[0].Timestamp,
			duplicateRecord[0].Data,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := repo.WriteData(duplicateRecord, tenantID)
	if err != nil {
		t.Fatalf("WriteData() unexpected error on duplicate key with ON CONFLICT: %v", err)
	}

	if err := mockPg.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newRepositoryWithMock(t *testing.T) (*datastorer.TimescaleWriteDataRepository, *timescale.MockPostgres) {
	t.Helper()

	ctx := context.Background()
	mockPg, err := timescale.NewMockPostgres()
	if err != nil {
		t.Fatalf("NewMockPostgres() unexpected error: %v", err)
	}

	t.Cleanup(func() {
		mockPg.Mock.ExpectClose()
		_ = mockPg.Close()
	})

	repo := datastorer.NewTimescaleWriteDataRepository(timescale.NewTimescaleMockDBConnection(mockPg), ctx)
	return repo, mockPg
}

func singleDataForTenant(tenantID uuid.UUID) []*datastorer.SensorData {
	return []*datastorer.SensorData{
		{
			SensorId:  uuid.New(),
			GatewayId: uuid.New(),
			TenantId:  tenantID,
			Profile:   datastorer.HeartRate,
			Timestamp: time.Unix(1000, 0).UTC(),
			Data:      []byte(`{"v":1}`),
		},
	}
}
