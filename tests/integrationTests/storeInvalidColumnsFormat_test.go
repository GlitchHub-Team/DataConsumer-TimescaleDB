package integrationtests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStoreData_InvalidSensorIDFormat_NotSaved(t *testing.T) {
	runInvalidColumnFormatTest(t, `{"sensorId":"not-a-uuid","gatewayId":"%s","tenantId":"%s","profile":"heart_rate","timestamp":"%s","data":{"heart_rate":80}}`)
}

func TestStoreData_InvalidTenantIDFormat_NotSaved(t *testing.T) {
	runInvalidColumnFormatTest(t, `{"sensorId":"%s","gatewayId":"%s","tenantId":"not-a-uuid","profile":"heart_rate","timestamp":"%s","data":{"heart_rate":80}}`)
}

func TestStoreData_InvalidGatewayIDFormat_NotSaved(t *testing.T) {
	runInvalidColumnFormatTest(t, `{"sensorId":"%s","gatewayId":"not-a-uuid","tenantId":"%s","profile":"heart_rate","timestamp":"%s","data":{"heart_rate":80}}`)
}

func TestStoreData_InvalidTimestampFormat_NotSaved(t *testing.T) {
	runInvalidColumnFormatTest(t, `{"sensorId":"%s","gatewayId":"%s","tenantId":"%s","profile":"heart_rate","timestamp":"not-a-time","data":{"heart_rate":80}}`)
}

func TestStoreData_InvalidProfileFormat_NotSaved(t *testing.T) {
	runInvalidColumnFormatTest(t, `{"sensorId":"%s","gatewayId":"%s","tenantId":"%s","profile":123,"timestamp":"%s","data":{"heart_rate":80}}`)
}

func runInvalidColumnFormatTest(t *testing.T, rawJSONPattern string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sensorID := uuid.New()
	gatewayID := uuid.New()
	ts := time.Now()
	subject := buildSubject(tenantID, gatewayID, sensorID)

	h := setupRealPipeline(t, subject)
	defer cleanupInsertedRow(t, h.db, tenantID, sensorID, gatewayID, ts)

	body := []byte(fmt.Sprintf(rawJSONPattern, sensorID, gatewayID, tenantID, ts))
	publishAndFlush(t, h.jsTest, subject, body)

	if err := waitRowCount(h.db, tenantID, sensorID, gatewayID, ts, 0, 5*time.Second); err != nil {
		t.Fatalf("row should not be stored for invalid column format: %v", err)
	}
}
