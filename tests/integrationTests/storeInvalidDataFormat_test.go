package integrationtests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStoreData_InvalidDataFormat_NotSaved(t *testing.T) {
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

	body := []byte(fmt.Sprintf(`{"sensorId":"%s","gatewayId":"%s","tenantId":"%s","profile":"HeartRate","timestamp":"%s","data":{"heartRate":}`, sensorID, gatewayID, tenantID, ts))
	publishAndFlush(t, h.js, subject, body)

	if err := waitRowCount(h.db, tenantID, sensorID, gatewayID, ts, 0, 5*time.Second); err != nil {
		t.Fatalf("row should not be stored for invalid data format: %v", err)
	}
}
