package integrationtests

import (
	"testing"
	"time"

	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
)

func TestStoreData_DuplicatedPublish_NotAllowed(t *testing.T) {
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

	body := mustBuildValidBody(t, sensorID, gatewayID, tenantID, string(datastorer.HeartRate), ts, []byte(`{"heartRate":66}`))
	publishAndFlush(t, h.js, subject, body)
	publishAndFlush(t, h.js, subject, body)

	if err := waitRowCount(h.db, tenantID, sensorID, gatewayID, ts, 1, 10*time.Second); err != nil {
		t.Fatalf("duplicate publish should not create a second row: %v", err)
	}
}
