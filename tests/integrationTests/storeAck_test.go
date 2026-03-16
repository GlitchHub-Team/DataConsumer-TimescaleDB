package integrationtests

import (
	"testing"
	"time"

	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
)

func TestStoreData_AckReceived_OnSuccessfulSave(t *testing.T) {
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

	body := mustBuildValidBody(t, sensorID, gatewayID, tenantID, string(datastorer.HeartRate), ts, []byte(`{"heartRate":95}`))
	publishAndFlush(t, h.js, subject, body)

	if err := waitRowCount(h.db, tenantID, sensorID, gatewayID, ts, 1, 10*time.Second); err != nil {
		t.Fatalf("row should be stored: %v", err)
	}

	if err := waitConsumerAckPending(h.consumer, 0, 15*time.Second); err != nil {
		t.Fatalf("expected ack pending to be 0 after successful save: %v", err)
	}
}
