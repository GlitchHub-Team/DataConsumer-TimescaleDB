package integrationtests

import (
	"encoding/json"
	"testing"
	"time"

	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
)

func TestStoreValidData_RealServices(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sensorID := uuid.New()
	gatewayID := uuid.New()
	ts := time.Now().UTC().Truncate(time.Microsecond)
	payload := json.RawMessage(`{"heart_rate":72,"status":"ok"}`)
	subject := buildSubject(tenantID, gatewayID, sensorID)

	h := setupRealPipeline(t, subject)

	defer func() {
		cleanupInsertedRow(t, h.db, tenantID, sensorID, gatewayID, ts)
	}()

	body := mustBuildValidBody(t, sensorID, gatewayID, tenantID, string(datastorer.HeartRate), ts, payload)
	publishAndFlush(t, h.jsTest, subject, body)

	storedProfile, storedPayload, err := waitStoredData(h.db, tenantID, sensorID, gatewayID, ts, 10*time.Second)
	if err != nil {
		t.Fatalf("waitStoredData() unexpected error: %v", err)
	}

	if storedProfile != string(datastorer.HeartRate) {
		t.Fatalf("stored profile got %q, want %q", storedProfile, datastorer.HeartRate)
	}

	if jsonDiff(storedPayload, []byte(payload)) {
		t.Fatalf("stored payload got %s, want %s", string(storedPayload), string(payload))
	}
}
