package integrationtests

import (
	"testing"
	"time"

	"DataConsumer/cmd/external/timescale"
	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
)

func TestStoreData_InvalidTenantID_NotSaved(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	tenantID := randomNonExistingTenantID()
	sensorID := uuid.New()
	gatewayID := uuid.New()
	ts := time.Now()
	subject := buildSubject(tenantID, gatewayID, sensorID)

	h := setupRealPipeline(t, subject)
	body := mustBuildValidBody(t, sensorID, gatewayID, tenantID, string(datastorer.HeartRate), ts, []byte(`{"heart_rate":77}`))
	publishAndFlush(t, h.jsTest, subject, body)

	for _, knownTenant := range timescale.MockTenantSchemas {
		if err := waitRowCount(h.db, knownTenant, sensorID, gatewayID, ts, 0, 5*time.Second); err != nil {
			t.Fatalf("row should not be stored in tenant schema %s: %v", knownTenant.String(), err)
		}
	}
}
