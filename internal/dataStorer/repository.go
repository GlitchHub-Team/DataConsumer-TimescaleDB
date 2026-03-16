package datastorer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"DataConsumer/cmd/external/timescale"

	"github.com/google/uuid"
)

type TimescaleWriteDataRepository struct {
	ctx          context.Context
	dbConnection *sql.DB
}

func NewTimescaleWriteDataRepository(dbConnection timescale.TimescaleDBConnection, ctx context.Context) *TimescaleWriteDataRepository {
	return &TimescaleWriteDataRepository{
		ctx:          ctx,
		dbConnection: dbConnection,
	}
}

func (r *TimescaleWriteDataRepository) WriteData(data []*SensorData, tenantId uuid.UUID) error {
	if len(data) == 0 {
		return nil
	}

	var b strings.Builder
	b.WriteString(`INSERT INTO "`)
	b.WriteString(tenantId.String())
	b.WriteString(`".sensor_data (sensor_id, gateway_id, tenant_id, profile, timestamp, data) VALUES `)

	argNum := 6
	base := 1

	args := make([]any, 0, len(data)*argNum)
	for i, item := range data {
		if i > 0 {
			b.WriteString(",")
		}

		b.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", base, base+1, base+2, base+3, base+4, base+5))
		base += argNum

		args = append(args,
			item.SensorId,
			item.GatewayId,
			item.TenantId,
			item.Profile,
			item.Timestamp,
			item.Data,
		)
	}

	_, err := r.dbConnection.ExecContext(r.ctx, b.String(), args...)
	return err
}
