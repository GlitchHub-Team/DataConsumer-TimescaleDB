package datasubscriber

import (
	"encoding/json"
	"time"

	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type SensorDataDTO struct {
	SensorId  uuid.UUID       `json:"sensorId"`
	GatewayId uuid.UUID       `json:"gatewayId"`
	TenantId  uuid.UUID       `json:"tenantId"`
	Profile   string          `json:"profile"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type NatsBatchProcessor struct {
	StoreDataUseCase datastorer.StoreDataUseCase
	Logger           *zap.Logger
}

func (c *NatsBatchProcessor) ProcessBatch(batch jetstream.MessageBatch) {
	tenantGroups := make(map[uuid.UUID][]*datastorer.SensorData)
	natsMessages := make(map[uuid.UUID][]jetstream.Msg)

	count := 0
	for msg := range batch.Messages() {
		var data SensorDataDTO
		if err := json.Unmarshal(msg.Data(), &data); err != nil {
			if err := msg.Term(); err != nil {
				c.Logger.Warn("Errore nel dire al server di non reinviare il messaggio", zap.Error(err))
			}
			continue
		}

		sensorData := &datastorer.SensorData{
			SensorId:  data.SensorId,
			GatewayId: data.GatewayId,
			TenantId:  data.TenantId,
			Profile:   datastorer.SensorProfile(data.Profile),
			Timestamp: data.Timestamp,
			Data:      data.Data,
		}

		tenantGroups[sensorData.TenantId] = append(tenantGroups[sensorData.TenantId], sensorData)
		natsMessages[sensorData.TenantId] = append(natsMessages[sensorData.TenantId], msg)
		count++
	}

	if count == 0 {
		return
	}

	for tenantId, dataList := range tenantGroups {
		err := c.StoreDataUseCase.StoreData(dataList, tenantId)

		if err != nil {
			c.Logger.Error("Errore durante l'inserimento massivo per tenant",
				zap.String("tenant", tenantId.String()), zap.Error(err))
		} else {
			for _, m := range natsMessages[tenantId] {
				if err := m.Ack(); err != nil {
					c.Logger.Warn("Impossibile inviare Ack", zap.Error(err))
				}
			}
			c.Logger.Info("Batch inserito con successo", zap.String("tenant", tenantId.String()), zap.Int("numSensorData", len(dataList)))
		}
	}
}

func NewNatsBatchProcessor(
	storeDataUseCase datastorer.StoreDataUseCase,
	logger *zap.Logger,
) *NatsBatchProcessor {
	return &NatsBatchProcessor{
		StoreDataUseCase: storeDataUseCase,
		Logger:           logger,
	}
}
