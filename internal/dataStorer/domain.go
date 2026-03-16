package datastorer

import (
	"time"

	"github.com/google/uuid"
)

type SensorProfile string

const (
	Ecg                  SensorProfile = "ECG"
	EnvironmentalSensing SensorProfile = "EnvironmentalSensing"
	HealthThermometer    SensorProfile = "HealthThermometer"
	HeartRate            SensorProfile = "HeartRate"
	PulseOximeter        SensorProfile = "PulseOximeter"
)

type SensorData struct {
	SensorId  uuid.UUID
	GatewayId uuid.UUID
	TenantId  uuid.UUID
	Profile   SensorProfile
	Timestamp time.Time
	Data      []byte
}

type StoreDataUseCase interface {
	StoreData(data []*SensorData, tenantId uuid.UUID) error
}

type WriteDataPort interface {
	WriteData(data []*SensorData, tenantId uuid.UUID) error
}
