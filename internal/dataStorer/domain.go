package datastorer

import (
	"time"

	"github.com/google/uuid"
)

type SensorProfile string

const (
	Ecg                  SensorProfile = "ecg_custom"
	EnvironmentalSensing SensorProfile = "environmental_sensing"
	HealthThermometer    SensorProfile = "health_thermometer"
	HeartRate            SensorProfile = "heart_rate"
	PulseOximeter        SensorProfile = "pulse_oximeter"
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
