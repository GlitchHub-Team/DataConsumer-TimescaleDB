package datastorer

import "github.com/google/uuid"

type StoreDataService struct {
	writeDataPort WriteDataPort
}

func (s *StoreDataService) StoreData(data []*SensorData, tenantId uuid.UUID) error {
	return s.writeDataPort.WriteData(data, tenantId)
}

func NewStoreDataService(writeDataPort WriteDataPort) *StoreDataService {
	return &StoreDataService{
		writeDataPort: writeDataPort,
	}
}
