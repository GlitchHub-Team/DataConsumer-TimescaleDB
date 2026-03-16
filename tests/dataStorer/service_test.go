package datastorer_test

import (
	"errors"
	"testing"

	datastorer "DataConsumer/internal/dataStorer"

	"github.com/google/uuid"
)

type mockWriteDataPort struct {
	called   bool
	received []*datastorer.SensorData
	err      error
}

func (m *mockWriteDataPort) WriteData(data []*datastorer.SensorData, tenantId uuid.UUID) error {
	m.called = true
	m.received = data
	return m.err
}

func TestStoreData_ReturnsNil_WhenWriteDataSucceeds(t *testing.T) {
	mockPort := &mockWriteDataPort{err: nil}
	service := datastorer.NewStoreDataService(mockPort)

	data := []*datastorer.SensorData{}
	tenantId := uuid.New()
	err := service.StoreData(data, tenantId)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !mockPort.called {
		t.Fatal("expected WriteData to be called")
	}
	if len(mockPort.received) != len(data) {
		t.Fatalf("expected %d items passed to WriteData, got %d", len(data), len(mockPort.received))
	}
}

func TestStoreData_ReturnsErr_WhenWriteDataFails(t *testing.T) {
	expectedErr := errors.New("write failed")
	mockPort := &mockWriteDataPort{err: expectedErr}
	service := datastorer.NewStoreDataService(mockPort)

	tenantId := uuid.New()
	err := service.StoreData(nil, tenantId)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if !mockPort.called {
		t.Fatal("expected WriteData to be called")
	}
}
