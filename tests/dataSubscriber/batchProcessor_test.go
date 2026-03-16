package datasubscriber_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	datastorer "DataConsumer/internal/dataStorer"
	datasubscriber "DataConsumer/internal/dataSubscriber"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type storeDataUseCaseMock struct {
	storeErr error
	calls    int
	stored   [][]*datastorer.SensorData
}

func (m *storeDataUseCaseMock) StoreData(data []*datastorer.SensorData, tenantId uuid.UUID) error {
	m.calls++
	copied := make([]*datastorer.SensorData, len(data))
	copy(copied, data)
	m.stored = append(m.stored, copied)
	return m.storeErr
}

type fakeBatch struct {
	msgs []jetstream.Msg
	err  error
}

func (b fakeBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(b.msgs))
	for _, msg := range b.msgs {
		ch <- msg
	}
	close(ch)
	return ch
}

func (b fakeBatch) Error() error {
	return b.err
}

type fakeMsg struct {
	payload   []byte
	ackCount  int
	termCount int
	ackErr    error
	termErr   error
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *fakeMsg) Data() []byte                              { return m.payload }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Subject() string                           { return "" }
func (m *fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) Ack() error {
	m.ackCount++
	return m.ackErr
}
func (m *fakeMsg) DoubleAck(_ context.Context) error  { return nil }
func (m *fakeMsg) Nak() error                         { return nil }
func (m *fakeMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m *fakeMsg) InProgress() error                  { return nil }
func (m *fakeMsg) Term() error {
	m.termCount++
	return m.termErr
}
func (m *fakeMsg) TermWithReason(_ string) error { return nil }

func TestNatsBatchProcessor_ProcessBatch_StoreAndAckOnValidPayload(t *testing.T) {
	storeMock := &storeDataUseCaseMock{}
	processor := datasubscriber.NewNatsBatchProcessor(storeMock, zap.NewNop())

	tenantID := uuid.New()
	sensorID := uuid.New()
	gatewayID := uuid.New()
	ts := time.Now()
	rawData := []byte(`{"value":72}`)

	dto := datasubscriber.SensorDataDTO{
		SensorId:  sensorID,
		GatewayId: gatewayID,
		TenantId:  tenantID,
		Profile:   "HeartRate",
		Timestamp: ts,
		Data:      rawData,
	}

	encoded, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("json.Marshal() unexpected error: %v", err)
	}

	msg := &fakeMsg{payload: encoded}
	batch := fakeBatch{msgs: []jetstream.Msg{msg}}

	processor.ProcessBatch(batch)

	if got, want := storeMock.calls, 1; got != want {
		t.Errorf("StoreData() call count got %d, want %d", got, want)
	}
	if got, want := len(storeMock.stored), 1; got != want {
		t.Fatalf("stored batches got %d, want %d", got, want)
	}
	if got, want := len(storeMock.stored[0]), 1; got != want {
		t.Fatalf("stored records got %d, want %d", got, want)
	}

	stored := storeMock.stored[0][0]
	if got, want := stored.SensorId, sensorID; got != want {
		t.Errorf("SensorId got %s, want %s", got, want)
	}
	if got, want := stored.GatewayId, gatewayID; got != want {
		t.Errorf("GatewayId got %s, want %s", got, want)
	}
	if got, want := stored.TenantId, tenantID; got != want {
		t.Errorf("TenantId got %s, want %s", got, want)
	}
	if got, want := stored.Profile, datastorer.SensorProfile("HeartRate"); got != want {
		t.Errorf("Profile got %s, want %s", got, want)
	}
	if got, want := stored.Timestamp, ts; !got.Equal(want) {
		t.Errorf("Timestamp got %s, want %s", got, want)
	}
	if got, want := string(stored.Data), string(rawData); got != want {
		t.Errorf("Data got %s, want %s", got, want)
	}

	if got, want := msg.ackCount, 1; got != want {
		t.Errorf("Ack() count got %d, want %d", got, want)
	}
	if got, want := msg.termCount, 0; got != want {
		t.Errorf("Term() count got %d, want %d", got, want)
	}
}

func TestNatsBatchProcessor_ProcessBatch_TermOnInvalidPayload(t *testing.T) {
	storeMock := &storeDataUseCaseMock{}
	processor := datasubscriber.NewNatsBatchProcessor(storeMock, zap.NewNop())

	msg := &fakeMsg{payload: []byte("not-json")}
	batch := fakeBatch{msgs: []jetstream.Msg{msg}}

	processor.ProcessBatch(batch)

	if got, want := storeMock.calls, 0; got != want {
		t.Errorf("StoreData() call count got %d, want %d", got, want)
	}
	if got, want := msg.termCount, 1; got != want {
		t.Errorf("Term() count got %d, want %d", got, want)
	}
	if got, want := msg.ackCount, 0; got != want {
		t.Errorf("Ack() count got %d, want %d", got, want)
	}
}

func TestNatsBatchProcessor_ProcessBatch_TermErrorOnInvalidPayload(t *testing.T) {
	storeMock := &storeDataUseCaseMock{}
	processor := datasubscriber.NewNatsBatchProcessor(storeMock, zap.NewNop())

	msg := &fakeMsg{payload: []byte("not-json"), termErr: errors.New("term failed")}
	batch := fakeBatch{msgs: []jetstream.Msg{msg}}

	processor.ProcessBatch(batch)

	if got, want := storeMock.calls, 0; got != want {
		t.Errorf("StoreData() call count got %d, want %d", got, want)
	}
	if got, want := msg.termCount, 1; got != want {
		t.Errorf("Term() count got %d, want %d", got, want)
	}
	if got, want := msg.ackCount, 0; got != want {
		t.Errorf("Ack() count got %d, want %d", got, want)
	}
}

func TestNatsBatchProcessor_ProcessBatch_NoAckWhenStoreFails(t *testing.T) {
	storeMock := &storeDataUseCaseMock{storeErr: errors.New("insert failed")}
	processor := datasubscriber.NewNatsBatchProcessor(storeMock, zap.NewNop())

	dto := datasubscriber.SensorDataDTO{
		SensorId:  uuid.New(),
		GatewayId: uuid.New(),
		TenantId:  uuid.New(),
		Profile:   "ECG",
		Timestamp: time.Now(),
		Data:      []byte(`{"v":1}`),
	}
	encoded, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("json.Marshal() unexpected error: %v", err)
	}

	msg := &fakeMsg{payload: encoded}
	batch := fakeBatch{msgs: []jetstream.Msg{msg}}

	processor.ProcessBatch(batch)

	if got, want := storeMock.calls, 1; got != want {
		t.Errorf("StoreData() call count got %d, want %d", got, want)
	}
	if got, want := msg.ackCount, 0; got != want {
		t.Errorf("Ack() count got %d, want %d", got, want)
	}
	if got, want := msg.termCount, 0; got != want {
		t.Errorf("Term() count got %d, want %d", got, want)
	}
}
