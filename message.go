package etl

import "time"

type Message interface {
	Payload() interface{}
	CreatedAt() time.Time
	ProcessingStartedAt() time.Time
}

type message struct {
	payload interface{}

	createdAt           time.Time
	processingStartedAt time.Time
}

func NewMessage(payload interface{}, optsSetters ...MessageOption) Message {
	msg := &message{
		payload:             payload,
		createdAt:           time.Now(),
		processingStartedAt: time.Now(),
	}

	applyMessageOptions(msg, optsSetters...)

	return msg
}

func (m *message) Payload() interface{} {
	return m.payload
}

func (m *message) CreatedAt() time.Time {
	return m.createdAt
}

func (m *message) ProcessingStartedAt() time.Time {
	return m.processingStartedAt
}
