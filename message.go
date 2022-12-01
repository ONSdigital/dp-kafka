package kafka

import (
	"context"

	"github.com/ONSdigital/dp-kafka/v3/interfaces"
	"github.com/Shopify/sarama"
)

type Message interfaces.Message

// SaramaMessage represents a Sarama specific Kafka message
type SaramaMessage struct {
	message      *sarama.ConsumerMessage
	session      sarama.ConsumerGroupSession
	upstreamDone chan struct{}
}

func NewSaramaMessage(m *sarama.ConsumerMessage, s sarama.ConsumerGroupSession, ud chan struct{}) *SaramaMessage {
	return &SaramaMessage{
		message:      m,
		session:      s,
		upstreamDone: ud,
	}
}

// GetData returns the message contents.
func (m SaramaMessage) GetData() []byte {
	return m.message.Value
}

// Context returns a context with traceid.
func (m SaramaMessage) Context() context.Context {
	ctx := context.Background()
	traceID := m.GetHeader(TraceIDHeaderKey)
	if traceID != "" {
		ctx = context.WithValue(ctx, TraceIDHeaderKey, traceID)
	}
	return ctx
}

// GetHeader takes a key for the header and returns the value if the key exist in the header.
func (m SaramaMessage) GetHeader(key string) string {
	for _, recordHeader := range m.message.Headers {
		if string(recordHeader.Key) == key {
			return string(recordHeader.Value)
		}
	}
	return ""
}

// Offset returns the message offset
func (m SaramaMessage) Offset() int64 {
	return m.message.Offset
}

// Mark marks the message as consumed, but doesn't commit the offset to the backend
func (m SaramaMessage) Mark() {
	m.session.MarkMessage(m.message, "metadata")
}

// Commit marks the message as consumed, and then commits the offset to the backend
func (m SaramaMessage) Commit() {
	m.session.MarkMessage(m.message, "metadata")
	m.session.Commit()
}

// Release closes the UpstreamDone channel, but doesn't mark the message or commit the offset
func (m SaramaMessage) Release() {
	SafeClose(m.upstreamDone)
}

// CommitAndRelease marks the message as consumed, commits the offset to the backend and releases the UpstreamDone channel
func (m SaramaMessage) CommitAndRelease() {
	m.session.MarkMessage(m.message, "metadata")
	m.session.Commit()
	SafeClose(m.upstreamDone)
}

// UpstreamDone returns the upstreamDone channel. Closing this channel notifies that the message has been consumed (same effect as calling Release)
func (m SaramaMessage) UpstreamDone() chan struct{} {
	return m.upstreamDone
}
