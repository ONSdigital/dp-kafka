package kafka

import (
	"github.com/Shopify/sarama"
)

//go:generate moq -out ./mock/message.go -pkg mock . Message

// Message represents a single kafka message.
type Message interface {

	// GetData returns the message contents.
	GetData() []byte

	// Mark marks the message as consumed, but doesn't commit the offset to the backend
	Mark()

	// Commit marks the message as consumed and commits its offset to the backend
	Commit()

	// Release closes the UpstreamDone channel for this message
	Release()

	// CommitAndRelease marks a message as consumed, commits it and closes the UpstreamDone channel
	CommitAndRelease()

	// Offset returns the message offset
	Offset() int64

	// UpstreamDone returns the upstreamDone channel. Closing this channel notifies that the message has been consumed
	UpstreamDone() chan struct{}
}

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
