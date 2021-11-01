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
func (M SaramaMessage) GetData() []byte {
	return M.message.Value
}

// Offset returns the message offset
func (M SaramaMessage) Offset() int64 {
	return M.message.Offset
}

// Mark marks the message as consumed, but doesn't commit the offset to the backend
func (M SaramaMessage) Mark() {
	M.session.MarkMessage(M.message, "metadata")
}

// Commit marks the message as consumed, and then commits the offset to the backend
func (M SaramaMessage) Commit() {
	M.session.MarkMessage(M.message, "metadata")
	M.session.Commit()
}

// Release closes the UpstreamDone channel, but doesn't mark the message or commit the offset
func (M SaramaMessage) Release() {
	SafeClose(M.upstreamDone)
}

// CommitAndRelease marks the message as consumed, commits the offset to the backend and releases the UpstreamDone channel
func (M SaramaMessage) CommitAndRelease() {
	M.session.MarkMessage(M.message, "metadata")
	M.session.Commit()
	SafeClose(M.upstreamDone)
}

// UpstreamDone returns the upstreamDone channel. Closing this channel notifies that the message has been consumed (same effect as calling Release)
func (M SaramaMessage) UpstreamDone() chan struct{} {
	return M.upstreamDone
}
