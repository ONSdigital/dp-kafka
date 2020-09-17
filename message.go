package kafka

import (
	"github.com/Shopify/sarama"
)

//go:generate moq -out ./kafkatest/mock_message.go -pkg kafkatest . Message

// Message represents a single kafka message.
type Message interface {

	// GetData returns the message contents.
	GetData() []byte

	// Commit the message's offset.
	Commit()

	// Offset returns the message offset
	Offset() int64
}

// SaramaMessage represents a Sarama specific Kafka message
type SaramaMessage struct {
	message *sarama.ConsumerMessage
	session sarama.ConsumerGroupSession
}

// GetData returns the message contents.
func (M SaramaMessage) GetData() []byte {
	return M.message.Value
}

// Offset returns the message offset
func (M SaramaMessage) Offset() int64 {
	return M.message.Offset
}

// Commit marks a message as consumed, and then commits the offset to the backend
func (M SaramaMessage) Commit() {
	M.session.MarkMessage(M.message, "metadata")
	M.session.Commit()
}
