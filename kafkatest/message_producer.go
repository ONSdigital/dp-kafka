package kafkatest

import kafka "github.com/ONSdigital/dp-kafka"

// NewMessageProducer creates a testing producer with new producerChannels
func NewMessageProducer() *MessageProducer {
	return &MessageProducer{
		pChannels: kafka.CreateProducerChannels(),
	}
}

// MessageProducer provides a mock that allows injection of the required output channel.
type MessageProducer struct {
	pChannels kafka.ProducerChannels
}

// Channels returns the stored channels
func (messageProducer *MessageProducer) Channels() *kafka.ProducerChannels {
	return &messageProducer.pChannels
}
