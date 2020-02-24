package kafkatest

import kafka "github.com/ONSdigital/dp-kafka"

// NewMessageProducer creates a testing producer with new producerChannels
func NewMessageProducer() *MessageProducer {
	return NewMessageProducerWithChannels(kafka.CreateProducerChannels())
}

// NewMessageProducerWithChannels creates a testing producer with the provided producerChannels
func NewMessageProducerWithChannels(pChannels kafka.ProducerChannels) *MessageProducer {
	return &MessageProducer{pChannels, 0}
}

// MessageProducer provides a mock that allows injection of the required output channel.
type MessageProducer struct {
	pChannels     kafka.ProducerChannels
	channelsCalls int
}

// Channels returns the stored channels
func (messageProducer *MessageProducer) Channels() *kafka.ProducerChannels {
	messageProducer.channelsCalls++
	return &messageProducer.pChannels
}

// ChannelsCalls returns the number of calls to Channels()
func (messageProducer *MessageProducer) ChannelsCalls() int {
	return messageProducer.channelsCalls
}
