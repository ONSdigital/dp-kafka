package kafkatest

import (
	kafka "github.com/ONSdigital/dp-kafka"
)

// NewMessageConsumer creates a testing consumer with new consumerGroupChannels
func NewMessageConsumer() *MessageConsumer {
	return NewMessageConsumerWithChannels(kafka.CreateConsumerGroupChannels(true))
}

// NewMessageConsumerWithChannels creates a testing consumer with the provided consumerGroupChannels
func NewMessageConsumerWithChannels(cgChannels kafka.ConsumerGroupChannels) *MessageConsumer {
	return &MessageConsumer{cgChannels}
}

// MessageConsumer is a mock that provides the stored schema channel.
type MessageConsumer struct {
	cgChannels kafka.ConsumerGroupChannels
}

// Channels returns the stored channels
func (consumer *MessageConsumer) Channels() *kafka.ConsumerGroupChannels {
	return &consumer.cgChannels
}

// CommitAndRelease commits the message, releases the listener to consume next
func (consumer *MessageConsumer) CommitAndRelease(m kafka.Message) {
	m.Commit()
	return
}
