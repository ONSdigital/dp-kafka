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
	return &MessageConsumer{cgChannels, 0, 0}
}

// MessageConsumer is a mock that provides the stored schema channel.
type MessageConsumer struct {
	cgChannels            kafka.ConsumerGroupChannels
	channelsCalls         int
	commitAndReleaseCalls int
}

// Channels returns the stored channels
func (consumer *MessageConsumer) Channels() *kafka.ConsumerGroupChannels {
	consumer.channelsCalls++
	return &consumer.cgChannels
}

// ChannelsCalls returns the number of calls to Channels()
func (consumer *MessageConsumer) ChannelsCalls() int {
	return consumer.channelsCalls
}

// CommitAndRelease commits the message, releases the listener to consume next, notifying the UpstreamDone chanel
func (consumer *MessageConsumer) CommitAndRelease(m kafka.Message) {
	consumer.commitAndReleaseCalls++
	m.Commit()
	consumer.cgChannels.UpstreamDone <- true
}

// CommitAndReleaseCalls returns the number of calls to CommitAndRelease()
func (consumer *MessageConsumer) CommitAndReleaseCalls() int {
	return consumer.commitAndReleaseCalls
}
