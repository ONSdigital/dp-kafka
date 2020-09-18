package kafka

import (
	"context"

	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// saramaCgHandler consumer-group handler used by sarama as a callback receiver
type saramaCgHandler struct {
	ctx      context.Context
	channels *ConsumerGroupChannels
}

// Setup is run by Sarama at the beginning of a new session, before ConsumeClaim.
func (sh *saramaCgHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "sarama consumer group session setup ok: a new go-routine will be created for each partition assigned to this consumer", log.INFO, log.Data{"memberID": session.MemberID(), "claims": session.Claims()})
	close(sh.channels.Ready)
	return nil
}

// Cleanup is run by Sarama at the end of a session, once all ConsumeClaim goroutines have exited
func (sh *saramaCgHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "sarama consumer group session cleanup finished: all go-routines have completed", log.INFO, log.Data{"memberID": session.MemberID(), "claims": session.Claims()})
	return nil
}

// ConsumeClaim is a callback called by Sarama in order to consume messages.
// Messages are consumed by starting a loop for ConsumerGroupClaim's Messages(),
// so that all messages sent to the partition corresponding to this ConsumeClaim call are consumed and forwarded to the upstream service.
//
// Sarama creates T*P(T) go-routines, where T is the number of topics and P(T) is the number of partitions per topic.
// In order to allow for consumer synchronization, a waitgroup is used (if syncConsume is enabled).
// Neverthelesse, the preferred consumption method would be to disable syncConsume and create a reasonable number of partitions per topic,
// expecting each consumer to be assigned P(T)/N(T), where N(T) is the number of consumers for a particular topic.
func (sh *saramaCgHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		select {
		case <-sh.channels.Closer:
			log.Event(sh.ctx, "closed kafka consumer ConsumeClaim go-routine via closer channel", log.INFO)
			return nil
		case <-sh.ctx.Done():
			log.Event(sh.ctx, "closed kafka consumer ConsumeClaim go-routine via context done", log.INFO)
			return nil
		// TODO we should check if there was a rebalance, and stop consuming too!
		default:
			log.Event(nil, "message claimed", log.INFO, log.Data{"value": string(message.Value), "messageOffset": message.Offset, "topic": message.Topic, "partition": message.Partition})
			return sh.consumeMessage(SaramaMessage{message, session, make(chan struct{})})
		}
	}
	return nil
}

// consumeMessage sends the message to the consumer Upstream channel, and waits for upstream done.
// Note that this doesn't make the consumer synchronous: we still have other go-routines processing messages.
// What makes a consumer synchronous is the Upstream channel buffer size. Please, change the Upstream buffer size
// if you want to control the number of concurrent messages to consume.
func (sh *saramaCgHandler) consumeMessage(msg SaramaMessage) error {
	sh.channels.Upstream <- msg
	<-msg.upstreamDone
	return nil
}
