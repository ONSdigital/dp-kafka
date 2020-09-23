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
	select {
	case <-sh.channels.Ready:
	default:
		close(sh.channels.Ready)
	}
	return nil
}

// Cleanup is run by Sarama at the end of a session, once all ConsumeClaim goroutines have exited
func (sh *saramaCgHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "sarama consumer group session cleanup finished: all go-routines have completed", log.INFO, log.Data{"memberID": session.MemberID(), "claims": session.Claims()})
	return nil
}

// ConsumeClaim is a callback called by Sarama in order to consume messages.
// Messages are consumed by starting a loop for ConsumerGroupClaim's Messages(),
// so that all messages sent to the partition corresponding to this ConsumeClaim call
// are consumed and forwarded to the upstream service.
//
// Sarama creates T*P(T) go-routines, where T is the number of topics and P(T) is the number of partitions per topic,
// expecting each consumer to be assigned P(T)/N(T), where N(T) is the number of consumers for a particular topic.
//
// Each go-routine will send a message to the shared Upstream channel,
// and then wait for the message specific upstreamDone channel to be closed.
func (sh *saramaCgHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		select {
		case <-sh.channels.Closer:
			log.Event(sh.ctx, "closed kafka consumer consume claim go-routine via closer channel", log.INFO)
			return nil
		default:
			return sh.consumeMessage(SaramaMessage{message, session, make(chan struct{})})
		}
	}
	return nil
}

// consumeMessage sends the message to the consumer Upstream channel, and waits for upstream done.
// Note that this doesn't make the consumer synchronous: we still have other go-routines processing messages.
func (sh *saramaCgHandler) consumeMessage(msg SaramaMessage) error {
	sh.channels.Upstream <- msg
	<-msg.upstreamDone
	return nil
}
