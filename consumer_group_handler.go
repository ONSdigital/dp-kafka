package kafka

import (
	"context"
	"errors"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
)

// saramaCgHandler consumer-group handler used by Sarama as a callback receiver
type saramaCgHandler struct {
	ctx                context.Context
	channels           *ConsumerGroupChannels // Channels are shared with ConsumerGroup
	state              *ConsumerState         // State is shared with ConsumerGroup
	chSessionConsuming chan struct{}          // aux channel that will be created on each session, before ConsumeClaim, and destroyed when the session ends for any reason
}

func NewSaramaCgHandler(ctx context.Context, channels *ConsumerGroupChannels, state *ConsumerState) *saramaCgHandler {
	return &saramaCgHandler{
		ctx:      ctx,
		channels: channels,
		state:    state,
	}
}

// Setup is run by Sarama at the beginning of a new session, before ConsumeClaim.
// It will close the Ready channel if it is not already closed.
func (sh *saramaCgHandler) Setup(session sarama.ConsumerGroupSession) error {
	if *sh.state != Starting && *sh.state != Consuming {
		return errors.New("wrong state to consume")
	}
	log.Info(session.Context(), "sarama consumer group session setup ok: a new go-routine will be created for each partition assigned to this consumer", log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	*sh.state = Consuming

	// Create a new chConsuming and a control go-routine
	// which closes chConsuming when we need to stop consuming for any reason
	// it sets the state according to the reason
	sh.chSessionConsuming = make(chan struct{})
	go sh.controlRoutine()
	return nil
}

// controlRoutine waits until we need to stop consuming for any reason,
// and then it closes sh.shConsuming channel so that we will stop consuming new messages
// - Closer channel closed: set state to 'Closing' and stop consuming
// - Consume channel closed: set state to 'Closing' and stop consuming
// - Received 'false' from consume channel: set state to 'Stoppig' and stop consuming
// - shConsuming channel closed: abort control routine and stop consuming
func (sh *saramaCgHandler) controlRoutine() {
	defer func() {
		select {
		case <-sh.chSessionConsuming:
		default:
			close(sh.chSessionConsuming)
		}
	}()

	for {
		select {
		case <-sh.channels.Closer: // consumer group is closing (valid scenario)
			*sh.state = Closing
			return
		case consume, ok := <-sh.channels.Consume:
			if !ok { // Consume channel is closed, so we should not be consuming and the consumer group is closing
				*sh.state = Closing
				return
			}
			if !consume { // Consume channel notifies that we should stop consuming new messages
				*sh.state = Stopping
				return
			}
		case <-sh.chSessionConsuming:
			return // if chConsuming is closed, this go-routine must exit
		}
	}
}

// Cleanup is run by Sarama at the end of a session, once all ConsumeClaim goroutines have exited
func (sh *saramaCgHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Info(session.Context(), "sarama consumer group session cleanup finished: all go-routines have completed", log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	// close sh.chConsuming if it was not already closed, to make sure that the control go-routine finishes
	select {
	case <-sh.chSessionConsuming:
	default:
		close(sh.chSessionConsuming)
	}

	// if state is still consuming, set it back to starting, as we are currently not consuming until the next session is alive
	// Note: if the state is something else, we don't want to change it (e.g. the consumer might be stopping or closing)
	if *sh.state == Consuming {
		*sh.state = Starting
	}

	return nil
}

// ConsumeClaim is a callback called by Sarama in order to consume messages.
// Messages are consumed by starting a loop for ConsumerGroupClaim's Messages(),
// so that all messages sent to the partition corresponding to this ConsumeClaim call
// are consumed and forwarded to the upstream service.
//
// Sarama creates T*P(T) go-routines, where T is the number of topics and P(T) is the number of partitions per topic,
// expecting each consumer to be assigned T*P(T)/N(T), where N(T) is the number of consumers for a particular topic.
//
// Each go-routine will send a message to the shared Upstream channel,
// and then wait for the message specific upstreamDone channel to be closed.
func (sh *saramaCgHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sh.chSessionConsuming: // when chConsuming is closed, we need to stop consuming
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil // claim ConsumerMessage channel is closed, stop consuming
			}
			// new message available to be consumed
			sh.consumeMessage(SaramaMessage{message, session, make(chan struct{})})
		}
	}
}

// consumeMessage sends the message to the consumer Upstream channel, and waits for upstream done.
// Note that this doesn't make the consumer synchronous: we still have other go-routines processing messages.
func (sh *saramaCgHandler) consumeMessage(msg SaramaMessage) {
	select {
	case sh.channels.Upstream <- msg: // Send message to Upsream channel to be consumed by the app
		<-msg.upstreamDone // Wait until the message is released
		return             // Message has been released
	case <-sh.chSessionConsuming:
		return // chConsuming is closed before the app reads Upstream channel, we need to stop consuming new messages now
	}
}
