package kafka

import (
	"context"
	"errors"

	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// saramaCgHandler consumer-group handler used by Sarama as a callback receiver
type saramaCgHandler struct {
	ctx         context.Context
	channels    *ConsumerGroupChannels // Channels are shared with ConsumerGroup
	state       *ConsumerState         // State is shared with ConsumerGroup
	chConsuming chan struct{}          // aux channel that will be created on each session, before ConsumeClaim
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
	log.Event(session.Context(), "sarama consumer group session setup ok: a new go-routine will be created for each partition assigned to this consumer", log.INFO, log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	// close Ready channel (if it is not already closed)
	select {
	case <-sh.channels.Ready:
	default:
		close(sh.channels.Ready)
	}

	*sh.state = Consuming

	// Create a new chConsuming and a control go-routine
	// which closes chConsuming when we need to stop consuming for any reason
	// it sets the state according to the reason
	sh.chConsuming = make(chan struct{})
	go func() {
		defer func() {
			select {
			case <-sh.chConsuming:
			default:
				close(sh.chConsuming)
			}
		}()

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
		case <-sh.chConsuming:
			return // if chConsuming is closed, this go-routine must exit
		}
	}()

	return nil
}

// Cleanup is run by Sarama at the end of a session, once all ConsumeClaim goroutines have exited
func (sh *saramaCgHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "sarama consumer group session cleanup finished: all go-routines have completed", log.INFO, log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	// close sh.chConsuming if it was not already closed, to make sure that the control go-routine finishes
	select {
	case <-sh.chConsuming:
	default:
		close(sh.chConsuming)
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
		case <-sh.chConsuming: // when chConsuming is closed, we need to stop consuming
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
	case <-sh.chConsuming:
		return // chConsuming is closed, we need to stop consuming
	}
}
