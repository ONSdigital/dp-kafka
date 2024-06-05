package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/ONSdigital/log.go/v2/log"
)

// SaramaHandler is a consumer-group handler used by Sarama as a callback receiver
// to setup/cleanup sessions and consume messages
type SaramaHandler struct {
	ctx       context.Context
	channels  *ConsumerGroupChannels // Channels are shared with ConsumerGroup
	state     *StateMachine          // State is shared with ConsumerGroup
	settingUp *StateChan             // aux channel that will be created on each session, before ConsumeClaim, and destroyed when the session ends
}

func newSaramaHandler(ctx context.Context, channels *ConsumerGroupChannels, state *StateMachine) *SaramaHandler {
	return &SaramaHandler{
		ctx:       ctx,
		channels:  channels,
		state:     state,
		settingUp: NewStateChan(),
	}
}

// Setup is run by Sarama at the beginning of a new session, before ConsumeClaim. The following actions are performed:
// - Set state to 'Consuming' (only if the state was Starting or Consuming - fail otherwise)
// - Create a new SessionConsuming channel and start the control go-routine
func (sh *SaramaHandler) Setup(session sarama.ConsumerGroupSession) error {
	if err := sh.state.SetIf([]State{Starting, Consuming}, Consuming); err != nil {
		return fmt.Errorf("wrong state to start consuming: %w", err)
	}
	log.Info(session.Context(), "kafka consumer group is consuming: sarama consumer group session setup ok: a new go-routine will be created for each partition assigned to this consumer", log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	sh.enterSession()
	go sh.controlRoutine()
	return nil
}

// controlRoutine waits until we need to stop consuming for any reason,
// and then it closes sh.shConsuming channel so that we will stop consuming new messages
// - Closer channel closed: set state to 'Closing' and stop consuming
// - Consume channel closed: set state to 'Closing' and stop consuming
// - Received 'false' from consume channel: set state to 'Stopping' and stop consuming
// - sessionFinished: abort control routine and stop consuming
//
// note: this func should only be executed after enterSession()
func (sh *SaramaHandler) controlRoutine() {
	for {
		select {
		case <-sh.channels.Closer: // consumer group is closing (valid scenario)
			sh.state.Set(Closing)
			sh.leaveSession()
			return
		case consume, ok := <-sh.channels.Consume:
			if !ok { // Consume channel is closed, so we should not be consuming and the consumer group is closing
				sh.state.Set(Closing)
				sh.leaveSession()
				return
			}
			if !consume { // Consume channel notifies that we should stop consuming new messages
				sh.state.Set(Stopping)
				sh.leaveSession()
				return
			}
		case <-sh.sessionFinished():
			return
		}
	}
}

// Cleanup is run by Sarama at the end of a session, once all ConsumeClaim goroutines have exited.
// - Close SessionConsuming channel
// - Set state to 'Starting' (only if it was consuming)
func (sh *SaramaHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Info(session.Context(), "kafka consumer group has finished consuming: sarama consumer group session cleanup finished: all go-routines have completed", log.Data{"memberID": session.MemberID(), "claims": session.Claims()})

	// close sh.chConsuming if it was not already closed, to make sure that the control go-routine finishes
	sh.leaveSession()

	// if state is still consuming, set it back to starting, as we are currently not consuming until the next session is alive
	// Note: if the state is something else, we don't want to change it (e.g. the consumer might be stopping or closing)
	// hence, if there is a transition error, it will be logged with info severity, but not propagated.
	if err := sh.state.SetIf([]State{Consuming}, Starting); err != nil {
		log.Info(sh.ctx, "Sarama session Cleanup done, with no state transition", log.Data{"err": err.Error(), "state": sh.state.Get().String()})
	} else {
		log.Info(sh.ctx, "Sarama session Cleanup done, with state transition", log.Data{"state": sh.state.Get().String()})
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
//
// note: this func should only be executed after enterSession()
func (sh *SaramaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sh.sessionFinished(): // stop consuming
			return nil
		case m, ok := <-claim.Messages():
			if !ok {
				return nil // claim ConsumerMessage channel is closed, stop consuming
			}
			// new message available to be consumed
			if err := sh.consumeMessage(NewSaramaMessage(m, session, make(chan struct{}))); err != nil {
				return fmt.Errorf("error consuming message: %w", err)
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

// consumeMessage sends the message to the consumer Upstream channel, and waits for upstream done.
// Note that this doesn't make the consumer synchronous: we still have other go-routines processing messages.
//
// note: this func should only be executed after enterSession()
func (sh *SaramaHandler) consumeMessage(msg *SaramaMessage) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send sarama message to upstream channel: %v", pErr)
		}
	}()

	select {
	//nolint
	case sh.channels.Upstream <- msg: // Send message to Upsream channel to be consumed by the app
		<-msg.UpstreamDone() // Wait until the message is released
		return nil           // Message has been released
	case <-sh.sessionFinished():
		return nil // session finished before the app reads Upstream channel, we need to stop consuming new messages now
	}
}

// enterSession leaves the settingUp state channel in a concurrency safe manner
// signaling that we have entered in a kafka consuming session
func (sh *SaramaHandler) enterSession() {
	sh.settingUp.leave()
}

// leaveSession enters the settingUp state channel in a concurrency safe manner
// signaling that we leave a kafka consuming session (no new messages will be consumed until we enter into the next session)
func (sh *SaramaHandler) leaveSession() {
	sh.settingUp.enter()
}

// sessionFinished returns the session channel,
// which will be closed when the current session finishes,
// or it is already closed if we are not in a session.
//
// Note: if you intend to wait on this channel during a session set-up (just after the consumer goes to 'Consuming' state),
// please acquire a read lock on sh.settingUp.RWMutex()
//
// You may consider using 'waitSessionFinish' instead, if you just need to wait for the session to finish.
func (sh *SaramaHandler) sessionFinished() chan struct{} {
	return sh.settingUp.Channel()
}

// waitSessionFinish blocks execution until the current session has finished, in a concurrency safe manner.
// If there is no session established, this call will not block.
func (sh *SaramaHandler) waitSessionFinish() {
	sessionMutex := sh.settingUp.RWMutex()
	sessionMutex.RLock()
	defer sessionMutex.RUnlock()

	<-sh.settingUp.channel
}
