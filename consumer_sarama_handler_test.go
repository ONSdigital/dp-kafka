package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/ONSdigital/dp-kafka/v4/mock"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testChanTimeout = 100 * time.Millisecond
)

// testClaims is a testing sarama claim corresponding to a topic with 5 assigned partitions
var testClaims = map[string][]int32{testTopic: {1, 2, 3, 4, 5}}

// saramaChannelBufferSize is the channel buffer size used by sarama (256 is the default value in Sarama)
var saramaChannelBufferSize = 256

func TestSetup(t *testing.T) {
	Convey("Given a saramaCgHandler with channels, and a sarama ConsumerGroupSession mock", t, func(c C) {
		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize, ErrorChanBufferSize)
		cgState := NewConsumerStateMachine()
		cgState.Set(Starting)
		cgHandler := newSaramaHandler(ctx, channels, cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}

		Convey("In 'Starting' state and with 'Initialised' channel still open", func() {
			cgHandler.state.Set(Starting)
			err := cgHandler.Setup(cgSession)
			defer cgHandler.leaveSession() // force the control go-routine to end

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the state is set to 'Consuming'", func() {
				So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})
		})

		Convey("When Setup is called in 'Consuming' state and with 'Initialised' channel closed", func() {
			cgHandler.state.Set(Consuming)
			close(channels.Initialised)
			err := cgHandler.Setup(cgSession)
			defer cgHandler.leaveSession() // force the control go-routine to end

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the 'Initialised' channel is closed", func() {
				validateChanClosed(c, channels.Initialised, true)
			})

			Convey("Then the state is set to 'Consuming'", func() {
				So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})
		})

		Convey("When Setup is called while in 'Stopping' state", func() {
			cgHandler.state.Set(Stopping)
			err := cgHandler.Setup(cgSession)

			Convey("Then the expected error is returned and no further action is taken", func() {
				So(err.Error(), ShouldEqual, "wrong state to start consuming: state transition from Stopping to Consuming is not allowed")
				validateChanClosed(c, channels.Initialised, false)
				So(cgHandler.state.Get(), ShouldEqual, Stopping)
			})
		})
	})
}

func TestControlRoutine(t *testing.T) {
	Convey("Given a saramaCgHandler with channels, a sarama ConsumerGroupSession mock in Consuming state and a newly created sessionConsuming channel", t, func() {
		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize, ErrorChanBufferSize)
		cgState := NewConsumerStateMachine()
		cgState.Set(Consuming)
		cgHandler := newSaramaHandler(ctx, channels, cgState)
		close(channels.Initialised)
		cgHandler.enterSession()

		// start control group in a go-routine
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cgHandler.controlRoutine()
		}()

		Convey("When the controlRoutine ends due to the 'Closer' channel being closed", func(c C) {
			close(channels.Closer)
			wg.Wait()

			Convey("Then the state is set to 'Closing' and 'sessionConsuming' channel is closed", func(c C) {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
				c.So(cgHandler.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the controlRoutine ends due to the 'Consume' channel being closed", func(c C) {
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing' and 'sessionConsuming' channel is closed", func(c C) {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
				c.So(cgHandler.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the controlRoutine ends due to the 'Consume' channel receiving a 'false' value", func(c C) {
			channels.Consume <- false
			wg.Wait()

			Convey("Then the state is set to 'Stopping' and 'sessionConsuming' channel is closed", func(c C) {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
				c.So(cgHandler.state.Get(), ShouldEqual, Stopping)
			})
		})

		Convey("When the Consume channel receives a 'true' value", func(c C) {
			channels.Consume <- true

			Convey("Then the state is not changed 'Consuming' and the 'sessionConsuming' channel is not closed", func(c C) {
				validateChanClosed(c, cgHandler.settingUp.channel, false)
				c.So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})
		})

		Convey("When the controlRoutine ends due to the 'sessionConsuming' channel being closed", func(c C) {
			cgHandler.leaveSession()
			wg.Wait()

			Convey("Then the state is not changed and 'sessionConsuming' channel remains closed", func(c C) {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
				c.So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})
		})
	})
}

func TestCleanup(t *testing.T) {
	Convey("Given a saramaCgHandler with channels, and a sarama ConsumerGroupSession mock", t, func(c C) {
		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize, ErrorChanBufferSize)
		cgState := NewConsumerStateMachine()
		cgState.Set(Consuming)
		cgHandler := newSaramaHandler(ctx, channels, cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}

		Convey("When Cleanup is called while in 'Consuming' state", func() {
			cgHandler.enterSession()
			cgHandler.state.Set(Consuming)
			err := cgHandler.Cleanup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then sessionConsuming channel is closed", func() {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
			})

			Convey("Then the state is set to 'Starting'", func() {
				So(cgHandler.state.Get(), ShouldEqual, Starting)
			})
		})

		Convey("When Cleanup is called while in 'Stopping' state", func() {
			cgHandler.enterSession()
			cgHandler.state.Set(Stopping)
			err := cgHandler.Cleanup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then sessionConsuming channel is closed", func() {
				validateChanClosed(c, cgHandler.settingUp.channel, true)
			})

			Convey("Then the state is not changed", func() {
				So(cgHandler.state.Get(), ShouldEqual, Stopping)
			})
		})
	})
}

func TestConsume(t *testing.T) {
	Convey("Given a saramaCgHandler with channels, a sarama ConsumerGroupSession, "+
		"one message being produced per partition, and as many parallel consumption "+
		"go-routines as partitions in the claim", t, func(c C) {
		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize, ErrorChanBufferSize)
		cgState := NewConsumerStateMachine()
		cgState.Set(Consuming)
		cgHandler := newSaramaHandler(ctx, channels, cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
			CommitFunc:      func() {},
			MarkMessageFunc: func(msg *sarama.ConsumerMessage, metadata string) {},
		}
		err := cgHandler.Setup(cgSession)
		So(err, ShouldBeNil)

		saramaMessagesChan := make(chan *sarama.ConsumerMessage, saramaChannelBufferSize)
		cgClaim := &mock.SaramaConsumerGroupClaimMock{
			MessagesFunc: func() <-chan *sarama.ConsumerMessage {
				return saramaMessagesChan
			},
		}

		// create one consumption go-routine per partition
		wgConsumeClaims := &sync.WaitGroup{}
		for range testClaims[testTopic] {
			wgConsumeClaims.Add(1)
			go func() {
				defer wgConsumeClaims.Done()
				err := cgHandler.ConsumeClaim(cgSession, cgClaim)
				c.So(err, ShouldBeNil)
			}()
		}

		Convey("And no message is received from Sarama message channel", func(c C) {
			var validateNoConsumption = func() {
				// consume any remaining message
				numConsum := consume(c, channels.Upstream)
				So(numConsum, ShouldEqual, 0)

				// validate all remaining messages have been consumed (marked and committed)
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, numConsum)
				So(len(cgSession.CommitCalls()), ShouldEqual, numConsum)
			}

			Convey("Then closing the Closer channel results in all the ConsumeClaim go-routines finising and the state being set to 'Closing'", func(c C) {
				close(channels.Closer)
				validateNoConsumption()
				So(cgHandler.state.Get(), ShouldEqual, Closing)
			})

			Convey("Then closing the Consume channel results in all the ConsumeClaim go-routines finising and the state being set to 'Closing'", func(c C) {
				close(channels.Consume)
				validateNoConsumption()
				So(cgHandler.state.Get(), ShouldEqual, Closing)
			})

			Convey("Then sending 'false' to the Consume channel results in all the ConsumeClaim go-routines finising and the state being set to 'Stopping'", func(c C) {
				channels.Consume <- false
				validateNoConsumption()
				So(cgHandler.state.Get(), ShouldEqual, Stopping)
			})
		})

		Convey("And one message per partition is received from the Sarama message channel", func(c C) {
			// produce one message per partition (identifiable by offset and partition)
			for i, partition := range testClaims[testTopic] {
				saramaMessagesChan <- &sarama.ConsumerMessage{
					Topic:     testTopic,
					Partition: partition,
					Offset:    int64(i),
				}
			}

			Convey("Then Messages can be consumed from the upstream channel. "+
				"Committing them results in the consumption go-routine being released "+
				"and the session being committed", func(c C) {
				// expect one message per partition
				numConsum := consume(c, channels.Upstream)
				So(numConsum, ShouldEqual, len(testClaims[testTopic]))

				// sarama closes the messages channel when a rebalance is due (ConsumeClaims need to end as soon as possible at this point)
				// force ConsumeClaim to finish execution for testing, by  by closing this channel
				close(saramaMessagesChan)

				// validate all messages have been consumed
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, len(testClaims[testTopic]))
				So(len(cgSession.CommitCalls()), ShouldEqual, len(testClaims[testTopic]))

				// State is still consuming
				So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})

			Convey("Then Messages finish being consumed even if the saramaMessageChan is already closed.", func(c C) {
				// sarama closes the messages channel when a rebalance is due (ConsumeClaims need to end as soon as possible at this point)
				// force ConsumeClaim to finish execution for testing, by  by closing this channel
				close(saramaMessagesChan)

				// expect one message per partition (already being consumed)
				numConsum := consume(c, channels.Upstream)
				So(numConsum, ShouldEqual, len(testClaims[testTopic]))

				// validate all messages have been consumed
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, len(testClaims[testTopic]))
				So(len(cgSession.CommitCalls()), ShouldEqual, len(testClaims[testTopic]))

				// State is still consuming
				So(cgHandler.state.Get(), ShouldEqual, Consuming)
			})

			Convey("Closing the closer channel results in only the remaining messages in the Upstream channel being consumed "+
				"before all the ConsumeClaim goroutines finish their execution", func(c C) {
				// close closer channel - no new messages will be consumed, even if saramaMessagesChan contains more messages.
				close(channels.Closer)

				// consume any remaining message
				numConsum := consume(c, channels.Upstream)

				// validate all remaining messages have been consumed (marked and committed)
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, numConsum)
				So(len(cgSession.CommitCalls()), ShouldEqual, numConsum)

				Convey("And the state is set to 'Closing'", func(c C) {
					// State is still closing
					So(cgHandler.state.Get(), ShouldEqual, Closing)
				})
			})

			Convey("Sending 'false' to the Consume channel results in only the remaining messages in the Upstream channel being consumed "+
				"before all the ConsumeClaim goroutines finish their execution", func(c C) {
				// send false to Consume channel - no new messages will be consumed, even if saramaMessagesChan contains more messages.
				channels.Consume <- false

				// consume any remaining message
				numConsum := consume(c, channels.Upstream)

				// validate all remaining messages have been consumed (marked and committed)
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, numConsum)
				So(len(cgSession.CommitCalls()), ShouldEqual, numConsum)

				Convey("And the state is set to 'Stopping'", func(c C) {
					So(cgHandler.state.Get(), ShouldEqual, Stopping)
				})
			})

			Convey("Closing the Consume channel results in only the remaining messages in the Upstream channel being consumed "+
				"before all the ConsumeClaim goroutines finish their execution", func(c C) {
				// send false to Consume channel - no new messages will be consumed, even if saramaMessagesChan contains more messages.
				close(channels.Consume)

				// consume any remaining message
				numConsum := consume(c, channels.Upstream)

				// validate all remaining messages have been consumed (marked and committed)
				wgConsumeClaims.Wait()
				So(len(cgSession.MarkMessageCalls()), ShouldEqual, numConsum)
				So(len(cgSession.CommitCalls()), ShouldEqual, numConsum)

				Convey("And the state is set to 'Closing'", func(c C) {
					So(cgHandler.state.Get(), ShouldEqual, Closing)
				})
			})
		})
	})
}

func TestConsumeMessage(t *testing.T) {
	Convey("Given a saramaCgHandler with Upstream channel closed", t, func(c C) {
		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize, ErrorChanBufferSize)
		cgState := NewConsumerStateMachine()
		cgState.Set(Starting)
		cgHandler := newSaramaHandler(ctx, channels, cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}
		close(cgHandler.channels.Upstream)

		Convey("Then consuming a sarama message results in the expected error being returned", func() {
			saramaMsg := &sarama.ConsumerMessage{
				Topic:     testTopic,
				Partition: 1,
				Offset:    int64(123),
			}

			err := cgHandler.consumeMessage(NewSaramaMessage(saramaMsg, cgSession, make(chan struct{})))
			So(err, ShouldResemble, errors.New("failed to send sarama message to upstream channel: send on closed channel"))
		})
	})
}

// consume consumes any remaining messages. Returns the number of messages consumed when the channel is closed or after a timeout expires
func consume(c C, ch chan Message) int {
	numConsum := 0
	for {
		delay := time.NewTimer(testChanTimeout)
		select {
		case msg, ok := <-ch:
			// Ensure timer is stopped and its resources are freed
			if !delay.Stop() {
				// if the timer has been stopped then read from the channel
				<-delay.C
			}
			if !ok {
				return numConsum
			}
			msg.CommitAndRelease()
			validateChanClosed(c, msg.UpstreamDone(), true)
			numConsum++
		case <-delay.C:
			return numConsum
		}
	}
}
