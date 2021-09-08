package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-kafka/v2/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// testClaims is a testing sarama claim corresponding to a topic with 5 assigned partitions
var testClaims = map[string][]int32{testTopic: {1, 2, 3, 4, 5}}

// saramaChannelBufferSize is the channel buffer size used by sarama (256 is the default value in Sarama)
var saramaChannelBufferSize = 256

func TestSetup(t *testing.T) {

	Convey("Given a saramaCgHandler with channels, and a sarama ConsumerGroupSession mock", t, func(c C) {

		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize)
		cgState := Starting
		cgHandler := NewSaramaCgHandler(ctx, channels, &cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}

		Convey("In 'Starting' state and with Ready channel still open", func() {
			*cgHandler.state = Starting
			err := cgHandler.Setup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the Ready channel is closed", func() {
				validateChannelClosed(c, channels.Ready, true)
			})

			Convey("Then the state is set to 'Consuming'", func() {
				So(*cgHandler.state, ShouldEqual, Consuming)
			})
		})

		Convey("When Setup is called in 'Consuming' state and with Ready channel closed", func() {
			*cgHandler.state = Consuming
			close(channels.Ready)
			err := cgHandler.Setup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the Ready channel is closed", func() {
				validateChannelClosed(c, channels.Ready, true)
			})

			Convey("Then the state is set to 'Consuming'", func() {
				So(*cgHandler.state, ShouldEqual, Consuming)
			})
		})

		Convey("When Setup is called while in 'Stopping' state", func() {
			*cgHandler.state = Stopping
			err := cgHandler.Setup(cgSession)

			Convey("Then the expected error is returned and no further action is taken", func() {
				So(err, ShouldResemble, errors.New("wrong state to consume"))
				validateChannelClosed(c, channels.Ready, false)
				So(*cgHandler.state, ShouldEqual, Stopping)
			})
		})
	})
}

func TestCleanup(t *testing.T) {

	Convey("Given a saramaCgHandler with channels, and a sarama ConsumerGroupSession mock", t, func(c C) {

		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize)
		cgState := Consuming
		cgHandler := NewSaramaCgHandler(ctx, channels, &cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}

		Convey("When Cleanup is called while in 'Consuming' state", func() {
			cgHandler.chConsuming = make(chan struct{})
			*cgHandler.state = Consuming
			err := cgHandler.Cleanup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then chConsuming channel is closed", func() {
				validateChannelClosed(c, cgHandler.chConsuming, true)
			})

			Convey("Then the state is set to 'Starting'", func() {
				So(*cgHandler.state, ShouldEqual, Starting)
			})
		})

		Convey("When Cleanup is called while in 'Stopping' state", func() {
			cgHandler.chConsuming = make(chan struct{})
			*cgHandler.state = Stopping
			err := cgHandler.Cleanup(cgSession)

			Convey("Then a nil error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then chConsuming channel is closed", func() {
				validateChannelClosed(c, cgHandler.chConsuming, true)
			})

			Convey("Then the state is not changed", func() {
				So(*cgHandler.state, ShouldEqual, Stopping)
			})
		})
	})
}

func TestConsume(t *testing.T) {

	Convey("Given a saramaCgHandler with channels, a sarama ConsumerGroupSession, "+
		"one message being produced per partition, and as many parallel consumption "+
		"go-routines as partitions in the claim", t, func(c C) {

		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize)
		cgState := Consuming
		cgHandler := NewSaramaCgHandler(ctx, channels, &cgState)
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
			CommitFunc:      func() {},
			MarkMessageFunc: func(msg *sarama.ConsumerMessage, metadata string) {},
		}
		cgHandler.Setup(cgSession)

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
				cgHandler.ConsumeClaim(cgSession, cgClaim)
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
				So(*cgHandler.state, ShouldEqual, Closing)
			})

			Convey("Then closing the Consume channel results in all the ConsumeClaim go-routines finising and the state being set to 'Closing'", func(c C) {
				close(channels.Consume)
				validateNoConsumption()
				So(*cgHandler.state, ShouldEqual, Closing)
			})

			Convey("Then sending 'false' to the Consume channel results in all the ConsumeClaim go-routines finising and the state being set to 'Stopping'", func(c C) {
				channels.Consume <- false
				validateNoConsumption()
				So(*cgHandler.state, ShouldEqual, Stopping)
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
				So(*cgHandler.state, ShouldEqual, Consuming)
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
				So(*cgHandler.state, ShouldEqual, Consuming)
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
					So(*cgHandler.state, ShouldEqual, Closing)
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
					So(*cgHandler.state, ShouldEqual, Stopping)
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
					So(*cgHandler.state, ShouldEqual, Closing)
				})
			})
		})
	})
}

// consume consumes any remaining messages. Returns the number of messages consumed when the channel is closed or after a timeout expires
func consume(c C, ch chan Message) int {
	numConsum := 0
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return numConsum
			}
			msg.CommitAndRelease()
			validateChannelClosed(c, msg.UpstreamDone(), true)
			numConsum++
		case <-time.After(TIMEOUT):
			return numConsum
		}
	}
}
