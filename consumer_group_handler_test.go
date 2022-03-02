package kafka

import (
	"context"
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

func TestSetupCleanup(t *testing.T) {

	Convey("Given a saramaCgHandler with channels, and a sarama ConsumerGroupSession mock", t, func(c C) {

		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize)
		cgHandler := &saramaCgHandler{ctx, channels}
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
		}

		Convey("Setup closes the Ready channel if it is open", func(c C) {
			err := cgHandler.Setup(cgSession)
			So(err, ShouldBeNil)
			validateChannelClosed(c, channels.Ready, true)
		})

		Convey("Setup does not fail if the Ready channel is already closed", func(c C) {
			close(channels.Ready)
			err := cgHandler.Setup(cgSession)
			So(err, ShouldBeNil)
			validateChannelClosed(c, channels.Ready, true)
		})

		Convey("Cleanup returns nil error", func() {
			err := cgHandler.Cleanup(cgSession)
			So(err, ShouldBeNil)
		})
	})
}

func TestConsume(t *testing.T) {

	Convey("Given a saramaCgHandler with channels, a sarama ConsumerGroupSession, "+
		"one message being produced per partition, and as many parallel consumption "+
		"go-routines as partitions in the claim", t, func(c C) {

		bufferSize := 1
		channels := CreateConsumerGroupChannels(bufferSize)
		cgHandler := &saramaCgHandler{ctx, channels}
		cgSession := &mock.SaramaConsumerGroupSessionMock{
			ContextFunc:  func() context.Context { return ctx },
			MemberIDFunc: func() string { return "123456789" },
			ClaimsFunc: func() map[string][]int32 {
				return testClaims
			},
			CommitFunc:      func() {},
			MarkMessageFunc: func(msg *sarama.ConsumerMessage, metadata string) {},
		}

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
		})
	})
}

// consume consumes any remaining messages. Returns the number of messages consumed when the channel is closed or after a timeout expires
func consume(c C, ch chan Message) int {
	numConsum := 0
	for {
		delay := time.NewTimer(TIMEOUT)
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
			validateChannelClosed(c, msg.UpstreamDone(), true)
			numConsum++
		case <-delay.C:
			return numConsum
		}
	}
}
