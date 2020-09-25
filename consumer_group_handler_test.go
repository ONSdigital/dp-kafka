package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// testClaims is a testing sarama claim corresponding to a topic with 5 assigned partitions
var testClaims = map[string][]int32{testTopic: {1, 2, 3, 4, 5}}

// saramaChannelBufferSize is the channel buffer size used by sarama
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

	Convey("Given a saramaCgHandler with channels, a sarama ConsumerGroupSession, one message being produced per partition, and as many parallel consumption go-routines as partitions in the claim", t, func(c C) {

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

		// produce two message per partition (identifiable by offset and partition)
		for i, partition := range testClaims[testTopic] {
			saramaMessagesChan <- &sarama.ConsumerMessage{
				Topic:     testTopic,
				Partition: partition,
				Offset:    int64(i),
			}
		}

		Convey("Then Messages can be consumed from the upstream channel. Committing them results in the consumption go-routine being released and the session being committed", func(c C) {
			// expect one message per partition
			for i := range testClaims[testTopic] {
				msg, ok := <-channels.Upstream
				So(ok, ShouldBeTrue)
				msg.Commit()
				marked := cgSession.MarkMessageCalls()[i].Msg
				So(marked.Offset, ShouldResemble, msg.Offset())
				validateChannelClosed(c, msg.UpstreamDone(), true)
			}
			validateNoMoreMessages(c, channels.Upstream)

			// sarama closes the messages channel when a rebalance is due (ConsumeClaims need to end as soon as possible at this point)
			// force ConsumeClaim to finish execution for testing, by  by closing this channel
			close(saramaMessagesChan)

			// validate all messages have been consumed
			wgConsumeClaims.Wait()
			So(len(cgSession.MarkMessageCalls()), ShouldEqual, len(testClaims[testTopic]))
			So(len(cgSession.CommitCalls()), ShouldEqual, len(testClaims[testTopic]))
		})

		Convey("Closing the closer channel results in only the remaining messages in the Upstream channel being consumed before all the ConsumeClaim goroutines finish their execution", func(c C) {

			// close closer channel - no new messages will be consumed, even if saramaMessagesChan contains more messages.
			close(channels.Closer)

			// consume any remaining message
			numConsum := consume(c, channels.Upstream)

			// validate all messages have been consumed
			wgConsumeClaims.Wait()
			So(len(cgSession.MarkMessageCalls()), ShouldEqual, numConsum)
			So(len(cgSession.CommitCalls()), ShouldEqual, numConsum)
		})
	})
}

// validate that there are no more messages in the provided message channel
func validateNoMoreMessages(c C, ch chan Message) {
	unexpectedMessage := false
	timeout := false
	select {
	case <-ch:
		unexpectedMessage = true
	case <-time.After(TIMEOUT):
		timeout = true
	}
	So(unexpectedMessage, ShouldBeFalse)
	So(timeout, ShouldBeTrue)
}

// consume consumes any remaining messages
func consume(c C, ch chan Message) int {
	numConsum := 0
	for {
		select {
		case msg := <-ch:
			msg.Commit()
			validateChannelClosed(c, msg.UpstreamDone(), true)
			numConsum++
		case <-time.After(3 * TIMEOUT):
			return numConsum
		}
	}
}
