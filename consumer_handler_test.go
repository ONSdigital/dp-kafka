package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type testTrueCommiter struct {
}

func (tc testTrueCommiter) Commit() bool  { return true }
func (tc testTrueCommiter) Error() string { return "test error with commit = true" }

type testFalseCommiter struct {
}

func (tc testFalseCommiter) Commit() bool  { return false }
func (tc testFalseCommiter) Error() string { return "test error with commit = false" }

func TestHandler(t *testing.T) {
	Convey("Calling 'listen' on a consumer group without a message handler has no effect", t, func() {
		cg := &ConsumerGroup{}
		cg.listen(ctx)
	})

	Convey("Given a consumer group with a successful message handler", t, func() {
		var receivedMessage Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:      &sync.RWMutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			numWorkers: 1,
			handler: func(ctx context.Context, workerID int, msg Message) error {
				defer wg.Done()
				receivedMessage = msg
				return nil
			},
		}
		cg.listen(ctx)

		Convey("When a message is received from the Upstream channel", func() {
			sentMessage := newMessage([]byte{2, 4, 8}, 7)
			cg.channels.Upstream <- sentMessage
			wg.Wait()

			Convey("Then the same message is sent to the handler", func() {
				So(receivedMessage, ShouldEqual, sentMessage)
			})

			Convey("Then the message is committed and released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{1, 3, 7}, 8)
				wg.Wait() // wait for another message, to make sure the first message has been fully processed and we can validate calls
				So(sentMessage.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a message handler that fails with a generic error", t, func() {
		var receivedMessage Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:      &sync.RWMutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			numWorkers: 1,
			handler: func(ctx context.Context, workerID int, msg Message) error {
				defer wg.Done()
				receivedMessage = msg
				return errors.New("generic error during message handling")
			},
		}
		cg.listen(ctx)

		Convey("When a message is received from the Upstream channel", func() {
			sentMessage := newMessage([]byte{2, 4, 8}, 7)
			cg.channels.Upstream <- sentMessage
			wg.Wait()

			Convey("Then the same message is sent to the handler", func() {
				So(receivedMessage, ShouldEqual, sentMessage)
			})

			Convey("Then the message is committed and released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{1, 3, 7}, 8)
				wg.Wait() // wait for another message, to make sure the first message has been fully processed and we can validate calls
				So(sentMessage.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a message handler that fails with a Commiter error type that returns true", t, func() {
		var receivedMessage Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:      &sync.RWMutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			numWorkers: 1,
			handler: func(ctx context.Context, workerID int, msg Message) error {
				defer wg.Done()
				receivedMessage = msg
				return testTrueCommiter{}
			},
		}
		cg.listen(ctx)

		Convey("When a message is received from the Upstream channel", func() {
			sentMessage := newMessage([]byte{2, 4, 8}, 7)
			cg.channels.Upstream <- sentMessage
			wg.Wait()

			Convey("Then the same message is sent to the handler", func() {
				So(receivedMessage, ShouldEqual, sentMessage)
			})

			Convey("Then the message is committed and released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{1, 3, 7}, 8)
				wg.Wait() // wait for another message, to make sure the first message has been fully processed and we can validate calls
				So(sentMessage.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a message handler that fails with a Commiter error type that returns false", t, func() {
		var receivedMessage Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:      &sync.RWMutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			numWorkers: 1,
			handler: func(ctx context.Context, workerID int, msg Message) error {
				defer wg.Done()
				receivedMessage = msg
				return testFalseCommiter{}
			},
		}
		cg.listen(ctx)

		Convey("When a message is received from the Upstream channel", func() {
			sentMessage := newMessage([]byte{2, 4, 8}, 7)
			cg.channels.Upstream <- sentMessage
			wg.Wait()

			Convey("Then the same message is sent to the handler", func() {
				So(receivedMessage, ShouldEqual, sentMessage)
			})

			Convey("Then the message is released but it is not commited", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{1, 3, 7}, 8)
				wg.Wait() // wait for another message, to make sure the first message has been fully processed and we can validate calls
				So(sentMessage.CommitCalls(), ShouldHaveLength, 0)
				So(sentMessage.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})
}

func TestBatchHandler(t *testing.T) {
	Convey("Calling 'listenBatch' on a consumer group without a batch handler has no effect", t, func() {
		cg := &ConsumerGroup{}
		cg.listenBatch(ctx)
	})

	Convey("Given a consumer group with a successful batch handler and a batchSize of 3", t, func() {
		var receivedBatch []Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:         &sync.RWMutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(3, ErrorChanBufferSize),
			batchSize:     3,
			batchWaitTime: 10 * time.Millisecond,
			batchHandler: func(ctx context.Context, batch []Message) error {
				defer wg.Done()
				receivedBatch = batch
				return nil
			},
		}
		cg.listenBatch(ctx)

		Convey("When three messages are received from the Upstream channel", func() {
			sentMessage1 := newMessage([]byte{2, 4, 8}, 7)
			sentMessage2 := newMessage([]byte{1, 3, 7}, 8)
			sentMessage3 := newMessage([]byte{3, 5, 9}, 9)
			cg.channels.Upstream <- sentMessage1
			cg.channels.Upstream <- sentMessage2
			cg.channels.Upstream <- sentMessage3
			wg.Wait()

			Convey("Then a batch containing the three messages is sent to the batch handler", func() {
				So(receivedBatch, ShouldResemble, []Message{sentMessage1, sentMessage2, sentMessage3})
			})

			Convey("Then the messages are marked and the last one commits itself and the other marks, they are also released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{0}, 10)
				wg.Wait() // wait for another message, to make sure that all messages in batch have been fully processed and we can validate calls
				So(sentMessage1.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage1.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage2.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage2.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage3.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage3.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("When a message is received from the Upstream channel and nothing else is received in the following batchWaitTime", func() {
			sentMessage1 := newMessage([]byte{2, 4, 8}, 7)
			cg.channels.Upstream <- sentMessage1
			wg.Wait()

			Convey("Then a batch containing the message is sent to the batch handler", func() {
				So(receivedBatch, ShouldResemble, []Message{sentMessage1})
			})

			Convey("Then the message is committed and released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{0}, 10)
				wg.Wait() // wait for another message, to make sure that all messages in batch have been fully processed and we can validate calls
				So(sentMessage1.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage1.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a batch handler that fails with a generic error", t, func() {
		var receivedBatch []Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:         &sync.RWMutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(3, ErrorChanBufferSize),
			batchSize:     3,
			batchWaitTime: 10 * time.Millisecond,
			batchHandler: func(ctx context.Context, batch []Message) error {
				defer wg.Done()
				receivedBatch = batch
				return errors.New("generic error during batch handling")
			},
		}
		cg.listenBatch(ctx)

		Convey("When three messages are received from the Upstream channel", func() {
			sentMessage1 := newMessage([]byte{2, 4, 8}, 7)
			sentMessage2 := newMessage([]byte{1, 3, 7}, 8)
			sentMessage3 := newMessage([]byte{3, 5, 9}, 9)
			cg.channels.Upstream <- sentMessage1
			cg.channels.Upstream <- sentMessage2
			cg.channels.Upstream <- sentMessage3
			wg.Wait()

			Convey("Then a batch containing the three messages is sent to the batch handler", func() {
				So(receivedBatch, ShouldResemble, []Message{sentMessage1, sentMessage2, sentMessage3})
			})

			Convey("Then the messages are marked and the last one commits itself and the other marks, they are also released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{0}, 10)
				wg.Wait() // wait for another message, to make sure that all messages in batch have been fully processed and we can validate calls
				So(sentMessage1.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage1.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage2.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage2.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage3.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage3.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a batch handler that fails with a Commiter error type that returns true", t, func() {
		var receivedBatch []Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:         &sync.RWMutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(3, ErrorChanBufferSize),
			batchSize:     3,
			batchWaitTime: 10 * time.Millisecond,
			batchHandler: func(ctx context.Context, batch []Message) error {
				defer wg.Done()
				receivedBatch = batch
				return testTrueCommiter{}
			},
		}
		cg.listenBatch(ctx)

		Convey("When three messages are received from the Upstream channel", func() {
			sentMessage1 := newMessage([]byte{2, 4, 8}, 7)
			sentMessage2 := newMessage([]byte{1, 3, 7}, 8)
			sentMessage3 := newMessage([]byte{3, 5, 9}, 9)
			cg.channels.Upstream <- sentMessage1
			cg.channels.Upstream <- sentMessage2
			cg.channels.Upstream <- sentMessage3
			wg.Wait()

			Convey("Then a batch containing the three messages is sent to the batch handler", func() {
				So(receivedBatch, ShouldResemble, []Message{sentMessage1, sentMessage2, sentMessage3})
			})

			Convey("Then the messages are marked and the last one commits itself and the other marks, they are also released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{0}, 10)
				wg.Wait() // wait for another message, to make sure that all messages in batch have been fully processed and we can validate calls
				So(sentMessage1.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage1.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage2.MarkCalls(), ShouldHaveLength, 1)
				So(sentMessage2.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage3.CommitCalls(), ShouldHaveLength, 1)
				So(sentMessage3.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given a consumer group with a batch handler that fails with a Commiter error type that returns false", t, func() {
		var receivedBatch []Message
		wg := sync.WaitGroup{}
		wg.Add(1)
		cg := &ConsumerGroup{
			mutex:         &sync.RWMutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(3, ErrorChanBufferSize),
			batchSize:     3,
			batchWaitTime: 10 * time.Millisecond,
			batchHandler: func(ctx context.Context, batch []Message) error {
				defer wg.Done()
				receivedBatch = batch
				return testFalseCommiter{}
			},
		}
		cg.listenBatch(ctx)

		Convey("When three messages are received from the Upstream channel", func() {
			sentMessage1 := newMessage([]byte{2, 4, 8}, 7)
			sentMessage2 := newMessage([]byte{1, 3, 7}, 8)
			sentMessage3 := newMessage([]byte{3, 5, 9}, 9)
			cg.channels.Upstream <- sentMessage1
			cg.channels.Upstream <- sentMessage2
			cg.channels.Upstream <- sentMessage3
			wg.Wait()

			Convey("Then a batch containing the three messages is sent to the batch handler", func() {
				So(receivedBatch, ShouldResemble, []Message{sentMessage1, sentMessage2, sentMessage3})
			})

			Convey("Then the messages are not marked nor committed, but they are released", func() {
				wg.Add(1)
				cg.channels.Upstream <- newMessage([]byte{0}, 10)
				wg.Wait() // wait for another message, to make sure that all messages in batch have been fully processed and we can validate calls
				So(sentMessage1.MarkCalls(), ShouldHaveLength, 0)
				So(sentMessage1.CommitCalls(), ShouldHaveLength, 0)
				So(sentMessage1.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage2.MarkCalls(), ShouldHaveLength, 0)
				So(sentMessage2.CommitCalls(), ShouldHaveLength, 0)
				So(sentMessage2.ReleaseCalls(), ShouldHaveLength, 1)
				So(sentMessage3.MarkCalls(), ShouldHaveLength, 0)
				So(sentMessage3.CommitCalls(), ShouldHaveLength, 0)
				So(sentMessage3.ReleaseCalls(), ShouldHaveLength, 1)
			})
		})
	})
}
