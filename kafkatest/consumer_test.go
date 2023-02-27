package kafkatest

import (
	"context"
	"sync"
	"testing"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConsumerMock(t *testing.T) {
	longRetry := time.Minute

	cgConfig := &kafka.ConsumerGroupConfig{
		Topic:          "test-topic",
		GroupName:      "test-group",
		BrokerAddrs:    []string{"addr1", "addr2", "addr3"},
		MinRetryPeriod: &longRetry, // set a long retry to prevent race conditions when we check IsInitialised()
		MaxRetryPeriod: &longRetry,
	}

	Convey("Given an uninitialised consumer mock", t, func() {
		cg, err := NewConsumer(
			ctx,
			cgConfig,
			&ConsumerConfig{
				InitAtCreation: false,
			},
		)
		So(err, ShouldBeNil)
		So(cg.Mock.IsInitialised(), ShouldBeFalse)

		Convey("It can be successfully initialised, closing 'Initialised' channel", func() {
			initClosed := false
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-cg.Mock.Channels().Initialised
				initClosed = true
			}()
			err := cg.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cg.Mock.IsInitialised(), ShouldBeTrue)
			wg.Wait()
			So(initClosed, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(cg)
		})
	})

	Convey("Given an initialised consumer mock", t, func() {
		cg, err := NewConsumer(
			ctx,
			cgConfig,
			&ConsumerConfig{
				InitAtCreation: true,
			},
		)
		So(err, ShouldBeNil)
		So(cg.Mock.IsInitialised(), ShouldBeTrue)

		Convey("Calling initialise again has no effect", func() {
			err := cg.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cg.Mock.IsInitialised(), ShouldBeTrue)
		})

		Convey("Messages are received in a synchronized fashion", func(c C) {
			payload1 := []byte{0, 1, 2, 3, 4, 5}
			message1, err := NewMessage(payload1, 1)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			payload2 := []byte{6, 7, 8, 9, 0}
			message2, err := NewMessage(payload2, 2)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			payload3 := []byte{10, 11, 12, 13, 14}
			message3, err := NewMessage(payload3, 3)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			wg := sync.WaitGroup{}
			receivedAll := false
			sentAll := false

			// Message consumer loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				rxMsg := <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message1)
				rxMsg.CommitAndRelease()

				rxMsg = <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message2)
				rxMsg.CommitAndRelease()

				rxMsg = <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message3)
				rxMsg.CommitAndRelease()
				receivedAll = true
			}()

			// Message sender loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				cg.Mock.Channels().Upstream <- message1
				cg.Mock.Channels().Upstream <- message2
				cg.Mock.Channels().Upstream <- message3
				sentAll = true
			}()

			// Check that the messages are released
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-message1.UpstreamDone()
				<-message2.UpstreamDone()
				<-message3.UpstreamDone()
			}()

			wg.Wait()
			So(sentAll, ShouldBeTrue)
			So(receivedAll, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(cg)
		})
	})

	Convey("Creating a consumer with a nil mock consumer configuration results in the default config being used", t, func() {
		cg, err := NewConsumer(
			ctx,
			cgConfig,
			nil,
		)
		So(err, ShouldBeNil)
		So(cg.cfg, ShouldResemble, DefaultConsumerConfig)

		Convey("And, accordingly, the consumer group being initialised", func() {
			So(cg.Mock.IsInitialised(), ShouldBeTrue)
		})
	})

	Convey("Creating a consumer with a nil kafka consumer configuration results in the expected error being returned", t, func() {
		_, err := NewConsumer(
			ctx,
			nil,
			nil,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "kafka consumer config must be provided")
	})

	Convey("Creating a consumer with an invalid kafka consumer configuration results in the expected error being returned", t, func() {
		_, err := NewConsumer(
			ctx,
			&kafka.ConsumerGroupConfig{},
			nil,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "failed to create consumer group for testing: failed to get consumer-group config: validation error: topic is compulsory but was not provided in config")
	})
}

func validateCloseConsumer(cg *Consumer) {
	closedUpstream := false
	closedErrors := false
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-cg.Mock.Channels().Upstream
		closedUpstream = true
		<-cg.Mock.Channels().Errors
		closedErrors = true
		<-cg.Mock.Channels().Closer
		closedCloser = true
		<-cg.Mock.Channels().Closed
		closedClosed = true
	}()
	err := cg.Mock.Close(ctx)
	So(err, ShouldBeNil)
	wg.Wait()

	So(closedUpstream, ShouldBeTrue)
	So(closedErrors, ShouldBeTrue)
	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}

func TestQueueMessage(t *testing.T) {
	cgConfig := &kafka.ConsumerGroupConfig{
		Topic:       "test-topic",
		GroupName:   "test-group",
		BrokerAddrs: []string{"addr1", "addr2", "addr3"},
	}

	Convey("Given a valid kafkatest consumer", t, func() {
		cg, err := NewConsumer(ctx, cgConfig, nil)
		So(err, ShouldBeNil)

		Convey("When a valid event is queued", func() {
			t0 := time.Now()
			event := &TestEvent{
				Field1: "value one",
				Field2: "value two",
			}
			err := cg.QueueMessage(TestSchema, event)
			So(err, ShouldBeNil)

			Convey("Then the expected Sarama message is sent to the mock's sarmaMessages channel", func() {
				delay := time.NewTimer(time.Second)

				select {
				case <-delay.C:
					t.Fail()
				case msg := <-cg.saramaMessages:
					if !delay.Stop() {
						<-delay.C
					}

					So(msg.Headers, ShouldResemble, []*sarama.RecordHeader{})
					So(msg.Timestamp, ShouldHappenOnOrBetween, t0, time.Now())
					So(msg.BlockTimestamp, ShouldHappenOnOrBetween, t0, time.Now())
					So(msg.Topic, ShouldEqual, "test-topic")
					So(msg.Offset, ShouldEqual, 1)

					queued := &TestEvent{}
					err := TestSchema.Unmarshal(msg.Value, queued)
					So(err, ShouldBeNil)
					So(queued, ShouldResemble, event)
				}
			})
		})

		Convey("When an invalid event is queued, then QueueMessage fails with the expected error", func() {
			err := cg.QueueMessage(TestSchema, "wrong")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "failed to marshal event with avro schema: unsupported interface type: string")
		})

		Convey("When a valid event queued but the saramaMessages channel is closed, then QueueMessage fails with the expected error", func() {
			close(cg.saramaMessages)
			event := &TestEvent{
				Field1: "value one",
				Field2: "value two",
			}
			err := cg.QueueMessage(TestSchema, event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "failed to send message to saramaMessages channel: failed to send ConsumerMessage value to channel: send on closed channel")
		})
	})
}

func TestConsume(t *testing.T) {
	cgConfig := &kafka.ConsumerGroupConfig{
		Topic:       "test-topic",
		GroupName:   "test-group",
		BrokerAddrs: []string{"addr1", "addr2", "addr3"},
	}

	Convey("Given a valid kafkatest consumer, with a valid handler registered", t, func() {
		cg, err := NewConsumer(ctx, cgConfig, nil)
		So(err, ShouldBeNil)

		handlerCalled := make(chan struct{})
		var handledMessage kafka.Message
		cg.cg.RegisterHandler(ctx, func(ctx context.Context, workerID int, msg kafka.Message) error {
			handledMessage = msg
			kafka.SafeClose(handlerCalled)
			return nil
		})

		Convey("And a valid message is queued", func() {
			event := &TestEvent{
				Field1: "value one",
				Field2: "value two",
			}
			err := cg.QueueMessage(TestSchema, event)
			So(err, ShouldBeNil)

			Convey("When the consumer group starts", func() {
				cg.cg.Start()

				Convey("Then the handler is called", func() {
					delay := time.NewTimer(time.Second)

					select {
					case <-delay.C:
						t.Fail()
					case <-handlerCalled:
						if !delay.Stop() {
							<-delay.C
						}
					}

					Convey("And the expected message has been handled", func() {
						So(handledMessage, ShouldNotBeNil)
						event := &TestEvent{}
						err = TestSchema.Unmarshal(handledMessage.GetData(), event)
						So(err, ShouldBeNil)
						So(event, ShouldResemble, &TestEvent{
							Field1: "value one",
							Field2: "value two",
						})

						Convey("And the consumer group can be successfully stopped", func() {
							cg.cg.Stop()
							cg.cg.StateWait(kafka.Stopped)
						})
					})
				})
			})
		})
	})
}
