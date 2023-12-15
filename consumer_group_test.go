package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v4/interfaces"
	"github.com/ONSdigital/dp-kafka/v4/mock"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testGroup    = "testGroup"
	errMock      = errors.New("sarama mock error")
	errNoBrokers = errors.New("kafka client has run out of available brokers to talk to (Is your cluster reachable?)")
)

func TestConsumerCreation(t *testing.T) {
	Convey("Providing a nil context results in the expected error being returned", t, func() {
		cg, err := NewConsumerGroupWithGenerators(
			nil,
			&ConsumerGroupConfig{},
			nil,
			nil,
		)
		So(cg, ShouldBeNil)
		So(err, ShouldResemble, errors.New("nil context was passed to consumer-group constructor"))
	})

	Convey("Providing an invalid kafka version results in the expected error being returned", t, func() {
		wrongVersion := "wrongVersion"
		cg, err := NewConsumerGroupWithGenerators(
			ctx,
			&ConsumerGroupConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				GroupName:    testGroup,
				BrokerAddrs:  testBrokers,
			},
			nil,
			nil,
		)
		So(cg, ShouldBeNil)
		So(err, ShouldResemble, fmt.Errorf("failed to get consumer-group config: %w",
			fmt.Errorf("error parsing kafka version: %w",
				errors.New("invalid version `wrongVersion`"))))
	})

	Convey("Given a successful creation of a consumer group", t, func() {
		testCtx, cancel := context.WithCancel(ctx)
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}
		cg, err := NewConsumerGroupWithGenerators(
			testCtx,
			&ConsumerGroupConfig{
				Topic:       testTopic,
				GroupName:   testGroup,
				BrokerAddrs: testBrokers,
			},
			func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, errors.New("uninitialised")
			},
			brokerGen,
		)
		So(err, ShouldBeNil)

		Convey("When the provided context is cancelled then the closed channel is closed and the state is 'Closing'", func(c C) {
			cancel()

			validateChanClosed(c, cg.channels.Closer, true)
			validateChanClosed(c, cg.channels.Closed, true)
			So(cg.state.Get(), ShouldEqual, Closing)
		})
	})
}

func TestConsumerInitialised(t *testing.T) {
	Convey("Given a correct initialization of a Kafka Consumer Group", t, func(c C) {
		chConsumeCalled := make(chan struct{})
		saramaConsumerGroupMock := saramaConsumerGroupHappy(chConsumeCalled)
		cgInitCalls := 0
		cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			cgInitCalls++
			return saramaConsumerGroupMock, nil
		}
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}

		consumer, err := NewConsumerGroupWithGenerators(
			ctx,
			&ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			cgInit,
			brokerGen,
		)

		Convey("Consumer is correctly created and initialised without error", func() {
			So(err, ShouldBeNil)
			So(consumer, ShouldNotBeNil)
			So(consumer.Channels().Upstream, ShouldNotBeNil)
			So(consumer.Channels().Errors, ShouldNotBeNil)
			So(cgInitCalls, ShouldEqual, 1)
			So(consumer.IsInitialised(), ShouldBeTrue)
		})

		Convey("We cannot initialise consumer again, but Initialise does not return an error", func() {
			err = consumer.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cgInitCalls, ShouldEqual, 1)
		})

		Convey("Calling initiailse with a nil context results in the expected error being returned", func() {
			err = consumer.Initialise(nil)
			So(err, ShouldResemble, errors.New("nil context was passed to consumer-group initialise"))
			So(cgInitCalls, ShouldEqual, 1)
		})

		Convey("The consumer is in 'stopped' state by default", func() {
			<-consumer.Channels().Initialised // wait until Initialised channel is closed to prevent any data race condition between writing and reading the state
			So(consumer.state.Get(), ShouldEqual, Stopped)
		})

		Convey("Closing the consumer closes Sarama-cluster consumer and closed channel", func(c C) {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			err := consumer.Close(ctx)
			So(err, ShouldBeNil)
			validateChanClosed(c, consumer.Channels().Closer, true)
			validateChanClosed(c, consumer.Channels().Closed, true)
			So(len(saramaConsumerGroupMock.CloseCalls()), ShouldEqual, 1)
		})
	})
}

func TestConsumerNotInitialised(t *testing.T) {
	Convey("Given that Sarama-cluster fails to create a new Consumer while we initialise our ConsumerGroup", t, func(c C) {
		cgInitCalls := 0
		cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			cgInitCalls++
			return nil, errNoBrokers
		}
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}

		consumer, err := NewConsumerGroupWithGenerators(
			ctx,
			&ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			cgInit,
			brokerGen,
		)

		Convey("Consumer is partially created with channels and checker, but it is not initialised", func() {
			So(err, ShouldBeNil)
			So(consumer, ShouldNotBeNil)
			So(consumer.Channels().Upstream, ShouldNotBeNil)
			So(consumer.Channels().Errors, ShouldNotBeNil)
			So(cgInitCalls, ShouldEqual, 1)
			So(consumer.IsInitialised(), ShouldBeFalse)
		})

		Convey("We can try to initialise the consumer again and the same error is returned", func() {
			err = consumer.Initialise(ctx)
			So(err, ShouldResemble, fmt.Errorf("failed to create a new sarama consumer group: %w", errNoBrokers))
			So(cgInitCalls, ShouldEqual, 2)
		})

		Convey("Closing the consumer closes the caller channels", func(c C) {
			err := consumer.Close(ctx)
			So(err, ShouldBeNil)
			validateChanClosed(c, consumer.Channels().Closer, true)
			validateChanClosed(c, consumer.Channels().Closed, true)
		})
	})
}

func TestState(t *testing.T) {
	Convey("Given a consumer group with a state machine", t, func() {
		cg := &ConsumerGroup{
			mutex: &sync.RWMutex{},
			state: NewConsumerStateMachine(),
		}
		cg.state.Set(Starting)

		Convey("then State() returns the current state", func() {
			So(cg.State(), ShouldEqual, Starting)
		})
	})

	Convey("Given a consumer group without a state machine", t, func() {
		cg := &ConsumerGroup{
			mutex: &sync.RWMutex{},
		}

		Convey("then State() returns Initialising state", func() {
			So(cg.State(), ShouldEqual, Initialising)
		})
	})
}

func TestStateWait(t *testing.T) {
	var waitForCheck = 10 * time.Millisecond

	Convey("Given a consumer group with a state machine", t, func() {
		cg := &ConsumerGroup{
			state: NewConsumerStateMachine(),
		}
		cg.state.Set(Initialising)

		var validateStateWait = func(newState State, shouldWait bool) {
			wg := &sync.WaitGroup{}
			mutex := &sync.Mutex{}

			isWaiting := true

			// go-routine that waits until newState is reached
			wg.Add(1)
			go func() {
				defer func() {
					mutex.Lock()
					isWaiting = false
					mutex.Unlock()
					wg.Done()
				}()
				cg.StateWait(newState)
			}()

			// sleep enough time to make sure the go-routine is either waiting or has finished execution
			time.Sleep(waitForCheck)

			// validate wether the go-routine is still waiting or not
			mutex.Lock()
			So(isWaiting, ShouldEqual, shouldWait)
			mutex.Unlock()

			// set the target state and wait for the go-routine to finish execution
			cg.state.Set(newState)
			wg.Wait()
		}

		Convey("Then Waiting for the current state returns straight away for the current state", func() {
			validateStateWait(Initialising, false)
		})

		Convey("Then Waiting for any other state, blocks execution until the state is reached", func() {
			validateStateWait(Starting, true)
			validateStateWait(Stopped, true)
			validateStateWait(Starting, true)
			validateStateWait(Consuming, true)
			validateStateWait(Stopping, true)
			validateStateWait(Closing, true)
		})
	})
}

func TestRegisterHandler(t *testing.T) {
	Convey("Given a consumer group without any handler", t, func() {
		cg := &ConsumerGroup{
			mutex:      &sync.RWMutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			numWorkers: 1,
		}

		Convey("When a halder is successfully registered", func() {
			var receivedMessage Message
			wg := sync.WaitGroup{}
			wg.Add(1)
			testHandler := func(ctx context.Context, workerID int, msg Message) error {
				defer wg.Done()
				receivedMessage = msg
				return nil
			}
			err := cg.RegisterHandler(ctx, testHandler)
			So(err, ShouldBeNil)

			Convey("Then the handler is called when a message is received from the Upstream channel", func() {
				sentMessage, err := newMessage([]byte{2, 4, 8}, 7)
				if err != nil {
					t.Errorf(errNewMessage)
				}

				sentMessage.CommitFunc = func() {}
				sentMessage.ReleaseFunc = func() {}
				cg.channels.Upstream <- sentMessage
				wg.Wait()
				So(receivedMessage, ShouldEqual, sentMessage)
			})
		})
	})

	Convey("Given a consumer group with a handler already registered", t, func() {
		cg := &ConsumerGroup{
			mutex:   &sync.RWMutex{},
			handler: func(ctx context.Context, workerID int, msg Message) error { return nil },
		}

		Convey("Then trying to register another handler fails with the expected error", func() {
			testHandler := func(ctx context.Context, workerID int, msg Message) error { return nil }
			err := cg.RegisterHandler(ctx, testHandler)
			So(err.Error(), ShouldEqual, "failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
		})
	})

	Convey("Given a consumer group with a batch handler already registered", t, func() {
		cg := &ConsumerGroup{
			mutex:        &sync.RWMutex{},
			batchHandler: func(ctx context.Context, batch []Message) error { return nil },
		}

		Convey("Then trying to register another handler fails with the expected error", func() {
			testHandler := func(ctx context.Context, workerID int, msg Message) error { return nil }
			err := cg.RegisterHandler(ctx, testHandler)
			So(err.Error(), ShouldEqual, "failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
		})
	})
}

func TestRegisterBatchHandler(t *testing.T) {
	Convey("Given a consumer group without any handler", t, func() {
		cg := &ConsumerGroup{
			mutex:         &sync.RWMutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(1, ErrorChanBufferSize),
			batchSize:     2,
			batchWaitTime: 100 * time.Millisecond,
		}

		Convey("When a batch halder is successfully registered", func() {
			var receivedBatch []Message
			wg := sync.WaitGroup{}
			wg.Add(1)
			testBatchHandler := func(ctx context.Context, batch []Message) error {
				defer wg.Done()
				receivedBatch = batch
				return nil
			}
			err := cg.RegisterBatchHandler(ctx, testBatchHandler)
			So(err, ShouldBeNil)

			Convey("Then the batch handler is called when the batch is full from messages received from the Upstream channel", func() {
				msg1, err := newMessage([]byte{1, 3, 5}, 1)
				if err != nil {
					t.Errorf(errNewMessage)
				}
				msg1.ReleaseFunc = func() {}

				msg2, err := newMessage([]byte{2, 4, 6}, 2)
				if err != nil {
					t.Errorf(errNewMessage)
				}
				msg2.ReleaseFunc = func() {}

				cg.channels.Upstream <- msg1
				cg.channels.Upstream <- msg2
				wg.Wait()
				So(receivedBatch, ShouldResemble, []Message{msg1, msg2})
			})
		})
	})

	Convey("Given a consumer group with a handler already registered", t, func() {
		cg := &ConsumerGroup{
			mutex:   &sync.RWMutex{},
			handler: func(ctx context.Context, workerID int, msg Message) error { return nil },
		}

		Convey("Then trying to register a batch handler fails with the expected error", func() {
			testBatchHandler := func(ctx context.Context, batch []Message) error { return nil }
			err := cg.RegisterBatchHandler(ctx, testBatchHandler)
			So(err.Error(), ShouldEqual, "failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
		})
	})

	Convey("Given a consumer group with a batch handler already registered", t, func() {
		cg := &ConsumerGroup{
			mutex:        &sync.RWMutex{},
			batchHandler: func(ctx context.Context, batch []Message) error { return nil },
		}

		Convey("Then trying to register another batch handler fails with the expected error", func() {
			testBatchHandler := func(ctx context.Context, batch []Message) error { return nil }
			err := cg.RegisterBatchHandler(ctx, testBatchHandler)
			So(err.Error(), ShouldEqual, "failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
		})
	})
}

func TestStart(t *testing.T) {
	Convey("Given a consumer group", t, func() {
		channels := &ConsumerGroupChannels{
			Consume: make(chan bool),
		}
		cg := &ConsumerGroup{
			channels: channels,
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}

		Convey("Calling Start in 'Initialising' state results 'Starting' initial state being set", func() {
			cg.state = NewConsumerStateMachine()
			err := cg.Start()
			So(err, ShouldBeNil)
			So(cg.initialState, ShouldEqual, Starting)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Stopping' state results in a 'false' value being sent to the Consume channel", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Stopping)
			var err error
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = cg.Start()
			}()

			val := <-channels.Consume
			wg.Wait()
			So(err, ShouldBeNil)
			So(val, ShouldBeTrue)
		})

		Convey("Calling Start in 'Stopped' state results in a 'false' value being sent to the Consume channel", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Stopped)
			var err error
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = cg.Start()
			}()

			val := <-channels.Consume
			wg.Wait()
			So(err, ShouldBeNil)
			So(val, ShouldBeTrue)
		})

		Convey("Calling Start in 'Starting' state has no effect", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Starting)
			err := cg.Start()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Consuming' state has no effect", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Consuming)
			err := cg.Start()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Closing' state returns the expected error", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Closing)
			err := cg.Start()
			So(err.Error(), ShouldResemble, "consummer cannot be started because it is closing")
			So(len(channels.Consume), ShouldEqual, 0)
		})
	})
}

func TestStop(t *testing.T) {
	Convey("Given a consumer group", t, func() {
		channels := &ConsumerGroupChannels{
			Consume: make(chan bool),
		}
		cg := &ConsumerGroup{
			channels: channels,
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}

		Convey("Calling Stop in 'Initialising' state results 'Stopped' initial state being set", func() {
			cg.state = NewConsumerStateMachine()
			err := cg.Stop()
			So(err, ShouldBeNil)
			So(cg.initialState, ShouldEqual, Stopped)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Stopping' has no effect", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Stopping)
			err := cg.Stop()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Stopped' state has no effect", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Stopped)
			err := cg.Stop()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Starting' results in a 'true' value being sent to the Consume channel", func(c C) {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Starting)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cg.Stop()
				c.So(err, ShouldBeNil)
			}()

			val := <-channels.Consume
			wg.Wait()
			So(val, ShouldBeFalse)
		})

		Convey("Calling Stop in 'Consuming' results in a 'true' value being sent to the Consume channel", func(c C) {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Consuming)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cg.Stop()
				c.So(err, ShouldBeNil)
			}()

			val := <-channels.Consume
			wg.Wait()
			So(val, ShouldBeFalse)
		})

		Convey("Calling StopAndWait in 'Consuming' results in a 'true' value being sent to the Consume channel and the call blocking until the sessionConsuming channel is closed", func(c C) {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Consuming)
			cg.saramaCgHandler = &SaramaHandler{
				settingUp: NewStateChan(),
			}
			cg.saramaCgHandler.enterSession()

			waiting := true
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				waiting = true
				err := cg.StopAndWait()
				c.So(err, ShouldBeNil)
				waiting = false
			}()

			val := <-channels.Consume
			time.Sleep(10 * time.Millisecond)
			So(waiting, ShouldBeTrue)

			cg.saramaCgHandler.leaveSession()
			wg.Wait()
			So(val, ShouldBeFalse)
		})

		Convey("Calling Stop in 'Closing' state has no effect", func() {
			cg.state = NewConsumerStateMachine()
			cg.state.Set(Closing)
			err := cg.Stop()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})
	})
}

func TestLogErrors(t *testing.T) {
	var (
		waitForCheck            = 10 * time.Millisecond
		testErrorChanBufferSize = 2
	)

	Convey("Given a consumer group with channels", t, func() {
		channels := CreateConsumerGroupChannels(1, testErrorChanBufferSize)
		cg := &ConsumerGroup{
			channels: channels,
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}
		mutex := &sync.Mutex{}

		Convey("When LogErrors is called", func() {
			cg.LogErrors(ctx)
			isLogging := true

			// go-routine that sets isLogging to false when the logging go-routine finishes
			go func() {
				cg.wgClose.Wait()
				mutex.Lock()
				isLogging = false
				mutex.Unlock()
			}()

			Convey("Then the go-routine consumes all errors from the error channel", func() {
				time.Sleep(waitForCheck)
				mutex.Lock()
				So(isLogging, ShouldBeTrue)
				mutex.Unlock()

				errs := []error{
					errors.New("err1"),
					errors.New("err2"),
					errors.New("err3"),
					errors.New("err4"),
					errors.New("err5"),
				}
				for _, err := range errs {
					errSend := SafeSendErr(cg.channels.Errors, err)
					So(errSend, ShouldBeNil)
				}

				time.Sleep(waitForCheck)
				So(len(cg.channels.Errors), ShouldEqual, 0)

				Convey("And closing the Closer channel results in the logging go-routine to end its execution", func() {
					close(channels.Closer)
					time.Sleep(waitForCheck)
					mutex.Lock()
					So(isLogging, ShouldBeFalse)
					mutex.Unlock()
				})

				Convey("And closing the Errors channel results in the logging go-routine to end its execution", func() {
					close(channels.Errors)
					time.Sleep(waitForCheck)
					mutex.Lock()
					So(isLogging, ShouldBeFalse)
					mutex.Unlock()
				})
			})
		})
	})
}

func TestOnHealthUpdate(t *testing.T) {
	Convey("Given a consumer-group", t, func() {
		cg := &ConsumerGroup{
			mutex:   &sync.RWMutex{},
			wgClose: &sync.WaitGroup{},
			state:   NewConsumerStateMachine(),
		}

		Convey("When a notification of an 'OK' health status is received, then the consumer-group is started", func() {
			cg.OnHealthUpdate(healthcheck.StatusOK)
			So(cg.initialState, ShouldEqual, Starting)
		})

		Convey("When a notification of an 'WARNING' health status is received, then the consumer-group is stopped", func() {
			cg.OnHealthUpdate(healthcheck.StatusWarning)
			So(cg.initialState, ShouldEqual, Stopped)
		})

		Convey("When a notification of an 'CRITICAL' health status is received, then the consumer-group is stopped", func() {
			cg.OnHealthUpdate(healthcheck.StatusCritical)
			So(cg.initialState, ShouldEqual, Stopped)
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Given an kafka consumergroup", t, func() {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		cg := &ConsumerGroup{
			channels: channels,
			group:    testGroup,
			topic:    testTopic,
			state:    NewConsumerStateMachine(),
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}

		Convey("Calling close with a nil context results in the expected error being returned", func(c C) {
			err := cg.Close(nil)
			So(err, ShouldResemble, errors.New("nil context was passed to consumer-group close"))
			validateChanClosed(c, channels.Closer, false)
			validateChanClosed(c, channels.Closed, false)
		})

		Convey("Which is uninitialised", func() {
			Convey("When Close is successfully called", func(c C) {
				err := cg.Close(ctx)
				So(err, ShouldBeNil)

				Convey("Then the Closed channel is closed", func(c C) {
					validateChanClosed(c, channels.Closed, true)
				})

				Convey("Then the state is set to Closing", func(c C) {
					So(cg.state.Get(), ShouldEqual, Closing)
				})

				Convey("Then calling Close again has no effect and no error is returned", func(c C) {
					err := cg.Close(ctx)
					So(err, ShouldBeNil)
					So(cg.state.Get(), ShouldEqual, Closing)
				})
			})
		})

		Convey("Which is initialised and the Sarama Client closes successfully", func() {
			saramaCg := &mock.SaramaConsumerGroupMock{
				CloseFunc: func() error {
					return nil
				},
			}
			cg.saramaCg = saramaCg

			Convey("When Close is successfully called", func() {
				err := cg.Close(ctx)
				So(err, ShouldBeNil)

				Convey("Then the sarama client is closed", func() {
					So(saramaCg.CloseCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("Which is initialised and the Sarama Client fails to close", func() {
			saramaCg := &mock.SaramaConsumerGroupMock{
				CloseFunc: func() error {
					return errMock
				},
			}
			cg.saramaCg = saramaCg

			Convey("When Close is called then the expected error is returned", func() {
				err := cg.Close(ctx)
				So(err, ShouldResemble, NewError(
					fmt.Errorf("error closing sarama consumer-group: %w", errMock),
					log.Data{"state": Closing.String(), "group": testGroup, "topic": testTopic}),
				)
			})
		})
	})
}

func TestConsumerStopped(t *testing.T) {
	Convey("Given a Kafka consumergroup in stopped state", t, func(c C) {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(),
			channels: channels,
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}
		cg.state.Set(Stopped)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cg.stoppedState(ctx, log.Data{})
		}()

		Convey("When the stoppedState go-routine ends due to the 'Consume' channel being closed", func(c C) {
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the stoppedState go-routine ends due to the 'Consume' receiving a 'true' value'", func(c C) {
			channels.Consume <- true
			wg.Wait()

			Convey("Then the state is set to 'Starting'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Starting)
			})
		})
	})
}

func TestConsumerStarting(t *testing.T) {
	Convey("Given a Kafka consumergroup in starting state and a successful SaramaConsumerGroup mock", t, func(c C) {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(),
			channels: channels,
			saramaCg: saramaConsumerGroupHappy(chConsumeCalled),
			mutex:    &sync.RWMutex{},
			wgClose:  &sync.WaitGroup{},
		}
		cg.state.Set(Starting)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cg.startingState(ctx, log.Data{})
		}()

		Convey("When the startingState go-routine ends due to the 'Closer' channel being closed", func(c C) {
			close(channels.Closer)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel being closed", func(c C) {
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' receiving a 'false' value'", func(c C) {
			channels.Consume <- false
			wg.Wait()

			Convey("Then the state is set to 'Stopping'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Stopping)
			})
		})

		Convey("When Consume finishes its execution, the loop starts again", func(c C) {
			<-chConsumeCalled // wait until Consume is called
			close(channels.Consume)
			wg.Wait()
		})
	})

	Convey("Given a Kafka consumergroup in starting state and a SaramaConsumerGroup mock that fails to consume", t, func(c C) {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
			state:          NewConsumerStateMachine(),
			channels:       channels,
			saramaCg:       saramaConsumerGroupConsumeFails(chConsumeCalled),
			mutex:          &sync.RWMutex{},
			wgClose:        &sync.WaitGroup{},
			minRetryPeriod: defaultMinRetryPeriod,
			maxRetryPeriod: defaultMaxRetryPeriod,
		}
		cg.state.Set(Starting)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cg.startingState(ctx, log.Data{})
		}()

		Convey("When the startingState go-routine ends due to the 'Closer' channel being closed after SaramaCg.Consume had previously failed", func(c C) {
			<-chConsumeCalled
			close(channels.Closer)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel being closed after SaramaCg.Consume had previously failed", func(c C) {
			<-chConsumeCalled
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel receiving a 'false' value after SaramaCg.Consume had previously failed", func(c C) {
			<-chConsumeCalled
			channels.Consume <- false
			wg.Wait()

			Convey("Then the state is set to 'Stopping'", func(c C) {
				c.So(cg.state.Get(), ShouldEqual, Stopping)
			})
		})
	})
}

func TestConsumeLoop(t *testing.T) {
	Convey("Given an uninitialised consumer group where we call createConsumerLoop", t, func(c C) {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		chConsumeCalled := make(chan struct{})
		saramaMock := saramaConsumerGroupHappy(chConsumeCalled)
		cg := &ConsumerGroup{
			state:        NewConsumerStateMachine(),
			topic:        testTopic,
			channels:     channels,
			saramaCg:     saramaMock,
			initialState: Stopped,
			mutex:        &sync.RWMutex{},
			wgClose:      &sync.WaitGroup{},
		}

		Convey("Where createConsumeLoop is called", func() {
			cg.createConsumeLoop(ctx)

			Convey("Then the 'Initialised' channel is closed", func(c C) {
				validateChanClosed(c, channels.Initialised, true)
			})

			Convey("then the state is set to the initial state ('Stopped')", func() {
				So(cg.state.Get(), ShouldEqual, Stopped)
			})

			Convey("when a 'true' value is sent to the Consume channel", func() {
				channels.Consume <- true

				Convey("then the the startingState loop sets the state to 'Starting' and sarama consumer starts consuming the expected topic", func() {
					<-chConsumeCalled
					So(cg.state.Get(), ShouldEqual, Starting)
					So(saramaMock.ConsumeCalls()[0].Topics, ShouldResemble, []string{testTopic})
				})
			})

			Convey("when the Closer channel is closed", func() {
				close(channels.Closer)

				Convey("then the state is set to 'Closing' and the loop finishes its execution", func() {
					cg.wgClose.Wait()
					So(cg.state.Get(), ShouldEqual, Closing)
				})
			})
		})
	})

	Convey("Given a consumer group", t, func() {
		invalidStates := []State{Stopped, Starting, Consuming, Stopping, Closing}
		st := NewConsumerStateMachine()
		for _, s := range invalidStates {
			Convey(fmt.Sprintf("Then calling createConsumeLoop while in %s state has no effect", s.String()), func() {
				st.Set(s)
				cg := &ConsumerGroup{state: st}
				cg.createConsumeLoop(ctx)
				So(cg, ShouldResemble, &ConsumerGroup{state: st})
			})
		}
	})
}

func TestCreateLoopUninitialised(t *testing.T) {
	Convey("Given a consumer group in Initialising state", t, func() {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		cg := &ConsumerGroup{
			state:          NewConsumerStateMachine(),
			channels:       channels,
			mutex:          &sync.RWMutex{},
			wgClose:        &sync.WaitGroup{},
			minRetryPeriod: defaultMinRetryPeriod,
			maxRetryPeriod: defaultMaxRetryPeriod,
		}

		Convey("Where createLoopUninitialised is called", func() {
			cg.createLoopUninitialised(ctx)

			Convey("Then closing the 'Initialised' channel results in the loop ending its execution", func(c C) {
				close(channels.Initialised)
				cg.wgClose.Wait()
			})
		})
	})

	Convey("Given a consumer group that already has an initialised Sarama client", t, func() {
		channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
		chConsumeCalled := make(chan struct{})
		saramaMock := saramaConsumerGroupHappy(chConsumeCalled)
		cg := &ConsumerGroup{
			state:        NewConsumerStateMachine(),
			topic:        testTopic,
			channels:     channels,
			saramaCg:     saramaMock,
			initialState: Stopped,
			mutex:        &sync.RWMutex{},
			wgClose:      &sync.WaitGroup{},
		}

		Convey("Then calling createLoopUninitialised has no effect", func() {
			cg.createLoopUninitialised(ctx)
			cg.wgClose.Wait()
		})
	})
}

// saramaConsumerGroupHappy returns a mocked SaramaConsumerGroup for testing where Consume returns nil and passes a 'true' value to the channel
func saramaConsumerGroupHappy(consumeCalled chan struct{}) *mock.SaramaConsumerGroupMock {
	return &mock.SaramaConsumerGroupMock{
		ErrorsFunc: func() <-chan error {
			return make(chan error)
		},
		ConsumeFunc: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
			select {
			case <-consumeCalled:
			default:
				close(consumeCalled)
			}
			return nil
		},
	}
}

// saramaConsumerGroupConsumeFails returns a mocked SaramaConsumerGroup for testing where Consume returns an error
// and passes a 'true' value to the channel
func saramaConsumerGroupConsumeFails(consumeCalled chan struct{}) *mock.SaramaConsumerGroupMock {
	return &mock.SaramaConsumerGroupMock{
		ErrorsFunc: func() <-chan error {
			return make(chan error)
		},
		ConsumeFunc: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
			select {
			case <-consumeCalled:
			default:
				close(consumeCalled)
			}
			return errMock
		},
	}
}
