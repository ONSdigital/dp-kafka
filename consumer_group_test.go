package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/mock"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testGroup    = "testGroup"
	errMock      = errors.New("sarama mock error")
	errNoBrokers = errors.New("kafka client has run out of available brokers to talk to (Is your cluster reachable?)")
	testBrokers  = []string{"localhost:12300", "localhost:12301"}
)

func TestConsumerCreationError(t *testing.T) {
	Convey("Providing an invalid kafka version results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		consumer, err := newConsumerGroup(
			ctx,
			&ConsumerGroupConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				GroupName:    testGroup,
				BrokerAddrs:  testBrokers,
			},
			nil,
		)
		So(consumer, ShouldBeNil)
		So(err, ShouldResemble, fmt.Errorf("error getting consumer-group config: %w",
			fmt.Errorf("error parsing kafka version for consumer-group config: %w",
				errors.New("invalid version `wrongVersion`"))))
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

		consumer, err := newConsumerGroup(
			ctx,
			&ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			cgInit)

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

		Convey("The consumer is in 'stopped' state by default", func() {
			<-consumer.Channels().Initialised // wait until Initialised channel is closed to prevent any data race condition between writing and reading the state
			So(consumer.state.Get(), ShouldEqual, Stopped)
		})

		Convey("Closing the consumer closes Sarama-cluster consumer and closed channel", func(c C) {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			consumer.Close(ctx)
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
		consumer, err := newConsumerGroup(
			ctx,
			&ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			cgInit)

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
			So(err, ShouldResemble, fmt.Errorf("error initialising consumer group: %w", errNoBrokers))
			So(cgInitCalls, ShouldEqual, 2)
		})

		Convey("Closing the consumer closes the caller channels", func(c C) {
			consumer.Close(ctx)
			validateChanClosed(c, consumer.Channels().Closer, true)
			validateChanClosed(c, consumer.Channels().Closed, true)
		})
	})
}

func TestState(t *testing.T) {
	Convey("Given a consumer group with a state machine", t, func() {
		cg := &ConsumerGroup{
			state: NewConsumerStateMachine(Starting),
		}

		Convey("then State() returns the string represenation of the current state", func() {
			So(cg.State(), ShouldEqual, Starting.String())
		})
	})

	Convey("Given a consumer group without a state machine", t, func() {
		cg := &ConsumerGroup{}

		Convey("then State() returns an empty string", func() {
			So(cg.State(), ShouldEqual, "")
		})
	})
}

func TestRegisterHandler(t *testing.T) {
	Convey("Given a consumer group without any handler", t, func() {
		cg := &ConsumerGroup{
			mutex:      &sync.Mutex{},
			wgClose:    &sync.WaitGroup{},
			channels:   CreateConsumerGroupChannels(1),
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
				sentMessage := newMessage([]byte{2, 4, 8}, 7)
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
			mutex:   &sync.Mutex{},
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
			mutex:        &sync.Mutex{},
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
			mutex:         &sync.Mutex{},
			wgClose:       &sync.WaitGroup{},
			channels:      CreateConsumerGroupChannels(1),
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
				msg1 := newMessage([]byte{1, 3, 5}, 1)
				msg1.ReleaseFunc = func() {}
				msg2 := newMessage([]byte{2, 4, 6}, 2)
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
			mutex:   &sync.Mutex{},
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
			mutex:        &sync.Mutex{},
			batchHandler: func(ctx context.Context, batch []Message) error { return nil },
		}

		Convey("Then trying to register another batch handler fails with the expected error", func() {
			testBatchHandler := func(ctx context.Context, batch []Message) error { return nil }
			err := cg.RegisterBatchHandler(ctx, testBatchHandler)
			So(err.Error(), ShouldEqual, "failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
		})
	})
}

func TestConsumerChecker(t *testing.T) {
	Convey("Given a valid connected broker", t, func() {
		mockBroker := &mock.SaramaBrokerMock{
			AddrFunc: func() string {
				return "localhost:9092"
			},
			ConnectedFunc: func() (bool, error) {
				return true, nil
			},
			GetMetadataFunc: func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
				return &sarama.MetadataResponse{
					Topics: []*sarama.TopicMetadata{
						{Name: testTopic},
					},
				}, nil
			},
		}

		Convey("And a consumer group connected to the same topic", func() {
			cg := &ConsumerGroup{
				topic:   testTopic,
				brokers: []SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct OK state", func() {
				checkState := healthcheck.NewCheckState(ServiceName)
				t0 := time.Now()
				cg.Checker(ctx, checkState)
				t1 := time.Now()
				So(*checkState.LastChecked(), ShouldHappenOnOrBetween, t0, t1)
				So(checkState.Status(), ShouldEqual, healthcheck.StatusOK)
				So(checkState.Message(), ShouldEqual, MsgHealthyConsumerGroup)
			})
		})

		Convey("And a consumer group connected to a different topic", func() {
			cg := &ConsumerGroup{
				topic:   "wrongTopic",
				brokers: []SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct to Critical state", func() {
				checkState := healthcheck.NewCheckState(ServiceName)
				t0 := time.Now()
				cg.Checker(ctx, checkState)
				t1 := time.Now()
				So(*checkState.LastChecked(), ShouldHappenOnOrBetween, t0, t1)
				So(checkState.Status(), ShouldEqual, healthcheck.StatusCritical)
				So(checkState.Message(), ShouldEqual, "unexpected metadata response for broker(s). Invalid brokers: [localhost:9092]")
			})
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
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}

		Convey("Calling Start in 'Initialising' state results 'Starting' initial state being set", func() {
			cg.state = NewConsumerStateMachine(Initialising)
			err := cg.Start()
			So(err, ShouldBeNil)
			So(cg.initialState, ShouldEqual, Starting)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Stopping' state results in a 'false' value being sent to the Consume channel", func() {
			cg.state = NewConsumerStateMachine(Stopping)
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
			cg.state = NewConsumerStateMachine(Stopped)
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
			cg.state = NewConsumerStateMachine(Starting)
			err := cg.Start()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Consuming' state has no effect", func() {
			cg.state = NewConsumerStateMachine(Consuming)
			err := cg.Start()
			So(err, ShouldBeNil)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Start in 'Closing' state returns the expected error", func() {
			cg.state = NewConsumerStateMachine(Closing)
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
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}

		Convey("Calling Stop in 'Initialising' state results 'Stopped' initial state being set", func() {
			cg.state = NewConsumerStateMachine(Initialising)
			cg.Stop()
			So(cg.initialState, ShouldEqual, Stopped)
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Stopping' has no effect", func() {
			cg.state = NewConsumerStateMachine(Stopping)
			cg.Stop()
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Stopped' state has no effect", func() {
			cg.state = NewConsumerStateMachine(Stopped)
			cg.Stop()
			So(len(channels.Consume), ShouldEqual, 0)
		})

		Convey("Calling Stop in 'Starting' results in a 'true' value being sent to the Consume channel", func() {
			cg.state = NewConsumerStateMachine(Starting)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				cg.Stop()
			}()

			val := <-channels.Consume
			wg.Wait()
			So(val, ShouldBeFalse)
		})

		Convey("Calling Stop in 'Consuming' results in a 'true' value being sent to the Consume channel", func() {
			cg.state = NewConsumerStateMachine(Consuming)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				cg.Stop()
			}()

			val := <-channels.Consume
			wg.Wait()
			So(val, ShouldBeFalse)
		})

		Convey("Calling Stop in 'Closing' state has no effect", func() {
			cg.state = NewConsumerStateMachine(Closing)
			cg.Stop()
			So(len(channels.Consume), ShouldEqual, 0)
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Given an kafka consumergroup", t, func() {
		channels := CreateConsumerGroupChannels(1)
		cg := &ConsumerGroup{
			channels: channels,
			group:    testGroup,
			topic:    testTopic,
			state:    NewConsumerStateMachine(Initialising),
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}

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
		channels := CreateConsumerGroupChannels(1)
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(Stopped),
			channels: channels,
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}
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
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(Starting),
			channels: channels,
			saramaCg: saramaConsumerGroupHappy(chConsumeCalled),
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}
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
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(Starting),
			channels: channels,
			saramaCg: saramaConsumerGroupConsumeFails(chConsumeCalled),
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}
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
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		saramaMock := saramaConsumerGroupHappy(chConsumeCalled)
		cg := &ConsumerGroup{
			state:        NewConsumerStateMachine(Initialising),
			topic:        testTopic,
			channels:     channels,
			saramaCg:     saramaMock,
			initialState: Stopped,
			mutex:        &sync.Mutex{},
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
		st := NewConsumerStateMachine(Initialising)
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
		channels := CreateConsumerGroupChannels(1)
		cg := &ConsumerGroup{
			state:    NewConsumerStateMachine(Initialising),
			channels: channels,
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
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
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		saramaMock := saramaConsumerGroupHappy(chConsumeCalled)
		cg := &ConsumerGroup{
			state:        NewConsumerStateMachine(Initialising),
			topic:        testTopic,
			channels:     channels,
			saramaCg:     saramaMock,
			initialState: Stopped,
			mutex:        &sync.Mutex{},
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
