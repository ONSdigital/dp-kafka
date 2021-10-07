package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/config"
	"github.com/ONSdigital/dp-kafka/v3/consumer/mock"
	"github.com/ONSdigital/dp-kafka/v3/health"
	healthMock "github.com/ONSdigital/dp-kafka/v3/health/mock"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testGroup    = "testGroup"
	errMock      = errors.New("sarama mock error")
	ctx          = context.Background()
	errNoBrokers = errors.New("kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
	testBrokers  = []string{"localhost:12300", "localhost:12301"}
)

func TestConsumerCreationError(t *testing.T) {

	Convey("Providing an invalid kafka version results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		consumer, err := newConsumerGroup(
			ctx,
			&config.ConsumerGroupConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				GroupName:    testGroup,
				BrokerAddrs:  testBrokers,
			},
			nil,
			nil,
		)
		So(consumer, ShouldBeNil)
		So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
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
			&config.ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			nil,
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
			<-consumer.Channels().Ready // wait until Ready channel is closed to prevent any data race condition between writing and reading the state
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
			&config.ConsumerGroupConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
				GroupName:   testGroup,
			},
			nil,
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
			So(err, ShouldEqual, errNoBrokers)
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

func TestChecker(t *testing.T) {
	Convey("Given a valid connected broker", t, func() {
		mockBroker := &healthMock.SaramaBrokerMock{
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
				brokers: []health.SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct OK state", func() {
				checkState := healthcheck.NewCheckState(health.ServiceName)
				t0 := time.Now()
				cg.Checker(ctx, checkState)
				t1 := time.Now()
				So(*checkState.LastChecked(), ShouldHappenOnOrBetween, t0, t1)
				So(checkState.Status(), ShouldEqual, healthcheck.StatusOK)
				So(checkState.Message(), ShouldEqual, health.MsgHealthyConsumerGroup)
			})
		})

		Convey("And a consumer group connected to a different topic", func() {
			cg := &ConsumerGroup{
				topic:   "wrongTopic",
				brokers: []health.SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct to Critical state", func() {
				checkState := healthcheck.NewCheckState(health.ServiceName)
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
				So(err, ShouldResemble, errMock)
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

			Convey("Then the ready channel is closed", func(c C) {
				validateChanClosed(c, channels.Ready, true)
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

			Convey("Then closing the ready channel results in the loop ending its execution", func(c C) {
				close(channels.Ready)
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

// validateChanClosed validates that a channel is closed before a timeout expires
func validateChanClosed(c C, ch chan struct{}, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(testChanTimeout):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}
