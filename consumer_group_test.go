package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-kafka/v2/mock"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testGroup = "testGroup"
	errMock   = errors.New("sarama mock error")
	ctx       = context.Background()
)

func TestConsumerCreationError(t *testing.T) {

	Convey("Providing an invalid kafka version results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		consumer, err := newConsumerGroup(
			ctx, testBrokers, testTopic, testGroup,
			nil,
			&ConsumerGroupConfig{KafkaVersion: &wrongVersion},
			nil,
		)
		So(consumer, ShouldBeNil)
		So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
	})

	Convey("Providing an incomplete ConsumerGroupChannels struct results in an ErrNoChannel error and consumer will not be initialised", t, func() {
		consumer, err := newConsumerGroup(
			ctx, testBrokers, testTopic, testGroup,
			&ConsumerGroupChannels{
				Upstream: make(chan Message),
			},
			nil,
			nil,
		)
		So(consumer, ShouldBeNil)
		So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Errors, Ready, Consume, Closer, Closed}})
	})
}

func TestConsumerInitialised(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Consumer Group", t, func(c C) {
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		saramaConsumerGroupMock := saramaConsumerGroupHappy(chConsumeCalled)
		cgInitCalls := 0
		cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			cgInitCalls++
			return saramaConsumerGroupMock, nil
		}

		consumer, err := newConsumerGroup(ctx, testBrokers, testTopic, testGroup, channels, nil, cgInit)

		Convey("Consumer is correctly created and initialised without error", func() {
			So(err, ShouldBeNil)
			So(consumer, ShouldNotBeNil)
			So(channels.Upstream, ShouldEqual, channels.Upstream)
			So(channels.Errors, ShouldEqual, channels.Errors)
			So(cgInitCalls, ShouldEqual, 1)
			So(consumer.IsInitialised(), ShouldBeTrue)
		})

		Convey("We cannot initialise consumer again, but Initialise does not return an error", func() {
			err = consumer.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cgInitCalls, ShouldEqual, 1)
		})

		Convey("The consumer is in 'stopped' state", func() {
			<-channels.Ready // wait until Ready channel is closed to prevent any data race condition between writing and reading the state
			So(consumer.state, ShouldEqual, Stopped)
		})

		Convey("StopListeningToConsumer closes closer channels, without actually closing sarama-cluster consumer", func(c C) {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, false)
		})

		Convey("Closing the consumer closes Sarama-cluster consumer and closed channel", func(c C) {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			consumer.Close(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, true)
			So(len(saramaConsumerGroupMock.CloseCalls()), ShouldEqual, 1)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func(c C) {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, true)
			So(len(saramaConsumerGroupMock.CloseCalls()), ShouldEqual, 1)
		})
	})
}

func TestConsumerNotInitialised(t *testing.T) {

	Convey("Given that Sarama-cluster fails to create a new Consumer while we initialise our ConsumerGroup", t, func(c C) {
		channels := CreateConsumerGroupChannels(1)
		cgInitCalls := 0
		cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			cgInitCalls++
			return nil, ErrSaramaNoBrokers
		}
		consumer, err := newConsumerGroup(ctx, testBrokers, testTopic, testGroup, channels, nil, cgInit)

		Convey("Consumer is partially created with channels and checker, but it is not initialised", func() {
			So(err, ShouldBeNil)
			So(consumer, ShouldNotBeNil)
			So(channels.Upstream, ShouldEqual, channels.Upstream)
			So(channels.Errors, ShouldEqual, channels.Errors)
			So(cgInitCalls, ShouldEqual, 1)
			So(consumer.IsInitialised(), ShouldBeFalse)
		})

		Convey("We can try to initialise the consumer again and the same error is returned", func() {
			err = consumer.Initialise(ctx)
			So(err, ShouldEqual, ErrSaramaNoBrokers)
			So(cgInitCalls, ShouldEqual, 2)
		})

		Convey("StopListeningToConsumer closes closer channel only", func(c C) {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, false)
		})

		Convey("Closing the consumer closes the caller channels", func(c C) {
			consumer.Close(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, true)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func(c C) {
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(c, channels.Closer, true)
			validateChannelClosed(c, channels.Closed, true)
		})
	})
}

func TestConsumerStopped(t *testing.T) {
	Convey("Given a Kafka consumergroup in stopped state", t, func(c C) {
		channels := CreateConsumerGroupChannels(1)
		cg := &ConsumerGroup{
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

		Convey("When the stoppedState go-routine ends due to the 'Closer' channel being closed", func(c C) {
			close(channels.Closer)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the stoppedState go-routine ends due to the 'Consume' channel being closed", func(c C) {
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the stoppedState go-routine ends due to the 'Consume' receiving a 'true' value'", func(c C) {
			channels.Consume <- true
			wg.Wait()

			Convey("Then the state is set to 'Starting'", func(c C) {
				c.So(cg.state, ShouldEqual, Starting)
			})
		})

		Convey("When the stoppedState go-routine ends due to 'StopListeningToConsumer' being called", func(c C) {
			cg.StopListeningToConsumer(ctx)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
			})
		})
	})
}

func TestConsumerStarting(t *testing.T) {
	Convey("Given a Kafka consumergroup in starting state and a successful SaramaConsumerGroup mock", t, func(c C) {
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
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
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel being closed", func(c C) {
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' receiving a 'false' value'", func(c C) {
			channels.Consume <- false
			wg.Wait()

			Convey("Then the state is set to 'Stopping'", func(c C) {
				c.So(cg.state, ShouldEqual, Stopping)
			})
		})

		Convey("When the startingState go-routine ends due to 'StopListeningToConsumer' being called", func(c C) {
			cg.StopListeningToConsumer(ctx)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
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
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel being closed after SaramaCg.Consume had previously failed", func(c C) {
			<-chConsumeCalled
			close(channels.Consume)
			wg.Wait()

			Convey("Then the state is set to 'Closing'", func(c C) {
				c.So(cg.state, ShouldEqual, Closing)
			})
		})

		Convey("When the startingState go-routine ends due to the 'Consume' channel receiving a 'false' value after SaramaCg.Consume had previously failed", func(c C) {
			<-chConsumeCalled
			channels.Consume <- false
			wg.Wait()

			Convey("Then the state is set to 'Stopping'", func(c C) {
				c.So(cg.state, ShouldEqual, Stopping)
			})
		})
	})
}

func TestConsumeLoop(t *testing.T) {
	Convey("Given a Kafka createConsumeLoop", t, func(c C) {
		channels := CreateConsumerGroupChannels(1)
		chConsumeCalled := make(chan struct{})
		cg := &ConsumerGroup{
			channels: channels,
			saramaCg: saramaConsumerGroupHappy(chConsumeCalled),
			mutex:    &sync.Mutex{},
			wgClose:  &sync.WaitGroup{},
		}
		cg.createConsumeLoop(ctx)
		<-channels.Ready // Wait until the initial loop is established (i.e. StoppedState)

		Convey("then the state is set to 'Stopped'", func() {
			So(cg.state, ShouldEqual, Stopped)
		})

		Convey("when a 'true' value is sent to the Consume channel", func() {
			channels.Consume <- true

			Convey("then the the startingState loop sets the state to 'Starting' and sarama consumer starts consuming", func() {
				<-chConsumeCalled
				So(cg.state, ShouldEqual, Starting)
			})
		})

		Convey("when the Closer channel is closed", func() {
			close(channels.Closer)

			Convey("then the state is set to 'Closing' and the loop finishes its execution", func() {
				cg.wgClose.Wait()
				So(cg.state, ShouldEqual, Closing)
			})
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
