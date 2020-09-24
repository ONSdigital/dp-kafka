package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testGroup = "testGroup"
)

var ctx = context.Background()

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
		So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Errors, Ready, Closer, Closed}})
	})
}

func TestConsumer(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Consumer Group", t, func() {

		channels := CreateConsumerGroupChannels(1)

		saramaConsumerGroupMock := &mock.SaramaConsumerGroupMock{
			ErrorsFunc: func() <-chan error {
				return make(chan error)
			},
			ConsumeFunc: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
				select {
				case <-channels.Ready:
				default:
					close(channels.Ready)
				}
				return nil
			},
		}

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

		Convey("We cannot initialise consumer again", func() {
			err = consumer.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cgInitCalls, ShouldEqual, 1)
		})

		Convey("StopListeningToConsumer closes closer channels, without actually closing sarama-cluster consumer", func() {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, false)
		})

		Convey("Closing the consumer closes Sarama-cluster consumer and closed channel", func() {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, true)
			So(len(saramaConsumerGroupMock.CloseCalls()), ShouldEqual, 1)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func() {
			saramaConsumerGroupMock.CloseFunc = func() error {
				return nil
			}
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, true)
			So(len(saramaConsumerGroupMock.CloseCalls()), ShouldEqual, 1)
		})

	})
}

func TestConsumerNotInitialised(t *testing.T) {

	Convey("Given that Sarama-cluster fails to create a new Consumer while we initialise our ConsumerGroup", t, func() {
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

		Convey("StopListeningToConsumer closes closer channel only", func() {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, false)
		})

		Convey("Closing the consumer closes the caller channels", func() {
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, true)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func() {
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer, true)
			validateChannelClosed(channels.Closed, true)
		})

	})

}
