package kafka_test

import (
	"context"
	"testing"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	. "github.com/smartystreets/goconvey/convey"
)

var testGroup = "testGroup"

// createSaramaClusterChannels creates sarama-cluster channels for testing
func createSaramaClusterChannels() (errsChan chan error, msgChan chan *sarama.ConsumerMessage, notiChan chan *cluster.Notification) {
	errsChan = make(chan error)
	msgChan = make(chan *sarama.ConsumerMessage)
	notiChan = make(chan *cluster.Notification)
	return
}

// createMockNewConsumer creates an cluster Consumer mock and returns it,
// as well as a NewConsumerFunc that returns the same cluster Consumer mock.
func createMockNewConsumer(
	errsChan chan error, msgChan chan *sarama.ConsumerMessage, notiChan chan *cluster.Notification) (
	*mock.SaramaClusterConsumerMock, func(addrs []string, groupID string, topics []string, config *cluster.Config) (kafka.SaramaClusterConsumer, error)) {
	// Create ConsmerMock
	var consumerMock = &mock.SaramaClusterConsumerMock{
		ErrorsFunc: func() <-chan error {
			return errsChan
		},
		MessagesFunc: func() <-chan *sarama.ConsumerMessage {
			return msgChan
		},
		NotificationsFunc: func() <-chan *cluster.Notification {
			return notiChan
		},
		CommitOffsetsFunc: func() error {
			return nil
		},
		CloseFunc: func() error {
			return nil
		},
	}
	// Function that returns AsyncProducerMock
	return consumerMock, func(addrs []string, groupID string, topics []string, config *cluster.Config) (kafka.SaramaClusterConsumer, error) {
		return consumerMock, nil
	}
}

// mockNewConsumerEmpty returns a Consumer mock with no methods implemented
// (i.e. if any mock method is called, the test will fail)
func mockNewConsumerEmpty(addrs []string, groupID string, topics []string, config *cluster.Config) (kafka.SaramaClusterConsumer, error) {
	return &mock.SaramaClusterConsumerMock{}, nil
}

// mockNewConsumerError returns a nil Consumer, and ErrSaramaNoBrokers
func mockNewConsumerError(addrs []string, groupID string, topics []string, config *cluster.Config) (kafka.SaramaClusterConsumer, error) {
	return nil, ErrSaramaNoBrokers
}

func TestConsumerMissingChannels(t *testing.T) {

	Convey("Given the intention to initialize a kafka Consumer Group", t, func() {
		ctx := context.Background()
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumerEmpty,
		}

		Convey("Providing an invalid ConsumerGroupChannels struct results in an ErrNoChannel error", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				kafka.ConsumerGroupChannels{
					Upstream: make(chan kafka.Message),
				},
				clusterCli,
			)
			So(consumer, ShouldNotBeNil)
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors, kafka.Closer, kafka.Closed, kafka.UpstreamDone}})
			So(len(clusterCli.NewConsumerCalls()), ShouldEqual, 0)
		})
	})
}

// TestConsumer tests that messages, errors, and closing events are correctly directed to the expected channels
func TestConsumer(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Consumer Group", t, func() {
		ctx := context.Background()

		// Create Sarama Cluster and consumer mock with channels
		errsChan, msgChan, notiChan := createSaramaClusterChannels()
		clusterConsumerMock, funcNewConsumer := createMockNewConsumer(errsChan, msgChan, notiChan)
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: funcNewConsumer,
		}

		// Create ConsumerGroup with channels
		channels := kafka.CreateConsumerGroupChannels(true)
		consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
			ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true, channels, clusterCli)
		expectedCheck := health.Check{Name: kafka.ServiceName}

		Convey("Consumer is correctly created without error", func() {
			So(err, ShouldBeNil)
			So(consumer, ShouldNotBeNil)
			So(consumer.Check, ShouldResemble, &expectedCheck)
			So(consumer.Incoming(), ShouldEqual, channels.Upstream)
			So(consumer.Errors(), ShouldEqual, channels.Errors)
			So(len(clusterCli.NewConsumerCalls()), ShouldEqual, 1)
			So(len(clusterConsumerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("We cannot initialize consumer again", func() {
			// InitializeSarama does not call NewConsumerCalls again
			err = consumer.InitializeSarama(ctx)
			So(err, ShouldBeNil)
			So(len(clusterCli.NewConsumerCalls()), ShouldEqual, 1)
			So(len(clusterConsumerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("StopListeningToConsumer closes closer and closed channels, without actually closing sarama-cluster consumer", func() {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
			So(len(clusterConsumerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Closing the consumer closes Sarama-cluster consumer", func() {
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
			So(len(clusterConsumerMock.CloseCalls()), ShouldEqual, 1)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func() {
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
			So(len(clusterConsumerMock.CloseCalls()), ShouldEqual, 1)
		})

	})
}

// TestConsumerNotInitialized validates that if sarama cluster cannot be initialized, we can still partially use our ConsumerGroup
func TestConsumerNotInitialized(t *testing.T) {

	Convey("Given that Sarama-cluster fails to create a new Consumer while we initialize our ConsumerGroup", t, func() {
		ctx := context.Background()
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumerError,
		}
		channels := kafka.CreateConsumerGroupChannels(true)
		consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
			ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true, channels, clusterCli)
		expectedCheck := health.Check{Name: kafka.ServiceName}

		Convey("Consumer is partially created with channels and checker, returning the Sarama error", func() {
			So(err, ShouldEqual, ErrSaramaNoBrokers)
			So(consumer, ShouldNotBeNil)
			So(consumer.Check, ShouldResemble, &expectedCheck)
			So(consumer.Incoming(), ShouldEqual, channels.Upstream)
			So(consumer.Errors(), ShouldEqual, channels.Errors)
			So(len(clusterCli.NewConsumerCalls()), ShouldEqual, 1)
		})

		Convey("We can try to initialize the consumer again", func() {
			// InitializeSarama does call NewConsumerCalls again
			err = consumer.InitializeSarama(ctx)
			So(err, ShouldEqual, ErrSaramaNoBrokers)
			So(len(clusterCli.NewConsumerCalls()), ShouldEqual, 2)
		})

		Convey("StopListeningToConsumer closes closer and closed channels", func() {
			consumer.StopListeningToConsumer(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
		})

		Convey("Closing the consumer closes the caller channels", func() {
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
		})

		Convey("Closing the consumer after StopListeningToConsumer channels doesn't panic because of channels being closed", func() {
			consumer.StopListeningToConsumer(ctx)
			consumer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
		})

	})

}
