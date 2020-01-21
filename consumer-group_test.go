package kafka_test

import (
	"context"
	"testing"

	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/mock"
	cluster "github.com/bsm/sarama-cluster"
	. "github.com/smartystreets/goconvey/convey"
)

var testGroup = "testGroup"

// createConsumerChannels creates local channels for testing
func createConsumerChannels() (chUpstream chan kafka.Message, chCloser, chClosed chan struct{},
	chErrors chan error, chUpstreamDone chan bool) {
	chUpstream = make(chan kafka.Message)
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
	chErrors = make(chan error)
	chUpstreamDone = make(chan bool, 1)
	return
}

func mockNewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (*cluster.Consumer, error) {
	return &cluster.Consumer{}, nil
}

func TestConsumerMissingChannels(t *testing.T) {

	Convey("Given the intention to initialize a kafka Consumer Group", t, func() {
		ctx := context.Background()
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumer,
		}
		chUpstream, chCloser, chClosed, chErrors, chUpstreamDone := createConsumerChannels()
		Convey("Missing one channel will cause ErrNoUpstreamChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				kafka.ConsumerGroupChannels{
					Closer:       chCloser,
					Closed:       chClosed,
					Errors:       chErrors,
					UpstreamDone: chUpstreamDone,
				},
				clusterCli)
			So(consumer, ShouldResemble, kafka.ConsumerGroup{})
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{"Upstream"}})
		})
		Convey("Missing some channels will cause ErrNoCloserChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				kafka.ConsumerGroupChannels{
					Upstream: chUpstream,
				},
				clusterCli)
			So(consumer, ShouldResemble, kafka.ConsumerGroup{})
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{"Errors", "Closer", "Closed", "UpstreamDone"}})
		})
	})
}
