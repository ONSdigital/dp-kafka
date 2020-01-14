package kafka_test

import (
	"testing"

	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/mock"
	cluster "github.com/bsm/sarama-cluster"
	. "github.com/smartystreets/goconvey/convey"
)

var testGroup = "testGroup"

// createConsumerChannels creates local channels for testing
func createConsumerChannels(sync bool) (chUpstream chan kafka.Message, chCloser, chClosed chan struct{}, chErrors chan error, chUpstreamDone chan bool) {
	if sync {
		// make the upstream channel buffered, so we can send-and-wait for upstreamDone
		chUpstream = make(chan kafka.Message, 1)
	} else {
		chUpstream = make(chan kafka.Message)
	}
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
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumer,
		}
		chUpstream, chCloser, chClosed, chErrors, chUpstreamDone := createConsumerChannels(true)
		Convey("Missing upstream channel will cause ErrNoUpstreamChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				nil, chCloser, chClosed, chErrors, chUpstreamDone, clusterCli)
			So(consumer, ShouldResemble, &kafka.ConsumerGroup{})
			So(err, ShouldEqual, kafka.ErrNoUpstreamChannel)
		})
		Convey("Missing closer channel will cause ErrNoCloserChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				chUpstream, nil, chClosed, chErrors, chUpstreamDone, clusterCli)
			So(consumer, ShouldResemble, &kafka.ConsumerGroup{})
			So(err, ShouldEqual, kafka.ErrNoCloserChannel)
		})
		Convey("Missing closed channel will cause ErrNoClosedChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				chUpstream, chCloser, nil, chErrors, chUpstreamDone, clusterCli)
			So(consumer, ShouldResemble, &kafka.ConsumerGroup{})
			So(err, ShouldEqual, kafka.ErrNoClosedChannel)
		})
		Convey("Missing errors channel will cause ErrNoErrorChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				chUpstream, chCloser, chClosed, nil, chUpstreamDone, clusterCli)
			So(consumer, ShouldResemble, &kafka.ConsumerGroup{})
			So(err, ShouldEqual, kafka.ErrNoErrorChannel)
		})
		Convey("Missing upstream-done channel will cause ErrNoUpstreadmDoneChannel", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				chUpstream, chCloser, chClosed, chErrors, nil, clusterCli)
			So(consumer, ShouldResemble, &kafka.ConsumerGroup{})
			So(err, ShouldEqual, kafka.ErrNoUpstreamDoneChannel)
		})
	})
}
