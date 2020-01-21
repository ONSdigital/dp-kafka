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

func mockNewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (*cluster.Consumer, error) {
	return &cluster.Consumer{}, nil
}

func TestConsumerMissingChannels(t *testing.T) {

	Convey("Given the intention to initialize a kafka Consumer Group", t, func() {
		ctx := context.Background()
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumer,
		}

		Convey("Providing an invalid ConsumerGroupChannels struct results in an ErrNoChannel error", func() {
			consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
				ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
				kafka.ConsumerGroupChannels{
					Upstream: make(chan kafka.Message),
				},
				clusterCli,
			)
			So(consumer, ShouldResemble, kafka.ConsumerGroup{})
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors, kafka.Closer, kafka.Closed, kafka.UpstreamDone}})
		})
	})
}
