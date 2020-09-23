package kafka

import (
	"context"
	"testing"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// testBrokers is a list of broker addresses for testing
var testBrokers = []string{"localhost:12300", "localhost:12301"}

// createMockBrokers creates mock brokers for testing, without providing topic metadata
func createMockBrokers(t *testing.T) (brokers []*sarama.MockBroker) {
	for _, addr := range testBrokers {
		mockBroker := sarama.NewMockBrokerAddr(t, 1, addr)
		mockBroker.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(mockBroker.Addr(), mockBroker.BrokerID()).
				SetLeader(testTopic, 0, mockBroker.BrokerID()),
		})
		brokers = append(brokers, mockBroker)
	}
	return brokers
}

// closeMockBrokers closes the mock brokers passed as parameter
func closeMockBrokers(brokers []*sarama.MockBroker) {
	for _, broker := range brokers {
		broker.Close()
	}
}

// createProducerForTesting creates a producer with a mock Sarama library for testing
func createProducerForTesting(brokerAddrs []string, topic string) (*Producer, error) {
	chSaramaErr, chSaramaIn := createSaramaChannels()
	asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn)
	pInit := func(addrs []string, conf *sarama.Config) (AsyncProducer, error) {
		return asyncProducerMock, nil
	}
	channels := CreateProducerChannels()
	return newProducer(ctx, brokerAddrs, topic, 123, testKafkaVersion, channels, pInit)
}

// createUninitialisedProducerForTesting creates a producer for testing without a valid AsyncProducer
func createUninitialisedProducerForTesting(brokerAddrs []string, topic string) (*Producer, error) {
	pInit := func(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
		return nil, ErrSaramaNoBrokers
	}
	channels := CreateProducerChannels()
	return newProducer(ctx, brokerAddrs, topic, 123, testKafkaVersion, channels, pInit)
}

// createConsumerForTesting creates a consumer with a mock Sarama library for testing
func createConsumerForTesting(brokerAddrs []string, topic string) (*ConsumerGroup, error) {
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
	cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return saramaConsumerGroupMock, nil
	}
	return newConsumerGroup(ctx, brokerAddrs, topic, testGroup, testKafkaVersion, channels, cgInit)
}

// createConsumerForTesting creates a consumer for testing without a valid Sarama ConsuerGroup
func createUninitialisedConsumerForTesting(brokerAddrs []string, topic string) (*ConsumerGroup, error) {
	cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return nil, ErrSaramaNoBrokers
	}
	channels := CreateConsumerGroupChannels(1)
	return newConsumerGroup(ctx, brokerAddrs, topic, testGroup, testKafkaVersion, channels, cgInit)
}

func TestKafkaProducerHealthcheck(t *testing.T) {

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {
		brokers := createMockBrokers(t)
		defer closeMockBrokers(brokers)

		// CheckState for test validation
		checkState := health.NewCheckState(ServiceName)

		Convey("Producer configured with right brokers and topic returns a successful Check structure", func() {
			producer, err := createProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusOK)
			So(checkState.Message(), ShouldEqual, MsgHealthyProducer)
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Uninitialised producer with right config returns a Critical Check structure", func() {
			producer, err := createUninitialisedProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(producer.IsInitialised(), ShouldBeFalse)
			producer.Checker(context.Background(), checkState)
			So(producer.IsInitialised(), ShouldBeFalse)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, ErrInitSarama.Error())
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Producer configured with right brokers and wrong topic returns a warning Check structure", func() {
			producer, err := createProducerForTesting(testBrokers, "wrongTopic")
			So(err, ShouldBeNil)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusWarning)
			So(checkState.Message(), ShouldEqual, "unexpected metadata response for broker(s). Invalid brokers: [localhost:12300 localhost:12301]")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Producer configured with different brokers and right topic returns a critical Check structure", func() {
			producer, err := createProducerForTesting([]string{"localhost:12399"}, testTopic)
			So(err, ShouldBeNil)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "broker(s) not reachable at addresses: [localhost:12399]")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Producer configured with no brokers and right topic returns a critical Check structure", func() {
			producer, err := createProducerForTesting([]string{}, testTopic)
			So(err, ShouldBeNil)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "No brokers defined")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})
	})
}

func TestKafkaConsumerHealthcheck(t *testing.T) {

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {
		brokers := createMockBrokers(t)
		defer closeMockBrokers(brokers)

		// CheckState for test validation
		checkState := health.NewCheckState(ServiceName)

		Convey("Consumer configured with right brokers and topic returns an OK Check structure", func() {
			consumer, err := createConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusOK)
			So(checkState.Message(), ShouldEqual, MsgHealthyConsumerGroup)
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Uninitialised consumer with right config returns a Critical Check structure", func() {
			consumer, err := createUninitialisedConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(consumer.IsInitialised(), ShouldBeFalse)
			consumer.Checker(context.Background(), checkState)
			So(consumer.IsInitialised(), ShouldBeFalse)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, ErrInitSarama.Error())
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Consumer configured with right brokers and wrong topic returns a warning Check structure", func() {
			consumer, err := createConsumerForTesting(testBrokers, "wrongTopic")
			So(err, ShouldBeNil)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusWarning)
			So(checkState.Message(), ShouldEqual, "unexpected metadata response for broker(s). Invalid brokers: [localhost:12300 localhost:12301]")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Consumer configured with different brokers and right topic returns a critical Check structure", func() {
			consumer, err := createConsumerForTesting([]string{"localhost:12399"}, testTopic)
			So(err, ShouldBeNil)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "broker(s) not reachable at addresses: [localhost:12399]")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Consumer configured with no brokers returns a critical Check structure", func() {
			consumer, err := createConsumerForTesting([]string{}, testTopic)
			So(err, ShouldBeNil)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "No brokers defined")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})
	})
}
