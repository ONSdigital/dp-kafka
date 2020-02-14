package kafka_test

import (
	"context"
	"testing"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
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
func createProducerForTesting(brokers []string, topic string) (kafka.Producer, error) {
	ctx := context.Background()
	chSaramaErr, chSaramaIn := createSaramaChannels()
	_, funcNewAsyncProducer := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn)
	saramaCli := &mock.SaramaMock{
		NewAsyncProducerFunc: funcNewAsyncProducer,
	}
	channels := kafka.CreateProducerChannels()
	return kafka.NewProducerWithSaramaClient(ctx, brokers, topic, 123, channels, saramaCli)
}

// createUninitialisedProducerForTesting creates a producer for testing without a valid AsyncProducer
func createUninitialisedProducerForTesting(brokers []string, topic string) (kafka.Producer, error) {
	ctx := context.Background()
	saramaCli := &mock.SaramaMock{
		NewAsyncProducerFunc: mockNewAsyncProducerError,
	}
	channels := kafka.CreateProducerChannels()
	return kafka.NewProducerWithSaramaClient(ctx, brokers, topic, 123, channels, saramaCli)
}

// createConsumerForTesting creates a consumer with a mock Sarama library for testing
func createConsumerForTesting(brokers []string, topic string) (kafka.ConsumerGroup, error) {
	ctx := context.Background()
	errsChan, msgChan, notiChan := createSaramaClusterChannels()
	_, funcNewConsumer := createMockNewConsumer(errsChan, msgChan, notiChan)
	clusterCli := &mock.SaramaClusterMock{
		NewConsumerFunc: funcNewConsumer,
	}
	channels := kafka.CreateConsumerGroupChannels(true)
	return kafka.NewConsumerWithClusterClient(
		ctx, brokers, topic, testGroup, kafka.OffsetNewest, true, channels, clusterCli)
}

// createUninitialisedConsumerForTesting creates a consumer for testing without a valid Sarama-cluster consumer
func createUninitialisedConsumerForTesting(brokers []string, topic string) (kafka.ConsumerGroup, error) {
	ctx := context.Background()
	clusterCli := &mock.SaramaClusterMock{
		NewConsumerFunc: mockNewConsumerError,
	}
	channels := kafka.CreateConsumerGroupChannels(true)
	return kafka.NewConsumerWithClusterClient(
		ctx, brokers, topic, testGroup, kafka.OffsetNewest, true, channels, clusterCli)
}

// TestKafkaProducerHealthcheck checks that the producer healthcheck fails with expected severities and errors
func TestKafkaProducerHealthcheck(t *testing.T) {

	brokers := createMockBrokers(t)
	defer closeMockBrokers(brokers)

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {

		// CheckState for test validation
		checkState := health.NewCheckState(kafka.ServiceName)

		Convey("Producer configured with right brokers and topic returns a successful Check structure", func() {
			producer, err := createProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusOK)
			So(checkState.Message(), ShouldEqual, kafka.MsgHealthyProducer)
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Uninitialised producer with right config returns a Critical Check structure", func() {
			producer, err := createUninitialisedProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(producer.IsInitialised(), ShouldBeFalse)
			producer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, kafka.ErrInitSarama.Error())
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

// TestKafkaConsumerHealthcheck checks that the consumer healthcheck fails with expected severities and errors
func TestKafkaConsumerHealthcheck(t *testing.T) {

	brokers := createMockBrokers(t)
	defer closeMockBrokers(brokers)

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {

		// CheckState for test validation
		checkState := health.NewCheckState(kafka.ServiceName)

		Convey("Consumer configured with right brokers and wrong topic returns an OK Check structure", func() {
			consumer, err := createConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusOK)
			So(checkState.Message(), ShouldEqual, kafka.MsgHealthyConsumerGroup)
			So(checkState.StatusCode(), ShouldEqual, 0)
		})

		Convey("Uninitialised consumer with right config returns a Critical Check structure", func() {
			consumer, err := createUninitialisedConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(consumer.IsInitialised(), ShouldBeFalse)
			consumer.Checker(context.Background(), checkState)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, kafka.ErrInitSarama.Error())
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
