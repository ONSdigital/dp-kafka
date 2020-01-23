package kafka_test

import (
	"context"
	"testing"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// testBrokers is a list of broker addresses for testing
var testBrokers = []string{"localhost:12300", "localhost:12301"}

// initial check that should be created by client constructor
var expectedInitialCheck = &health.Check{
	Name: kafka.ServiceName,
}

// create a successful check without lastFailed value
func createSuccessfulCheck(t time.Time, msg string) health.Check {
	return health.Check{
		Name:        kafka.ServiceName,
		LastSuccess: &t,
		LastChecked: &t,
		Status:      health.StatusOK,
		Message:     msg,
	}
}

// create a critical check without lastSuccess value
func createCriticalCheck(t time.Time, msg string) health.Check {
	return health.Check{
		Name:        kafka.ServiceName,
		LastFailure: &t,
		LastChecked: &t,
		Status:      health.StatusCritical,
		Message:     msg,
	}
}

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

		Convey("Producer configured with right brokers and topic returns a successful Check structure", func() {
			producer, err := createProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			validateSuccessfulProducerCheck(&producer)
			So(producer.Check.LastFailure, ShouldBeNil)
		})

		Convey("Uninitialised producer with right config returns a Critical Check structure", func() {
			producer, err := createUninitialisedProducerForTesting(testBrokers, testTopic)
			So(err, ShouldResemble, ErrSaramaNoBrokers)
			validateCriticalProducerCheck(&producer, kafka.ErrInitSarama.Error())
			So(producer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Producer configured with right brokers and wrong topic returns a warning Check structure", func() {
			producer, err := createProducerForTesting(testBrokers, "wrongTopic")
			So(err, ShouldBeNil)
			So(producer.Check, ShouldResemble, expectedInitialCheck)
			validateWarningProducerCheck(&producer, "unexpected metadata response for broker(s). Invalid brokers: [localhost:12300 localhost:12301]")
			So(producer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Producer configured with different brokers and right topic returns a critical Check structure", func() {
			producer, err := createProducerForTesting([]string{"localhost:12399"}, testTopic)
			So(err, ShouldBeNil)
			So(producer.Check, ShouldResemble, expectedInitialCheck)
			validateCriticalProducerCheck(&producer, "broker(s) not reachable at addresses: [localhost:12399]")
			So(producer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Producer configured with no brokers and right topic returns a critical Check structure", func() {
			producer, err := createProducerForTesting([]string{}, testTopic)
			So(err, ShouldBeNil)
			So(producer.Check, ShouldResemble, expectedInitialCheck)
			validateCriticalProducerCheck(&producer, "No brokers defined")
			So(producer.Check.LastSuccess, ShouldBeNil)
		})
	})
}

// TestKafkaConsumerHealthcheck checks that the consumer healthcheck fails with expected severities and errors
func TestKafkaConsumerHealthcheck(t *testing.T) {

	brokers := createMockBrokers(t)
	defer closeMockBrokers(brokers)

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {

		Convey("Consumer configured with right brokers and wrong topic returns an OK Check structure", func() {
			consumer, err := createConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			validateSuccessfulConsumerGroupCheck(&consumer)
			So(consumer.Check.LastFailure, ShouldBeNil)
		})

		Convey("Uninitialised consumer with right config returns a Critical Check structure", func() {
			consumer, err := createUninitialisedConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldResemble, ErrSaramaNoBrokers)
			validateCriticalConsumerGroupCheck(&consumer, kafka.ErrInitSarama.Error())
			So(consumer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Consumer configured with right brokers and wrong topic returns a warning Check structure", func() {
			consumer, err := createConsumerForTesting(testBrokers, "wrongTopic")
			So(err, ShouldBeNil)
			So(consumer.Check, ShouldResemble, expectedInitialCheck)
			validateWarningConsumerGroupCheck(&consumer, "unexpected metadata response for broker(s). Invalid brokers: [localhost:12300 localhost:12301]")
			So(consumer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Consumer configured with different brokers and right topic returns a critical Check structure", func() {
			consumer, err := createConsumerForTesting([]string{"localhost:12399"}, testTopic)
			So(err, ShouldBeNil)
			So(consumer.Check, ShouldResemble, expectedInitialCheck)
			validateCriticalConsumerGroupCheck(&consumer, "broker(s) not reachable at addresses: [localhost:12399]")
			So(consumer.Check.LastSuccess, ShouldBeNil)
		})

		Convey("Consumer configured with no brokers returns a critical Check structure", func() {
			consumer, err := createConsumerForTesting([]string{}, testTopic)
			So(err, ShouldBeNil)
			So(consumer.Check, ShouldResemble, expectedInitialCheck)
			validateCriticalConsumerGroupCheck(&consumer, "No brokers defined")
			So(consumer.Check.LastSuccess, ShouldBeNil)
		})
	})
}

func TestCheckerSuccessHistory(t *testing.T) {

	Convey("Given that we have a producer and a consumer with previous successful checks", t, func() {

		consumer, err := createConsumerForTesting([]string{"localhost:12399"}, testTopic)
		So(err, ShouldBeNil)
		So(consumer.Check, ShouldResemble, expectedInitialCheck)

		producer, err := createProducerForTesting([]string{"localhost:12399"}, testTopic)
		So(err, ShouldBeNil)
		So(producer.Check, ShouldResemble, expectedInitialCheck)

		lastCheckTime := time.Now().UTC().Add(-1 * time.Minute)
		previousCheckConsumer := createSuccessfulCheck(lastCheckTime, kafka.MsgHealthyConsumerGroup)
		previousCheckProducer := createSuccessfulCheck(lastCheckTime, kafka.MsgHealthyProducer)
		consumer.Check = &previousCheckConsumer
		producer.Check = &previousCheckProducer

		Convey("A new healthcheck keeps the non-overwritten values for consumer", func() {
			validateCriticalConsumerGroupCheck(&consumer, "broker(s) not reachable at addresses: [localhost:12399]")
			So(consumer.Check.LastSuccess, ShouldResemble, &lastCheckTime)
		})

		Convey("A new healthcheck keeps the non-overwritten values for producer", func() {
			validateCriticalProducerCheck(&producer, "broker(s) not reachable at addresses: [localhost:12399]")
			So(consumer.Check.LastSuccess, ShouldResemble, &lastCheckTime)
		})
	})

}

func TestCheckerFailureHistory(t *testing.T) {

	Convey("Given that we have a producer and a consumer with previous failed checks", t, func() {

		brokers := createMockBrokers(t)
		defer closeMockBrokers(brokers)

		consumer, err := createConsumerForTesting(testBrokers, testTopic)
		So(err, ShouldBeNil)
		So(consumer.Check, ShouldResemble, expectedInitialCheck)

		producer, err := createProducerForTesting(testBrokers, testTopic)
		So(err, ShouldBeNil)
		So(producer.Check, ShouldResemble, expectedInitialCheck)

		lastCheckTime := time.Now().UTC().Add(-1 * time.Minute)
		previousCheckConsumer := createCriticalCheck(lastCheckTime, "consumer error")
		previousCheckProducer := createCriticalCheck(lastCheckTime, "producer error")
		consumer.Check = &previousCheckConsumer
		producer.Check = &previousCheckProducer

		Convey("A new healthcheck keeps the non-overwritten values for consumer", func() {
			validateSuccessfulConsumerGroupCheck(&consumer)
			So(consumer.Check.LastFailure, ShouldResemble, &lastCheckTime)
		})

		Convey("A new healthcheck keeps the non-overwritten values for producer", func() {
			validateSuccessfulProducerCheck(&producer)
			So(consumer.Check.LastFailure, ShouldResemble, &lastCheckTime)
		})
	})

}

func validateSuccessfulProducerCheck(cli *kafka.Producer) (check *health.Check) {
	t0 := time.Now().UTC()
	check, err := cli.Checker(nil)
	t1 := time.Now().UTC()
	So(err, ShouldBeNil)
	validateSuccessfulCheck(check, kafka.MsgHealthyProducer, t0, t1)
	So(cli.Check, ShouldResemble, check)
	return check
}

func validateSuccessfulConsumerGroupCheck(cli *kafka.ConsumerGroup) (check *health.Check) {
	t0 := time.Now().UTC()
	check, err := cli.Checker(nil)
	t1 := time.Now().UTC()
	So(err, ShouldBeNil)
	validateSuccessfulCheck(check, kafka.MsgHealthyConsumerGroup, t0, t1)
	So(cli.Check, ShouldResemble, check)
	return check
}

func validateSuccessfulCheck(check *health.Check, msgHealthy string, t0 time.Time, t1 time.Time) {
	So(check.Name, ShouldEqual, kafka.ServiceName)
	So(check.Status, ShouldEqual, health.StatusOK)
	So(check.Message, ShouldEqual, msgHealthy)
	So(*check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(*check.LastSuccess, ShouldHappenOnOrBetween, t0, t1)
}

func validateWarningProducerCheck(cli *kafka.Producer, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, expectedMessage, health.StatusWarning, t0, t1)
	So(cli.Check, ShouldResemble, check)
	return check, err
}

func validateWarningConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, expectedMessage, health.StatusWarning, t0, t1)
	So(cli.Check, ShouldResemble, check)
	return check, err
}

func validateCriticalProducerCheck(cli *kafka.Producer, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, expectedMessage, health.StatusCritical, t0, t1)
	So(cli.Check, ShouldResemble, check)
	return check, err
}

func validateCriticalConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, expectedMessage, health.StatusCritical, t0, t1)
	So(cli.Check, ShouldEqual, check)
	return check, err
}

func validateUnsuccessfulCheck(check *health.Check, expectedMessage string, expectedSeverity string, t0 time.Time, t1 time.Time) {
	So(check.Name, ShouldEqual, kafka.ServiceName)
	So(check.Status, ShouldEqual, expectedSeverity)
	So(check.Message, ShouldEqual, expectedMessage)
	So(*check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(*check.LastFailure, ShouldHappenOnOrBetween, t0, t1)
}
