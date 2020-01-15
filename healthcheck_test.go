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

// createMockBrokers creates mock brokers for testing, without providing topic metadata
func createMockBrokers(t *testing.T) (brokers []*sarama.MockBroker) {
	for _, addr := range testBrokers {
		mockBroker := sarama.NewMockBrokerAddr(t, 1, addr)
		mockBroker.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(mockBroker.Addr(), mockBroker.BrokerID()),
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
func createProducerForTesting(brokers []string) (kafka.Producer, error) {
	ctx := context.Background()
	chSaramaErr, chSaramaIn := createSaramaChannels()
	saramaCli := &mock.SaramaMock{
		NewAsyncProducerFunc: createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn),
	}
	chOut, chErr, chCloser, chClosed := createProducerChannels()
	return kafka.NewProducerWithSaramaClient(
		ctx, brokers, testTopic, 123,
		chOut, chErr, chCloser, chClosed, saramaCli)
}

// createConsumerForTestig creates a consumer with a mock Sarama library for testing
func createConsumerForTestig(brokers []string) (*kafka.ConsumerGroup, error) {
	ctx := context.Background()
	clusterCli := &mock.SaramaClusterMock{
		NewConsumerFunc: mockNewConsumer,
	}
	chUpstream, chCloser, chClosed, chErrors, chUpstreamDone := createConsumerChannels(true)
	return kafka.NewConsumerWithChannelsAndClusterClient(
		ctx, brokers, testTopic, testGroup, kafka.OffsetNewest, true,
		chUpstream, chCloser, chClosed, chErrors, chUpstreamDone, clusterCli)
}

// TestKafkaProducerHealthcheck tests that the producer healthcheck fails with expected severities and errors
func TestKafkaProducerHealthcheck(t *testing.T) {

	brokers := createMockBrokers(t)
	defer closeMockBrokers(brokers)

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {

		Convey("Producer configured with those brokers returns a warning Check structure", func() {
			producer, err := createProducerForTesting(testBrokers)
			So(err, ShouldBeNil)
			validateWarningProducerCheck(&producer, "unexpected metadata response for broker/s. Invalid brokers: [localhost:12300 localhost:12301]")
		})

		Convey("Producer configured with different brokers returns a critical Check structure", func() {
			producer, err := createProducerForTesting([]string{"localhost:12399"})
			So(err, ShouldBeNil)
			validateCriticalProducerCheck(&producer, "broker/s not reachable at addresses: [localhost:12399]")
		})
	})
}

// TestKafkaConsumerHealthcheck tests that the consumer healthcheck fails with expected severities and errors
func TestKafkaConsumerHealthcheck(t *testing.T) {

	brokers := createMockBrokers(t)
	defer closeMockBrokers(brokers)

	Convey("Given that kafka brokers are available, without topic metadata", t, func() {

		Convey("Consumer configured with those brokers return  a warning Check structure", func() {
			consumer, err := createConsumerForTestig(testBrokers)
			So(err, ShouldBeNil)
			validateWarningConsumerGroupCheck(consumer, "unexpected metadata response for broker/s. Invalid brokers: [localhost:12300 localhost:12301]")
		})

		Convey("Consumer configured with different brokers returns a critical Check structure", func() {
			consumer, err := createConsumerForTestig([]string{"localhost:12399"})
			So(err, ShouldBeNil)
			validateCriticalConsumerGroupCheck(consumer, "broker/s not reachable at addresses: [localhost:12399]")
		})
	})
}

func validateSuccessfulProducerCheck(cli *kafka.Producer) (check *health.Check) {
	t0 := time.Now().UTC()
	check, err := cli.Checker(nil)
	t1 := time.Now().UTC()
	So(err, ShouldBeNil)
	validateSuccessfulCheck(check, t0, t1, kafka.MsgHealthyProducer)
	return check
}

func validateSuccessfulConsumerGroupCheck(cli *kafka.ConsumerGroup) (check *health.Check) {
	t0 := time.Now().UTC()
	check, err := cli.Checker(nil)
	t1 := time.Now().UTC()
	So(err, ShouldBeNil)
	validateSuccessfulCheck(check, t0, t1, kafka.MsgHealthyConsumerGroup)
	return check
}

func validateSuccessfulCheck(check *health.Check, t0 time.Time, t1 time.Time, msgHealthy string) {
	So(check.Name, ShouldEqual, kafka.ServiceName)
	So(check.Status, ShouldEqual, health.StatusOK)
	So(check.Message, ShouldEqual, msgHealthy)
	So(check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastSuccess, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastFailure, ShouldHappenBefore, t0)
}

func validateWarningProducerCheck(cli *kafka.Producer, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedMessage, health.StatusWarning)
	return check, err
}

func validateWarningConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedMessage, health.StatusWarning)
	return check, err
}

func validateCriticalProducerCheck(cli *kafka.Producer, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedMessage, health.StatusCritical)
	return check, err
}

func validateCriticalConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedMessage, health.StatusCritical)
	return check, err
}

func validateUnsuccessfulCheck(check *health.Check, t0 time.Time, t1 time.Time, expectedMessage string, expectedSeverity string) {
	So(check.Name, ShouldEqual, kafka.ServiceName)
	So(check.Status, ShouldEqual, expectedSeverity)
	So(check.Message, ShouldEqual, expectedMessage)
	So(check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastSuccess, ShouldHappenBefore, t0)
	So(check.LastFailure, ShouldHappenOnOrBetween, t0, t1)
}
