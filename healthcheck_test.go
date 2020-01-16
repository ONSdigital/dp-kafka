package kafka_test

import (
	"context"
	"testing"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/mock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestKafkaProducerOk(t *testing.T) {

	Convey("Given that kafka broker is available", t, func() {
		ctx := context.Background()
		chSaramaErr, chSaramaIn := createSaramaChannels()
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn),
		}
		chOut, chErr, chCloser, chClosed := createProducerChannels()
		producer, err := kafka.NewProducerWithSaramaClient(
			ctx, testBrokers, testTopic, 123,
			chOut, chErr, chCloser, chClosed, saramaCli)

		So(err, ShouldBeNil)

		Convey("Producer returns a successful Check structure", func() {
			validateSuccessfulProducerCheck(&producer)
		})
	})
}

func TestKafkaConsumerOk(t *testing.T) {

	Convey("Given that kafka broker is available", t, func() {
		ctx := context.Background()
		clusterCli := &mock.SaramaClusterMock{
			NewConsumerFunc: mockNewConsumer,
		}
		chUpstream, chCloser, chClosed, chErrors, chUpstreamDone := createConsumerChannels(true)
		consumer, err := kafka.NewConsumerWithChannelsAndClusterClient(
			ctx, testBrokers, testTopic, testGroup, kafka.OffsetNewest, true,
			chUpstream, chCloser, chClosed, chErrors, chUpstreamDone, clusterCli)
		So(err, ShouldBeNil)

		Convey("Consumer returns a successful Check structure", func() {
			validateSuccessfulConsumerGroupCheck(consumer)
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
	So(check.StatusCode, ShouldEqual, 200)
	So(check.Message, ShouldEqual, msgHealthy)
	So(check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastSuccess, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastFailure, ShouldHappenBefore, t0)
}

func validateWarningProducerCheck(cli *kafka.Producer, expectedCode int, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedCode, expectedMessage, health.StatusWarning)
	return check, err
}

func validateWarningConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedCode int, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedCode, expectedMessage, health.StatusWarning)
	return check, err
}

func validateCriticalProducerCheck(cli *kafka.Producer, expectedCode int, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedCode, expectedMessage, health.StatusCritical)
	return check, err
}

func validateCriticalConsumerGroupCheck(cli *kafka.ConsumerGroup, expectedCode int, expectedMessage string) (check *health.Check, err error) {
	t0 := time.Now().UTC()
	check, err = cli.Checker(nil)
	t1 := time.Now().UTC()
	validateUnsuccessfulCheck(check, t0, t1, expectedCode, expectedMessage, health.StatusCritical)
	return check, err
}

func validateUnsuccessfulCheck(check *health.Check, t0 time.Time, t1 time.Time, expectedCode int, expectedMessage string, expectedSeverity string) {
	So(check.Name, ShouldEqual, kafka.ServiceName)
	So(check.Status, ShouldEqual, expectedSeverity)
	So(check.StatusCode, ShouldEqual, expectedCode)
	So(check.Message, ShouldEqual, expectedMessage)
	So(check.LastChecked, ShouldHappenOnOrBetween, t0, t1)
	So(check.LastSuccess, ShouldHappenBefore, t0)
	So(check.LastFailure, ShouldHappenOnOrBetween, t0, t1)
}
