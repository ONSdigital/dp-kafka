package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v5/interfaces"
	"github.com/ONSdigital/dp-kafka/v5/mock"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testBroker0 = "localhost:12300"
	testBroker1 = "localhost:12301"
	testBroker2 = "localhost:12302"
	testBroker3 = "localhost:12303"
)

const (
	testTopic  = "testTopic"
	testTopic2 = "testTopic2"
)

// testBrokers is a list of valid broker addresses for testing
var testBrokers = []string{testBroker0, testBroker1, testBroker2}

var ctx = context.Background()

func createMockBrokers(t *testing.T) (brokers map[string]*sarama.MockBroker) {
	var (
		partition int32 = 0
		leaderID  int32 = 0 // broker0 is the leader
	)

	// Broker 0 is the leader of testTopic partition 0
	broker0 := sarama.NewMockBrokerAddr(t, 0, testBroker0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(testTopic, partition, leaderID),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	// Broker 1 is available for testTopic partition 0
	broker1 := sarama.NewMockBrokerAddr(t, 0, testBroker1)
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetLeader(testTopic, partition, leaderID),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	// Broker 2 is available for testTopic partition 0
	broker2 := sarama.NewMockBrokerAddr(t, 0, testBroker2)
	broker2.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker2.Addr(), broker2.BrokerID()).
			SetLeader(testTopic, partition, leaderID),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	// Broker 3 is the leader of testTopic2 partition 0
	broker3 := sarama.NewMockBrokerAddr(t, 0, testBroker3)
	broker3.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker3.Addr(), broker3.BrokerID()).
			SetLeader(testTopic2, partition, broker3.BrokerID()),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	return map[string]*sarama.MockBroker{
		broker0.Addr(): broker0,
		broker1.Addr(): broker1,
		broker2.Addr(): broker2,
		broker3.Addr(): broker3,
	}
}

// closeMockBrokers closes the mock brokers passed as parameter
func closeMockBrokers(brokers map[string]*sarama.MockBroker) {
	for _, broker := range brokers {
		broker.Close()
	}
}

// createProducerForTesting creates a producer with a mock Sarama library for testing
func createProducerForTesting(brokerAddrs []string, topic string) (*Producer, error) {
	chSaramaErr, chSaramaIn, chSaramaSuccesses := createSaramaChannels()
	asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn, chSaramaSuccesses)
	pInit := func(addrs []string, conf *sarama.Config) (interfaces.SaramaAsyncProducer, error) {
		return asyncProducerMock, nil
	}
	pConfig := &ProducerConfig{
		BrokerAddrs: brokerAddrs,
		Topic:       topic,
	}
	return NewProducerWithGenerators(ctx, pConfig, pInit, SaramaNewBroker)
}

// createUninitialisedProducerForTesting creates a producer for testing without a valid AsyncProducer
func createUninitialisedProducerForTesting(brokerAddrs []string, topic string) (*Producer, error) {
	pInit := func(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
		return nil, ErrSaramaNoBrokers
	}
	pConfig := &ProducerConfig{
		BrokerAddrs: brokerAddrs,
		Topic:       topic,
	}
	return NewProducerWithGenerators(ctx, pConfig, pInit, SaramaNewBroker)
}

// createConsumerForTesting creates a consumer with a mock Sarama library for testing
func createConsumerForTesting(brokerAddrs []string, topic string) (*ConsumerGroup, error) {
	channels := CreateConsumerGroupChannels(1, ErrorChanBufferSize)
	saramaConsumerGroupMock := &mock.SaramaConsumerGroupMock{
		ErrorsFunc: func() <-chan error {
			return make(chan error)
		},
		ConsumeFunc: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
			select {
			case <-channels.Initialised:
			default:
				close(channels.Initialised)
			}
			return nil
		},
	}
	cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return saramaConsumerGroupMock, nil
	}
	cgConfig := &ConsumerGroupConfig{
		BrokerAddrs: brokerAddrs,
		Topic:       topic,
		GroupName:   testGroup,
	}
	return NewConsumerGroupWithGenerators(ctx, cgConfig, cgInit, SaramaNewBroker)
}

// createConsumerForTesting creates a consumer for testing without a valid Sarama ConsuerGroup
func createUninitialisedConsumerForTesting(brokerAddrs []string, topic string) (*ConsumerGroup, error) {
	cgInit := func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return nil, ErrSaramaNoBrokers
	}
	cgConfig := &ConsumerGroupConfig{
		BrokerAddrs: brokerAddrs,
		Topic:       topic,
		GroupName:   testGroup,
	}
	return NewConsumerGroupWithGenerators(ctx, cgConfig, cgInit, SaramaNewBroker)
}

func TestKafkaProducerHealthcheck(t *testing.T) {
	Convey("Given 3 kafka brokers for one topic and 1 for another (Producer Health Check)", t, func() {
		brokers := createMockBrokers(t)
		defer closeMockBrokers(brokers)

		// CheckState for test validation
		checkState := health.NewCheckState(ServiceName)

		Convey("And a producer configured with the right brokers and topic", func() {
			p, err := createProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK'", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, MsgHealthyProducer)
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a producer configured with the right topic and 2 reachable and 1 unreachable brokers", func() {
			p, err := createProducerForTesting([]string{testBroker0, testBroker1, "localhost:0000"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK' because at least 2 brokers are reachable and valid", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, "broker(s) not reachable at: [localhost:0000]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a producer configured with the right topic and 3 reachable brokers, but only 2 containing the topic in its metadata", func() {
			p, err := createProducerForTesting([]string{testBroker0, testBroker1, testBroker3}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK' because at least 2 brokers are reachable and valid", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, "topic testTopic not available in broker(s): [localhost:12303]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a producer configured with the right topic and only 1 reachable broker", func() {
			p, err := createProducerForTesting([]string{testBroker0, "localhost:0000", "localhost:1111"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL' because at least 2 valid brokers are required", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldBeIn, []string{
					"broker(s) not reachable at: [localhost:0000 localhost:1111]",
					"broker(s) not reachable at: [localhost:1111 localhost:0000]",
				})
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a producer configured with a topic that is not available in the brokers metadata", func() {
			p, err := createProducerForTesting(testBrokers, "anotherTopic")
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldBeIn, []string{
					"topic anotherTopic not available in broker(s): [localhost:12300 localhost:12301 localhost:12302]",
					"topic anotherTopic not available in broker(s): [localhost:12300 localhost:12302 localhost:12301]",
					"topic anotherTopic not available in broker(s): [localhost:12301 localhost:12300 localhost:12302]",
					"topic anotherTopic not available in broker(s): [localhost:12301 localhost:12302 localhost:12300]",
					"topic anotherTopic not available in broker(s): [localhost:12302 localhost:12300 localhost:12301]",
					"topic anotherTopic not available in broker(s): [localhost:12302 localhost:12301 localhost:12300]",
				})
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a producer configured with one reachable broker, one unreachable broker and one reachable broker with the wrong topic", func() {
			p, err := createProducerForTesting([]string{testBroker2, testBroker3, "localhost:0000"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldEqual, "broker(s) not reachable at: [localhost:0000], topic testTopic not available in broker(s): [localhost:12303]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And an uninitialised producer", func() {
			p, err := createUninitialisedProducerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(p.IsInitialised(), ShouldBeFalse)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := p.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusWarning)
				So(checkState.Message(), ShouldEqual, "kafka producer is not initialised")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})
	})
}

func TestKafkaConsumerHealthcheck(t *testing.T) {
	Convey("Given 3 kafka brokers for one topic and 1 for another (Consumer Health Check)", t, func() {
		brokers := createMockBrokers(t)
		defer closeMockBrokers(brokers)

		// CheckState for test validation
		checkState := health.NewCheckState(ServiceName)

		Convey("And a consumer-group configured with the right brokers and topic", func() {
			cg, err := createConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK'", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, MsgHealthyConsumerGroup)
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a consumer-group configured with the right topic and a reachable and unreachable broker", func() {
			cg, err := createConsumerForTesting([]string{testBroker0, "localhost:0000"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK' because at least one broker is reachable and valid", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, "broker(s) not reachable at: [localhost:0000]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a consumer-group configured with the right topic and 2 reachable brokers, but only one containing the topic in its metadata", func() {
			cg, err := createConsumerForTesting([]string{testBroker0, testBroker3}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'OK' because at least one broker is reachable and valid", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusOK)
				So(checkState.Message(), ShouldEqual, "topic testTopic not available in broker(s): [localhost:12303]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a consumer-group configured with the right topic and only unreachable brokers", func() {
			cg, err := createConsumerForTesting([]string{"localhost:0000", "localhost:1111"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldBeIn, []string{
					"broker(s) not reachable at: [localhost:0000 localhost:1111]",
					"broker(s) not reachable at: [localhost:1111 localhost:0000]",
				})
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a consumer-group configured with a topic that is not available in the brokers metadata", func() {
			cg, err := createConsumerForTesting(testBrokers, "test2")
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldBeIn, []string{
					"topic test2 not available in broker(s): [localhost:12300 localhost:12301 localhost:12302]",
					"topic test2 not available in broker(s): [localhost:12300 localhost:12302 localhost:12301]",
					"topic test2 not available in broker(s): [localhost:12301 localhost:12300 localhost:12302]",
					"topic test2 not available in broker(s): [localhost:12301 localhost:12302 localhost:12300]",
					"topic test2 not available in broker(s): [localhost:12302 localhost:12301 localhost:12300]",
					"topic test2 not available in broker(s): [localhost:12302 localhost:12300 localhost:12301]",
				})
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And a consumer-group configured with one unreachable broker and one reachable broker with the wrong topic", func() {
			cg, err := createConsumerForTesting([]string{testBroker3, "localhost:0000"}, testTopic)
			So(err, ShouldBeNil)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusCritical)
				So(checkState.Message(), ShouldEqual, "broker(s) not reachable at: [localhost:0000], topic testTopic not available in broker(s): [localhost:12303]")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})

		Convey("And an uninitialised consumer-group", func() {
			cg, err := createUninitialisedConsumerForTesting(testBrokers, testTopic)
			So(err, ShouldBeNil)
			So(cg.IsInitialised(), ShouldBeFalse)
			Convey("Then Checker sets the health status to 'CRITICAL'", func() {
				err := cg.Checker(context.Background(), checkState)
				So(err, ShouldBeNil)
				So(checkState.Status(), ShouldEqual, health.StatusWarning)
				So(checkState.Message(), ShouldEqual, "kafka consumer-group is not initialised")
				So(checkState.StatusCode(), ShouldEqual, 0)
			})
		})
	})
}
