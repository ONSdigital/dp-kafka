package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testMaxMessageBytes          = 1234
	testKeepAlive                = 3 * time.Second
	testRetryMax                 = 10
	testRetryBackoff             = 5 * time.Second
	testProducerRetryBackoffFunc = func(retries, maxRetries int) time.Duration { return time.Second }
	testConsumerRetryBackoffFunc = func(retries int) time.Duration { return time.Second }
	testKafkaVersion             = "1.0.2"
	testOffsetNewest             = OffsetNewest
	testTLS                      = "TLS"
)

func TestProducerConfig(t *testing.T) {

	Convey("Given a valid kafka version", t, func() {

		kafkaVersion, err := sarama.ParseKafkaVersion(testKafkaVersion)
		So(err, ShouldBeNil)

		Convey("getProducerConfig with nil producerConfig results in the default sarama config being returned", func() {
			config, err := getProducerConfig(nil)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, sarama.V1_0_0_0)
			So(config.Net.KeepAlive, ShouldEqual, 0)
			So(config.Producer.MaxMessageBytes, ShouldEqual, 1000000)
			So(config.Producer.Retry.Max, ShouldEqual, 3)
			So(config.Producer.Retry.Backoff, ShouldEqual, 100*time.Millisecond)
			So(config.Producer.Retry.BackoffFunc, ShouldBeNil)
		})

		Convey("getProducerConfig with a producerConfig with some values results in the expected values being overwritten in the default sarama config", func() {
			pConfig := &ProducerConfig{
				MaxMessageBytes: &testMaxMessageBytes,
				RetryBackoff:    &testRetryBackoff,
			}
			config, err := getProducerConfig(pConfig)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, sarama.V1_0_0_0)
			So(config.Net.KeepAlive, ShouldEqual, 0)
			So(config.Producer.MaxMessageBytes, ShouldEqual, testMaxMessageBytes)
			So(config.Producer.Retry.Max, ShouldEqual, 3)
			So(config.Producer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Producer.Retry.BackoffFunc, ShouldBeNil)
			So(config.Net.TLS.Enable, ShouldBeFalse)
		})

		Convey("getProducerConfig with a valid fully-populated producerConfig results in the expected values being overwritten in the default sarama config", func() {
			pConfig := &ProducerConfig{
				KafkaVersion:     &testKafkaVersion,
				MaxMessageBytes:  &testMaxMessageBytes,
				KeepAlive:        &testKeepAlive,
				RetryMax:         &testRetryMax,
				RetryBackoff:     &testRetryBackoff,
				RetryBackoffFunc: &testProducerRetryBackoffFunc,
				SecurityConfig: SecurityConfig{
					Protocol:           &testTLS,
					InsecureSkipVerify: true,
				},
			}
			config, err := getProducerConfig(pConfig)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, kafkaVersion)
			So(config.Net.KeepAlive, ShouldEqual, testKeepAlive)
			So(config.Producer.MaxMessageBytes, ShouldEqual, testMaxMessageBytes)
			So(config.Producer.Retry.Max, ShouldEqual, testRetryMax)
			So(config.Producer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Producer.Retry.BackoffFunc, ShouldEqual, testProducerRetryBackoffFunc)
			So(config.Net.TLS.Enable, ShouldBeTrue)
			So(config.Net.TLS.Config.InsecureSkipVerify, ShouldBeTrue)
		})

		Convey("getProducerConfig with producerConfig containing an invalid kafka version returns the expected error", func() {
			wrongVersion := "wrongVersion"
			pConfig := &ProducerConfig{
				KafkaVersion: &wrongVersion,
			}
			config, err := getProducerConfig(pConfig)
			So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
			So(config, ShouldBeNil)
		})

	})
}

func TestConsumerGroupConfig(t *testing.T) {
	Convey("Given a valid kafka version", t, func() {
		kafkaVersion, err := sarama.ParseKafkaVersion(testKafkaVersion)
		So(err, ShouldBeNil)

		Convey("getConsumerGroupConfig with nil consumerGroupConfig results in the default sarama config being returned, with the compulsory hardcoded values", func() {
			config, err := getConsumerGroupConfig(nil)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, sarama.V1_0_0_0)
			So(config.Consumer.MaxWaitTime, ShouldEqual, 50*time.Millisecond)
			So(config.Consumer.Offsets.Initial, ShouldEqual, sarama.OffsetOldest)
			So(config.Consumer.Return.Errors, ShouldBeTrue)
			So(config.Consumer.Group.Rebalance.Strategy, ShouldEqual, sarama.BalanceStrategyRoundRobin)
			So(config.Consumer.Group.Session.Timeout, ShouldEqual, time.Second*10)
			So(config.Net.KeepAlive, ShouldEqual, 0)
			So(config.Consumer.Retry.Backoff, ShouldEqual, 2*time.Second)
			So(config.Consumer.Retry.BackoffFunc, ShouldBeNil)
			So(config.Net.TLS.Enable, ShouldBeFalse)
		})

		Convey("getConsumerGroupConfig with a consumerGroupConfig with some values results in the expected values being overwritten in the default sarama config", func() {
			cgConfig := &ConsumerGroupConfig{
				RetryBackoff: &testRetryBackoff,
			}
			config, err := getConsumerGroupConfig(cgConfig)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, sarama.V1_0_0_0)
			So(config.Consumer.MaxWaitTime, ShouldEqual, 50*time.Millisecond)
			So(config.Consumer.Offsets.Initial, ShouldEqual, sarama.OffsetOldest)
			So(config.Consumer.Return.Errors, ShouldBeTrue)
			So(config.Consumer.Group.Rebalance.Strategy, ShouldEqual, sarama.BalanceStrategyRoundRobin)
			So(config.Consumer.Group.Session.Timeout, ShouldEqual, time.Second*10)
			So(config.Net.KeepAlive, ShouldEqual, 0)
			So(config.Consumer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Consumer.Retry.BackoffFunc, ShouldBeNil)
		})

		Convey("getConsumerGroupConfig with a valid fully-populated consumerGroupConfig results in the expected values being overwritten in the default sarama config", func() {
			cgConfig := &ConsumerGroupConfig{
				KafkaVersion:     &testKafkaVersion,
				KeepAlive:        &testKeepAlive,
				RetryBackoff:     &testRetryBackoff,
				RetryBackoffFunc: &testConsumerRetryBackoffFunc,
				Offset:           &testOffsetNewest,
				SecurityConfig: SecurityConfig{
					Protocol: &testTLS,
				},
			}
			config, err := getConsumerGroupConfig(cgConfig)
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, kafkaVersion)
			So(config.Consumer.MaxWaitTime, ShouldEqual, 50*time.Millisecond)
			So(config.Consumer.Return.Errors, ShouldBeTrue)
			So(config.Consumer.Group.Rebalance.Strategy, ShouldEqual, sarama.BalanceStrategyRoundRobin)
			So(config.Consumer.Group.Session.Timeout, ShouldEqual, time.Second*10)
			So(config.Net.KeepAlive, ShouldEqual, testKeepAlive)
			So(config.Consumer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Consumer.Retry.BackoffFunc, ShouldEqual, testConsumerRetryBackoffFunc)
			So(config.Consumer.Offsets.Initial, ShouldEqual, testOffsetNewest)
			So(config.Net.TLS.Enable, ShouldBeTrue)
			So(config.Net.TLS.Config.InsecureSkipVerify, ShouldBeFalse)

		})

		Convey("getConsumerGroupConfig with consumerGroupConfig containing an invalid kafka version returns the expected error", func() {
			wrongVersion := "wrongVersion"
			cgConfig := &ConsumerGroupConfig{
				KafkaVersion: &wrongVersion,
			}
			config, err := getConsumerGroupConfig(cgConfig)
			So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
			So(config, ShouldBeNil)
		})

		Convey("getConsumerGroupConfig with consumerGroupConfig containing an invalid offset returns the expected error", func() {
			wrongOffset := int64(678)
			cgConfig := &ConsumerGroupConfig{
				Offset: &wrongOffset,
			}
			config, err := getConsumerGroupConfig(cgConfig)
			So(err, ShouldResemble, errors.New("offset value incorrect"))
			So(config, ShouldBeNil)
		})
	})
}
