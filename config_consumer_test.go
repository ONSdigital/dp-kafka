package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConsumerGroupConfig(t *testing.T) {
	Convey("Given a valid kafka version", t, func() {
		kafkaVersion, err := sarama.ParseKafkaVersion(testKafkaVersion)
		So(err, ShouldBeNil)

		Convey("getConsumerGroupConfig with a consumerGroupConfig with some values results in the expected values being overwritten in the default sarama config", func() {
			cgConfig := &ConsumerGroupConfig{
				RetryBackoff: &testRetryBackoff,
				Topic:        testTopic,
				BrokerAddrs:  testBrokerAddrs,
				GroupName:    testGroupName,
			}
			config, err := cgConfig.Get()
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

			Convey("And the default values are set for the non-kafka configuration", func() {
				So(*cgConfig.NumWorkers, ShouldEqual, defaultNumWorkers)
				So(*cgConfig.BatchSize, ShouldEqual, defaultBatchSize)
				So(*cgConfig.BatchWaitTime, ShouldEqual, defaultBatchWaitTime)
			})
		})

		Convey("getConsumerGroupConfig with a valid fully-populated consumerGroupConfig results in the expected values being overwritten in the default sarama config", func() {
			numWorkers := 3
			batchSize := 50
			batchWaitTime := 500 * time.Millisecond
			cgConfig := &ConsumerGroupConfig{
				KafkaVersion:     &testKafkaVersion,
				KeepAlive:        &testKeepAlive,
				RetryBackoff:     &testRetryBackoff,
				RetryBackoffFunc: &testConsumerRetryBackoffFunc,
				Offset:           &testOffsetNewest,
				SecurityConfig:   &SecurityConfig{},
				Topic:            testTopic,
				BrokerAddrs:      testBrokerAddrs,
				GroupName:        testGroupName,
				NumWorkers:       &numWorkers,
				BatchSize:        &batchSize,
				BatchWaitTime:    &batchWaitTime,
			}
			config, err := cgConfig.Get()
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

			Convey("And the default values are set for the non-kafka configuration", func() {
				So(*cgConfig.NumWorkers, ShouldEqual, numWorkers)
				So(*cgConfig.BatchSize, ShouldEqual, batchSize)
				So(*cgConfig.BatchWaitTime, ShouldEqual, batchWaitTime)
			})

		})

		Convey("getConsumerGroupConfig with consumerGroupConfig containing an invalid kafka version returns the expected error", func() {
			wrongVersion := "wrongVersion"
			cgConfig := &ConsumerGroupConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				BrokerAddrs:  testBrokerAddrs,
				GroupName:    testGroupName,
			}
			config, err := cgConfig.Get()
			So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
			So(config, ShouldBeNil)
		})

		Convey("getConsumerGroupConfig with consumerGroupConfig containing an invalid offset returns the expected error", func() {
			wrongOffset := int64(678)
			cgConfig := &ConsumerGroupConfig{
				Offset:      &wrongOffset,
				Topic:       testTopic,
				BrokerAddrs: testBrokerAddrs,
				GroupName:   testGroupName,
			}
			config, err := cgConfig.Get()
			So(err, ShouldResemble, errors.New("offset value incorrect"))
			So(config, ShouldBeNil)
		})
	})
}
