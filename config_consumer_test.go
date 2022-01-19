package kafka

import (
	"errors"
	"fmt"
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
			So(config.Consumer.Offsets.Initial, ShouldEqual, OffsetOldest)
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
				So(*cgConfig.MinRetryPeriod, ShouldEqual, defaultMinRetryPeriod)
				So(*cgConfig.MaxRetryPeriod, ShouldEqual, defaultMaxRetryPeriod)
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
				MinRetryPeriod:   &testMinRetryPeriod,
				MaxRetryPeriod:   &testMaxRetryPeriod,
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

			Convey("And the values are overwritten for the non-kafka configuration", func() {
				So(*cgConfig.NumWorkers, ShouldEqual, numWorkers)
				So(*cgConfig.BatchSize, ShouldEqual, batchSize)
				So(*cgConfig.BatchWaitTime, ShouldEqual, batchWaitTime)
				So(*cgConfig.MinRetryPeriod, ShouldEqual, testMinRetryPeriod)
				So(*cgConfig.MaxRetryPeriod, ShouldEqual, testMaxRetryPeriod)
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
			So(err, ShouldResemble, fmt.Errorf("error parsing kafka version: %w", errors.New("invalid version `wrongVersion`")))
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

func TestConsumerGroupConfigValidation(t *testing.T) {
	Convey("Given a consumer group config", t, func() {
		cfg := ConsumerGroupConfig{
			Topic:             testTopic,
			GroupName:         testGroupName,
			BrokerAddrs:       testBrokerAddrs,
			MinRetryPeriod:    &testMinRetryPeriod,
			MaxRetryPeriod:    &testMaxRetryPeriod,
			MinBrokersHealthy: &testMinBrokersHealthy,
		}

		Convey("With all values being valid, then Validate does not return an error", func() {
			err := cfg.Validate()
			So(err, ShouldBeNil)
		})

		Convey("With an empty topic value, then Validate fails with the expected error", func() {
			cfg.Topic = ""
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("topic is compulsory but was not provided in config"))
		})

		Convey("With an empty groupName value, then Validate fails with the expected error", func() {
			cfg.GroupName = ""
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("groupName is compulsory but was not provided in config"))
		})

		Convey("With an empty array of broker addrs, then Validate fails with the expected error", func() {
			cfg.BrokerAddrs = []string{}
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("brokerAddrs is compulsory but was not provided in config"))
		})

		Convey("With a zero value for MinRetryPeriod, then Validate fails with the expected error", func() {
			var minRetryPeriod time.Duration = 0
			cfg.MinRetryPeriod = &minRetryPeriod
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("minRetryPeriod must be greater than zero"))
		})

		Convey("With a zero value for MaxRetryPeriod, then Validate fails with the expected error", func() {
			var maxRetryPeriod time.Duration = 0
			cfg.MaxRetryPeriod = &maxRetryPeriod
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("maxRetryPeriod must be greater than zero"))
		})

		Convey("With MinRetryPeriod greater than MaxRetryPeriod, then Validate fails with the expected error", func() {
			minRetryPeriod := 1001 * time.Millisecond
			maxRetryPeriod := time.Second
			cfg.MinRetryPeriod = &minRetryPeriod
			cfg.MaxRetryPeriod = &maxRetryPeriod
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("minRetryPeriod must be smaller or equal to maxRetryPeriod"))
		})

		Convey("With a zero value for MinBrokersHealthy, then Validate fails with the expected error", func() {
			minBrokersHealthy := 0
			cfg.MinBrokersHealthy = &minBrokersHealthy
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("minBrokersHealthy must be greater than zero"))
		})

		Convey("With a zero value for MinBrokersHealthy greater than the total number of brokers, then Validate fails with the expected error", func() {
			minBrokersHealthy := 4
			cfg.MinBrokersHealthy = &minBrokersHealthy
			err := cfg.Validate()
			So(err, ShouldResemble, errors.New("minBrokersHealthy must be smaller or equal to the total number of brokers provided in brokerAddrs"))
		})
	})
}
