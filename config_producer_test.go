package kafka

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

func TestProducerConfig(t *testing.T) {
	Convey("Given a valid kafka version", t, func() {

		kafkaVersion, err := sarama.ParseKafkaVersion(testKafkaVersion)
		So(err, ShouldBeNil)

		Convey("getProducerConfig with a producerConfig with some values results in the expected values being overwritten in the default sarama config", func() {
			pConfig := &ProducerConfig{
				MaxMessageBytes: &testMaxMessageBytes,
				RetryBackoff:    &testRetryBackoff,
				Topic:           testTopic,
				BrokerAddrs:     testBrokerAddrs,
			}
			config, err := pConfig.Get()
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, sarama.V1_0_0_0)
			So(config.Net.KeepAlive, ShouldEqual, 0)
			So(config.Producer.MaxMessageBytes, ShouldEqual, testMaxMessageBytes)
			So(config.Producer.Retry.Max, ShouldEqual, 3)
			So(config.Producer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Producer.Retry.BackoffFunc, ShouldBeNil)
			So(config.Net.TLS.Enable, ShouldBeFalse)

			Convey("And the default values are set for the non-kafka configuration", func() {
				So(*pConfig.MinRetryPeriod, ShouldEqual, defaultMinRetryPeriod)
				So(*pConfig.MaxRetryPeriod, ShouldEqual, defaultMaxRetryPeriod)
			})
		})

		Convey("getProducerConfig with a valid fully-populated producerConfig results in the expected values being overwritten in the default sarama config", func() {
			pConfig := &ProducerConfig{
				KafkaVersion:     &testKafkaVersion,
				MaxMessageBytes:  &testMaxMessageBytes,
				KeepAlive:        &testKeepAlive,
				RetryMax:         &testRetryMax,
				RetryBackoff:     &testRetryBackoff,
				RetryBackoffFunc: &testProducerRetryBackoffFunc,
				SecurityConfig: &SecurityConfig{
					InsecureSkipVerify: true,
				},
				Topic:          testTopic,
				BrokerAddrs:    testBrokerAddrs,
				MinRetryPeriod: &testMinRetryPeriod,
				MaxRetryPeriod: &testMaxRetryPeriod,
			}
			config, err := pConfig.Get()
			So(err, ShouldBeNil)
			So(config.Version, ShouldResemble, kafkaVersion)
			So(config.Net.KeepAlive, ShouldEqual, testKeepAlive)
			So(config.Producer.MaxMessageBytes, ShouldEqual, testMaxMessageBytes)
			So(config.Producer.Retry.Max, ShouldEqual, testRetryMax)
			So(config.Producer.Retry.Backoff, ShouldEqual, testRetryBackoff)
			So(config.Producer.Retry.BackoffFunc, ShouldEqual, testProducerRetryBackoffFunc)
			So(config.Net.TLS.Enable, ShouldBeTrue)
			So(config.Net.TLS.Config.InsecureSkipVerify, ShouldBeTrue)

			Convey("And the values are overwritten for the non-kafka configuration", func() {
				So(*pConfig.MinRetryPeriod, ShouldEqual, testMinRetryPeriod)
				So(*pConfig.MaxRetryPeriod, ShouldEqual, testMaxRetryPeriod)
			})
		})

		Convey("getProducerConfig with producerConfig containing an invalid kafka version returns the expected error", func() {
			wrongVersion := "wrongVersion"
			pConfig := &ProducerConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				BrokerAddrs:  testBrokerAddrs,
			}
			config, err := pConfig.Get()
			So(err, ShouldResemble, fmt.Errorf("error parsing kafka version: %w", errors.New("invalid version `wrongVersion`")))
			So(config, ShouldBeNil)
		})

		Convey("getProducerConfig with producerConfig without a compulsory value returns the expected error", func() {
			pConfig := &ProducerConfig{
				Topic: testTopic,
			}
			config, err := pConfig.Get()
			So(err, ShouldResemble, fmt.Errorf("validation error: %w", errors.New("brokerAddrs is compulsory but was not provided in config")))
			So(config, ShouldBeNil)
		})
	})
}

func TestProducerValidate(t *testing.T) {
	Convey("Validating a producer config without BrokerAddrs returns the expected error", t, func() {
		pConfig := &ProducerConfig{
			Topic: testTopic,
		}
		err := pConfig.Validate()
		So(err, ShouldResemble, errors.New("brokerAddrs is compulsory but was not provided in config"))
	})

	Convey("Validating a producer config without Topic returns the expected error", t, func() {
		pConfig := &ProducerConfig{
			BrokerAddrs: testBrokerAddrs,
		}
		err := pConfig.Validate()
		So(err, ShouldResemble, errors.New("topic is compulsory but was not provided in config"))
	})
}

func TestProducerConfigValidation(t *testing.T) {
	Convey("Given a producer config", t, func() {
		cfg := ProducerConfig{
			Topic:             testTopic,
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
			var minRetryPeriod time.Duration = 1001 * time.Millisecond
			var maxRetryPeriod time.Duration = time.Second
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
