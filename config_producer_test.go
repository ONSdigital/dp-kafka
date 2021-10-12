package kafka

import (
	"errors"
	"testing"

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
				Topic:       testTopic,
				BrokerAddrs: testBrokerAddrs,
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
		})

		Convey("getProducerConfig with producerConfig containing an invalid kafka version returns the expected error", func() {
			wrongVersion := "wrongVersion"
			pConfig := &ProducerConfig{
				KafkaVersion: &wrongVersion,
				Topic:        testTopic,
				BrokerAddrs:  testBrokerAddrs,
			}
			config, err := pConfig.Get()
			So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
			So(config, ShouldBeNil)
		})

		Convey("getProducerConfig with producerConfig without a compulsory value returns the expected error", func() {
			pConfig := &ProducerConfig{
				Topic: testTopic,
			}
			config, err := pConfig.Get()
			So(err, ShouldResemble, errors.New("brokerAddrs is compulsory but was not provided in config"))
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
