package kafkatest

import (
	"context"
	"sync"
	"testing"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/avro"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	errNewMessage = "failed to create new message"
)

var testSchema = `{
	"type": "record",
	"name": "test-schema",
	"fields": [
	  {"name": "field1", "type": "string", "default": ""},
	  {"name": "field2", "type": "string", "default": ""}
	]
  }`

var TestSchema = &avro.Schema{
	Definition: testSchema,
}

type TestEvent struct {
	Field1 string `avro:"field1"`
	Field2 string `avro:"field2"`
}

func TestNewProducer(t *testing.T) {
	longRetry := time.Minute

	pConfig := &kafka.ProducerConfig{
		Topic:          "test-topic",
		BrokerAddrs:    []string{"addr1", "addr2", "addr3"},
		MinRetryPeriod: &longRetry, // set a long retry to prevent race conditions when we check IsInitialised()
		MaxRetryPeriod: &longRetry,
	}

	Convey("Given an uninitialised producer mock", t, func() {
		p, err := NewProducer(
			ctx,
			pConfig,
			&ProducerConfig{
				InitAtCreation: false,
			},
		)
		So(err, ShouldBeNil)
		So(p.Mock.IsInitialised(), ShouldBeFalse)

		Convey("It can be successfully initialised, closing 'Initialised' channel", func() {
			initClosed := false
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-p.Mock.Channels().Initialised
				initClosed = true
			}()
			err := p.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(p.Mock.IsInitialised(), ShouldBeTrue)
			wg.Wait()
			So(initClosed, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseProducer(p)
		})
	})

	Convey("Given an initialised producer mock", t, func() {
		p, err := NewProducer(
			ctx,
			pConfig,
			&ProducerConfig{
				InitAtCreation: true,
			},
		)
		So(err, ShouldBeNil)
		So(p.Mock.IsInitialised(), ShouldBeTrue)

		Convey("Calling initialise again has no effect", func() {
			err := p.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(p.Mock.IsInitialised(), ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseProducer(p)
		})
	})

	Convey("Creating a producer with a nil mock producer configuration results in the default config being used", t, func() {
		p, err := NewProducer(
			ctx,
			pConfig,
			nil,
		)
		So(err, ShouldBeNil)
		So(p.cfg, ShouldResemble, DefaultProducerConfig)

		Convey("And, accordingly, the producer being initialised", func() {
			So(p.Mock.IsInitialised(), ShouldBeTrue)
		})
	})

	Convey("Creating a producer with a nil kafka producer configuration results in the expected error being returned", t, func() {
		_, err := NewProducer(
			ctx,
			nil,
			nil,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "kafka producer config must be provided")
	})

	Convey("Creating a producer with an invalid kafka producer configuration results in the expected error being returned", t, func() {
		_, err := NewProducer(
			ctx,
			&kafka.ProducerConfig{},
			nil,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "failed to create producer for testing: failed to get producer config: validation error: topic is compulsory but was not provided in config")
	})
}

func TestWaitForMessageSent(t *testing.T) {
	pConfig := &kafka.ProducerConfig{
		Topic:       "test-topic",
		BrokerAddrs: []string{"addr1", "addr2", "addr3"},
	}

	Convey("Given a valid kafkatest producer", t, func() {
		p, err := NewProducer(ctx, pConfig, nil)
		So(err, ShouldBeNil)

		Convey("When a valid event is sent", func(c C) {
			go func() {
				err := p.Mock.Send(context.Background(), TestSchema, &TestEvent{
					Field1: "value one",
					Field2: "value two",
				})
				c.So(err, ShouldBeNil)
			}()

			Convey("Then WaitForMessageSent correctly reads and unmarshals the message without error", func() {
				event := &TestEvent{}
				err := p.WaitForMessageSent(TestSchema, event, time.Second)
				So(err, ShouldBeNil)
				So(event, ShouldResemble, &TestEvent{
					Field1: "value one",
					Field2: "value two",
				})
			})

			Convey("Then WaitForMessageSent with the wrong avro schema fails to unmarhsal the event with the expected error", func() {
				event := &TestEvent{}
				err := p.WaitForMessageSent(&avro.Schema{}, event, time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "error unmarshalling sent message: Unknown type name: ")
			})
		})

		Convey("When the producer's Closer channel is closed", func(c C) {
			go func() {
				kafka.SafeClose(p.Mock.Channels().Closer)
			}()

			Convey("Then WaitForMessageSent fails with the expected error when the Closer channel is closed", func() {
				event := &TestEvent{}
				err := p.WaitForMessageSent(TestSchema, event, time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "closer channel closed")
			})
		})

		Convey("When the mock's saramaMessage channel is closed", func(c C) {
			go func() {
				close(p.saramaMessages)
			}()

			Convey("Then WaitForMessageSent fails with the expected error", func() {
				event := &TestEvent{}
				err := p.WaitForMessageSent(TestSchema, event, time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "sarama messages channel closed")
			})
		})

		Convey("When nothing is sent, then WaitForMessageSent fails after the timeout expires", func() {
			timeout := 100 * time.Millisecond

			event := &TestEvent{}
			t0 := time.Now()
			err := p.WaitForMessageSent(TestSchema, event, timeout)
			t1 := time.Now()

			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "timeout while waiting for kafka message being produced")
			So(t1, ShouldHappenOnOrAfter, t0.Add(timeout))
		})
	})
}

func TestWaitNoMessageSent(t *testing.T) {
	pConfig := &kafka.ProducerConfig{
		Topic:       "test-topic",
		BrokerAddrs: []string{"addr1", "addr2", "addr3"},
	}

	Convey("Given a valid kafkatest producer", t, func() {
		p, err := NewProducer(ctx, pConfig, nil)
		So(err, ShouldBeNil)

		Convey("When no event is sent within the time window", func(c C) {
			timeWindow := 100 * time.Millisecond

			t0 := time.Now()
			err := p.WaitNoMessageSent(timeWindow)
			t1 := time.Now()

			Convey("Then WaitNoMessageSent returns after the expected time has elapsed, with no error", func() {
				So(err, ShouldBeNil)
				So(t1, ShouldHappenOnOrAfter, t0.Add(timeWindow))
			})
		})

		Convey("When a valid event is sent", func(c C) {
			go func() {
				err := p.Mock.Send(context.Background(), TestSchema, &TestEvent{
					Field1: "value one",
					Field2: "value two",
				})
				c.So(err, ShouldBeNil)
			}()

			Convey("Then WaitNoMessageSent fails with the expected error", func() {
				err := p.WaitNoMessageSent(time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unexpected message was sent within the time window")
			})
		})

		Convey("When the producer's Closer channel is closed", func(c C) {
			go func() {
				kafka.SafeClose(p.Mock.Channels().Closer)
			}()

			Convey("Then WaitNoMessageSent fails with the expected error", func() {
				err := p.WaitNoMessageSent(time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "closer channel closed")
			})
		})

		Convey("When the mock's saramaMessage channel is closed", func(c C) {
			go func() {
				close(p.saramaMessages)
			}()

			Convey("Then WaitForMessageSent fails with the expected error", func() {
				err := p.WaitNoMessageSent(time.Second)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "sarama messages channel closed")
			})
		})
	})
}

func validateCloseProducer(p *Producer) {
	closedOutput := false
	closedErrors := false
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-p.Mock.Channels().Output
		closedOutput = true
		<-p.Mock.Channels().Errors
		closedErrors = true
		<-p.Mock.Channels().Closer
		closedCloser = true
		<-p.Mock.Channels().Closed
		closedClosed = true
	}()
	err := p.Mock.Close(ctx)
	So(err, ShouldBeNil)
	wg.Wait()

	So(closedOutput, ShouldBeTrue)
	So(closedErrors, ShouldBeTrue)
	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}
