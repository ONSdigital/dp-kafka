package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// timeout for test channels message propagation
const TIMEOUT = 100 * time.Millisecond

// ErrSaramaNoBrokers is the error returned by Sarama when trying to create an AsyncProducer without available brokers
var ErrSaramaNoBrokers = errors.New("kafka client has run out of available brokers to talk to (Is your cluster reachable?)")

// createSaramaChannels creates sarama channels for testing
func createSaramaChannels() (saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) {
	saramaErrsChan = make(chan *sarama.ProducerError)
	saramaInputChan = make(chan *sarama.ProducerMessage)
	return
}

// createMockErrorsFunc returns a mock for AsyncProducer ErrorsFunc with the provided Sarama errors Channel
func createMockErrorsFunc(saramaErrsChan chan *sarama.ProducerError) func() <-chan *sarama.ProducerError {
	return func() <-chan *sarama.ProducerError {
		return saramaErrsChan
	}
}

// AsyncProducer InputFunc returns a mock for AsyncProducer InputFunc with the provied Sarama input channel
func createMockInputFunc(saramaInputChan chan *sarama.ProducerMessage) func() chan<- *sarama.ProducerMessage {
	return func() chan<- *sarama.ProducerMessage {
		return saramaInputChan
	}
}

// createMockNewAsyncProducerComplete creates an AsyncProducer mock and returns it.
func createMockNewAsyncProducerComplete(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) *mock.SaramaAsyncProducerMock {
	return &mock.SaramaAsyncProducerMock{
		ErrorsFunc: createMockErrorsFunc(saramaErrsChan),
		InputFunc:  createMockInputFunc(saramaInputChan),
		CloseFunc:  func() error { return nil },
	}
}

// GetFromSaramaChans select Sarama channels, and return whichever is triggered (only one value per call).
// If none is triggered after timeout, timeout will be triggered
func GetFromSaramaChans(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) (
	input *sarama.ProducerMessage, err *sarama.ProducerError, timeout bool) {
	select {
	case input := <-saramaInputChan:
		return input, nil, false
	case err := <-saramaErrsChan:
		return nil, err, false
	case <-time.After(TIMEOUT):
		return nil, nil, true
	}
}

func TestProducerCreation(t *testing.T) {
	Convey("Providing a nil context results in the expected error being returned", t, func() {
		p, err := newProducer(
			nil,
			&ProducerConfig{},
			nil,
		)
		So(p, ShouldBeNil)
		So(err, ShouldResemble, errors.New("nil context was passed to producer constructor"))
	})

	Convey("Providing an invalid config (kafka version) results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		p, err := newProducer(
			ctx,
			&ProducerConfig{
				KafkaVersion: &wrongVersion,
				BrokerAddrs:  testBrokers,
				Topic:        testTopic,
			},
			nil,
		)
		So(p, ShouldBeNil)
		So(err, ShouldResemble, fmt.Errorf("failed to get producer config: %w",
			fmt.Errorf("error parsing kafka version: %w",
				errors.New("invalid version `wrongVersion`"))))
	})

	Convey("Given a successful creation of a producer", t, func() {
		testCtx, cancel := context.WithCancel(ctx)
		p, err := newProducer(
			testCtx,
			&ProducerConfig{
				Topic:       testTopic,
				BrokerAddrs: testBrokers,
			},
			func(addrs []string, conf *sarama.Config) (SaramaAsyncProducer, error) {
				return nil, errors.New("uninitialised")
			},
		)
		So(err, ShouldBeNil)

		Convey("When the provided context is cancelled then the closed channel is closed", func(c C) {
			cancel()
			validateChanClosed(c, p.channels.Closer, true)
			validateChanClosed(c, p.channels.Closed, true)
		})
	})
}

func TestProducer(t *testing.T) {
	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		chSaramaErr, chSaramaIn := createSaramaChannels()
		asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn)
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (SaramaAsyncProducer, error) {
			pInitCalls++
			return asyncProducerMock, nil
		}
		producer, err := newProducer(
			ctx,
			&ProducerConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
			},
			pInit,
		)

		Convey("Producer is correctly created and initialised without error", func(c C) {
			So(err, ShouldBeNil)
			So(producer, ShouldNotBeNil)
			So(producer.Channels().Output, ShouldEqual, producer.Channels().Output)
			So(producer.Channels().Errors, ShouldEqual, producer.Channels().Errors)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
			So(pInitCalls, ShouldEqual, 1)
			So(producer.IsInitialised(), ShouldBeTrue)
			validateChanClosed(c, producer.Channels().Initialised, true)
		})

		Convey("We cannot initialise producer again", func() {
			err = producer.Initialise(ctx)
			So(err, ShouldBeNil)
			So(pInitCalls, ShouldEqual, 1)
		})

		Convey("Calling initiailse with a nil context results in the expected error being returned", func() {
			err = producer.Initialise(nil)
			So(err, ShouldResemble, errors.New("nil context was passed to producer initialise"))
			So(pInitCalls, ShouldEqual, 1)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {

			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- []byte(message)

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(saramaIn.Value, ShouldEqual, message)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Errors from Sarama AsyncProducer are redirected to the caller's errors channel", func() {
			// Send error to Sarama channel
			producerError := &sarama.ProducerError{
				Msg: &sarama.ProducerMessage{
					Topic: testTopic,
				},
				Err: errors.New("error text"),
			}
			chSaramaErr <- producerError
			validateChannelReceivesError(producer.Channels().Errors, producerError)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Closing the producer closes Sarama producer and channels", func(c C) {
			producer.Close(ctx)
			validateChanClosed(c, producer.Channels().Closer, true)
			validateChanClosed(c, producer.Channels().Closed, true)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 1)
		})

		Convey("Calling close with a nil context results in the expected error being returned", func(c C) {
			err := producer.Close(nil)
			So(err, ShouldResemble, errors.New("nil context was passed to producer close"))
			validateChanClosed(c, producer.Channels().Closer, false)
			validateChanClosed(c, producer.Channels().Closed, false)
		})
	})
}

// validateChannelReceivesError validates that an error channel receives a specific error
func validateChannelReceivesError(ch chan error, expectedErr error) {
	var (
		rxErr   error
		timeout bool
	)
	select {
	case e, ok := <-ch:
		if !ok {
			break
		}
		rxErr = e
	case <-time.After(TIMEOUT):
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxErr, ShouldResemble, expectedErr)
}

// validateChanClosed validates that a channel is closed before a timeout expires
func validateChanClosed(c C, ch chan struct{}, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

func TestProducerNotInitialised(t *testing.T) {
	Convey("Given that Sarama fails to create a new AsyncProducer while we initialise our Producer", t, func() {
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
			pInitCalls++
			return nil, ErrSaramaNoBrokers
		}
		producer, err := newProducer(
			ctx,
			&ProducerConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
			},
			pInit,
		)

		Convey("Producer is partially created with channels and checker and is not initialised", func(c C) {
			So(err, ShouldBeNil)
			So(producer, ShouldNotBeNil)
			So(producer.Channels().Output, ShouldEqual, producer.Channels().Output)
			So(producer.Channels().Errors, ShouldEqual, producer.Channels().Errors)
			So(pInitCalls, ShouldEqual, 1)
			So(producer.IsInitialised(), ShouldBeFalse)
			validateChanClosed(c, producer.Channels().Initialised, false)
		})

		Convey("We can try to initialise producer again, with the same error being returned", func() {
			err = producer.Initialise(ctx)
			So(err, ShouldResemble, fmt.Errorf("failed to create a new sarama producer: %w", ErrSaramaNoBrokers))
			So(pInitCalls, ShouldEqual, 2)
		})

		Convey("Messages from the caller's output channel are redirected to Error channel", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- []byte(message)

			// Read and validate error
			validateChannelReceivesError(producer.Channels().Errors, errors.New("producer is not initialised"))
		})

		Convey("Closing the producer closes the caller channels", func(c C) {
			producer.Close(ctx)
			validateChanClosed(c, producer.Channels().Closer, true)
			validateChanClosed(c, producer.Channels().Closed, true)
		})
	})
}

func TestChecker(t *testing.T) {
	Convey("Given a valid connected broker", t, func() {
		mockBroker := &mock.SaramaBrokerMock{
			AddrFunc: func() string {
				return "localhost:9092"
			},
			ConnectedFunc: func() (bool, error) {
				return true, nil
			},
			GetMetadataFunc: func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
				return &sarama.MetadataResponse{
					Topics: []*sarama.TopicMetadata{
						{Name: testTopic},
					},
				}, nil
			},
		}

		Convey("And a consumer group connected to the same topic", func() {
			p := &Producer{
				topic:   testTopic,
				brokers: []SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct OK state", func() {
				checkState := healthcheck.NewCheckState(ServiceName)
				t0 := time.Now()
				p.Checker(ctx, checkState)
				t1 := time.Now()
				So(*checkState.LastChecked(), ShouldHappenOnOrBetween, t0, t1)
				So(checkState.Status(), ShouldEqual, healthcheck.StatusOK)
				So(checkState.Message(), ShouldEqual, MsgHealthyProducer)
			})
		})

		Convey("And a consumer group connected to a different topic", func() {
			p := &Producer{
				topic:   "wrongTopic",
				brokers: []SaramaBroker{mockBroker},
			}

			Convey("Calling Checker updates the check struct to Critical state", func() {
				checkState := healthcheck.NewCheckState(ServiceName)
				t0 := time.Now()
				p.Checker(ctx, checkState)
				t1 := time.Now()
				So(*checkState.LastChecked(), ShouldHappenOnOrBetween, t0, t1)
				So(checkState.Status(), ShouldEqual, healthcheck.StatusCritical)
				So(checkState.Message(), ShouldEqual, "unexpected metadata response for broker(s). Invalid brokers: [localhost:9092]")
			})
		})
	})
}
