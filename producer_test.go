package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/ONSdigital/dp-kafka/v2/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// kafka topic that will be used for testing
const testTopic = "testTopic"

// timeout for test channels message propagation
const TIMEOUT = 100 * time.Millisecond

// ErrSaramaNoBrokers is the error returned by Sarama when trying to create an AsyncProducer without available brokers
var ErrSaramaNoBrokers = errors.New("kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")

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

// TestProducerMissingChannels checks wrong producer creation because of channels not provided by caller
func TestProducerMissingChannels(t *testing.T) {

	Convey("Providing an invalid config (kafka version) results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		producer, err := newProducer(
			ctx, testBrokers, testTopic,
			nil,
			&ProducerConfig{KafkaVersion: &wrongVersion},
			nil,
		)
		So(producer, ShouldBeNil)
		So(err, ShouldResemble, errors.New("invalid version `wrongVersion`"))
	})

	Convey("Providing an invalid ProducerChannels struct results in an ErrNoChannel error and producer will not be initialised", t, func() {
		producer, err := newProducer(
			ctx, testBrokers, testTopic,
			&ProducerChannels{
				Output: make(chan []byte),
			},
			nil,
			nil,
		)
		So(producer, ShouldBeNil)
		So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Errors, Ready, Closer, Closed}})
		So(producer.IsInitialised(), ShouldBeFalse)
	})
}

// TestProducer checks that messages, errors, and closing events are correctly directed to the expected channels
func TestProducer(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		chSaramaErr, chSaramaIn := createSaramaChannels()
		asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn)
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (SaramaAsyncProducer, error) {
			pInitCalls++
			return asyncProducerMock, nil
		}
		channels := CreateProducerChannels()
		producer, err := newProducer(ctx, testBrokers, testTopic, channels, nil, pInit)

		Convey("Producer is correctly created and initialised without error", func(c C) {
			So(err, ShouldBeNil)
			So(producer, ShouldNotBeNil)
			So(channels.Output, ShouldEqual, channels.Output)
			So(channels.Errors, ShouldEqual, channels.Errors)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
			So(pInitCalls, ShouldEqual, 1)
			So(producer.IsInitialised(), ShouldBeTrue)
			validateStructChanClosed(c, channels.Ready, true)
		})

		Convey("We cannot initialise producer again", func() {
			err = producer.Initialise(ctx)
			So(err, ShouldBeNil)
			So(pInitCalls, ShouldEqual, 1)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {

			// Send message to local kafka output chan
			message := "HELLO"
			channels.Output <- []byte(message)

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
			validateChannelReceivesError(channels.Errors, producerError)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Closing the producer closes Sarama producer and channels", func(c C) {
			producer.Close(ctx)
			validateStructChanClosed(c, channels.Closer, true)
			validateStructChanClosed(c, channels.Closed, true)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 1)
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

// validateStructChanClosed validates that a channel is closed before a timeout expires
func validateStructChanClosed(c C, ch chan struct{}, expectedClosed bool) {
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

// validateMessageChanClosed validates that a channel is closed before a timeout expires
func validateMessageChanClosed(c C, ch chan Message, expectedClosed bool) {
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

// validateMessageChanClosed validates that a channel is closed before a timeout expires
func validateErrorChanClosed(c C, ch chan error, expectedClosed bool) {
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

// validateBoolChanClosed validates that a bool channel is closed before a timeout expires
func validateBoolChanClosed(c C, ch chan bool, expectedClosed bool) {
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
		channels := CreateProducerChannels()
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
			pInitCalls++
			return nil, ErrSaramaNoBrokers
		}
		producer, err := newProducer(ctx, testBrokers, testTopic, channels, nil, pInit)

		Convey("Producer is partially created with channels and checker and is not initialised", func(c C) {
			So(err, ShouldBeNil)
			So(producer, ShouldNotBeNil)
			So(channels.Output, ShouldEqual, channels.Output)
			So(channels.Errors, ShouldEqual, channels.Errors)
			So(pInitCalls, ShouldEqual, 1)
			So(producer.IsInitialised(), ShouldBeFalse)
			validateStructChanClosed(c, channels.Ready, false)
		})

		Convey("We can try to initialise producer again, with the same error being returned", func() {
			err = producer.Initialise(ctx)
			So(err, ShouldResemble, ErrSaramaNoBrokers)
			So(pInitCalls, ShouldEqual, 2)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			channels.Output <- []byte(message)

			// Read and validate error
			validateChannelReceivesError(channels.Errors, ErrUninitialisedProducer)
		})

		Convey("Closing the producer closes the caller channels", func(c C) {
			producer.Close(ctx)
			validateStructChanClosed(c, channels.Closer, true)
			validateStructChanClosed(c, channels.Closed, true)
		})

	})
}
