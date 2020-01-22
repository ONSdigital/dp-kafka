package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// kafka topic that will be used for testing
const testTopic = "testTopic"

// timeout for test channels message propagation
const TIMEOUT = 1 * time.Second

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

// mockCloseFunc is the mock for AsyncProducer Close() function without error
func mockCloseFunc() error {
	return nil
}

// createMockNewAsyncProducerComplete creates an AsyncProducer mock and returns it,
// as well as a NewAsyncProducerFunc that returns the same AsyncProducer mock.
func createMockNewAsyncProducerComplete(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) (*mock.AsyncProducerMock, func(
	addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error)) {
	// Create AsyncProducerMock
	var asyncProducer = &mock.AsyncProducerMock{
		ErrorsFunc: createMockErrorsFunc(saramaErrsChan),
		InputFunc:  createMockInputFunc(saramaInputChan),
		CloseFunc:  mockCloseFunc,
	}
	// Function that returns AsyncProducerMock
	return asyncProducer, func(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
		return asyncProducer, nil
	}
}

// mockNewAsyncProducerEmpty returns an AsyncProducer mock with no methods implemented
// (i.e. if any mock method is called, the test will fail)
func mockNewAsyncProducerEmpty(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return &mock.AsyncProducerMock{}, nil
}

// mockNewAsyncProducerError returns a nil AsyncProducer, and ErrSaramaNoBrokers
func mockNewAsyncProducerError(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return nil, ErrSaramaNoBrokers
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

// TestProducerMissingChannels tests wrong producer creation because of channels not provided by caller
func TestProducerMissingChannels(t *testing.T) {

	Convey("Given the intention to initialize a kafka Producer", t, func() {
		ctx := context.Background()
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerEmpty,
		}

		Convey("Providing an invalid ProducerChannels struct results in an ErrNoChannel error", func() {
			producer, err := kafka.NewProducerWithSaramaClient(
				ctx, testBrokers, testTopic, 123,
				kafka.ProducerChannels{
					Output: make(chan []byte),
				},
				saramaCli,
			)
			So(producer, ShouldNotBeNil)
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors, kafka.Closer, kafka.Closed}})
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 0)
		})
	})
}

// TestProducer tests that messages, errors, and closing events are correctly directed to the expected channels
func TestProducer(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		ctx := context.Background()
		chSaramaErr, chSaramaIn := createSaramaChannels()
		asyncProducerMock, funcNewAsyncProducer := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn)
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: funcNewAsyncProducer,
		}
		channels := kafka.CreateProducerChannels()
		producer, err := kafka.NewProducerWithSaramaClient(
			ctx, testBrokers, testTopic, 123, channels, saramaCli)

		expectedCheck := health.Check{Name: kafka.ServiceName}

		Convey("Producer is correctly created without error", func() {
			So(err, ShouldBeNil)
			So(producer, ShouldNotBeNil)
			So(producer.Check, ShouldResemble, &expectedCheck)
			So(producer.Output(), ShouldEqual, channels.Output)
			So(producer.Errors(), ShouldEqual, channels.Errors)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("We cannot initialize producer again", func() {
			// InitializeSarama does not call NewAsyncProducer again
			err = producer.InitializeSarama(ctx)
			So(err, ShouldBeNil)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			msg := kafkatest.NewMessage([]byte(message))
			channels.Output <- msg.GetData()
			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			// Validate that message was received by sarama message chain, with no error.
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

		Convey("Closing the producer closes Sarama producer and channels", func() {
			producer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 1)
		})

	})
}

// validateChannelReceivesError validates that an error channel receives a specific error
func validateChannelReceivesError(ch chan error, expectedErr error) {
	var (
		rxErr   error = nil
		timeout bool  = false
	)
	select {
	case e := <-ch:
		rxErr = e
	case <-time.After(TIMEOUT):
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxErr, ShouldResemble, expectedErr)
}

// validateChannelClosed validates that a channel is closed before a timeout expires
func validateChannelClosed(ch chan struct{}) {
	var (
		closed  bool = false
		timeout bool = false
	)
	select {
	case <-ch:
		closed = true
	case <-time.After(TIMEOUT):
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(closed, ShouldBeTrue)
}

// TestProducerNotInitialized validates that if sarama client cannot be initialized, we can still partially use our Producer
func TestProducerNotInitialized(t *testing.T) {

	Convey("Given that Sarama fails to create a new AsyncProducer while we initialize our Producer", t, func() {
		ctx := context.Background()
		saramaCliWithErr := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerError,
		}
		channels := kafka.CreateProducerChannels()
		producer, err := kafka.NewProducerWithSaramaClient(
			ctx, testBrokers, testTopic, 123, channels, saramaCliWithErr)

		expectedCheck := health.Check{Name: kafka.ServiceName}

		Convey("Producer is partially created with channels and checker, returning the Sarama error", func() {
			So(err, ShouldEqual, ErrSaramaNoBrokers)
			So(producer, ShouldNotBeNil)
			So(producer.Check, ShouldResemble, &expectedCheck)
			So(producer.Output(), ShouldEqual, channels.Output)
			So(producer.Errors(), ShouldEqual, channels.Errors)
			So(len(saramaCliWithErr.NewAsyncProducerCalls()), ShouldEqual, 1)
		})

		Convey("We can try to initialize producer again", func() {
			// InitializeSarama does call NewAsyncProducer again
			err = producer.InitializeSarama(ctx)
			So(err, ShouldEqual, ErrSaramaNoBrokers)
			So(len(saramaCliWithErr.NewAsyncProducerCalls()), ShouldEqual, 2)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {
			So(true, ShouldBeTrue)
			// TODO We should create errors if messages are sent to Output channel in an non initialized producer
		})

		Convey("Closing the producer closes Sarama producer and channels", func() {
			producer.Close(ctx)
			validateChannelClosed(channels.Closer)
			validateChannelClosed(channels.Closed)
		})

	})
}
