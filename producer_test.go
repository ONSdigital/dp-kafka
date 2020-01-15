package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

// createSaramaChannels creates sarama channels for testing
func createSaramaChannels() (saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) {
	saramaErrsChan = make(chan *sarama.ProducerError)
	saramaInputChan = make(chan *sarama.ProducerMessage)
	return
}

// createProducerChannels creates local producer channels for testing
func createProducerChannels() (chOut chan []byte, chErr chan error, chCloser, chClosed chan struct{}) {
	chOut = make(chan []byte)
	chErr = make(chan error)
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
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

// createAsyncProducer returns a mock AsyncProducer with the provided Sarama channels
func createAsyncProducer(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) *mock.AsyncProducerMock {
	var asyncProducer = &mock.AsyncProducerMock{
		ErrorsFunc: createMockErrorsFunc(saramaErrsChan),
		InputFunc:  createMockInputFunc(saramaInputChan),
	}
	return asyncProducer
}

// createMockNewAsyncProducerComplete returns a mock NewAsyncProducerFunc that returns
// a new AsyncProducer with provided Sarama channels
func createMockNewAsyncProducerComplete(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) func(
	addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return func(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
		return createAsyncProducer(saramaErrsChan, saramaInputChan), nil
	}
}

// mockNewAsyncProducerEmpty returns an AsyncProducer mock with no methods implemented
// (i.e. if any mock method is called, the test will fail)
func mockNewAsyncProducerEmpty(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return &mock.AsyncProducerMock{}, nil
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
		chOut, chErr, chCloser, chClosed := createProducerChannels()
		Convey("Missing outputChan will cause ErrNoOputputChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(
				ctx, testBrokers, testTopic, 123,
				nil, chErr, chCloser, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoOputputChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing errorsChan will cause ErrNoErrorChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(
				ctx, testBrokers, testTopic, 123,
				chOut, nil, chCloser, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoErrorChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing closerChan will cause ErrNoCloserChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(
				ctx, testBrokers, testTopic, 123,
				chOut, chErr, nil, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoCloserChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing closedChan will cause ErrNoClosedChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(
				ctx, testBrokers, testTopic, 123,
				chOut, chErr, chCloser, nil, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoClosedChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
	})
}

// TestProducerChannels tests that messages, errors, and closing events are correctly directed to the expected channels
func TestProducerChannels(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		ctx := context.Background()
		chSaramaErr, chSaramaIn := createSaramaChannels()
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn),
		}
		chOut, chErr, chCloser, chClosed := createProducerChannels()
		producer, err := kafka.NewProducerWithSaramaClient(
			ctx, testBrokers, testTopic, 123,
			chOut, chErr, chCloser, chClosed, saramaCli)

		Convey("Producer is correctly created without error", func() {
			// Validate proudcer correctly created
			So(producer, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})

		Convey("Messages from the caller's output channel are redirected to Sarama AsyncProducer", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			msg := kafkatest.NewMessage([]byte(message))
			chOut <- msg.GetData()
			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			// Validate that message was received by sarama message chain, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(saramaIn.Value, ShouldEqual, message)
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
			// Read local error channel with timeout
			var (
				localErr error = nil
				timeout  bool  = false
			)
			select {
			case e := <-chErr:
				localErr = e
			case <-time.After(TIMEOUT):
				timeout = true
			}
			// Validate that error was received in local error chain
			So(timeout, ShouldBeFalse)
			So(localErr, ShouldNotBeNil)
			So(localErr, ShouldResemble, producerError)
		})

		Convey("closing local closer's channel causes kafka producer to close the closed channel", func() {
			close(chCloser)
			// Read local closed channel with timeout
			var (
				localClosed bool = false
				timeout     bool = false
			)
			select {
			case <-chClosed:
				localClosed = true
			case <-time.After(TIMEOUT):
				timeout = true
			}
			// Validate that kafka closed correctly
			So(timeout, ShouldBeFalse)
			So(localClosed, ShouldBeTrue)
		})

	})
}
