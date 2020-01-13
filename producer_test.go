package kafka_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	"github.com/ONSdigital/dp-kafka/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

const testTopic = "testTopic"

// var testBrokers = []string{"1.1.1.1:1111", "2.2.2.2:2222"}
var testBrokers = []string{"localhost:9092"}

// Sarama channels for testing (i.e. correspond to sarama channels)
var (
	saramaErrsChan  = make(chan *sarama.ProducerError)
	saramaInputChan = make(chan *sarama.ProducerMessage)
)

var (
	chOut    = make(chan []byte)
	chErr    = make(chan error)
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
)

// AsyncProducer ErrorsFunc mock that returns the testing saramaErrsChan channel
func mockErrorsFunc() <-chan *sarama.ProducerError {
	return saramaErrsChan
}

// AsyncProducer InputFunc mock that returns the testing saramaInputChan channel
func mockInputFunc() chan<- *sarama.ProducerMessage {
	return saramaInputChan
}

// Mock AsyncProducerwith implemented methods for testing
var asyncProducer = &mock.AsyncProducerMock{
	ErrorsFunc: mockErrorsFunc,
	InputFunc:  mockInputFunc,
}

// mockNewAsyncProducerEmpty returns an AsyncProducer mock with all the necessary methods implemented for testing
func mockNewAsyncProducerComplete(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return asyncProducer, nil
}

// mockNewAsyncProducerEmpty returns an AsyncProducer mock with no methods implemented
// (i.e. if any mock method is called, the test will fail)
func mockNewAsyncProducerEmpty(addrs []string, conf *sarama.Config) (kafka.AsyncProducer, error) {
	return &mock.AsyncProducerMock{}, nil
}

// Test wrong Producer creation because of channels not provided by caller
func TestProducerMissingChannels(t *testing.T) {
	Convey("Given the intention to initialize a kafka Producer", t, func() {
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerEmpty,
		}
		// outputChan, errorsChan := make(chan []byte), make(chan error)
		// closerChan, closedChan := make(chan struct{}), make(chan struct{})
		Convey("Missing outputChan will cause ErrNoOputputChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
				nil, chErr, chCloser, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoOputputChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing errorsChan will cause ErrNoErrorChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
				chOut, nil, chCloser, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoErrorChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing closerChan will cause ErrNoCloserChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
				chOut, chErr, nil, chClosed, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoCloserChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
		Convey("Missing closedChan will cause ErrNoClosedChannel", func() {
			producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
				chOut, chErr, chCloser, nil, saramaCli)
			So(producer, ShouldResemble, kafka.Producer{})
			So(err, ShouldEqual, kafka.ErrNoClosedChannel)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)
		})
	})
}

// GetFromSaramaChans select Sarama channels, and return whichever is triggered (only one value per call).
// No default, to prevent race conditions, but timeout defined to prevent long tests.
func GetFromSaramaChans() (input *sarama.ProducerMessage, err *sarama.ProducerError, timeout bool) {
	select {
	case input := <-saramaInputChan:
		return input, nil, false
	case err := <-saramaErrsChan:
		return nil, err, false
	case <-time.After(1 * time.Second):
		return nil, nil, true
	}
}

// GetFromLocalChans selects provided local channels, and return whichever is triggered (only one value per call).
// No default, to prevent race conditions.
func GetFromLocalChans(chOut chan []byte, chErr chan error, chCloser, chClosed chan struct{}) (out []byte, err error, closer, closed *struct{}, timeout bool) {
	select {
	case out := <-chOut:
		return out, nil, nil, nil, false
	case err := <-chErr:
		return nil, err, nil, nil, false
	case closer := <-chCloser:
		return nil, nil, &closer, nil, false
	case closed := <-chClosed:
		return nil, nil, nil, &closed, false
	case <-time.After(1 * time.Second):
		return nil, nil, nil, nil, true
	}
}

// TestSendMessage validates that messages sent to the local output channel are correctly forwarded to Sarama Input channel
func TestSendMessage(t *testing.T) {

	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerComplete,
		}
		// chOut, chErr := make(chan []byte), make(chan error)
		// chCloser, chClosed := make(chan struct{}), make(chan struct{})
		producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
			chOut, chErr, chCloser, chClosed, saramaCli)

		Convey("Messages from output channel are redirected to sarama AsyncProducer", func() {

			// Validate proudcer correctly created
			So(producer, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)

			// Prepare concurrent validtion of Sarama channels with waitGroup
			var (
				saramaIn  *sarama.ProducerMessage = nil
				saramaErr *sarama.ProducerError   = nil
				timeout   bool                    = false
			)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				saramaIn, saramaErr, timeout = GetFromSaramaChans()
				wg.Done()
			}()

			// Send message to local kafka output chan, and wait for WaitGroup to finish
			message := "HELLO"
			msg := kafkatest.NewMessage([]byte(message))
			chOut <- msg.GetData()
			wg.Wait()

			// Validate that message was received in local message chain, and nothing else
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(saramaIn.Value, ShouldEqual, message)
		})
	})
}

// TestReceiveError validates that errors comming from Sarama error channel are correctly forwarded to the local error channel
func TestReceiveError(t *testing.T) {

	Convey("Given a correct initialization of a kafka Producer", t, func() {
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerComplete,
		}
		producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
			chOut, chErr, chCloser, chClosed, saramaCli)

		Convey("Errors from sarama AsyncProducer are redirected to the errors channel", func() {

			// Validate proudcer correctly created
			So(producer, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)

			// Prepare concurrent validtion of local channels with waitGroup
			var (
				localOut    []byte    = nil
				localErr    error     = nil
				localCloser *struct{} = nil
				localClosed *struct{} = nil
				timeout     bool      = false
			)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				localOut, localErr, localCloser, localClosed, timeout = GetFromLocalChans(chOut, chErr, chCloser, chClosed)
				wg.Done()
			}()

			// Send Error to Sarama channel, and wait for wait goup to finish
			producerError := &sarama.ProducerError{
				Msg: &sarama.ProducerMessage{
					Topic: testTopic,
				},
				Err: errors.New("error text"),
			}
			saramaErrsChan <- producerError
			wg.Wait()

			// Validate that error was received in local error chain, and nothing else
			So(timeout, ShouldBeFalse)
			So(localOut, ShouldBeNil)
			So(localCloser, ShouldBeNil)
			So(localClosed, ShouldBeNil)
			So(localErr, ShouldNotBeNil)
			So(localErr, ShouldResemble, producerError)
		})
		Convey("Closer channel closes the client correctly", func() {
		})
	})
}

func TestCloseKafka(t *testing.T) {

	Convey("Given a correct initialization of a kafka Producer", t, func() {
		saramaCli := &mock.SaramaMock{
			NewAsyncProducerFunc: mockNewAsyncProducerComplete,
		}
		// chOut, chErr := make(chan []byte), make(chan error)
		// chCloser, chClosed := make(chan struct{}), make(chan struct{})
		producer, err := kafka.NewProducerWithSaramaClient(testBrokers, testTopic, 123,
			chOut, chErr, chCloser, chClosed, saramaCli)

		Convey("Errors from sarama AsyncProducer are redirected to the errors channel", func() {

			// Validate proudcer correctly created
			So(producer, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(saramaCli.NewAsyncProducerCalls()), ShouldEqual, 1)

			// Prepare concurrent validtion of local channels with waitGroup
			var (
				localOut    []byte    = nil
				localErr    error     = nil
				localCloser *struct{} = nil
				localClosed *struct{} = nil
				timeout     bool      = false
			)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				localOut, localErr, localCloser, localClosed, timeout = GetFromLocalChans(chOut, chErr, chCloser, chClosed)
				wg.Done()
			}()

			// Send data to local Closer channel, and wait for wait goup to finish
			type closerStruct struct{}
			closerVar := closerStruct{}
			chCloser <- closerVar
			wg.Wait()

			// Validate that error was received in local error chain, and nothing else
			So(timeout, ShouldBeFalse)
			So(localOut, ShouldBeNil)
			So(localCloser, ShouldNotBeNil)
			So(localClosed, ShouldBeNil)
			So(localErr, ShouldBeNil)
		})
		Convey("Closer channel closes the client correctly", func() {
		})
	})
}
