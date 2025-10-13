package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-kafka/v4/avro"
	"github.com/ONSdigital/dp-kafka/v4/interfaces"
	"github.com/ONSdigital/dp-kafka/v4/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// timeout for test channels message propagation
const TIMEOUT = 100 * time.Millisecond

// ErrSaramaNoBrokers is the error returned by Sarama when trying to create an AsyncProducer without available brokers
var ErrSaramaNoBrokers = errors.New("kafka client has run out of available brokers to talk to (Is your cluster reachable?)")

// createSaramaChannels creates sarama channels for testing
func createSaramaChannels() (saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage, saramaSuccessesChan chan *sarama.ProducerMessage) {
	saramaErrsChan = make(chan *sarama.ProducerError)
	saramaInputChan = make(chan *sarama.ProducerMessage)
	saramaSuccessesChan = make(chan *sarama.ProducerMessage)
	return
}

// createMockErrorsFunc returns a mock for AsyncProducer ErrorsFunc with the provided Sarama errors Channel
func createMockErrorsFunc(saramaErrsChan chan *sarama.ProducerError) func() <-chan *sarama.ProducerError {
	return func() <-chan *sarama.ProducerError {
		return saramaErrsChan
	}
}

func createMockSuccessesFunc(saramaSuccessesChan chan *sarama.ProducerMessage) func() <-chan *sarama.ProducerMessage {
	return func() <-chan *sarama.ProducerMessage {
		return saramaSuccessesChan
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
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage, saramaSuccessesChan chan *sarama.ProducerMessage) *mock.SaramaAsyncProducerMock {
	return &mock.SaramaAsyncProducerMock{
		ErrorsFunc:    createMockErrorsFunc(saramaErrsChan),
		InputFunc:     createMockInputFunc(saramaInputChan),
		CloseFunc:     func() error { return nil },
		SuccessesFunc: createMockSuccessesFunc(saramaSuccessesChan),
	}
}

// GetFromSaramaChans select Sarama channels, and return whichever is triggered (only one value per call).
// If none is triggered after timeout, timeout will be triggered
func GetFromSaramaChans(
	saramaErrsChan chan *sarama.ProducerError, saramaInputChan chan *sarama.ProducerMessage) (
	input *sarama.ProducerMessage, err *sarama.ProducerError, timeout bool) {
	delay := time.NewTimer(TIMEOUT)
	select {
	case input := <-saramaInputChan:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		return input, nil, false
	case err := <-saramaErrsChan:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		return nil, err, false
	case <-delay.C:
		return nil, nil, true
	}
}

func TestProducerCreation(t *testing.T) {
	Convey("Providing a nil context results in the expected error being returned", t, func() {
		p, err := NewProducerWithGenerators(
			nil,
			&ProducerConfig{},
			nil,
			nil,
		)
		So(p, ShouldBeNil)
		So(err, ShouldResemble, errors.New("nil context was passed to producer constructor"))
	})

	Convey("Providing an invalid config (kafka version) results in an error being returned and consumer not being initialised", t, func() {
		wrongVersion := "wrongVersion"
		p, err := NewProducerWithGenerators(
			ctx,
			&ProducerConfig{
				KafkaVersion: &wrongVersion,
				BrokerAddrs:  testBrokers,
				Topic:        testTopic,
			},
			nil,
			nil,
		)
		So(p, ShouldBeNil)
		So(err, ShouldResemble, fmt.Errorf("failed to get producer config: %w",
			fmt.Errorf("error parsing kafka version: %w",
				errors.New("invalid version `wrongVersion`"))))
	})

	Convey("Given a successful creation of a producer", t, func() {
		testCtx, cancel := context.WithCancel(ctx)
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}

		p, err := NewProducerWithGenerators(
			testCtx,
			&ProducerConfig{
				Topic:       testTopic,
				BrokerAddrs: testBrokers,
			},
			func(addrs []string, conf *sarama.Config) (interfaces.SaramaAsyncProducer, error) {
				return nil, errors.New("uninitialised")
			},
			brokerGen,
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
		chSaramaErr, chSaramaIn, chSaramaSuccesses := createSaramaChannels()
		asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn, chSaramaSuccesses)
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (interfaces.SaramaAsyncProducer, error) {
			pInitCalls++
			return asyncProducerMock, nil
		}
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}
		testTraceID := "mytraceid"
		ctx = context.WithValue(ctx, TraceIDHeaderKey, testTraceID)
		producer, err := NewProducerWithGenerators(
			ctx,
			&ProducerConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
			},
			pInit,
			brokerGen,
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
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Add custom header for kafka producer", func() {
			testHeader := "test"
			testValue := "testValue"
			producer.AddHeader(testHeader, testValue)

			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(extractHeaderValue(saramaIn, testHeader), ShouldEqual, testValue)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Use consumer driven traceid header", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(extractHeaderValue(saramaIn, TraceIDHeaderKey), ShouldNotBeEmpty)
			So(extractHeaderValue(saramaIn, TraceIDHeaderKey), ShouldEqual, testTraceID)
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
			validateChanReceivesErr(producer.Channels().Errors, producerError)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Closing the producer closes Sarama producer and channels", func(c C) {
			err := producer.Close(ctx)
			So(err, ShouldBeNil)
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

// TestProducer checks that messages, errors, and closing events are correctly directed to the expected channels
func TestProducer_WithDefaultContext(t *testing.T) {
	Convey("Given a correct initialization of a Kafka Producer", t, func() {
		chSaramaErr, chSaramaIn, chSaramaSuccesses := createSaramaChannels()
		asyncProducerMock := createMockNewAsyncProducerComplete(chSaramaErr, chSaramaIn, chSaramaSuccesses)
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (interfaces.SaramaAsyncProducer, error) {
			pInitCalls++
			return asyncProducerMock, nil
		}
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}
		producer, err := NewProducerWithGenerators(
			ctx,
			&ProducerConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
			},
			pInit,
			brokerGen,
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
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Add custom header for kafka producer", func() {
			testHeader := "test"
			testValue := "testValue"
			producer.AddHeader(testHeader, testValue)

			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(extractHeaderValue(saramaIn, testHeader), ShouldEqual, testValue)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Use consumer driven traceid header", func() {
			// Send message to local kafka output chan
			message := "HELLO"
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read sarama channels with timeout
			saramaIn, saramaErr, timeout := GetFromSaramaChans(chSaramaErr, chSaramaIn)
			saramaInValueEncoded, _ := saramaIn.Value.Encode()

			// Validate that message was received by sarama message chan, with no error.
			So(timeout, ShouldBeFalse)
			So(saramaErr, ShouldBeNil)
			So(saramaIn.Topic, ShouldEqual, testTopic)
			So(string(saramaInValueEncoded), ShouldEqual, message)
			So(extractHeaderValue(saramaIn, TraceIDHeaderKey), ShouldNotBeEmpty)
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
			validateChanReceivesErr(producer.Channels().Errors, producerError)
			So(len(asyncProducerMock.CloseCalls()), ShouldEqual, 0)
		})

		Convey("Closing the producer closes Sarama producer and channels", func(c C) {
			err := producer.Close(ctx)
			So(err, ShouldBeNil)
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

func TestProducerNotInitialised(t *testing.T) {
	Convey("Given that Sarama fails to create a new AsyncProducer while we initialise our Producer", t, func() {
		pInitCalls := 0
		pInit := func(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
			pInitCalls++
			return nil, ErrSaramaNoBrokers
		}
		brokerGen := func(addr string) interfaces.SaramaBroker {
			return &mock.SaramaBrokerMock{
				CloseFunc: func() error { return nil },
			}
		}

		producer, err := NewProducerWithGenerators(
			ctx,
			&ProducerConfig{
				BrokerAddrs: testBrokers,
				Topic:       testTopic,
			},
			pInit,
			brokerGen,
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
			producer.Channels().Output <- BytesMessage{Value: []byte(message), Context: context.Background()}

			// Read and validate error
			validateChanReceivesErr(producer.Channels().Errors, errors.New("producer is not initialised"))
		})

		Convey("Closing the producer closes the caller channels", func(c C) {
			err := producer.Close(ctx)
			So(err, ShouldBeNil)
			validateChanClosed(c, producer.Channels().Closer, true)
			validateChanClosed(c, producer.Channels().Closed, true)
		})
	})
}

func TestSend(t *testing.T) {
	var TestSchema = &avro.Schema{
		Definition: `{
			"type": "record",
			"name": "test-name",
			"fields": [
			  {"name": "resource_id", "type": "string", "default": ""},
			  {"name": "resource_url", "type": "string", "default": ""}
			]
		  }`,
	}

	type TestEvent struct {
		ResourceID  string `avro:"resource_id"`
		ResourceURL string `avro:"resource_url"`
	}

	Convey("Given a producer with an Output channel", t, func() {
		p := Producer{
			mutex: &sync.RWMutex{},
			channels: &ProducerChannels{
				Output: make(chan BytesMessage),
			},
		}

		testEvent := TestEvent{
			ResourceID:  "res1",
			ResourceURL: "http://res1.com",
		}
		expectedBytes, err := TestSchema.Marshal(testEvent)
		So(err, ShouldBeNil)

		Convey("Then sending a valid message results in the expected message being sent to the Output channel", func(c C) {
			go func() {
				rx := <-p.channels.Output
				c.So(rx.Value, ShouldResemble, expectedBytes)
			}()

			err = p.Send(context.Background(), TestSchema, testEvent)
			So(err, ShouldBeNil)
		})

		Convey("Then sending an invalid message results in the expected error being returned", func() {
			type DifferentEvent struct {
				ResourceID    int    `avro:"resource_id"`
				SomethingElse string `avro:"something_else"`
			}

			err = p.Send(context.Background(), TestSchema, DifferentEvent{
				SomethingElse: "should fail to marshal",
			})
			So(err, ShouldResemble, fmt.Errorf("failed to marshal event with avro schema: %w",
				errors.New("unsupported field type")),
			)
		})

		Convey("Then sending a valid message after the Output channel is closed results in the expected error being returned", func() {
			close(p.channels.Output)
			err = p.Send(context.Background(), TestSchema, testEvent)
			So(err, ShouldResemble, fmt.Errorf("failed to send marshalled message to output channel: %w",
				errors.New("failed to send byte array value to channel: send on closed channel")),
			)
		})
	})
}

func TestSendJSON(t *testing.T) {
	type TestEvent struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	Convey("Given a producer with an Output channel", t, func() {
		p := Producer{
			mutex: &sync.RWMutex{},
			channels: &ProducerChannels{
				Output: make(chan BytesMessage),
			},
		}

		Convey("And a valid event struct", func() {
			ev := TestEvent{ID: "123", Name: "Alice"}
			expected, err := json.Marshal(ev)
			So(err, ShouldBeNil)

			Convey("When SendJSON is called", func(c C) {
				done := make(chan struct{})
				go func() {
					defer close(done)
					rx := <-p.channels.Output
					c.So(rx.Value, ShouldResemble, expected)
				}()

				err := p.SendJSON(context.Background(), ev)

				Convey("Then it succeeds and sends the marshalled bytes to Output", func() {
					So(err, ShouldBeNil)
					<-done
				})
			})
		})

		Convey("And an invalid JSON message is sent", func() {
			type Bad struct {
				C chan struct{} `json:"c"`
			}
			bad := Bad{C: make(chan struct{})}

			Convey("When SendJSON is called", func() {
				err := p.SendJSON(context.Background(), bad)

				Convey("Then an error is returned", func() {
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, "failed to marshal event as JSON")
				})
			})
		})

		Convey("And the Output channel is closed", func() {
			close(p.channels.Output)

			ev := TestEvent{ID: "123", Name: "Alice"}

			Convey("When SendJSON is called", func() {
				err := p.SendJSON(context.Background(), ev)

				Convey("Then it returns a wrapped send error", func() {
					So(err, ShouldResemble, fmt.Errorf(
						"failed to send marshalled JSON message to output channel: %w",
						errors.New("failed to send byte array value to channel: send on closed channel"),
					))
				})
			})
		})
	})
}

func TestSendBytes(t *testing.T) {
	Convey("Given a producer with an Output channel", t, func() {
		p := Producer{
			mutex: &sync.RWMutex{},
			channels: &ProducerChannels{
				Output: make(chan BytesMessage),
			},
		}

		payload := []byte(`{"raw":true,"n":1}`)

		Convey("And a valid byte payload", func(c C) {
			Convey("When SendBytes is called", func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					rx := <-p.channels.Output
					c.So(rx.Value, ShouldResemble, payload)
				}()

				err := p.SendBytes(context.Background(), payload)

				Convey("Then it succeeds and writes the payload unchanged", func() {
					So(err, ShouldBeNil)
					<-done
				})
			})
		})

		Convey("And the Output channel is closed", func() {
			close(p.channels.Output)

			Convey("When SendBytes is called", func() {
				err := p.SendBytes(context.Background(), payload)

				Convey("Then it returns a wrapped send error", func() {
					So(err, ShouldResemble, fmt.Errorf(
						"failed to send raw bytes to output channel: %w",
						errors.New("failed to send byte array value to channel: send on closed channel"),
					))
				})
			})
		})
	})
}

func extractHeaderValue(sMessage *sarama.ProducerMessage, key string) string {
	for _, header := range sMessage.Headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}
