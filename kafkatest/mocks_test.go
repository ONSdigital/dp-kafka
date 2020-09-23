package kafkatest

import (
	"context"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestProducerMock(t *testing.T) {

	Convey("Given an uninitialised producer mock", t, func() {

		producerMock := NewMessageProducer(false)
		So(producerMock.IsInitialised(), ShouldBeFalse)

		Convey("It can be successfully initialised, closing Ready channel", func() {
			readyClosed := false
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-producerMock.Channels().Ready
				readyClosed = true
			}()
			producerMock.Initialise(context.Background())
			So(producerMock.IsInitialised(), ShouldBeTrue)
			wg.Wait()
			So(readyClosed, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseProducer(producerMock)
		})
	})

	Convey("Given an initialised producer mock", t, func() {
		producerMock := NewMessageProducer(true)
		So(producerMock.IsInitialised(), ShouldBeTrue)

		Convey("Calling initialise again has no effect", func() {
			producerMock.Initialise(context.Background())
			So(producerMock.IsInitialised(), ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseProducer(producerMock)
		})

	})
}

func validateCloseProducer(producerMock *MessageProducer) {
	closedOutput := false
	closedErrors := false
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-producerMock.Channels().Output
		closedOutput = true
		<-producerMock.Channels().Errors
		closedErrors = true
		<-producerMock.Channels().Closer
		closedCloser = true
		<-producerMock.Channels().Closed
		closedClosed = true
	}()
	producerMock.Close(context.Background())
	wg.Wait()

	So(closedOutput, ShouldBeTrue)
	So(closedErrors, ShouldBeTrue)
	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}

func TestConsumerMock(t *testing.T) {

	Convey("Given an uninitialised consumer mock", t, func() {
		consumerMock := NewMessageConsumer(false)
		So(consumerMock.IsInitialised(), ShouldBeFalse)

		Convey("It can be successfully initialised, closing Ready channel", func() {
			readyClosed := false
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-consumerMock.Channels().Ready
				readyClosed = true
			}()
			consumerMock.Initialise(context.Background())
			So(consumerMock.IsInitialised(), ShouldBeTrue)
			wg.Wait()
			So(readyClosed, ShouldBeTrue)
		})

		Convey("It can successfully stop listening", func() {
			validateStopListening(consumerMock)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(consumerMock)
		})

		Convey("It can successfully stop listening and then close", func() {
			validateStopListening(consumerMock)
			validateCloseConsumer(consumerMock)
		})

	})

	Convey("Given an initialised consumer mock", t, func() {
		consumerMock := NewMessageConsumer(true)
		So(consumerMock.IsInitialised(), ShouldBeTrue)

		Convey("Calling initialise again has no effect", func() {
			consumerMock.Initialise(context.Background())
			So(consumerMock.IsInitialised(), ShouldBeTrue)
		})

		Convey("Messages are received in a synchronized fashion", func(c C) {
			payload1 := []byte{0, 1, 2, 3, 4, 5}
			message1 := NewMessage(payload1, 0)

			payload2 := []byte{6, 7, 8, 9, 0}
			message2 := NewMessage(payload2, 0)

			payload3 := []byte{10, 11, 12, 13, 14}
			message3 := NewMessage(payload3, 0)

			wg := sync.WaitGroup{}
			receivedAll := false
			sentAll := false

			// Message consumer loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				rxMsg := <-consumerMock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message1)
				rxMsg.Commit()

				rxMsg = <-consumerMock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message2)
				rxMsg.Commit()

				rxMsg = <-consumerMock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message3)
				rxMsg.Commit()
				receivedAll = true
			}()

			// Message sender loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				consumerMock.Channels().Upstream <- message1
				consumerMock.Channels().Upstream <- message2
				consumerMock.Channels().Upstream <- message3
				sentAll = true
			}()

			wg.Wait()
			So(sentAll, ShouldBeTrue)
			So(receivedAll, ShouldBeTrue)
		})

		Convey("It can successfully stop listening", func() {
			validateStopListening(consumerMock)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(consumerMock)
		})

		Convey("It can successfully stop listening and then close", func() {
			validateStopListening(consumerMock)
			validateCloseConsumer(consumerMock)
		})
	})
}

func validateCloseConsumer(consumerMock *MessageConsumer) {
	closedUpstream := false
	closedErrors := false
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-consumerMock.Channels().Upstream
		closedUpstream = true
		<-consumerMock.Channels().Errors
		closedErrors = true
		<-consumerMock.Channels().Closer
		closedCloser = true
		<-consumerMock.Channels().Closed
		closedClosed = true
	}()
	consumerMock.Close(context.Background())
	wg.Wait()

	So(closedUpstream, ShouldBeTrue)
	So(closedErrors, ShouldBeTrue)
	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}

func validateStopListening(consumerMock *MessageConsumer) {
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-consumerMock.Channels().Closer
		closedCloser = true
		<-consumerMock.Channels().Closed
		closedClosed = true
	}()
	consumerMock.StopListeningToConsumer(context.Background())
	wg.Wait()

	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}
