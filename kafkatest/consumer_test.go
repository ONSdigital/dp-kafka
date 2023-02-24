package kafkatest

import (
	"sync"
	"testing"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v3"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConsumerMock(t *testing.T) {
	longRetry := time.Minute

	cgConfig := &kafka.ConsumerGroupConfig{
		Topic:          "test-topic",
		GroupName:      "test-group",
		BrokerAddrs:    []string{"addr1", "addr2", "addr3"},
		MinRetryPeriod: &longRetry, // set a long retry to prevent race conditions when we check IsInitialised()
		MaxRetryPeriod: &longRetry,
	}

	Convey("Given an uninitialised consumer mock", t, func() {
		cg, err := NewConsumer(
			ctx,
			cgConfig,
			&ConsumerConfig{
				InitAtCreation: false,
			},
		)
		So(err, ShouldBeNil)
		So(cg.Mock.IsInitialised(), ShouldBeFalse)

		Convey("It can be successfully initialised, closing 'Initialised' channel", func() {
			initClosed := false
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-cg.Mock.Channels().Initialised
				initClosed = true
			}()
			err := cg.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cg.Mock.IsInitialised(), ShouldBeTrue)
			wg.Wait()
			So(initClosed, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(cg)
		})
	})

	Convey("Given an initialised consumer mock", t, func() {
		cg, err := NewConsumer(
			ctx,
			cgConfig,
			&ConsumerConfig{
				InitAtCreation: true,
			},
		)
		So(err, ShouldBeNil)
		So(cg.Mock.IsInitialised(), ShouldBeTrue)

		Convey("Calling initialise again has no effect", func() {
			err := cg.Mock.Initialise(ctx)
			So(err, ShouldBeNil)
			So(cg.Mock.IsInitialised(), ShouldBeTrue)
		})

		Convey("Messages are received in a synchronized fashion", func(c C) {
			payload1 := []byte{0, 1, 2, 3, 4, 5}
			message1, err := NewMessage(payload1, 1)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			payload2 := []byte{6, 7, 8, 9, 0}
			message2, err := NewMessage(payload2, 2)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			payload3 := []byte{10, 11, 12, 13, 14}
			message3, err := NewMessage(payload3, 3)
			if err != nil {
				t.Errorf(errNewMessage)
			}

			wg := sync.WaitGroup{}
			receivedAll := false
			sentAll := false

			// Message consumer loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				rxMsg := <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message1)
				rxMsg.CommitAndRelease()

				rxMsg = <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message2)
				rxMsg.CommitAndRelease()

				rxMsg = <-cg.Mock.Channels().Upstream
				c.So(rxMsg, ShouldResemble, message3)
				rxMsg.CommitAndRelease()
				receivedAll = true
			}()

			// Message sender loop
			wg.Add(1)
			go func() {
				defer wg.Done()
				cg.Mock.Channels().Upstream <- message1
				cg.Mock.Channels().Upstream <- message2
				cg.Mock.Channels().Upstream <- message3
				sentAll = true
			}()

			// Check that the messages are released
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-message1.UpstreamDone()
				<-message2.UpstreamDone()
				<-message3.UpstreamDone()
			}()

			wg.Wait()
			So(sentAll, ShouldBeTrue)
			So(receivedAll, ShouldBeTrue)
		})

		Convey("It can be successfully closed", func() {
			validateCloseConsumer(cg)
		})
	})
}

func validateCloseConsumer(cg *Consumer) {
	closedUpstream := false
	closedErrors := false
	closedCloser := false
	closedClosed := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-cg.Mock.Channels().Upstream
		closedUpstream = true
		<-cg.Mock.Channels().Errors
		closedErrors = true
		<-cg.Mock.Channels().Closer
		closedCloser = true
		<-cg.Mock.Channels().Closed
		closedClosed = true
	}()
	err := cg.Mock.Close(ctx)
	So(err, ShouldBeNil)
	wg.Wait()

	So(closedUpstream, ShouldBeTrue)
	So(closedErrors, ShouldBeTrue)
	So(closedCloser, ShouldBeTrue)
	So(closedClosed, ShouldBeTrue)
}
