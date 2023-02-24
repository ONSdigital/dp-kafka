package kafkatest

import (
	"context"
	"sync"
	"testing"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v3"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	errNewMessage = "failed to create new message"
)

func TestProducerMock(t *testing.T) {
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
