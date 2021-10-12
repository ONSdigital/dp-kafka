package kafka

import (
	"errors"
	"testing"

	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

// createProducerChannels creates local producer channels for testing
func createProducerChannels() (chOutput chan []byte, chErrors chan error, chReady, chCloser, chClosed chan struct{}) {
	chOutput = make(chan []byte)
	chErrors = make(chan error)
	chReady = make(chan struct{})
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
	return
}

func TestProducerChannelsValidate(t *testing.T) {

	Convey("Given a set of producer channels", t, func() {
		chOutput, chErrors, chReady, chCloser, chClosed := createProducerChannels()

		Convey("ProducerChannels with all required channels has a successful validation", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldBeNil)
		})

		Convey("Missing Output channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Output}},
			))
		})

		Convey("Missing Errors channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors}},
			))
		})

		Convey("Missing Ready channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Ready}},
			))
		})

		Convey("Missing Closer channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closer}},
			))
		})

		Convey("Missing Closed channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closed}},
			))
		})

		Convey("Missing multiple channels in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Ready, Closer, Closed}},
			))
		})

	})
}

// createConsumerChannels creates local consumer channels for testing
func createConsumerChannels() (chUpstream chan Message, chReady chan struct{}, chConsume chan bool, chCloser, chClosed chan struct{},
	chErrors chan error) {
	chUpstream = make(chan Message)
	chReady = make(chan struct{})
	chConsume = make(chan bool)
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
	chErrors = make(chan error)
	return
}

func TestConsumerGroupChannelsValidate(t *testing.T) {

	Convey("Given a set of consumer group channels", t, func() {
		chUpstream, chReady, chConsume, chCloser, chClosed, chErrors := createConsumerChannels()

		Convey("ConsumerGroupChannels with all required channels has a successful validation", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Consume:  chConsume,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldBeNil)
		})

		Convey("Missing Upstream channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Ready:   chReady,
				Consume: chConsume,
				Closer:  chCloser,
				Closed:  chClosed,
				Errors:  chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Upstream}},
			))
		})

		Convey("Missing Ready channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Consume:  chConsume,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Ready}},
			))
		})

		Convey("Missing Consume channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Consume}},
			))
		})

		Convey("Missing Closer channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Consume:  chConsume,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Closer}},
			))
		})

		Convey("Missing Closed channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Consume:  chConsume,
				Closer:   chCloser,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Closed}},
			))
		})

		Convey("Missing Errors channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Consume:  chConsume,
				Closer:   chCloser,
				Closed:   chClosed,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Errors}},
			))
		})

		Convey("Missing multiple channels in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Consume, Closer, Closed}},
			))
		})
	})
}
