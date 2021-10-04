package producer

import (
	"testing"

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
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Output}})
		})

		Convey("Missing Errors channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Errors}})
		})

		Convey("Missing Ready channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Ready}})
		})

		Convey("Missing Closer channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Closer}})
		})

		Convey("Missing Closed channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Closed}})
		})

		Convey("Missing multiple channels in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: chOutput,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &ErrNoChannel{ChannelNames: []string{Errors, Ready, Closer, Closed}})
		})

	})
}
