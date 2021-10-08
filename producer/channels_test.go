package producer

import (
	"errors"
	"testing"

	"github.com/ONSdigital/dp-kafka/v3/kafkaerror"
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
			pCh := Channels{
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
			pCh := Channels{
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Output}},
			))
		})

		Convey("Missing Errors channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := Channels{
				Output: chOutput,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors}},
			))
		})

		Convey("Missing Ready channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := Channels{
				Output: chOutput,
				Errors: chErrors,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Ready}},
			))
		})

		Convey("Missing Closer channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := Channels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closer}},
			))
		})

		Convey("Missing Closed channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := Channels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closed}},
			))
		})

		Convey("Missing multiple channels in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := Channels{
				Output: chOutput,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Ready, Closer, Closed}},
			))
		})

	})
}
