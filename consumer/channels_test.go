package consumer

import (
	"errors"
	"testing"

	"github.com/ONSdigital/dp-kafka/v3/kafkaerror"
	"github.com/ONSdigital/dp-kafka/v3/message"
	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

// createConsumerChannels creates local consumer channels for testing
func createConsumerChannels() (chUpstream chan message.Message, chReady chan struct{}, chConsume chan bool, chCloser, chClosed chan struct{},
	chErrors chan error) {
	chUpstream = make(chan message.Message)
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
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
			So(err, ShouldResemble, kafkaerror.NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Consume, Closer, Closed}},
			))
		})
	})
}
