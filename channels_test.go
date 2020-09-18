package kafka_test

import (
	"testing"

	kafka "github.com/ONSdigital/dp-kafka"
	. "github.com/smartystreets/goconvey/convey"
)

// createConsumerChannels creates local consumer channels for testing
func createConsumerChannels() (chUpstream chan kafka.Message, chReady, chCloser, chClosed chan struct{},
	chErrors chan error) {
	chUpstream = make(chan kafka.Message)
	chReady = make(chan struct{})
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
	chErrors = make(chan error)
	return
}

// createProducerChannels creates local producer channels for testing
func createProducerChannels() (chOutput chan []byte, chErrors chan error, chReady, chCloser, chClosed chan struct{}) {
	chOutput = make(chan []byte)
	chErrors = make(chan error)
	chReady = make(chan struct{})
	chCloser = make(chan struct{})
	chClosed = make(chan struct{})
	return
}

func TestConsumerGroupChannelsValidate(t *testing.T) {

	Convey("Given a set of consumer group channels", t, func() {
		chUpstream, chReady, chCloser, chClosed, chErrors := createConsumerChannels()

		Convey("ConsumerGroupChannels with all required channels has a successful validation", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldBeNil)
		})

		Convey("Missing Upstream channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
				Errors: chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Upstream}})
		})

		Convey("Missing Ready channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Ready}})
		})

		Convey("Missing Closer channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Closer}})
		})

		Convey("Missing Closed channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closer:   chCloser,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Closed}})
		})

		Convey("Missing Errors channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closer:   chCloser,
				Closed:   chClosed,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors}})
		})

		Convey("Missing UpstreamDone channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
				Closer:   chCloser,
				Closed:   chClosed,
				Errors:   chErrors,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.UpstreamDone}})
		})

		Convey("Missing multiple channels in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := kafka.ConsumerGroupChannels{
				Upstream: chUpstream,
				Ready:    chReady,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors, kafka.Closer, kafka.Closed, kafka.UpstreamDone}})
		})

	})
}

func TestProducerChannelsValidate(t *testing.T) {

	Convey("Given a set of producer channels", t, func() {
		chOutput, chErrors, chReady, chCloser, chClosed := createProducerChannels()

		Convey("ProducerChannels with all required channels has a successful validation", func() {
			pCh := kafka.ProducerChannels{
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
			pCh := kafka.ProducerChannels{
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Output}})
		})

		Convey("Missing Errors channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := kafka.ProducerChannels{
				Output: chOutput,
				Ready:  chReady,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors}})
		})

		Convey("Missing Ready channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := kafka.ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Closer: chCloser,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Ready}})
		})

		Convey("Missing Closer channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := kafka.ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closed: chClosed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Closer}})
		})

		Convey("Missing Closed channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := kafka.ProducerChannels{
				Output: chOutput,
				Errors: chErrors,
				Ready:  chReady,
				Closer: chCloser,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Closed}})
		})

		Convey("Missing multiple channels in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := kafka.ProducerChannels{
				Output: chOutput,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, &kafka.ErrNoChannel{ChannelNames: []string{kafka.Errors, kafka.Ready, kafka.Closer, kafka.Closed}})
		})

	})
}
