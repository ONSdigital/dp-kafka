package kafka

import (
	"errors"
	"testing"

	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

// createProducerChannels creates local producer channels for testing
func createProducerChannels() (output chan []byte, errs chan error, init, closer, closed chan struct{}) {
	output = make(chan []byte)
	errs = make(chan error)
	init = make(chan struct{})
	closer = make(chan struct{})
	closed = make(chan struct{})
	return
}

func TestProducerChannelsValidate(t *testing.T) {

	Convey("Given a set of producer channels", t, func() {
		output, errs, init, closer, closed := createProducerChannels()

		Convey("ProducerChannels with all required channels has a successful validation", func() {
			pCh := ProducerChannels{
				Output:      output,
				Errors:      errs,
				Initialised: init,
				Closer:      closer,
				Closed:      closed,
			}
			err := pCh.Validate()
			So(err, ShouldBeNil)
		})

		Convey("Missing Output channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Errors:      errs,
				Initialised: init,
				Closer:      closer,
				Closed:      closed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Output}},
			))
		})

		Convey("Missing Errors channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output:      output,
				Initialised: init,
				Closer:      closer,
				Closed:      closed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors}},
			))
		})

		Convey("Missing Initialised channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: output,
				Errors: errs,
				Closer: closer,
				Closed: closed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Initialised}},
			))
		})

		Convey("Missing Closer channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output:      output,
				Errors:      errs,
				Initialised: init,
				Closed:      closed,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closer}},
			))
		})

		Convey("Missing Closed channel in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output:      output,
				Errors:      errs,
				Initialised: init,
				Closer:      closer,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Closed}},
			))
		})

		Convey("Missing multiple channels in ProducerChannels results in an ErrNoChannel error", func() {
			pCh := ProducerChannels{
				Output: output,
			}
			err := pCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate producer because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Initialised, Closer, Closed}},
			))
		})

	})
}

// createConsumerChannels creates local consumer channels for testing
func createConsumerChannels() (upstream chan Message, consume chan bool,
	init, closer, closed chan struct{}, errs chan error) {
	upstream = make(chan Message)
	init = make(chan struct{})
	consume = make(chan bool)
	closer = make(chan struct{})
	closed = make(chan struct{})
	errs = make(chan error)
	return
}

func TestConsumerGroupChannelsValidate(t *testing.T) {

	Convey("Given a set of consumer group channels", t, func() {
		upstream, consume, init, closer, closed, errs := createConsumerChannels()

		Convey("ConsumerGroupChannels with all required channels has a successful validation", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
				Consume:     consume,
				Closer:      closer,
				Closed:      closed,
				Errors:      errs,
			}
			err := cCh.Validate()
			So(err, ShouldBeNil)
		})

		Convey("Missing Upstream channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Initialised: init,
				Consume:     consume,
				Closer:      closer,
				Closed:      closed,
				Errors:      errs,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Upstream}},
			))
		})

		Convey("Missing Initialised channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream: upstream,
				Consume:  consume,
				Closer:   closer,
				Closed:   closed,
				Errors:   errs,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Initialised}},
			))
		})

		Convey("Missing Consume channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
				Closer:      closer,
				Closed:      closed,
				Errors:      errs,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Consume}},
			))
		})

		Convey("Missing Closer channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
				Consume:     consume,
				Closed:      closed,
				Errors:      errs,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Closer}},
			))
		})

		Convey("Missing Closed channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
				Consume:     consume,
				Closer:      closer,
				Errors:      errs,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Closed}},
			))
		})

		Convey("Missing Errors channel in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
				Consume:     consume,
				Closer:      closer,
				Closed:      closed,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Errors}},
			))
		})

		Convey("Missing multiple channels in ConsumerGroupChannels results in an ErrNoChannel error", func() {
			cCh := ConsumerGroupChannels{
				Upstream:    upstream,
				Initialised: init,
			}
			err := cCh.Validate()
			So(err, ShouldResemble, NewError(
				errors.New("failed to validate consumer group because some channels are missing"),
				log.Data{"missing_channels": []string{Errors, Consume, Closer, Closed}},
			))
		})
	})
}
