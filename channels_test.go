package kafka

import (
	"errors"
	"testing"
	"time"

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
func createConsumerChannels() (upstream chan Message, consume chan bool, init, closer, closed chan struct{}, errs chan error) {
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

func TestSafeClose(t *testing.T) {
	Convey("A struct channel can be safely closed and closing it again does not panic", t, func(c C) {
		ch := make(chan struct{})
		So(SafeClose(ch), ShouldBeTrue)
		validateChanClosed(c, ch, true)
		So(SafeClose(ch), ShouldBeFalse)
	})

	Convey("A bool channel can be safely closed and closing it again does not panic", t, func(c C) {
		ch := make(chan bool)
		So(SafeCloseBool(ch), ShouldBeTrue)
		validateChanBoolClosed(c, ch, true)
		So(SafeCloseBool(ch), ShouldBeFalse)
	})

	Convey("A Message channel can be safely closed and closing it again does not panic", t, func(c C) {
		ch := make(chan Message)
		So(SafeCloseMessage(ch), ShouldBeTrue)
		validateChanMessageClosed(c, ch, true)
		So(SafeCloseMessage(ch), ShouldBeFalse)
	})

	Convey("An Error channel can be safely closed and closing it again does not panic", t, func(c C) {
		ch := make(chan error)
		So(SafeCloseErr(ch), ShouldBeTrue)
		validateChanErrClosed(c, ch, true)
		So(SafeCloseErr(ch), ShouldBeFalse)
	})

	Convey("A byte array channel can be safely closed and closing it again does not panic", t, func(c C) {
		ch := make(chan []byte)
		So(SafeCloseBytes(ch), ShouldBeTrue)
		validateChanBytesClosed(c, ch, true)
		So(SafeCloseBytes(ch), ShouldBeFalse)
	})
}

// validateChanClosed validates that a struct channel is closed before a timeout expires
func validateChanClosed(c C, ch chan struct{}, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanBoolClosed validates that a boolean channel is closed before a timeout expires
func validateChanBoolClosed(c C, ch chan bool, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanMessageClosed validates that a Message channel is closed before a timeout expires
func validateChanMessageClosed(c C, ch chan Message, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanBytesClosed validates that a byte array  channel is closed before a timeout expires
func validateChanBytesClosed(c C, ch chan []byte, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanMessageClosed validates that an Error channel is closed before a timeout expires
func validateChanErrClosed(c C, ch chan error, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	select {
	case _, ok := <-ch:
		if !ok {
			closed = true
		}
	case <-time.After(TIMEOUT):
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}
