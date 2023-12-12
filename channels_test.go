package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

// createProducerChannels creates local producer channels for testing
func createProducerChannels() (output chan BytesMessage, errs chan error, init, closer, closed chan struct{}) {
	output = make(chan BytesMessage)
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
		ch := make(chan BytesMessage)
		So(SafeCloseBytes(ch), ShouldBeTrue)
		validateChanBytesClosed(c, ch, true)
		So(SafeCloseBytes(ch), ShouldBeFalse)
	})
}

func TestSafeSendBool(t *testing.T) {
	Convey("A bool value can be safely sent to a bool chan an does not panic if it is closed", t, func(c C) {
		ch := make(chan bool)
		go func() {
			err := SafeSendBool(ch, true)
			c.So(err, ShouldBeNil)
		}()
		validateChanReceivesBool(ch, true)
		close(ch)
		err := SafeSendBool(ch, true)
		So(err, ShouldResemble, errors.New("failed to send bool value to channel: send on closed channel"))
	})
}

func TestSafeSendBytes(t *testing.T) {
	Convey("A byte array value can be safely sent to a byte array chan an does not panic if it is closed", t, func(c C) {
		ch := make(chan BytesMessage)
		message := BytesMessage{Value: []byte{1, 2, 3}, Context: context.Background()}
		go func() {
			err := SafeSendBytes(ch, message)
			c.So(err, ShouldBeNil)
		}()
		validateChanReceivesBytes(ch, []byte{1, 2, 3})
		close(ch)
		err := SafeSendBytes(ch, message)
		So(err, ShouldResemble, errors.New("failed to send byte array value to channel: send on closed channel"))
	})
}

func TestSafeSendProducerMessage(t *testing.T) {
	Convey("A ProducerMessage value can be safely sent to a ProducerMessage chan an does not panic if it is closed", t, func(c C) {
		ch := make(chan *sarama.ProducerMessage)
		go func() {
			err := SafeSendProducerMessage(context.Background(), ch, &sarama.ProducerMessage{Topic: "testTopic"})
			c.So(err, ShouldBeNil)
		}()
		validateChanReceivesProducerMessage(ch, &sarama.ProducerMessage{Topic: "testTopic"})
		close(ch)
		err := SafeSendProducerMessage(context.Background(), ch, &sarama.ProducerMessage{Topic: "testTopic"})
		So(err, ShouldResemble, errors.New("failed to send ProducerMessage value to channel: send on closed channel"))
	})
}

func TestSafeSendConsumerMessage(t *testing.T) {
	Convey("A ConsumerMessage value can be safely sent to a ConsumerMessage chan an does not panic if it is closed", t, func(c C) {
		ch := make(chan *sarama.ConsumerMessage)
		go func() {
			err := SafeSendConsumerMessage(ch, &sarama.ConsumerMessage{Topic: "testTopic"})
			c.So(err, ShouldBeNil)
		}()
		validateChanReceivesConsumerMessage(ch, &sarama.ConsumerMessage{Topic: "testTopic"})
		close(ch)
		err := SafeSendConsumerMessage(ch, &sarama.ConsumerMessage{Topic: "testTopic"})
		So(err, ShouldResemble, errors.New("failed to send ConsumerMessage value to channel: send on closed channel"))
	})
}

func TestSafeSendErr(t *testing.T) {
	Convey("An error value can be safely sent to an error chan an does not panic if it is closed", t, func(c C) {
		ch := make(chan error)
		val := errors.New("error to be sent")
		go func() {
			err := SafeSendErr(ch, val)
			c.So(err, ShouldBeNil)
		}()
		validateChanReceivesErr(ch, val)
		close(ch)
		err := SafeSendErr(ch, val)
		So(err, ShouldResemble, errors.New("failed to send err value to channel: send on closed channel"))
	})
}

// validateChanClosed validates that a struct channel is closed before a timeout expires
func validateChanClosed(c C, ch chan struct{}, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case _, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			closed = true
		}
	case <-delay.C:
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
	delay := time.NewTimer(TIMEOUT)
	select {
	case _, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			closed = true
		}
	case <-delay.C:
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
	delay := time.NewTimer(TIMEOUT)
	select {
	case _, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			closed = true
		}
	case <-delay.C:
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanBytesClosed validates that a byte array  channel is closed before a timeout expires
func validateChanBytesClosed(c C, ch chan BytesMessage, expectedClosed bool) {
	var (
		closed  bool
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case _, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			closed = true
		}
	case <-delay.C:
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
	delay := time.NewTimer(TIMEOUT)
	select {
	case _, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			closed = true
		}
	case <-delay.C:
		timeout = true
	}
	c.So(timeout, ShouldNotEqual, expectedClosed)
	c.So(closed, ShouldEqual, expectedClosed)
}

// validateChanReceivesBool validates that a bool channel receives a provided bool value
func validateChanReceivesBool(ch chan bool, expectedVal bool) {
	var (
		rxVal   bool
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case val, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			break
		}
		rxVal = val
	case <-delay.C:
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxVal, ShouldResemble, expectedVal)
}

// validateChanReceivesBytes validates that a byte array channel receives a provided byte array value
func validateChanReceivesBytes(ch chan BytesMessage, expectedVal []byte) {
	var (
		rxVal   []byte
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case val, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			break
		}
		rxVal = val.Value
	case <-delay.C:
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxVal, ShouldResemble, expectedVal)
}

// validateChanReceivesProducerMessage validates that a ProducerMessage channel receives a provided ProducerMessage
func validateChanReceivesProducerMessage(ch chan *sarama.ProducerMessage, expectedVal *sarama.ProducerMessage) {
	var (
		rxVal   *sarama.ProducerMessage
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case e, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			break
		}
		rxVal = e
	case <-delay.C:
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxVal, ShouldResemble, expectedVal)
}

// validateChanReceivesConsumerMessage validates that a ConsumerMessage channel receives a provided ConsumerMessage
func validateChanReceivesConsumerMessage(ch chan *sarama.ConsumerMessage, expectedVal *sarama.ConsumerMessage) {
	var (
		rxVal   *sarama.ConsumerMessage
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case e, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			break
		}
		rxVal = e
	case <-delay.C:
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxVal, ShouldResemble, expectedVal)
}

// validateChanReceivesErr validates that an error channel receives a provided error
func validateChanReceivesErr(ch chan error, expectedErr error) {
	var (
		rxVal   error
		timeout bool
	)
	delay := time.NewTimer(TIMEOUT)
	select {
	case e, ok := <-ch:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		if !ok {
			break
		}
		rxVal = e
	case <-delay.C:
		timeout = true
	}
	So(timeout, ShouldBeFalse)
	So(rxVal, ShouldResemble, expectedErr)
}
