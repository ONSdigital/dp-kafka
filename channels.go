package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
)

// channel names
const (
	Errors      = "Errors"
	Initialised = "Initialised"
	Consume     = "Consume"
	Closer      = "Closer"
	Closed      = "Closed"
	Upstream    = "Upstream"
	Output      = "Output"
)

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream    chan Message
	Errors      chan error
	Initialised chan struct{}
	Consume     chan bool
	Closer      chan struct{}
	Closed      chan struct{}
	State       *ConsumerStateChannels
}

// ConsumerStateChannels represents the channels that are used to notify of consumer-group state changes
type ConsumerStateChannels struct {
	Initialising *StateChan
	Stopped      *StateChan
	Starting     *StateChan
	Consuming    *StateChan
	Stopping     *StateChan
	Closing      *StateChan
}

// StateChan provides a concurrency-safe channel for state machines,
// representing one state.
type StateChan struct {
	channel chan struct{}
	mutex   *sync.RWMutex
}

// NewStateChan creates a new StateChan with a new struct channel and read-write mutex
func NewStateChan() *StateChan {
	return &StateChan{
		channel: make(chan struct{}),
		mutex:   &sync.RWMutex{},
	}
}

// Enter signals that the state has been reached, by closing the channel in a concurrency-safe manner
func (sc *StateChan) enter() {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	SafeClose(sc.channel)
}

// Leave signals that the state has been left by resetting the channel in a concurrency-safe manner
func (sc *StateChan) leave() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.channel = make(chan struct{})
}

// Get returns the channel wrapped by this StateChan struct in a concurrency-safe manner.
// Note: if you intend to wait on this channel, you will need to acquire a read lock on RWMutex()
// if you want to wait on the channel while a concurrent sc.leave() call may happen.
func (sc *StateChan) Channel() chan struct{} {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	return sc.channel
}

// RWMutex returns the read-write mutex, so that callers can use it to prevent possible race conditions, if needed
func (sc *StateChan) RWMutex() *sync.RWMutex {
	return sc.mutex
}

// Wait blocks the calling thread until the state is reached (channel is closed)
func (sc *StateChan) Wait() {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	<-sc.channel
}

type BytesMessage struct {
	Context context.Context
	Value   []byte
}

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output      chan BytesMessage
	Errors      chan error
	Initialised chan struct{}
	Closer      chan struct{}
	Closed      chan struct{}
}

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels.
// You can provide the buffer size to determine the number of messages that will be buffered
// in the upstream channel (to receive messages)
// The State channels are not initialised until the state machine is created.
func CreateConsumerGroupChannels(upstreamBufferSize, errorsBufferSize int) *ConsumerGroupChannels {
	var upstream chan Message
	if upstreamBufferSize > 0 {
		upstream = make(chan Message, upstreamBufferSize)
	} else {
		upstream = make(chan Message)
	}

	var err chan error
	if errorsBufferSize > 0 {
		err = make(chan error, errorsBufferSize)
	} else {
		err = make(chan error)
	}

	return &ConsumerGroupChannels{
		Upstream:    upstream,
		Errors:      err,
		Initialised: make(chan struct{}),
		Consume:     make(chan bool),
		Closer:      make(chan struct{}),
		Closed:      make(chan struct{}),
	}
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() *ProducerChannels {
	return &ProducerChannels{
		Output:      make(chan BytesMessage),
		Errors:      make(chan error),
		Initialised: make(chan struct{}),
		Closer:      make(chan struct{}),
		Closed:      make(chan struct{}),
	}
}

// Validate returns an Error with a list of missing channels if any consumer channel is nil
func (consumerChannels *ConsumerGroupChannels) Validate() error {
	missingChannels := []string{}
	if consumerChannels.Upstream == nil {
		missingChannels = append(missingChannels, Upstream)
	}
	if consumerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if consumerChannels.Initialised == nil {
		missingChannels = append(missingChannels, Initialised)
	}
	if consumerChannels.Consume == nil {
		missingChannels = append(missingChannels, Consume)
	}
	if consumerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if consumerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return NewError(
			errors.New("failed to validate consumer group because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}

// Validate returns an error with a list of missing channels if any producer channel is nil
func (producerChannels *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if producerChannels.Output == nil {
		missingChannels = append(missingChannels, Output)
	}
	if producerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if producerChannels.Initialised == nil {
		missingChannels = append(missingChannels, Initialised)
	}
	if producerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if producerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return NewError(
			errors.New("failed to validate producer because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}

// SafeClose closes a struct{} channel and ignores the panic if the channel was already closed
func SafeClose(ch chan struct{}) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	justClosed = true
	return
}

// SafeCloseMessage closes a Message channel and ignores the panic if the channel was already closed
func SafeCloseMessage(ch chan Message) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	justClosed = true
	return
}

// SafeCloseBytes closes a byte array channel and ignores the panic if the channel was already closed
func SafeCloseBytes(ch chan BytesMessage) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	justClosed = true
	return
}

// SafeCloseBool closes a bool channel and ignores the panic if the channel was already closed
func SafeCloseBool(ch chan bool) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	justClosed = true
	return
}

// SafeCloseErr closes an error channel and ignores the panic if the channel was already closed
func SafeCloseErr(ch chan error) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	justClosed = true
	return
}

// SafeSendBool sends a provided bool value to the provided bool chan and returns an error instead of panicking if the channel is closed
func SafeSendBool(ch chan bool, val bool) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send bool value to channel: %v", pErr)
		}
	}()
	ch <- val
	return
}

// SafeSendProducerMessage sends a provided ProducerMessage value to the provided ProducerMessage chan and returns an error instead of panicking if the channel is closed
func SafeSendProducerMessage(ctx context.Context, ch chan<- *sarama.ProducerMessage, val *sarama.ProducerMessage, otelEnabled bool) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send ProducerMessage value to channel: %v", pErr)
		}
	}()

	if otelEnabled {
		otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(val))
	}
	ch <- val
	return
}

// SafeSendConsumerMessage sends a provided ConsumerMessage value to the provided ProducerMessage chan and returns an error instead of panicking if the channel is closed
func SafeSendConsumerMessage(ch chan<- *sarama.ConsumerMessage, val *sarama.ConsumerMessage) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send ConsumerMessage value to channel: %v", pErr)
		}
	}()
	ch <- val
	return
}

// SafeSendBytes sends a provided byte array value to the provided byte array chan and returns an error instead of panicking if the channel is closed
func SafeSendBytes(ch chan BytesMessage, val BytesMessage) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send byte array value to channel: %v", pErr)
		}
	}()
	ch <- val
	return
}

// SafeSendErr sends a provided error value to the provided error chan and returns an error instead of panicking if the channel is closed
func SafeSendErr(ch chan error, val error) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("failed to send err value to channel: %v", pErr)
		}
	}()
	ch <- val
	return
}
