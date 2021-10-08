package producer

// channel names
const (
	Errors       = "Errors"
	Ready        = "Ready"
	Consume      = "Consume"
	Closer       = "Closer"
	Closed       = "Closed"
	Upstream     = "Upstream"
	UpstreamDone = "UpstreamDone"
	Output       = "Output"
)

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output chan []byte
	Errors chan error
	Ready  chan struct{}
	Closer chan struct{}
	Closed chan struct{}
}

// Validate returns ErrNoChannel if any producer channel is nil
func (producerChannels *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if producerChannels.Output == nil {
		missingChannels = append(missingChannels, Output)
	}
	if producerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if producerChannels.Ready == nil {
		missingChannels = append(missingChannels, Ready)
	}
	if producerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if producerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() *ProducerChannels {
	return &ProducerChannels{
		Output: make(chan []byte),
		Errors: make(chan error),
		Ready:  make(chan struct{}),
		Closer: make(chan struct{}),
		Closed: make(chan struct{}),
	}
}
