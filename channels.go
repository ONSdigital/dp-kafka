package kafka

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream     chan Message
	Errors       chan error
	Closer       chan struct{}
	Closed       chan struct{}
	UpstreamDone chan bool
}

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output chan []byte
	Errors chan error
	Closer chan struct{}
	Closed chan struct{}
}

// Validate returns ErrNoChannel if any consumer channel is nil
func (cCh *ConsumerGroupChannels) Validate() error {
	missingChannels := []string{}
	if cCh.Upstream == nil {
		missingChannels = append(missingChannels, "Upstream")
	}
	if cCh.Errors == nil {
		missingChannels = append(missingChannels, "Errors")
	}
	if cCh.Closer == nil {
		missingChannels = append(missingChannels, "Closer")
	}
	if cCh.Closed == nil {
		missingChannels = append(missingChannels, "Closed")
	}
	if cCh.UpstreamDone == nil {
		missingChannels = append(missingChannels, "UpstreamDone")
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// Validate returns ErrNoChannel if any producer channel is nil
func (pCh *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if pCh.Output == nil {
		missingChannels = append(missingChannels, "Output")
	}
	if pCh.Errors == nil {
		missingChannels = append(missingChannels, "Errors")
	}
	if pCh.Closer == nil {
		missingChannels = append(missingChannels, "Closer")
	}
	if pCh.Closed == nil {
		missingChannels = append(missingChannels, "Closed")
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// CreateConsumerGroupChannels initializes a ConsumerGroupChannels with new channels according to sync
func CreateConsumerGroupChannels(sync bool) ConsumerGroupChannels {
	var chUpstream chan Message
	if sync {
		// Sync -> upstream channel buffered, so we can send-and-wait for upstreamDone
		chUpstream = make(chan Message, 1)
	} else {
		// not sync -> upstream channel un-buffered
		chUpstream = make(chan Message)
	}
	return ConsumerGroupChannels{
		Upstream:     chUpstream,
		Errors:       make(chan error),
		Closer:       make(chan struct{}),
		Closed:       make(chan struct{}),
		UpstreamDone: make(chan bool, 1),
	}
}

// CreateProducerChannels initializes a ProducerChannels with new channels.
func CreateProducerChannels() ProducerChannels {
	return ProducerChannels{
		Output: make(chan []byte),
		Errors: make(chan error),
		Closer: make(chan struct{}),
		Closed: make(chan struct{}),
	}
}
