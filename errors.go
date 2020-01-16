package kafka

import "errors"

var (
	ErrShutdownTimedOut      = errors.New("Shutdown context timed out")
	ErrNoOputputChannel      = errors.New("Output Channel does not exist")
	ErrNoErrorChannel        = errors.New("Error Channel does not exist")
	ErrNoCloserChannel       = errors.New("Closer Channel does not exist")
	ErrNoClosedChannel       = errors.New("Closed Channel does not exist")
	ErrNoUpstreamChannel     = errors.New("Upstream Channel does not exist")
	ErrNoUpstreamDoneChannel = errors.New("UpstreamDone Channel does not eixst")
)
