package interfaces

import (
	"context"
)

//go:generate moq -out ../mock/message.go -pkg mock . Message

// Message represents a single kafka message.
type Message interface {

	// GetData returns the message contents.
	GetData() []byte

	// GetHeader takes a key for the header and returns the value if the key exist in the header.
	GetHeader(key string) string

	// Context returns a context with traceid.
	Context() context.Context

	// Mark marks the message as consumed, but doesn't commit the offset to the backend
	Mark()

	// Commit marks the message as consumed and commits its offset to the backend
	Commit()

	// Release closes the UpstreamDone channel for this message
	Release()

	// CommitAndRelease marks a message as consumed, commits it and closes the UpstreamDone channel
	CommitAndRelease()

	// Offset returns the message offset
	Offset() int64

	// UpstreamDone returns the upstreamDone channel. Closing this channel notifies that the message has been consumed
	UpstreamDone() chan struct{}
}
