package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

// Common constants
const (
	OffsetNewest = sarama.OffsetNewest
	OffsetOldest = sarama.OffsetOldest
)

// InitRetryPeriod is the time between initialisation retries (for producers and consumer gropus)
var InitRetryPeriod = 1500 * time.Millisecond

// SetMaxMessageSize sets the Sarama MaxRequestSize and MaxResponseSize values to the provided maxSize
func SetMaxMessageSize(maxSize int32) {
	sarama.MaxRequestSize = maxSize
	sarama.MaxResponseSize = maxSize
}
