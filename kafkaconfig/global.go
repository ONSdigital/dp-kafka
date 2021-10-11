package kafkaconfig

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// Common constants
const (
	OffsetNewest = sarama.OffsetNewest
	OffsetOldest = sarama.OffsetOldest
)

// InitRetryPeriod is the initial time period between initialisation retries (for producers and consumer gropus)
var InitRetryPeriod = 250 * time.Millisecond

// ConsumeErrRetryPeriod is the initial time period between consumer retries on error (for consumer groups)
var ConsumeErrRetryPeriod = 250 * time.Millisecond

// MaxRetryInterval is the maximum time between retries (plus or minus a random amount)
var MaxRetryInterval = 31 * time.Second

// SetMaxMessageSize sets the Sarama MaxRequestSize and MaxResponseSize values to the provided maxSize
func SetMaxMessageSize(maxSize int32) {
	sarama.MaxRequestSize = maxSize
	sarama.MaxResponseSize = maxSize
}

// SetMaxRetryInterval sets MaxRetryInterval to its duration argument
func SetMaxRetryInterval(maxPause time.Duration) {
	MaxRetryInterval = maxPause
}

// GetRetryTime will return a duration based on the attempt and initial retry time.
// It uses the algorithm `2^n` where `n` is the attempt number (double the previous)
// plus a randomization factor of ±25% of the initial retry time
// (so that the server isn't being hit at the same time by many clients)
func GetRetryTime(attempt int, retryTime time.Duration) time.Duration {
	n := math.Pow(2, float64(attempt-1))
	retryPause := time.Duration(n) * retryTime
	// large values of `attempt` cause overflow (above is zero), so test for <=0
	if retryPause > MaxRetryInterval || retryPause <= 0 {
		retryPause = MaxRetryInterval
	}
	rnd := (rand.Int63n(50) - 25) * retryTime.Milliseconds() / 100
	return retryPause + time.Duration(rnd)*time.Millisecond
}

// WaitWithTimeout blocks until all go-routines tracked by a WaitGroup are done,
// or until the timeout defined in a context expires.
// It returns true only if the context timeout expired
func WaitWithTimeout(ctx context.Context, wg *sync.WaitGroup) bool {
	chWaiting := make(chan struct{})
	go func() {
		defer close(chWaiting)
		wg.Wait()
	}()
	select {
	case <-chWaiting:
		return false
	case <-ctx.Done():
		return true
	}
}
