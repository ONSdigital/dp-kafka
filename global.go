package kafka

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

// ConsumeErrRetryPeriod is the initial time period between consume retrials on error (for consumer groups)
var ConsumeErrRetryPeriod = 250 * time.Millisecond

// SetMaxMessageSize sets the Sarama MaxRequestSize and MaxResponseSize values to the provided maxSize
func SetMaxMessageSize(maxSize int32) {
	sarama.MaxRequestSize = maxSize
	sarama.MaxResponseSize = maxSize
}

// getRetryTime will return a time based on the attempt and initial retry time.
// It uses the algorithm 2^n where n is the attempt number (double the previous) and
// a randomization factor of between 0-5ms so that the server isn't being hit constantly
// at the same time by many clients.
func getRetryTime(attempt int, retryTime time.Duration) time.Duration {
	n := (math.Pow(2, float64(attempt)))
	rand.Seed(time.Now().Unix())
	rnd := time.Duration(rand.Intn(4)+1) * time.Millisecond
	return (time.Duration(n) * retryTime) - rnd
}

// waitWithTimeout blocks until all go-routines tracked by a WaitGroup are done,
// or until the timeout defined in a context expires.
// It returns true only if the context timeout expired
func waitWithTimeout(ctx context.Context, wg *sync.WaitGroup) bool {
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
