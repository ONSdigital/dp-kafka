package kafka

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

// init is executed only once, when the package is imported.
// Global variables are set here, to prevent any data race during runtime.
func init() {
	// disable metrics to prevent memory leak on broker.Open()
	metrics.UseNilMetrics = true
}

// SetMaxMessageSize sets the Sarama MaxRequestSize and MaxResponseSize values to the provided maxSize
func SetMaxMessageSize(maxSize int32) {
	sarama.MaxRequestSize = maxSize
	sarama.MaxResponseSize = maxSize
}

// maxAttempts calculates the maximum number of attempts
// such that starting with the provided retryTime
// and applying an exponential algorithm between iterations (2^n)
// the maxRetryTime value is never exceeded.
// Note that attempt 0 is considered the first attempt
func maxAttempts(retryTime, maxRetryTime time.Duration) int {
	maxAttempts := math.Log2(float64(maxRetryTime / retryTime))
	return int(math.Floor(maxAttempts))
}

// GetRetryTime will return a duration based on the attempt and initial retry time.
// It uses the algorithm `2^n` where `n` is the modulerised attempt number,
// so that we don't get values greater than 'maxRetryTime'.
// A randomization factor of ±25% is added to the initial retry time
// so that the server isn't being hit at the same time by many clients.
// The first attempt is assumed to be number 1, any negative or 0 value will be assumed to be 1.
func GetRetryTime(attempt int, retryTime, maxRetryTime time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1 // negative or 0 values will be assumed to be 'first attempt'
	}

	// Calculate basic retryPause according to the modular number of attempt
	modulus := maxAttempts(retryTime, maxRetryTime) + 1
	modAttempt := (attempt - 1) % modulus
	n := math.Pow(2, float64(modAttempt))
	retryPause := time.Duration(n) * retryTime

	// add or subtract a random factor of up to 25%
	rnd := (rand.Int63n(50) - 25) * retryTime.Milliseconds() / 100
	retryPause += time.Duration(rnd) * time.Millisecond

	// cap the value to maxRetryTime if it was exceeded
	if retryPause > maxRetryTime {
		retryPause = maxRetryTime
	}

	return retryPause
}

// WaitWithTimeout blocks until all go-routines tracked by a WaitGroup are done or a timeout expires.
// It returns true if the timeout expired, or false if the waitgroup finished before the timeout.
func WaitWithTimeout(wg *sync.WaitGroup, tout time.Duration) bool {
	chWaiting := make(chan struct{})
	go func() {
		defer SafeClose(chWaiting)
		wg.Wait()
	}()

	delay := time.NewTimer(tout)
	select {
	case <-chWaiting:
		// Ensure timer is stopped and its resources are freed
		if !delay.Stop() {
			// if the timer has been stopped then read from the channel
			<-delay.C
		}
		return false
	case <-delay.C:
		return true
	}
}
