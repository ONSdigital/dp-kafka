package kafka

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// SetMaxMessageSize sets the Sarama MaxRequestSize and MaxResponseSize values to the provided maxSize
func SetMaxMessageSize(maxSize int32) {
	sarama.MaxRequestSize = maxSize
	sarama.MaxResponseSize = maxSize
}

// maxAttempts calculates the maximum number of attempts
// such that starting with the provided retryTime
// and applying an exponential algorithm between iterations (2^n)
// the MaxRetryInterval value is never exceeded.
// Note that attempt 0 is considered the first attempt
func maxAttempts(retryTime, maxRetryTime time.Duration) int {
	maxAttempts := math.Log2(float64(maxRetryTime / retryTime))
	return int(math.Floor(maxAttempts))
}

// GetRetryTime will return a duration based on the attempt and initial retry time.
// It uses the algorithm `2^n` where `n` is the modulerised attempt number,
// so that we don't get values greater than MaxRetryInterval.
// A randomization factor of ±25% is added to the initial retry time
// so that the server isn't being hit at the same time by many clients.
// The first attempt is assumed to be number 1
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
	retryPause = retryPause + time.Duration(rnd)*time.Millisecond

	// cap the value to MaxRetryInterval if it was exceeded
	if retryPause > maxRetryTime {
		retryPause = maxRetryTime
	}

	return retryPause
}

// WaitWithTimeout blocks until all go-routines tracked by a WaitGroup are done,
// or until the timeout defined in a context expires.
// It returns true only if the context timeout expired
func WaitWithTimeout(wg *sync.WaitGroup) bool {
	chWaiting := make(chan struct{})
	go func() {
		defer SafeClose(chWaiting)
		wg.Wait()
	}()

	select {
	case <-chWaiting:
		return false
	case <-time.After(500 * time.Millisecond):
		return true
	}
}
