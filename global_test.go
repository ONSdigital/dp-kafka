package kafka

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMaxAttempts(t *testing.T) {
	Convey("Given a valid retry interval and power of 2 MaxRetryInterval then 'maxAttempts' returns the expected value for number of attempts", t, func() {
		maxRetryTime := 32 * time.Second
		retryTime := 1 * time.Second
		m := maxAttempts(retryTime, maxRetryTime)
		So(m, ShouldEqual, 5)
	})

	Convey("Given a valid retry interval and MaxRetryInterval then 'maxAttempts' returns the expected value for number of attempts", t, func() {
		maxRetryTime := 31 * time.Second
		retryTime := 1 * time.Second
		m := maxAttempts(retryTime, maxRetryTime)
		So(m, ShouldEqual, 4)
	})
}

func TestGetRetryTime(t *testing.T) {
	Convey("given valid max retry and initial retry intervals", t, func() {
		maxRetryTime := 25601 * time.Millisecond // near 25.6 sec, so that MaxRetryInterval check is likely triggered
		initialInterval := 200 * time.Millisecond

		Convey("Then providing a value lower than 1 to GetRetryTime results in a value within 25 percent of initialInterval being returned", func() {
			interval := GetRetryTime(-1, initialInterval, maxRetryTime)
			So(interval, ShouldBeBetweenOrEqual, initialInterval*75/100, initialInterval*125/100)
			interval = GetRetryTime(0, initialInterval, maxRetryTime)
			So(interval, ShouldBeBetweenOrEqual, initialInterval*75/100, initialInterval*125/100)
			interval = GetRetryTime(1, initialInterval, maxRetryTime)
			So(interval, ShouldBeBetweenOrEqual, initialInterval*75/100, initialInterval*125/100)
		})

		Convey("Then the interval for each iteration has a value that falls within 25 percent of 2^n, resetting n every time that MaxRetryInterval is reached", func() {
			expectedInterval := initialInterval
			for attempt := 1; attempt < 100; attempt++ {
				interval := GetRetryTime(attempt, initialInterval, maxRetryTime)
				So(interval, ShouldBeBetweenOrEqual, expectedInterval*75/100, expectedInterval*125/100)
				So(interval, ShouldBeLessThanOrEqualTo, MaxRetryInterval)

				expectedInterval *= 2
				if expectedInterval >= maxRetryTime {
					expectedInterval = initialInterval
				}
			}
		})
	})
}
