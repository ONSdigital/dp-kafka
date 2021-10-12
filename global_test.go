package kafka

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetRetryTime(t *testing.T) {

	Convey("Given a valid retry interval", t, func() {

		var initialInterval_ms int64 = 200
		initialInterval := time.Duration(initialInterval_ms) * time.Millisecond

		Convey("getRetryTime returns sane progression over many attempts", func() {
			maxInterval_ms := MaxRetryInterval.Milliseconds()
			expectedInterval := initialInterval
			powerHasWrapped := false

			for attempt := 1; attempt < 100; attempt++ {
				// get computed interval
				interval := GetRetryTime(attempt, initialInterval)
				interval_ms := interval.Milliseconds()

				expectedInterval_ms := expectedInterval.Milliseconds()

				So(interval_ms, ShouldBeGreaterThanOrEqualTo, initialInterval_ms/100*75)
				if powerHasWrapped {
					So(interval_ms, ShouldBeGreaterThan, maxInterval_ms-initialInterval_ms)
				} else {
					// when values have not wrapped/overflowed
					possiblyOverInterval := expectedInterval_ms + initialInterval_ms
					if possiblyOverInterval < MaxRetryInterval.Milliseconds() {
						// and not over MaxRetryInterval yet
						So(interval_ms, ShouldBeBetween,
							expectedInterval_ms-initialInterval_ms,
							expectedInterval_ms+initialInterval_ms)
					}

				}
				So(interval_ms, ShouldBeLessThan, maxInterval_ms+initialInterval_ms)

				expectedInterval *= 2
				if expectedInterval <= 0 {
					powerHasWrapped = true
				}
			}
		})
	})

}
