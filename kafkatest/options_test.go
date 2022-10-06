package kafkatest

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestOptionalHeaders(t *testing.T) {
	t.Parallel()

	Convey("Given valid headers", t, func() {
		headers := Headers{
			"key":  "value",
			"key2": "value2",
		}

		Convey("When OptionalHeaders is called", func() {
			headerFunc := OptionalHeaders(headers)

			Convey("Then a function is returned to update the headers in the option", func() {
				So(headerFunc, ShouldNotBeNil)

				Convey("And calling the function will update the header within the options", func() {
					options := &Options{}

					err := headerFunc(options)
					So(err, ShouldBeNil)

					So(options.Headers, ShouldNotBeEmpty)
					So(options.Headers["key"], ShouldEqual, "value")
					So(options.Headers["key2"], ShouldEqual, "value2")
				})
			})
		})
	})

	Convey("Given empty headers", t, func() {
		headers := Headers{}

		Convey("When OptionalHeaders is called", func() {
			headerFunc := OptionalHeaders(headers)

			Convey("Then a function is returned to update the headers in the option", func() {
				So(headerFunc, ShouldNotBeNil)

				Convey("And calling the function will update the header to be empty within the options", func() {
					options := &Options{}

					err := headerFunc(options)
					So(err, ShouldBeNil)

					So(options.Headers, ShouldBeEmpty)
				})
			})
		})
	})
}
