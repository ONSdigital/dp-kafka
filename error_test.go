package kafka

import (
	"fmt"
	"testing"

	"github.com/ONSdigital/log.go/v2/log"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

type testError struct {
	err     error
	logData map[string]interface{}
}

func (e testError) Error() string {
	if e.err == nil {
		return "nil"
	}
	return e.err.Error()
}

func (e testError) Unwrap() error {
	return e.err
}

func (e testError) LogData() map[string]interface{} {
	return e.logData
}

func TestUnwrapLogDataHappy(t *testing.T) {
	Convey("Given an error chain with wrapped logData", t, func() {
		err1 := &testError{
			err: errors.New("original error"),
			logData: log.Data{
				"log": "data",
			},
		}

		err2 := &testError{
			err: fmt.Errorf("err1: %w", err1),
			logData: log.Data{
				"additional": "data",
			},
		}

		err3 := &testError{
			err: fmt.Errorf("err2: %w", err2),
			logData: log.Data{
				"final": "data",
			},
		}

		Convey("When UnwrapLogData(err) is called", func() {
			logData := UnwrapLogData(err3)
			expected := log.Data{
				"final":      "data",
				"additional": "data",
				"log":        "data",
			}

			So(logData, ShouldResemble, expected)
		})
	})

	Convey("Given an error chain with intermittent wrapped logData", t, func() {
		err1 := &testError{
			err: errors.New("original error"),
			logData: log.Data{
				"log": "data",
			},
		}

		err2 := &testError{
			err: fmt.Errorf("err1: %w", err1),
		}

		err3 := &testError{
			err: fmt.Errorf("err2: %w", err2),
			logData: log.Data{
				"final": "data",
			},
		}

		Convey("When UnwrapLogData(err) is called", func() {
			logData := UnwrapLogData(err3)
			expected := log.Data{
				"final": "data",
				"log":   "data",
			}

			So(logData, ShouldResemble, expected)
		})
	})

	Convey("Given an error chain with wrapped logData with duplicate key values", t, func() {
		err1 := &testError{
			err: errors.New("original error"),
			logData: log.Data{
				"log":        "data",
				"duplicate":  "duplicate_data1",
				"request_id": "ADB45F",
			},
		}

		err2 := &testError{
			err: fmt.Errorf("err1: %w", err1),
			logData: log.Data{
				"additional": "data",
				"duplicate":  "duplicate_data2",
				"request_id": "ADB45F",
			},
		}

		err3 := &testError{
			err: fmt.Errorf("err2: %w", err2),
			logData: log.Data{
				"final":      "data",
				"duplicate":  "duplicate_data3",
				"request_id": "ADB45F",
			},
		}

		Convey("When UnwrapLogData(err) is called", func() {
			logData := UnwrapLogData(err3)
			expected := log.Data{
				"final":      "data",
				"additional": "data",
				"log":        "data",
				"duplicate": []interface{}{
					"duplicate_data3",
					"duplicate_data2",
					"duplicate_data1",
				},
				"request_id": "ADB45F",
			}

			So(logData, ShouldResemble, expected)
		})
	})
}

func TestStackTraceHappy(t *testing.T) {
	Convey("Given an error with embedded stack trace from pkg/errors", t, func() {
		err := testCallStackFunc1()
		Convey("When stackTrace(err) is called", func() {
			st := stackTrace(err)
			So(len(st), ShouldEqual, 19)

			So(st[0].File, ShouldContainSubstring, "dp-kafka/error_test.go")
			So(st[0].Line, ShouldEqual, 176)
			So(st[0].Function, ShouldEqual, "testCallStackFunc3")

			So(st[1].File, ShouldContainSubstring, "dp-kafka/error_test.go")
			So(st[1].Line, ShouldEqual, 172)
			So(st[1].Function, ShouldEqual, "testCallStackFunc2")

			So(st[2].File, ShouldContainSubstring, "dp-kafka/error_test.go")
			So(st[2].Line, ShouldEqual, 168)
			So(st[2].Function, ShouldEqual, "testCallStackFunc1")
		})
	})
}

func testCallStackFunc1() error {
	return testCallStackFunc2()
}

func testCallStackFunc2() error {
	return testCallStackFunc3()
}

func testCallStackFunc3() error {
	cause := errors.New("I am the cause")
	return errors.Wrap(cause, "I am the context")
}
