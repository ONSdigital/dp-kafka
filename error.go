package kafka

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/ONSdigital/log.go/v2/log"
)

// Error is the handler package's error type. Is not meant to be compared as a
// a type, but information should be extracted via the interfaces
// it implements with callback functions. Is not guaranteed to remain exported
// so shouldn't be treated as such.
type Error struct {
	err     error
	logData map[string]interface{}
}

// NewError creates a new Error
func NewError(err error, logData map[string]interface{}) *Error {
	return &Error{
		err:     err,
		logData: logData,
	}
}

// Error implements the Go standard error interface
func (e *Error) Error() string {
	if e.err == nil {
		return "{nil error}"
	}
	return e.err.Error()
}

// LogData implements the DataLogger interface which allows you extract
// embedded log.Data from an error
func (e *Error) LogData() map[string]interface{} {
	return e.logData
}

// Unwrap returns the wrapped error
func (e *Error) Unwrap() error {
	return e.err
}

// UnwrapLogData recursively unwraps logData from an error
func UnwrapLogData(err error) log.Data {
	var data []log.Data

	for err != nil && errors.Unwrap(err) != nil {
		if lderr, ok := err.(dataLogger); ok {
			if d := lderr.LogData(); d != nil {
				data = append(data, d)
			}
		}

		err = errors.Unwrap(err)
	}

	// flatten []log.Data into single log.Data with slice
	// entries for duplicate keyed entries, but not for duplicate
	// key-value pairs
	logData := log.Data{}
	for _, d := range data {
		for k, v := range d {
			if val, ok := logData[k]; ok {
				if !reflect.DeepEqual(val, v) {
					if s, ok := val.([]interface{}); ok {
						s = append(s, v)
						logData[k] = s
					} else {
						logData[k] = []interface{}{val, v}
					}
				}
			} else {
				logData[k] = v
			}
		}
	}

	return logData
}

// stackTrace recursively unwraps the error looking for the deepest
// level at which the error was wrapped with a stack trace from
// github.com/pkg/errors (or conforms to the StackTracer interface)
// and returns the slice of stack frames. These are of type
// log.go/EventStackTrace so can be used directly with log.Go's
// available API to preserve the correct error logging format
func stackTrace(err error) []log.EventStackTrace {
	var serr stacktracer
	var resp []log.EventStackTrace

	for errors.Unwrap(err) != nil && errors.As(err, &serr) {
		st := serr.StackTrace()
		resp = make([]log.EventStackTrace, 0)
		for _, f := range st {
			line, _ := strconv.Atoi(fmt.Sprintf("%d", f))
			resp = append(resp, log.EventStackTrace{
				File:     fmt.Sprintf("%+s", f),
				Function: fmt.Sprintf("%n", f),
				Line:     line,
			})
		}

		err = errors.Unwrap(err)
	}

	return resp
}
