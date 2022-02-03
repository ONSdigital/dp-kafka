package kafka

import "github.com/pkg/errors"

type dataLogger interface {
	LogData() map[string]interface{}
}
type stacktracer interface {
	StackTrace() errors.StackTrace
}
