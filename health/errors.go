package health

import "fmt"

// ErrBrokersNotReachable is an Error type for 'Broker Not reachable' with a list of unreachable addresses
type ErrBrokersNotReachable struct {
	Addrs []string
}

// Error returns the error message with a list of unreachable addresses
func (e *ErrBrokersNotReachable) Error() string {
	return fmt.Sprintf("broker(s) not reachable at addresses: %v", e.Addrs)
}

// ErrInvalidBrokers is an Error type for 'Invalid topic info' with a list of invalid broker addresses
type ErrInvalidBrokers struct {
	Addrs []string
}

// Error returns the error message with a list of broker addresses that returned unexpected responses
func (e *ErrInvalidBrokers) Error() string {
	return fmt.Sprintf("unexpected metadata response for broker(s). Invalid brokers: %v", e.Addrs)
}
