package kafkatest

type Options struct {
	Headers map[string]string
}

type Headers map[string]string

func OptionalHeaders(h Headers) func(o *Options) error {
	return func(o *Options) error {
		o.Headers = h
		return nil
	}
}
