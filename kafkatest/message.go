package kafkatest

// NewMessage is a helper function that returns a [MessageMock] with the supplied body returned by the mocked `GetData`
// function.
func NewMessage(body []byte) *MessageMock {
	return &MessageMock{
		GetDataFunc: func() []byte {
			return body
		},
	}
}
