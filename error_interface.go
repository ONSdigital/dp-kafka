package kafka

type dataLogger interface {
	LogData() map[string]interface{}
}
