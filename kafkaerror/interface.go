package kafkaerror

type dataLogger interface {
	LogData() map[string]interface{}
}
