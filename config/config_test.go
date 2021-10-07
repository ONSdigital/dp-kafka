package config

import (
	"time"

	"github.com/ONSdigital/dp-kafka/v3/global"
)

var (
	testMaxMessageBytes          = 1234
	testKeepAlive                = 3 * time.Second
	testRetryMax                 = 10
	testRetryBackoff             = 5 * time.Second
	testProducerRetryBackoffFunc = func(retries, maxRetries int) time.Duration { return time.Second }
	testConsumerRetryBackoffFunc = func(retries int) time.Duration { return time.Second }
	testKafkaVersion             = "1.0.2"
	testOffsetNewest             = global.OffsetNewest
	testTopic                    = "someTopic"
	testGroupName                = "someGroupName"
	testBrokerAddrs              = []string{"kafka:9092", "kafka:9093", "kafka:9094"}
)
