# KafkaTest

This package contains mocks intended to be used by users of this library for testing.

## Empty mocks

If you require to implement your own mock functionality, you can use the empty mocks, which are created using `moq` to implement the interfaces `kafkatest.ConsumerGroup`, `kafkatest.Producer` and `Message`

These kind of mocks are recommended for unit-test, where you may only need to check that a particular function has been called with the expected parameters.

These interfaces expose the same methods as the real Producer and ConsumerGroup structs.
You can instantiate the mocks like so:

```go
consumer := kafkatest.IConsumerGroupMock{...}
```

```go
producer := kafkatest.IProducerMock{...}
```

```go
message := kafkatest.MessageMock{...}
```

## Functional mocks

The previous mocks have been extended by implementing functionality that emulates a real Producer, Consumer and message, but without communicating with any real Kafka broker.

These kind of mocks are recommended for component-test, where you may want to have a fully functional mock that behaves like the real library, but without the overhead of deploying a full kafka cluster.

If you require a functional mock to test how you interact with kafka, you can use these mocks (`kafkatest.MessageConsumer`, `kafaktest.MessageProducer` and `kafkatest.Message`) like so:

### Consumer

1- Create consumer mock

```go
kafkaConsumer, err := kafkatest.NewConsumer(
    ctx,
    &kafka.ConsumerGroupConfig{
        BrokerAddrs:       Addr,
        Topic:             ConsumedTopic,
        GroupName:         GroupName,
        MinBrokersHealthy: &ConsumerMinBrokersHealthy,
        KafkaVersion:      &Version,
    },
    &kafkatest.ConsumerConfig{
        NumPartitions:     10,
        ChannelBufferSize: 10,
		InitAtCreation:    false,
	},
)
```

Please, provide the kafka ConsumerGroupConfig as you would do for a real kafka consumer, and the required `kafkatest.ConsumerConfig` according to your needs for the mock.

This will create a new kafkatest consumer, with `NumPartitions` (e.g. 10) go-routines running the sarama handler, one emuling each kafka partition.

The sarama message and error channels will have `ChannelBufferSize` (e.g. 10)

And the consumer will successfully initialise at creation time if `InitAtCreation` is true. Otherwise, it will fail to initialise at creation time, but it will succeed shortly after when `Initialise()` is called.

If no `kafkatest.ConsumerConfig` is provided, the default values will be used


2- Use the mock:

You can provide the `Mock` inside the `kafkatest.Consumer` to your service under test. For example, you may override a service kafka getter function like so:

```go
service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka) (kafka.IConsumerGroup, error) {
    kafkaConsumer, err := kafkatest.NewConsumer(...)
    return kafkaConsumer.Mock
}
```

3- Queue messages to the mock

Usually, when you use this consumer for testing, you want to queue kafka events, so that they are consumed by the service under test that is using the kafka consumer.

To queue a new messages to be consumed by the mock, you can call `QueueMessage` with the schema and event that you want to be queued for consumption, like so:

```go
// create event that will be queued (matches schema.MySchema)
event := &models.MyEvent{
    Field1: "value one",
    FieldN: "value N"
}

// queue the event with the corresponding schema
if err := kafkaConsumer.QueueMessage(schema.MySchema, event); err != nil {
	return fmt.Errorf("failed to queue event: %w", err)
}
```

### Producer

1- Create producer mock

```go
kafkaProducer, err := kafkatest.NewProducer(
    ctx,
    &kafka.ProducerConfig{
        BrokerAddrs:       Addr,
        Topic:             ProducerTopic,
        MinBrokersHealthy: &ProducerMinBrokersHealthy,
        KafkaVersion:      &Version,
    },
    &kafkatest.ProducerConfig{
        ChannelBufferSize: 10,
		InitAtCreation:    false,
	},
)
```

Please, provide the kafka ProducerConfig as you would do for a real kafka producer, and the required `kafkatest.ProducerConfig` according to your needs for the mock.

The sarama message and error channels will have `ChannelBufferSize` (e.g. 10)

And the producer will successfully initialise at creation time if `InitAtCreation` is true. Otherwise, it will fail to initialise at creation time, but it will succeed shortly after when `Initialise()` is called.

If no `kafkatest.ProducerConfig` is provided, the default values will be used

2- Use the mock:

You can provide the `Mock` inside the `kafkatest.Producer` to your service under test. For example, you may override a service kafka getter function like so:

```go
service.GetKafkaProducer = func(ctx context.Context, cfg *config.Kafka) (kafka.IProducer, error) {
    kafkaProducer, err := kafkatest.NewProducer(...)
    return kafkaProducer.Mock
}
```

3- Wait for message to be sent

Usually, when you use this consumer for testing, you want to check that a message is sent, so that it can be validated.

To expect a message to be sent through the mock, you can call `WaitForMessageSent` with the expected schema and an event pointer. The function will block until a message is sent or the provided timeout expires. If the event is sent, it will be unmarshaled to the provided pointer.

```go
// create empty event pointer of the type you expect (matches schema.MySchema)
var e = &models.MyEvent{}

// wait for an event to be sent, with the corresponding schema
if err := kafkaProducer.WaitForMessageSent(schema.MySchema, e, timeout); err != nil {
	return fmt.Errorf("failed to expect sent message: %w", err)
}
```

### Message

```go
message := kafkatest.NewMessage(data, offset)
```
