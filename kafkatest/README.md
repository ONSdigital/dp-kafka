# KafkaTest

This package contains mocks intended to be used by users of this library for testing.

## Empty mocks

If you require to implement your own mock functionality, you can use the empty mocks, which are created using `moq` to implement the interfaces `kafkatest.ConsumerGroup`, `kafkatest.Producer` and `Message`

These kind of mocks are recommended for unit-test, where you may only need to check that a particular function has been called with the expected parameters.

These interfaces expose the same methods as the real Producer and ConsumerGroup structs.
You can instantiate the mocks like so:
```
consumer := kafkatest.ConsumerGroupMock{...}
```
```
producer := kafkatest.ProducerMock{...}
```
```
message := kafkatest.MessageMock{...}
```

## Functional mocks

The previous mocks have been extended by implementing functionality that emulates a real Producer, Consumer and message, but without communicating with any real Kafka broker.

These kind of mocks are recommended for component-test, where you may want to have a fully functional mock that behaves like the real library, but without the overhead of deploying a full kafka cluster.

If you require a functional mock to test how you interact with kafka, you can use these mocks (`kafkatest.MessageConsumer`, `kafaktest.MessageProducer` and `kafkatest.Message`) like so:

```
consumer := kafkatest.NewMessageConsumer(true)
```

```
producer := kafkatest.NewMessageProducer(true)
```

```
message := kafkatest.NewMessage(data, offset)
```

// TODO document a bit more go-routines, etc.