# KafkaTest

This package contains mocks intended to be used by users of this library for testing.

## Empty mocks

If you require to implement your own mock functionality, you can use the empty mocks, which are created using `moq` to implement the interfaces `kafkatest.ConsumerGroup`, `kafkatest.Producer` and `Message`

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

The previous mocks have been extended by implementing functionality that emulates a real Producer, Consumer and message; but without communicating with any real Kafka broker. If you require a functional mock to test how you interact with kafka, you can use these mocks (`kafkatest.MessageConsumer`, `kafaktest.MessageProducer` and `kafkatest.Message`) like so:

```
consumer := kafkatest.NewMessageConsumer(true)
```

```
producer := kafkatest.NewMessageProducer(true)
```

```
message := kafkatest.NewMessage(data, offset)
```
