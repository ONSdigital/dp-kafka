# Kafka-Test

This package contains mocks intended to be used by users of this library for testing.

## Empty mocks

If you require to implement your own mock functionality, you can use the empty mocks, which are created using `moq` to implement the interfaces `kafkatest.ConsumerGroup` and `kafkatest.Producer`.

There interfaces expose the same methods as the real Producer and ConsumerGroup structs.
You can instantiate the mocks like so:
```
consumer := kafkatest.ConsumerGroupMock{...}
```
```
producer := kafkatest.ProducerMock{...}
```

## Functional mocks

The previous mocks have been extended by implementing functionality that emulates a real Producer and Consumer, but without communicating with any real Kafka broker. If you require a functional mock to test how you interact with kafka, you can use these mocks (`kafkatest.MessageConsumer` and `kafaktest.MessageProducer`) like so:

```
consumer := kafkatest.MessageConsumer{...}
```
```
producer := kafkatest.MessageProducer{...}
```

These functional mocks use `kafkatest.Message`, which implements the Sarama Message interface (also implemented by real messages handled by Sarama).
