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

## Message

There is additionally a helper function for creating a [Message] mock which returns the given body via its GetData
function.

```go
message := kafkatest.NewMessage(data)
```
