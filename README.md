dp-kafka
=======

Kafka client wrapper using channels to abstract kafka consumers and producers.

## Life-cycle

### Creation

Kafka producers and consumers can be created with constructors that accept the required channels and configuration. You may create the channels using `CreateProducerChannels` and `CreateConsumerChannels` respectively.

Example: create a kafka producer
```
pChannels := kafka.CreateProducerChannels()
producer, err := kafka.NewProducer(
	ctx, cfg.Brokers, cfg.ProducedTopic, cfg.KafkaMaxBytes, pChannels)
```

Example: create a kafka consumer
```
cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaSync)
consumer, err := kafka.NewConsumerGroup(
	ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest, cfg.KafkaSync, cgChannels)
```

For consumers, is recommended to use `sync=true` - where, when you have read a message from `Incoming()`,
the listener for messages will block (and not read the next message from kafka)
until you signal that the message has been consumed (typically with `CommitAndRelease(msg)`).
Otherwise, if the application gets shutdown (e.g. interrupt signal), and has to be shutdown,
the consumer may not be shutdown in a timely manner (because it is blocked sending the read message to `Incoming()`).

please, note that if you do not provide the necessary channels, an `ErrNoChannel` error will be returned by the constructors, which must be considered fatal.

The constructor tires to initialise the producer/consumer by creating the underlying client. If the initialisation fails, a non-fatal error is returned; you can try to initialise it again later.

### Initialisation

A producer/consumer might have not been successfully initialised at creation time. If this is the case, you can always try to initialise it by calling `InitialiseSarama`. To validate the initialisation state, please call `IsInitialised`.

If a producer/consumer is not initialised, it cannot contact the kafka broker.

An uninitialised kafka producer cannot send messages, and any attempt to do so will result in an error being sent to the Errors channel.

An uninitialised kafka consumer group will not receive any message.

### Closing

Producers can be closed calling the `Close` method.

For graceful handling of Closing consumers, it is advised to use the `StopListeningToConsumer` method prior to the `Close` method. This will allow inflight messages to be completed and successfully call commit so that the message does not get replayed once the application restarts.

## Health-check

The health status of a consumer or producer can be obtained by calling `Checker` method, which returns a Check structure with the information:
```
check, err = cli.Checker(ctx)
```

- If a broker cannot be reached, a CRITICAL check is returned. 
- If all brokers can be reached, but a broker does not provide the expected topic metadata, a WARNING check is returned.
- If all brokers can be reached and return the expected topic metadata, we try to initialise the consumer/producer. If it was already initialised, or the initialisation is successful, an OK check is returned.

## Example

See the [example source file](cmd/kafka-example/main.go) for a typical usage.