# dp-kafka

Kafka client wrapper using channels to abstract kafka consumers and producers. This library is built on top of [Sarama](https://github.com/Shopify/sarama)

## Configuration

By default, the library assumes plaintext connections,
unless the configuration argument has a non-nil `SecurityConfig` field.

## Life-cycle

### Setup app to use AWS MSK

> Amazon Managed Streaming for Apache Kafka (Amazon MSK) is a fully managed service that enables you to build and run applications that use Apache Kafka to process streaming data.
  
As of 2021, our apps have migrated from running our own Kafka to using Amazon MSK as it provides the control-plane operations, such as those for creating, updating, and deleting clusters and lets you use Apache Kafka data-plane operations, such as those for producing and consuming data.

To use AWS MSK, please do the following:

#### 1. Add kafka topics to manifest

First of all, we need to update the manifest of the app to contain the kafka topics which the app uses. This is done as follows:

1. Create feature branch for the upcoming changes to the manifest of the app in [`dp-configs`][dp-configs]
2. In the [manifest of the app][dp-configs-manifests], add the revelant kafka topics at the end of the file as follows

  ```go
  kafka:
    topics:
      - name: `topic name 1` (e.g. `content-published`)
        subnets: [ `web` or `publishing` or both `web, publishing` ]
        access: [ `read` or `write` ]
      - name: `topic name 2` 
        subnets: [ `web` or `publishing` or both `web, publishing` ]
        access: [ `read` or `write` ]
  ```

   | Field           | Explanation
   | --------------- | -------------
   | `name`          | The name of the kafka topic which the app uses
   | `subnets`       | The subnet where the kafka topic is being used. Either, it is used in `web`, `publishing` or both `web and publishing`
   | `access`        | Determines whether the app uses the kafka topic to produce messages or consume messages. If the app produces messages to the topic, then the access is `write` and if the app consumes messages from the topic, it has `read` access

   **An example of adding kafka topics to the manifest can be found [here][manifest-example]**
3. `Review` and `merge` these changes to continue with the [next step](#create-client-certificate-for-the-app---run-key-admin-script)

#### 2. Create client certificate for the app - Run `key-admin` script

Next, we need to create a `client certificate` for the app so that it can connect and authenticate to `AWS MSK` using `TLS` and its client certificate. This can be achieved by running the `key-admin`.
  
##### Notes

1. The `key-admin` script checks the manifests of the apps in [`dp-configs`][dp-configs] to see whether a client certificate needs to be created for the app by checking whether any kafka topics are mentioned in the manifest. Therefore, please make sure that your local machine is on the **`master`** branch for [`dp-configs`][dp-configs] which contains all your changes to the [previous step](#add-kafka-topics-to-manifest).
2. Remember to do `Step 4` of the [Client Certificate README][client-cert-readme] to inject the relevant certificate details into the app's secrets using the `--secrets` argument unless if an existing app is being [migrated to AWS MSK](#migration-to-aws-msk) in which this step is done later
3. Please remember to come back to this README after completing this task to continue with the process.
  
**Follow the steps explained in the Client Certificate README - <https://github.com/ONSdigital/dp-setup/tree/develop/csr/private> to run `key-admin`**

#### 3. Apply kafka topics to AWS MKS - Run `topic-manager` script

Next, we need to apply the kafka topics used by the app to AWS MSK. This can be achieved by running the `topic-manager` script. Running the script informs AWS MSK of:

- any new topics (topic manager creates them)
- new clients/apps (i.e. certs) using the service:
  - topic manager authorises the clients/apps (i.e. certs) to gain the right access (read and/or write) to its topics
  - renewed certs do not need a re-run of topic manager

##### Notes

1. The `topic-manager` script checks the manifests of the apps in [`dp-configs`][dp-configs] to see if any changes to kafka topics (adding or deleting topics) needs to be applied to AWS MSK by checking the kafka topics mentioned in the manifest. Therefore, please make sure that your local machine is on the **`master`** branch for [`dp-configs`][dp-configs] which contains all your changes to the [previous step](#add-kafka-topics-to-manifest).
2. Please remember to come back to this README after completing this task to continue with the process.
  
**Follow the steps explained in the Kafka Setup Tools README -  <https://github.com/ONSdigital/dp-setup/tree/develop/scripts/kafka> to run `topic-manager`**

#### 4. Add configs in the app to use AWS MSK

Once all the basic components has been setup for the app to use AWS MSK from the previous steps, the app itself needs to be updated to use the AWS MSK configurations. To achieve this, please do the following:

1. Update the app `README.md` to include kafka configurations to connect to AWS MSK using TLS

   ```go
   | Environment variable         | Default                                   | Description
   | ---------------------------- | ----------------------------------------- | -----------
   | KAFKA_ADDR                   | localhost:9092                            | The kafka broker addresses (can be comma separated)
   | KAFKA_VERSION                | "1.0.2"                                   | The kafka version that this service expects to connect to
   | KAFKA_SEC_PROTO              | _unset_                                   | if set to `TLS`, kafka connections will use TLS [[1]](#notes_1)
   | KAFKA_SEC_CA_CERTS           | _unset_                                   | CA cert chain for the server cert [[1]](#notes_1)
   | KAFKA_SEC_CLIENT_KEY         | _unset_                                   | PEM for the client key [[1]](#notes_1)
   | KAFKA_SEC_CLIENT_CERT        | _unset_                                   | PEM for the client certificate [[1]](#notes_1)
   | KAFKA_SEC_SKIP_VERIFY        | false                                     | ignores server certificate issues if `true` [[1]](#notes_1)
   ```

   ```go
   **Notes:**

       1. <a name="notes_1">For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)</a>
   ```

2. Add the configurations to `config.go`

   ```go
   // KafkaTLSProtocolFlag informs service to use TLS protocol for kafka
   const KafkaTLSProtocolFlag = "TLS"
   ```

   ```go
   type Config struct {
        // TO-REMOVE: this struct already contains other configs in the app but update this struct accordingly with the following organised layout
        KafkaConfig                KafkaConfig
    }

    // KafkaConfig contains the config required to connect to Kafka
    type KafkaConfig struct {
        BindAddr                 []string `envconfig:"KAFKA_ADDR"                            json:"-"`
        Version                  string   `envconfig:"KAFKA_VERSION"`
        SecProtocol              string   `envconfig:"KAFKA_SEC_PROTO"`
        SecCACerts               string   `envconfig:"KAFKA_SEC_CA_CERTS"`
        SecClientKey             string   `envconfig:"KAFKA_SEC_CLIENT_KEY"                  json:"-"`
        SecClientCert            string   `envconfig:"KAFKA_SEC_CLIENT_CERT"`
        SecSkipVerify            bool     `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
    }
   ```

   ```go
   // TO-REMOVE: set the default values of the kafka configs in the Get() as the following
   KafkaConfig: KafkaConfig{
            BindAddr:                 []string{"localhost:9092"},
            Version:                  "1.0.2",
            SecProtocol:              "",
            SecCACerts:               "",
            SecClientCert:            "",
            SecClientKey:             "",
            SecSkipVerify:            false,
    },
   ```

3. Update the test to check config default values in `config_test.go` accordingly

   ```go
   So(cfg.KafkaConfig.BindAddr[0], ShouldEqual, "localhost:9092")
   So(cfg.KafkaConfig.Version, ShouldEqual, "1.0.2")
   So(cfg.KafkaConfig.SecProtocol, ShouldEqual, "")
   So(cfg.KafkaConfig.SecCACerts, ShouldEqual, "")
   So(cfg.KafkaConfig.SecClientCert, ShouldEqual, "")
   So(cfg.KafkaConfig.SecClientKey, ShouldEqual, "")
   So(cfg.KafkaConfig.SecSkipVerify, ShouldEqual, false)
   ```
  
   **An example of adding kafka configs to the app can be found [here][app-kafka-config-example]**
  
4. Continue to the next step in the [Creation of Kafka Producer and/or Consumer](#creation)

### Creation

Kafka producers and consumers can be created with constructors that accept the required channels and configuration. You may create the channels using `CreateProducerChannels` and `CreateConsumerChannels` respectively:

```go
    // Create Producer with channels and config
    pChannels := kafka.CreateProducerChannels()
    pConfig := &kafka.ProducerConfig{
        KafkaVersion:    &kafkaConfig.Version,
        MaxMessageBytes: &kafkaConfig.MaxBytes,
    }
    if kafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
        pConfig.SecurityConfig = kafka.GetSecurityConfig(
            kafkaConfig.SecCACerts,
            kafkaConfig.SecClientCert,
            kafkaConfig.SecClientKey,
            kafkaConfig.SecSkipVerify,
        )
    }
    producer, err := kafka.NewProducer(ctx, kafkaConfig.BindAddr, kafkaConfig.ProducedTopic, pChannels, pConfig)
```

```go
    // Create ConsumerGroup with channels and config
    cgChannels := kafka.CreateConsumerGroupChannels(kafkaConfig.ParallelMessages)
    cgConfig := &kafka.ConsumerGroupConfig{KafkaVersion: &kafkaConfig.Version}
    if kafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
        cgConfig.SecurityConfig = kafka.GetSecurityConfig(
            kafkaConfig.SecCACerts,
            kafkaConfig.SecClientCert,
            kafkaConfig.SecClientKey,
            kafkaConfig.SecSkipVerify,
        )
    }
    cg, err := kafka.NewConsumerGroup(ctx, kafkaConfig.BindAddr, kafkaConfig.ConsumedTopic, kafkaConfig.ConsumedGroup, cgChannels, cgConfig)
```

For consumers, you can specify the batch size that determines the number of messages to be stored in the Upstream channel. It is recommended to provide a batch size equal to the number of parallel messages that are consumed.

You can provide an optional config parameter to the constructor (`ProducerConfig` and `ConsumerGroupConfig`). Any provided configuration will overwrite the default sarama config, or you can pass a nil value to use the default sarama config.

The constructor tries to initialise the producer/consumer by creating the underlying Sarama client, but failing to initialise it is not considered a fatal error, hence the constructor will not error.

please, note that if you do not provide the necessary channels, an `ErrNoChannel` error will be returned by the constructors, which must be considered fatal.

### Initialisation

If the producer/consumer can establish a connection with the Kafka cluster, it will be initialised at creation time, which is usually the case. But it might not be able to do so, for example if the kafka cluster is not running. If a producer/consumer is not initialised, it cannot contact the kafka broker, and it cannot send or receive any message. Any attempt to send a message in this state will result in an error being sent to the Errors channel.

An uninitialised producer/consumer will try to initialise later, asynchronously, in a retry loop following an exponential backoff strategy. You may also try to initialise it calling `Initialise()`. In any case, when the initialisation succeeds, the initialisation loop will exit, and it will start producing/consuming.

You can check if a producer/consumer is initialised by calling `IsInitialised()` or wait for it to be initialised by waiting for the Ready channel to be closed, like so:

```go
    // wait in a parallel go-routine
    go func() {
        <-channels.Ready
        doStuff()
    }()
```

```go
    // block until kafka is initialised
    <-channels.Ready
    doStuff()
```

Waiting for this channel is a convenient hook, but not a necessary requirement.

### Message production

Messages are sent to Kafka by sending them to a producer Output channel, as byte arrays:

```go
    // send message
    pChannels.Output <- []byte(msg)
```

### Message consumption

Messages can be consumed by creating an infinite consumption loop. Once a message has finished being processed, you need to call `Commit()`, so that Sarama releases the go-routine consuming a message and Kafka knows that the message does not need to be delivered again (marks the message, and commits the offset):

```go
// consumer loop
func consume(upstream chan kafka.Message) {
    for {
        msg := <-upstream
        doStuff(msg)
        msg.Commit()
    }
}
```

You may create a single go-routine to consume messages sequentially, or multiple parallel go-routines (workers) to consume them concurrently:

```go
    // single consume go-routine
    go consume(channels.Upstream)
```

```go
    // multiple workers to consume messages in parallel
    for w := 1; w <= kafkaConfig.ParallelMessages; w++ {
        go consume(channels.Upstream)
    }
```

You can consume up to as may messages in parallel as partitions are assigned to your consumer, more info in the deep dive section.

#### Message consumption deep dive

Sarama creates as many go-routines as partitions are assigned to the consumer, for the topic being consumed.

For example, if we have a topic with 60 partitions and we have 2 instances of a service that consumes that topic running at the same time, kafka will assign 30 partitions to each one.

Then Sarama will create 30 parallel go-routines, which this library uses in order to send messages to the upstream channel. Each go-routine waits for the message to finish being processed by waiting for the message-specific `upstreamDone` channel to be closed, like so:

```go
    channels.Upstream <- msg
    <-msg.upstreamDone
```

Each Sarama consumption go routine exists only during a particular session. Sessions are periodically destroyed and created by Sarama, according to Kafka events like a cluster re-balance (where the number of partitions assigned to a consumer may change). It is important that messages are released as soon as possible when this happens. The default message consumption timeout is 10 seconds in this scenario (determined by `config.Consumer.Group.Session.Timeout`).

When a session finishes, we call Consume() again, which tries to establish a new session. If an error occurs trying to establish a new session, it will be retried following an exponential backoff strategy.

### Closing

Producers can be closed by calling the `Close` method.

For graceful handling of Closing consumers, it is advised to use the `StopListeningToConsumer` method prior to the `Close` method. This will allow inflight messages to be completed and successfully call commit so that the message does not get replayed once the application restarts.

The `Closer` channel is used to signal to all the loops that they need to exit because the consumer is being closed.

After successfully closing a producer or consumer, the corresponding `Closed` channel is closed.

## Health-check

The health status of a consumer or producer can be obtained by calling `Checker` method, which updates the provided CheckState structure with the relevant information:

```go
check, err = cli.Checker(ctx)
```

- If a broker cannot be reached, the Status is set to CRITICAL.
- If all brokers can be reached, but a broker does not provide the expected topic metadata, the Status is set to WARNING.
- If all brokers can be reached and return the expected topic metadata, we try to initialise the consumer/producer. If it was already initialised, or the initialisation is successful, the Status is set to OK.

## Examples

See the [examples](examples/README.md) below for some typical usages of this library.

- [Producer example](examples/producer/main.go) [TLS enabled]
- [Sequential consumer example](examples/consumer-sequential/main.go) [TLS enabled]
- [Concurrent consumer example](examples/consumer-concurrent/main.go)

## Testing

Some mocks are provided, so that you can test your code interactions with this library. [More details here.](kafkatest/README.md)

## Migration to AWS MSK

1. [Add kafka topics to manifest](#1-add-kafka-topics-to-manifest)

2. [Create client certificate for the app - Run `key-admin` script](#2-create-client-certificate-for-the-app---run-key-admin-script)
   - **Do not merge the amended secrets after running the script. This should be done at STEP 7**
  
3. [Apply kafka topics to AWS MKS - Run `topic-manager` script](#3-apply-kafka-topics-to-aws-mks---run-topic-manager-script)

4. [Add configs in the app to use AWS MSK](#4-add-configs-in-the-app-to-use-aws-msk)

5. [Update creating kafka producer and/or consumer with initialising `SecurityConfig`](#creation)

6. Deploy app
   - The app will still be using old kafka but it is ready to use AWS MSK. This is because TLS is not enabled by default
  
7. Deploy amended `secrets` from STEP 2 with configs to enable TLS connection

8. Redeploy app to pick up the amended secrets and to use AWS MSK

[//]: # (Reference Links and Images)
   [app-kafka-config-example]: <https://github.com/ONSdigital/dp-dimension-extractor/pull/84/files>
   [client-cert-readme]: <https://github.com/ONSdigital/dp-setup/tree/develop/csr/private>
   [dp-configs]: <https://github.com/ONSdigital/dp-configs>
   [dp-configs-manifests]: <https://github.com/ONSdigital/dp-configs/tree/master/manifests>
   [dp-setup]: <git@github.com:ONSdigital/dp-setup.git>
   [manifest-example]: <https://github.com/ONSdigital/dp-configs/blob/master/manifests/dp-import-api.yml>
