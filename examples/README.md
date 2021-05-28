# dp-kafka usage examples

This folder contains some examples of typical usages of this library, as well as a docker-compose file to run a kafka cluster with 3 kafka nodes and a single zookeeper node. Source: [WurstMeister](https://github.com/wurstmeister/kafka-docker).

## Configuration

Common configuration for these examples uses the following environment variables:

Environment Variable | Default                  | Description
-------------------- | -------                  | ---
KAFKA_ADDR           | `localhost:9092,localhost:9093,localhost:9094` | comma-separated list of brokers
KAFKA_VERSION        | `1.0.2`                  | version of Kafka we will connect to
KAFKA_PRODUCED_TOPIC | `myTopic`                | topic used by the producer example
KAFKA_CONSUMED_TOPIC | `myTopic`                | topic consumed by the example consumers
KAFKA_CONSUMED_GROUP | `kafka-example-consumer` | consumer group name used by example consumers

### TLS examples

Currently, TLS is an option in the `producer` and `consumer-sequential` examples only.

## Run kafka cluster

You can run the default kafka cluster, with a single broker, that comes with dp-compose.

Or alternatively, you can run a 3-node kafka cluster by starting docker-compose with the provided compose file:

```sh
$ docker-compose --file ./docker-compose-kafka-cluster.yml up
```

The kafka brokers are accessible on `localhost`, ports `9092`, `9093` and `9094`

### Create topic with partitions

Once the cluster is running, you can create a topic with partitions, using either:

- the producer example in this repo, or
- the kafka-topics script that comes with kafka

#### Create a topic using the producer example

Use the `producer` example to create a topic.

##### Topic Config

Set the [config as above](#configuration) - including the produced topic.
Currently, you must change the number of partitions and replication factor in the source.

Use these additional env vars:

Env var | Desc
------- | ---
KAFKA_PRODUCED_TOPIC_CREATE      | set to true to trigger the producer to create the topic
KAFKA_PRODUCED_TOPIC_CREATE_ONLY | set to true to exit the producer after topic creation (i.e. do not begin producing)

##### Topic creation using producer

Use the `producer` to create a topic, by running, e.g.:

```sh
$ cd producer
$ KAFKA_PRODUCED_TOPIC_CREATE=1 KAFKA_PRODUCED_TOPIC_CREATE_ONLY=1 go run main.go
```

#### Create a topic using kafka-topics

Using the `kafka-topics` program that comes with kafka itself:

```sh
$ kafka-topics --create --topic topic-name --bootstrap-server localhost:9092 --partitions 60 --replication-factor 3
```

Please, replace `topic-name` with the name of the topic that you want to create, and feel free to use any broker (9092, 9093 or 9094) as bootstrap-server.

## Producer

The [producer example](producer/main.go) creates a kafka producer that listens to standard input, and sends the typed message when you hit enter.

You can run this example like so:

```sh
$ cd producer
$ go run main.go
```

See [above](#Create-a-topic-using-the-example-producer) for using the producer to create a topic.

## Consumer (sequential)

The [sequential consumer example](consumer-sequential/main.go) creates a kafka consumer that consumes messages one at a time.

It has a configurable sleep during message consumption, so that you can test scenarios with consumption delays.

You can run this example like so:

```sh
$ cd consumer-sequential
$ go run main.go
```

## Consumer (concurrent)

The [concurrent consumer example](consumer-concurrent/main.go) creates a kafka consumer that consumes messages concurrently, with 3 parallel workers.

It has a configurable sleep during message consumption, so that you can test scenarios with consumption delays.

You can run this example like so:

```sh
$ cd consumer-concurrent
$ go run main.go
```

## Consumer (batch)

The [batch consumer example](consumer-batch/main.go) creates a kafka consumer that adds messages to a batch and releases them. Then, once the batch is full, it processes all the messages, marks each one of them as consumed and commits the offsets at the end, by committing the last message.

It has a configurable sleep during message consumption, so that you can test scenarios with consumption delays.

You can run this example like so:

```sh
$ cd consumer-batch
$ go run main.go
```
