package kafkatest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/avro"
	"github.com/ONSdigital/dp-kafka/v4/mock"
)

type ConsumerConfig struct {
	NumPartitions     int  // number of partitions assigned to the consumer
	ChannelBufferSize int  // buffer size
	InitAtCreation    bool // Determines if the consumer is initialised or not when it's created, or it will be initialised later
}

var DefaultConsumerConfig = &ConsumerConfig{
	NumPartitions:     30,
	ChannelBufferSize: 30,
	InitAtCreation:    true,
}

// Consumer is an extension of the moq ConsumerGroup
// with implementation of required functions and Sarama mocks to emulate a fully functional Kafka ConsumerGroup
type Consumer struct {
	cfg                 *ConsumerConfig // Mock configuration
	mutex               *sync.Mutex
	wgSaramaConsumers   *sync.WaitGroup              // Waitgroup for sarama-handler consumer funcs during a session
	saramaErrors        chan error                   // Sarama level error channel
	saramaMessages      chan *sarama.ConsumerMessage // Sarama level consumed message channel
	saramaSessionID     int64                        // ID of the Sarama session (incremental)
	currentOffset       int64                        // last offset assigned to a message
	saramaCancelSession context.CancelFunc           // Context cancel func to end a Sarama session
	saramaMock          sarama.ConsumerGroup         // Internal sarama consumer group mock
	cg                  *kafka.ConsumerGroup         // Internal consumer group
	Mock                *IConsumerGroupMock          // Implements all moq functions so users can validate calls
}

// consumerGroupInitialiser returns the saramaMock, or an error if it is nil (i.e. Sarama not initialised yet)
func (cg *Consumer) consumerGroupInitialiser(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	if cg.saramaMock == nil {
		return nil, errors.New("sarama mock not initialised")
	}
	return cg.saramaMock, nil
}

// newSaramaSessionID increments the sessionID and returns the corresponding string
func (cg *Consumer) newSaramaSessionID() string {
	cg.saramaSessionID++
	return strconv.FormatInt(cg.saramaSessionID, 10)
}

// newOffset increments the offset and returns the new value
func (cg *Consumer) newOffset() int64 {
	cg.currentOffset++
	return cg.currentOffset
}

// createSaramaConsumeFunc returns a mocked Sarama 'Consume' function,
// based on the [following interface](https://github.com/IBM/sarama/blob/main/consumer_group.go#L19).
// This mock implements the following life-cycle, corresponding to the same steps described in the real 'Consume' func:
//  1. A new session is created, with a new ID, the provided topic
//     and the number of partitions according to cfg
//  2. Before processing starts, the handler's Setup() hook is called to notify the user
//     of the claims and allow any necessary preparation or alteration of state.
//  3. For each of the assigned claims the handler's ConsumeClaim() function is then called
//     in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
//     from concurrent reads/writes.
//  4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
//     parent context is canceled or when 'RebalanceCluster' is called, which emulates a server-side rebalance.
//  5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
//     to allow the user to perform any final tasks before a rebalance.
//  6. Finally, marked offsets are committed one last time before claims are released.
func (cg *Consumer) createSaramaConsumeFunc(topic string) func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		// 1. Create mocked sarama session
		var saramaSession sarama.ConsumerGroupSession
		saramaSession, cg.saramaCancelSession = NewSaramaConsumerGroupSessionMock(
			cg.newSaramaSessionID(),
			topic,
			cg.cfg.NumPartitions,
		)
		defer func() {
			cg.saramaCancelSession()     // cancel session, just in case it has not already been cancelled
			cg.saramaCancelSession = nil // after the session finishes, this func should not exist any more
		}()

		// define go-routine to execute 'ConsumeClaim'
		// (4) the first one to exit will cancel the session, causing the rest to exit
		consumeClaim := func(partition int) {
			defer func() {
				cg.saramaCancelSession()
				cg.wgSaramaConsumers.Done()
			}()
			claim := NewSaramaConsumerGroupClaimMock(cg.saramaMessages)
			cg.cg.SaramaCgHandler().ConsumeClaim(saramaSession, claim)
		}

		// 2. Before processing starts, call the handler's Setup() hook
		if err := cg.cg.SaramaCgHandler().Setup(saramaSession); err != nil {
			return fmt.Errorf("sarama session set-up failed: %w", err)
		}

		// 3. Start one go-routine per partition to run 'ConsumeClaim' in parallel
		cg.wgSaramaConsumers.Add(cg.cfg.NumPartitions)
		for partition := 1; partition <= cg.cfg.NumPartitions; partition++ {
			go consumeClaim(partition)
		}

		// 4. Wait until all consume claims have exited
		cg.wgSaramaConsumers.Wait() // TODO consider adding a timeout of Config.Consumer.Group.Rebalance.Timeout, as per sarama documentation

		// 5. Once all go-routines have exited, call the handler's Cleanup() hook
		if err := cg.cg.SaramaCgHandler().Cleanup(saramaSession); err != nil {
			return fmt.Errorf("sarama session clean-up failed: %w", err)
		}

		return nil
	}
}

// NewConsumer creates a testing consumer for testing.
// It behaves like a real consuemr-group, without network communication
func NewConsumer(ctx context.Context, cgConfig *kafka.ConsumerGroupConfig, cfg *ConsumerConfig) (*Consumer, error) {
	if cgConfig == nil {
		return nil, errors.New("kafka consumer config must be provided")
	}
	if cfg == nil {
		cfg = DefaultConsumerConfig
	}

	cg := &Consumer{
		cfg:               cfg,
		wgSaramaConsumers: &sync.WaitGroup{},
		mutex:             &sync.Mutex{},
		saramaErrors:      make(chan error, cfg.ChannelBufferSize),
		saramaMessages:    make(chan *sarama.ConsumerMessage, cfg.ChannelBufferSize),
		saramaSessionID:   0,
		currentOffset:     0,
	}

	saramaMock := &mock.SaramaConsumerGroupMock{
		CloseFunc: func() error {
			kafka.SafeCloseErr(cg.saramaErrors)
			return nil
		},
		ErrorsFunc: func() <-chan error {
			return cg.saramaErrors
		},
		ConsumeFunc: cg.createSaramaConsumeFunc(cgConfig.Topic),
	}

	// If sarama consumer group is initialised at creation, we need to set it before creating the consumer group
	if cfg.InitAtCreation {
		cg.saramaMock = saramaMock
	}

	// Create consumer group
	var err error
	cg.cg, err = kafka.NewConsumerGroupWithGenerators(
		ctx,
		cgConfig,
		cg.consumerGroupInitialiser,
		SaramaBrokerGenerator(cgConfig.Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group for testing: %w", err)
	}

	// Set saramaMock again, in case it was not set at creation time
	cg.saramaMock = saramaMock

	// Map all the mock funcs to the internal consumer group functions
	cg.Mock = &IConsumerGroupMock{
		ChannelsFunc:             cg.cg.Channels,
		CheckerFunc:              cg.cg.Checker,
		CloseFunc:                cg.cg.Close,
		InitialiseFunc:           cg.cg.Initialise,
		IsInitialisedFunc:        cg.cg.IsInitialised,
		LogErrorsFunc:            cg.cg.LogErrors,
		OnHealthUpdateFunc:       cg.cg.OnHealthUpdate,
		RegisterBatchHandlerFunc: cg.cg.RegisterBatchHandler,
		RegisterHandlerFunc:      cg.cg.RegisterHandler,
		StartFunc:                cg.cg.Start,
		StateFunc:                cg.cg.State,
		StateWaitFunc:            cg.cg.StateWait,
		StopFunc:                 cg.cg.Stop,
		StopAndWaitFunc:          cg.cg.StopAndWait,
	}

	return cg, nil
}

// QueueMessage will put the provided message to the testing consumption queue, so that it is consumed when the consumer is ready to do so.
// This emulates a message being received by a kafka broker, which is kept until a consumer consumes it.
func (cg *Consumer) QueueMessage(schema *avro.Schema, event interface{}) error {
	bytes, err := schema.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event with avro schema: %w", err)
	}

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	msgTime := time.Now()
	msg := &sarama.ConsumerMessage{
		Headers:        []*sarama.RecordHeader{},
		Timestamp:      msgTime,
		BlockTimestamp: msgTime,

		Value:  bytes,
		Topic:  cg.cg.Topic(),
		Offset: cg.newOffset(),
	}

	if err := kafka.SafeSendConsumerMessage(cg.saramaMessages, msg); err != nil {
		return fmt.Errorf("failed to send message to saramaMessages channel: %w", err)
	}
	return nil
}

// QueueJSON will marshal the provided event to JSON and queue it for consumption.
// This emulates a message being received by a Kafka broker, which is kept until a consumer consumes it.
func (cg *Consumer) QueueJSON(event interface{}) error {
	bytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event as JSON: %w", err)
	}
	return cg.QueueBytes(bytes)
}

// QueueBytes enqueues a pre-encoded payload encoded in bytes.
func (cg *Consumer) QueueBytes(b []byte) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	msgTime := time.Now()
	msg := &sarama.ConsumerMessage{
		Headers:        []*sarama.RecordHeader{},
		Timestamp:      msgTime,
		BlockTimestamp: msgTime,
		Value:          b,
		Topic:          cg.cg.Topic(),
		Offset:         cg.newOffset(),
	}

	if err := kafka.SafeSendConsumerMessage(cg.saramaMessages, msg); err != nil {
		return fmt.Errorf("failed to send message to saramaMessages channel: %w", err)
	}
	return nil
}

// RebalanceCluster emulates a serer-side rebalance, which will cancel any active session
func (cg *Consumer) RebalanceCluster(ctx context.Context) {
	if cg.saramaCancelSession == nil {
		return
	}
	cg.saramaCancelSession()
}
