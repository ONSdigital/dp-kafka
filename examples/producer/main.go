package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/ONSdigital/dp-kafka/v2/config"
	"github.com/ONSdigital/dp-kafka/v2/global"
	"github.com/ONSdigital/dp-kafka/v2/producer"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-producer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	ProducedTopic           string        `envconfig:"KAFKA_PRODUCED_TOPIC"`
	WaitForProducerReady    bool          `envconfig:"KAFKA_WAIT_PRODUCER_READY"`
	KafkaSecProtocol        string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts         string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert      string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey       string        `envconfig:"KAFKA_SEC_CLIENT_KEY" json:"-"`
	KafkaSecSkipVerify      bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Chomp                   bool          `envconfig:"CHOMP_MSG"`
}

// period of time between tickers
const ticker = 3 * time.Second

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Fatal(ctx, "fatal runtime error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		sig := <-signals
		log.Warn(ctx, "os signal received", log.Data{"signal": sig})
		cancel()
	}()

	// Read Config
	cfg := &Config{
		Brokers:                 []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "1.0.2",
		ProducedTopic:           "myTopic",
		WaitForProducerReady:    true,
		GracefulShutdownTimeout: 5 * time.Second,
		Chomp:                   false,
	}
	if err := envconfig.Process("", cfg); err != nil {
		return err
	}

	// run kafka Producer
	producer, err := runProducer(ctx, cancel, cfg)
	if err != nil {
		return err
	}

	// blocks until context is done
	<-ctx.Done()
	return closeProducer(ctx, producer, cfg.GracefulShutdownTimeout)
}

func getProducerConfig(cfg *Config) *config.ProducerConfig {
	pCfg := &config.ProducerConfig{
		MaxMessageBytes: &cfg.KafkaMaxBytes,
		KafkaVersion:    &cfg.KafkaVersion,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		pCfg.SecurityConfig = config.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}
	return pCfg
}

func runProducer(ctx context.Context, cancel context.CancelFunc, cfg *Config) (*producer.Producer, error) {
	stdinChannel := make(chan string)

	log.Info(ctx, "[KAFKA-TEST] Starting Producer (stdin sent to producer)", log.Data{"config": cfg})
	global.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create Producer with channels and config
	pChannels := producer.CreateProducerChannels()
	pConfig := getProducerConfig(cfg)
	producer, err := producer.NewProducer(ctx, cfg.Brokers, cfg.ProducedTopic, pChannels, pConfig)
	if err != nil {
		return nil, err
	}

	// go-routine to log errors from error channel
	pChannels.LogErrors(ctx, "[KAFKA-TEST] Producer error")

	// Producer not initialised at creation time. It will retry to initialise later, we may want to block until it is initialised
	if !producer.IsInitialised() {
		if cfg.WaitForProducerReady {
			log.Warn(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Waiting until we can initialise it.")
			waitForInitialised(ctx, pChannels)
		} else {
			log.Warn(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Will be initialised later.")
		}
	}

	// Create loop-control channel and context
	eventLoopContext, eventLoopCancel := context.WithCancel(ctx)
	eventLoopDone := make(chan bool)

	// stdin reader loop
	go func(ch chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if cfg.Chomp {
				line = line[:len(line)-1]
			}
			ch <- line
		}
		eventLoopCancel()
		<-eventLoopDone
		close(ch)
	}(stdinChannel)

	// eventLoop
	go func() {
		defer cancel()
		defer close(eventLoopDone)
		for {
			select {

			case <-time.After(ticker):
				log.Info(ctx, "[KAFKA-TEST] tick")

			case <-eventLoopContext.Done():
				log.Info(ctx, "[KAFKA-TEST] Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return

			case stdinLine := <-stdinChannel:
				// Used for this example to write messages to kafka consumer topic (should not be needed in applications)
				pChannels.Output <- []byte(stdinLine)
				log.Info(ctx, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()
	return producer, nil
}

func closeProducer(ctx context.Context, producer *producer.Producer, gracefulShutdownTimeout time.Duration) error {
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout})
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutown gracefully closes up
	var shutdownErr error = nil

	// background graceful shutdown
	go func() {
		defer cancel()
		log.Info(ctx, "[KAFKA-TEST] Closing kafka producer")
		shutdownErr = producer.Close(ctx)
		log.Info(ctx, "[KAFKA-TEST] Closed kafka producer")
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		log.Warn(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.FormatErrors([]error{ctx.Err()}))
		return ctx.Err()
	}

	if shutdownErr != nil {
		err := fmt.Errorf("error while closing kafka producer: %w", shutdownErr)
		log.Error(ctx, "failed to shutdown gracefully ", err)
		return err
	}

	log.Info(ctx, "graceful shutdown was successful")
	return nil
}

// waitForInitialised blocks until the producer is initialised or closed
func waitForInitialised(ctx context.Context, pChannels *producer.ProducerChannels) {
	select {
	case <-pChannels.Ready:
		log.Warn(ctx, "[KAFKA-TEST] Producer is now initialised.")
	case <-pChannels.Closer:
		log.Warn(ctx, "[KAFKA-TEST] Producer is being closed.")
	case <-ctx.Done():
		log.Warn(ctx, "[KAFKA-TEST] Producer context done.")
	}
}
