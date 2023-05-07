package main

import (
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/itzg/go-flagsfiller"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"os"
	"time"
)

// FlushTimeout is the amount of time to wait for queued messages to be sent
const FlushTimeout = 15 * time.Millisecond

// ConsumerGroupId is the string that identifies this consumer
const ConsumerGroupId = "kafka-availability-tester"

// StatsPeriod is the time between when statistics are logged
const StatsPeriod = 10 * time.Second

type Config struct {
	BootstrapServer      string        `usage:"the server to initially connect to"`
	Topic                string        `usage:"the topic to produce and consume to and from" default:"test"`
	SendPeriod           time.Duration `usage:"is the time between messages are produced to the topic" default:"100ms"`
	Authentication       string        `usage:"whether authentication should be enabled. Either none or oauthbearer" default:"none"`
	CACertPath           string        `usage:"path to the ca-cert used to validate tls certs the kafka brokers present"`
	AzureTokenConfigPath string        `usage:"path to the toml file with Azure AD configuration"`
}

type TokenFetcher interface {
	Fetch() (*kafka.OAuthBearerToken, error)
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	config, err := parseFlags()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(-1)
	}
	var fetcher TokenFetcher
	if config.AzureTokenConfigPath != "" {
		fetcher, err = NewAzureTokenFetcherFromConfig(config.AzureTokenConfigPath)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(-2)
		}
	}
	err = run(config, fetcher)
	if err != nil {
		log.Errorf("%v", err)
		os.Exit(-3)
	}
}

func parseFlags() (*Config, error) {
	var config Config
	filler := flagsfiller.New()
	err := filler.Fill(flag.CommandLine, &config)
	if err != nil {
		return nil, err
	}

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	if config.BootstrapServer == "" {
		return nil, fmt.Errorf("required flag --bootstrap-server is missing")
	}

	switch config.Authentication {
	case "none":
		break
	case "oauthbearer":
		if config.CACertPath == "" {
			return nil, fmt.Errorf("flag --ca-cert-path is required if --authentication=oauthbearer")
		}
		if config.AzureTokenConfigPath == "" {
			return nil, fmt.Errorf("--azure-token-config-path must be set when authentication is 'oauthbearer'")
		}
	default:
		return nil, fmt.Errorf("unknown authentication mode '%s', valid modes are 'oauthbearer' and 'none'", config.Authentication)
	}

	return &config, nil
}

func run(config *Config, fetcher TokenFetcher) error {
	var toStop []Stoppable
	installStoppingSignalHandler(&toStop, os.Interrupt)

	log.Infof("Connecting to boostrap address '%s'", config.BootstrapServer)
	configMap := buildConfigMap(config)

	statsEventSink := StartNewStatSink()
	watcher := NewStateWatcher(StatsEventSink{statsEventSink})

	producer, err := NewProducer(&configMap, config.Topic, watcher, fetcher)
	if err != nil {
		return fmt.Errorf("failed to create producer %w", err)
	}
	toStop = append(toStop, producer)

	configMap["group.id"] = ConsumerGroupId

	consumer, err := NewConsumer(&configMap, watcher, fetcher)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	toStop = append(toStop, consumer)

	err = consumer.subscribeAndConsume(config.Topic)
	if err != nil {
		log.Panicf("Failed to subscribeAndConsume: %v", err)
	}

	log.Infof("Producing a message every %s", config.SendPeriod)
	producer.run(config.SendPeriod)

	startPeriodicStatLogger(statsEventSink)

	waiter := makeWaiter()
	toStop = append(toStop, waiter)
	waiter.waitUntilStopped()

	return nil
}

func buildConfigMap(config *Config) kafka.ConfigMap {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServer,
	}
	if config.Authentication == "oauthbearer" {
		configMap["security.protocol"] = "SASL_SSL"
		configMap["ssl.ca.location"] = config.CACertPath
		configMap["sasl.mechanism"] = "OAUTHBEARER"
	}
	return configMap
}

func startPeriodicStatLogger(sink *StatSink) {
	ticker := time.NewTicker(StatsPeriod)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Infof("%s", sink.MakeStats())
			}
		}
	}()
}
