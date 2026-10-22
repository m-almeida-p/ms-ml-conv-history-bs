package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/yaperos/lib-go-kafkalizer/v3/pkg/encoding"
	confluent "github.com/yaperos/lib-go-kafkalizer/v3/pkg/pubsub/confluent-adapter"
	"github.com/yaperos/lib-go-kafkalizer/v3/pkg/pubsub/core"
	libkafka "github.com/yaperos/lib-go-kafkalizer/v3/pkg/pubsub/kafka"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
)

const clientID = "ms-ml-conv-history-bs"

var _ resource.RecordSource = (*Source)(nil)

// subscriber is the part of the library's Subscriber the source uses.
type subscriber interface {
	Subscribe(ctx context.Context, topics []string, callback core.BrokerSubscriberCallback, rebalanceCallback core.ConsumerGroupRebalanceCallback) error
	CommitOffsets(offsets []core.TopicPartitionInfo) ([]core.TopicPartitionInfo, error)
	Close() error
}

// engineFactory builds the library's consumer engine.
type engineFactory func(brokers, clientID, groupID string, conf *core.ClientConfiguration) (core.SubscriberEngine, error)

// Source reads one topic through Yape's lib-go-kafkalizer. It is configured so
// that this service decides when an offset is committed: auto commit is off, and
// the library's own error handling is off, since that path publishes to a
// dead-letter engine this service does not have.
//
// The library's loop stops on SIGINT or SIGTERM, which it listens for itself, or
// on a fatal broker error, and in both cases Consume returns nil. It does not
// watch ctx, and it only logs an error that the callback returns and polls on, so
// the consumer tells a fatal stop from a clean one by its own context (see
// Consumer.Run) and stops handling records once that context is cancelled.
type Source struct {
	sub    subscriber
	topics []string
}

// NewSource connects a consumer to the topic in cfg. Broker credentials, when the
// cluster needs them, come from KAFKA_CLIENT_SASL_KEY and KAFKA_CLIENT_SASL_SECRET.
func NewSource(cfg config.KafkaConfig) (*Source, error) {
	return newSource(cfg, confluentEngine)
}

func confluentEngine(brokers, clientID, groupID string, conf *core.ClientConfiguration) (core.SubscriberEngine, error) {
	engine, err := confluent.NewConfluentConsumerEngine(brokers, clientID, groupID, conf)
	if err != nil {
		return nil, err
	}

	return engine, nil
}

func newSource(cfg config.KafkaConfig, factory engineFactory) (*Source, error) {
	engine, err := factory(cfg.Brokers, clientID, cfg.GroupID, clientConfiguration(cfg))
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	sub, err := libkafka.NewSubscriber(engine, unusedEncoding{}, libkafka.WithAutomaticErrorHandling(false))
	if err != nil {
		return nil, fmt.Errorf("create kafka subscriber: %w", err)
	}

	return &Source{sub: sub, topics: []string{cfg.Topic}}, nil
}

// clientConfiguration is what reaches librdkafka beyond the broker list and ids.
func clientConfiguration(cfg config.KafkaConfig) *core.ClientConfiguration {
	conf := core.ClientConfiguration{
		// The service commits an offset only after the event is stored.
		"enable.auto.commit":   false,
		"auto.offset.reset":    cfg.AutoOffsetReset,
		"max.poll.interval.ms": cfg.MaxPollIntervalMs,
		"session.timeout.ms":   cfg.SessionTimeoutMs,
	}

	key, secret := os.Getenv(core.BrokerSaslKeyEnv), os.Getenv(core.BrokerSaslSecretEnv)
	if key != "" && secret != "" {
		conf[core.SASLProtocolConfig] = core.SASLSSLProtocol
		conf[core.SASLMechanismsConfig] = core.SASLPlainMechanism
		conf[core.SASLKeyConfig] = key
		conf[core.SASLSecretConfig] = secret
	}

	return &conf
}

// Consume implements resource.RecordSource.
func (s *Source) Consume(ctx context.Context, handle func(resource.Record) error) error {
	callback := func(m *core.BrokerMessage, err error) error {
		if err != nil {
			return err
		}
		record, err := toRecord(m)
		if err != nil {
			return err
		}

		return handle(record)
	}

	return s.sub.Subscribe(ctx, s.topics, callback, nil)
}

// Commit implements resource.RecordSource. Kafka expects the offset of the next
// record to read, so it commits record.Offset + 1.
func (s *Source) Commit(record resource.Record) error {
	topic := record.Topic
	committed, err := s.sub.CommitOffsets([]core.TopicPartitionInfo{{Topic: &topic, Partition: record.Partition, Offset: record.Offset + 1}})
	if err != nil {
		return err
	}
	for _, info := range committed {
		if info.Error != nil {
			return info.Error
		}
	}

	return nil
}

// Close closes the consumer. The library also closes it when Consume returns.
func (s *Source) Close() error {
	return s.sub.Close()
}

func toRecord(m *core.BrokerMessage) (resource.Record, error) {
	if m == nil || m.Topic == nil {
		return resource.Record{}, errors.New("broker record has no topic")
	}
	offset, err := strconv.ParseInt(m.Offset, 10, 64)
	if err != nil {
		return resource.Record{}, fmt.Errorf("broker record %s/%d has an offset that is not a number", *m.Topic, m.Partition)
	}

	return resource.Record{Topic: *m.Topic, Partition: m.Partition, Offset: offset, Key: m.Key, Value: m.Raw}, nil
}

var errUnused = errors.New("this service decodes records itself")

// unusedEncoding satisfies the library's constructor. The service reads the raw
// bytes and decodes them in Decode, so none of these is called.
type unusedEncoding struct{}

func (unusedEncoding) GetSchema(string) (*encoding.EventSchema, error) { return nil, errUnused }
func (unusedEncoding) RefreshSchemaCache(...string) error              { return errUnused }
func (unusedEncoding) EncodeRaw([]byte, int) ([]byte, error)           { return nil, errUnused }
func (unusedEncoding) EncodeJSON(any, string) ([]byte, error)          { return nil, errUnused }
func (unusedEncoding) EncodeAvro(any, string) ([]byte, error)          { return nil, errUnused }
func (unusedEncoding) DecodeToJson([]byte, any, string) (any, error)   { return nil, errUnused }
func (unusedEncoding) DecodeToAvro([]byte, any, string) (any, error)   { return nil, errUnused }
