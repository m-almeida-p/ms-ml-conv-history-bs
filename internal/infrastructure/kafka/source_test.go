package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yaperos/lib-go-kafkalizer/v3/pkg/pubsub/core"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
)

// fakeEngine stands in for the confluent consumer engine.
type fakeEngine struct {
	records     []*core.BrokerMessage
	callbackErr []error
	closeErr    error
	closed      int

	commitErr  error
	commitInfo []core.TopicPartitionInfo
	committed  []core.TopicPartitionInfo
}

func (f *fakeEngine) Subscribe(_ context.Context, _ []string, callback core.BrokerSubscriberCallback, _ core.ConsumerGroupRebalanceCallback) error {
	for _, r := range f.records {
		f.callbackErr = append(f.callbackErr, callback(r, nil))
	}

	return nil
}

func (f *fakeEngine) Close() error {
	f.closed++

	return f.closeErr
}

func (f *fakeEngine) CommitOffsets(offsets []core.TopicPartitionInfo) ([]core.TopicPartitionInfo, error) {
	f.committed = offsets
	if f.commitErr != nil {
		return nil, f.commitErr
	}

	return f.commitInfo, nil
}

func (f *fakeEngine) Poll(int) core.BrokerEvent    { return core.BrokerEvent{} }
func (f *fakeEngine) GetRebalanceProtocol() string { return "" }
func (f *fakeEngine) AssignmentLost() bool         { return false }
func (f *fakeEngine) Seek([]core.TopicPartitionInfo) ([]core.TopicPartitionInfo, error) {
	return nil, nil
}

func kafkaConfig() config.KafkaConfig {
	return config.KafkaConfig{Brokers: "kafka:29092", Topic: topic, GroupID: "history", AutoOffsetReset: "earliest", MaxPollIntervalMs: 300000, SessionTimeoutMs: 45000}
}

func TestClientConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.KafkaConfig
		key    string
		secret string
		want   core.ClientConfiguration
	}{
		{
			name: "noAutoCommitAndTheConfiguredOffsetReset",
			cfg:  kafkaConfig(),
			want: core.ClientConfiguration{"enable.auto.commit": false, "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000, "session.timeout.ms": 45000},
		},
		{
			name: "latestOffsetReset",
			cfg:  config.KafkaConfig{AutoOffsetReset: "latest", MaxPollIntervalMs: 20000, SessionTimeoutMs: 6000},
			want: core.ClientConfiguration{"enable.auto.commit": false, "auto.offset.reset": "latest", "max.poll.interval.ms": 20000, "session.timeout.ms": 6000},
		},
		{
			name: "credentialsTurnOnSASLOverTLS",
			cfg:  kafkaConfig(), key: "k", secret: "s",
			want: core.ClientConfiguration{
				"enable.auto.commit": false, "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000, "session.timeout.ms": 45000,
				"security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN", "sasl.username": "k", "sasl.password": "s",
			},
		},
		{
			name: "aKeyWithoutASecretIsIgnored",
			cfg:  kafkaConfig(), key: "k",
			want: core.ClientConfiguration{"enable.auto.commit": false, "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000, "session.timeout.ms": 45000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(core.BrokerSaslKeyEnv, tt.key)
			t.Setenv(core.BrokerSaslSecretEnv, tt.secret)

			assert.Equal(t, tt.want, *clientConfiguration(tt.cfg))
		})
	}
}

func TestNewSource(t *testing.T) {
	t.Setenv(core.BrokerSaslKeyEnv, "")
	t.Setenv(core.BrokerSaslSecretEnv, "")

	t.Run("passesBrokersIdsAndSettingsToTheEngine", func(t *testing.T) {
		engine := &fakeEngine{}
		var gotBrokers, gotClient, gotGroup string
		var gotConf *core.ClientConfiguration

		src, err := newSource(kafkaConfig(), func(brokers, clientID, groupID string, conf *core.ClientConfiguration) (core.SubscriberEngine, error) {
			gotBrokers, gotClient, gotGroup, gotConf = brokers, clientID, groupID, conf
			return engine, nil
		})

		require.NoError(t, err)
		assert.Equal(t, "kafka:29092", gotBrokers)
		assert.Equal(t, "ms-ml-conv-history-bs", gotClient)
		assert.Equal(t, "history", gotGroup)
		assert.Equal(t, false, (*gotConf)["enable.auto.commit"], "the engine gets auto commit off")
		assert.Equal(t, []string{topic}, src.topics)
	})

	t.Run("automaticErrorHandlingIsOff", func(t *testing.T) {
		// With it on, the library hands a callback error to a dead-letter engine
		// this service does not have, and dereferences nil. With it off the error
		// comes back as it is.
		boom := errors.New("not stored")
		engine := &fakeEngine{records: []*core.BrokerMessage{{Topic: strPtr(topic), Offset: "3"}}}
		src, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
			return engine, nil
		})
		require.NoError(t, err)

		require.NotPanics(t, func() {
			err = src.Consume(context.Background(), func(resource.Record) error { return boom })
		})

		require.NoError(t, err)
		require.Len(t, engine.callbackErr, 1)
		assert.ErrorIs(t, engine.callbackErr[0], boom)
		assert.Equal(t, 1, engine.closed, "the library closes the consumer when the loop ends")
	})

	t.Run("engineFailureIsReturned", func(t *testing.T) {
		_, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
			return nil, errors.New("no brokers")
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "create kafka consumer: no brokers")
	})

	t.Run("noEngineIsRefusedByTheLibrary", func(t *testing.T) {
		_, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
			return nil, nil
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "create kafka subscriber")
	})
}

func TestNewSourceWithTheConfluentClient(t *testing.T) {
	t.Setenv(core.BrokerSaslKeyEnv, "")
	t.Setenv(core.BrokerSaslSecretEnv, "")

	tests := []struct {
		name    string
		mutate  func(*config.KafkaConfig)
		wantErr bool
	}{
		// librdkafka connects lazily, so creating a consumer needs no broker.
		{name: "validSettingsCreateAConsumer", mutate: func(c *config.KafkaConfig) { c.Brokers = "localhost:1" }},
		{name: "invalidOffsetResetIsRefused", mutate: func(c *config.KafkaConfig) { c.Brokers, c.AutoOffsetReset = "localhost:1", "middle" }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := kafkaConfig()
			tt.mutate(&cfg)

			src, err := NewSource(cfg)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "create kafka consumer")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, src)
			assert.NoError(t, src.Close())
		})
	}
}

func TestSourceConsume(t *testing.T) {
	tests := []struct {
		name       string
		record     *core.BrokerMessage
		handleErr  error
		wantMsg    *resource.Record
		wantCbErr  string
		wantCalled bool
	}{
		{
			name:       "recordBecomesAMessage",
			record:     &core.BrokerMessage{Topic: strPtr(topic), Partition: 2, Offset: "41", Key: []byte("k"), Raw: []byte("v")},
			wantMsg:    &resource.Record{Topic: topic, Partition: 2, Offset: 41, Key: []byte("k"), Value: []byte("v")},
			wantCalled: true,
		},
		{
			name:       "handlerErrorGoesBackToTheLibrary",
			record:     &core.BrokerMessage{Topic: strPtr(topic), Offset: "1"},
			handleErr:  errors.New("left uncommitted"),
			wantMsg:    &resource.Record{Topic: topic, Offset: 1},
			wantCbErr:  "left uncommitted",
			wantCalled: true,
		},
		{name: "recordWithoutATopic", record: &core.BrokerMessage{Offset: "1"}, wantCbErr: "no topic"},
		{name: "offsetNotANumber", record: &core.BrokerMessage{Topic: strPtr(topic), Partition: 4, Offset: "Invalid"}, wantCbErr: topic + "/4 has an offset that is not a number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := &fakeEngine{records: []*core.BrokerMessage{tt.record}}
			src, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
				return engine, nil
			})
			require.NoError(t, err)
			var got *resource.Record

			err = src.Consume(context.Background(), func(m resource.Record) error {
				got = &m
				return tt.handleErr
			})

			require.NoError(t, err)
			require.Len(t, engine.callbackErr, 1)
			if tt.wantCbErr != "" {
				require.Error(t, engine.callbackErr[0])
				assert.Contains(t, engine.callbackErr[0].Error(), tt.wantCbErr)
			} else {
				assert.NoError(t, engine.callbackErr[0])
			}
			assert.Equal(t, tt.wantMsg, got)
			assert.Equal(t, tt.wantCalled, got != nil)
		})
	}
}

func TestSourceConsumeReportsACloseFailure(t *testing.T) {
	engine := &fakeEngine{closeErr: errors.New("close failed")}
	src, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
		return engine, nil
	})
	require.NoError(t, err)

	err = src.Consume(context.Background(), func(resource.Record) error { return nil })

	require.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

func TestSourceCommit(t *testing.T) {
	rowErr := errors.New("partition revoked")

	tests := []struct {
		name       string
		commitErr  error
		commitInfo []core.TopicPartitionInfo
		wantErr    string
	}{
		{name: "commitsTheNextOffset", commitInfo: []core.TopicPartitionInfo{{Partition: 2, Offset: 42}}},
		{name: "engineError", commitErr: errors.New("broker not available"), wantErr: "broker not available"},
		{name: "partitionError", commitInfo: []core.TopicPartitionInfo{{Partition: 2, Error: rowErr}}, wantErr: "partition revoked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := &fakeEngine{commitErr: tt.commitErr, commitInfo: tt.commitInfo}
			src, err := newSource(kafkaConfig(), func(string, string, string, *core.ClientConfiguration) (core.SubscriberEngine, error) {
				return engine, nil
			})
			require.NoError(t, err)

			err = src.Commit(resource.Record{Topic: topic, Partition: 2, Offset: 41})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, engine.committed, 1)
			assert.Equal(t, topic, *engine.committed[0].Topic)
			assert.Equal(t, int32(2), engine.committed[0].Partition)
			assert.Equal(t, int64(42), engine.committed[0].Offset, "Kafka expects the next offset to read")
		})
	}
}

func TestUnusedEncodingRefusesEverything(t *testing.T) {
	enc := unusedEncoding{}

	_, errSchema := enc.GetSchema("s")
	_, errRaw := enc.EncodeRaw(nil, 1)
	_, errJSON := enc.EncodeJSON(nil, "t")
	_, errAvro := enc.EncodeAvro(nil, "t")
	_, errToJSON := enc.DecodeToJson(nil, nil, "t")
	_, errToAvro := enc.DecodeToAvro(nil, nil, "t")

	tests := []struct {
		name string
		err  error
	}{
		{"getSchema", errSchema}, {"refresh", enc.RefreshSchemaCache()}, {"encodeRaw", errRaw}, {"encodeJSON", errJSON},
		{"encodeAvro", errAvro}, {"decodeToJSON", errToJSON}, {"decodeToAvro", errToAvro},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.ErrorIs(t, tt.err, errUnused)
		})
	}
}

func strPtr(s string) *string { return &s }
