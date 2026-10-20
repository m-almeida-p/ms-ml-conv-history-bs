package history_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipOrFail skips a test whose local stack is missing. With
// PIPELINE_REQUIRE_STACK set (make pipeline), a missing stack fails the test, so a
// run that proves nothing cannot pass.
func skipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()

	if os.Getenv("PIPELINE_REQUIRE_STACK") != "" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

// stack is the local cluster the pipeline tests run against.
type stack struct {
	brokers    string
	agentDSN   string
	connectURL string
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}

// requireKafka skips the test when the local Kafka does not answer.
func requireKafka(t *testing.T) stack {
	t.Helper()

	s := stack{
		brokers:    env("PIPELINE_KAFKA_BROKERS", "localhost:9092"),
		agentDSN:   env("PIPELINE_AGENT_DSN", "postgres://postgres@localhost:5432/postgres?sslmode=disable"),
		connectURL: env("PIPELINE_CONNECT_URL", "http://localhost:8083"),
	}

	admin := adminClient(t, s.brokers)
	// The first connection of a cold client can take several seconds.
	if _, err := admin.GetMetadata(nil, false, 15000); err != nil {
		skipOrFail(t, "local Kafka at %s does not answer (start the cluster): %v", s.brokers, err)
	}

	return s
}

// requireDebezium skips the test unless the outbox connector runs and the agent's
// database has the outbox.
func requireDebezium(t *testing.T, s stack) {
	t.Helper()

	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(s.connectURL + "/connectors/conversation-outbox/status")
	if err != nil {
		skipOrFail(t, "Kafka Connect at %s does not answer: %v", s.connectURL, err)
	}
	defer resp.Body.Close()

	var status struct {
		Connector struct{ State string } `json:"connector"`
		Tasks     []struct{ State string }
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&status) != nil ||
		status.Connector.State != "RUNNING" || len(status.Tasks) == 0 || status.Tasks[0].State != "RUNNING" {
		skipOrFail(t, "the conversation-outbox connector is not running (register it with local/debezium/register.sh)")
	}

	pool, err := pgxpool.New(context.Background(), s.agentDSN)
	require.NoError(t, err)
	defer pool.Close()

	var hasSequence bool
	err = pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'conversation_outbox' AND column_name = 'sequence')`).Scan(&hasSequence)
	if err != nil || !hasSequence {
		skipOrFail(t, "the agent database has no conversation_outbox with a sequence column")
	}
}

func adminClient(t *testing.T, brokers string) *ckafka.AdminClient {
	t.Helper()

	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	return admin
}

// createTopic makes a topic with three partitions and removes it at the end.
func createTopic(t *testing.T, brokers, name string) {
	t.Helper()

	admin := adminClient(t, brokers)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []ckafka.TopicSpecification{{Topic: name, NumPartitions: 3, ReplicationFactor: 1}})
	require.NoError(t, err)
	require.Equal(t, ckafka.ErrNoError, results[0].Error.Code(), results[0].Error.String())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = admin.DeleteTopics(ctx, []string{name})
	})
}

type producer struct {
	t *testing.T
	p *ckafka.Producer
}

func newProducer(t *testing.T, brokers string) *producer {
	t.Helper()

	p, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	require.NoError(t, err)
	t.Cleanup(p.Close)

	return &producer{t: t, p: p}
}

// send writes one record and waits until the broker has it.
func (p *producer) send(topic string, partition int32, key, value []byte) {
	p.t.Helper()

	done := make(chan ckafka.Event, 1)
	require.NoError(p.t, p.p.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            key, Value: value,
	}, done))

	select {
	case ev := <-done:
		require.NoError(p.t, ev.(*ckafka.Message).TopicPartition.Error)
	case <-time.After(15 * time.Second):
		p.t.Fatal("the broker did not confirm the record")
	}
}

// framed puts the Schema Registry header in front of JSON, as the connector does.
func framed(schemaID byte, body []byte) []byte {
	return append([]byte{0, 0, 0, 0, schemaID}, body...)
}

// outboxKey and outboxValue make the record Debezium would publish for a row.
func outboxKey(id int64) []byte {
	return framed(3, []byte(fmt.Sprintf(`{"id":%d}`, id)))
}

func outboxValue(id int64, conversationID string, sequence int, eventName string, payload map[string]any) []byte {
	payloadJSON, _ := json.Marshal(payload)
	metadataJSON, _ := json.Marshal(map[string]string{"actor": "user", "id_yape_account": "acc-pipeline"})
	body, _ := json.Marshal(map[string]any{
		"id": id, "conversation_id": conversationID, "sequence": sequence, "event_name": eventName, "event_version": 1,
		"payload": string(payloadJSON), "metadata": string(metadataJSON),
		"correlation_id": "corr-pipeline", "id_yape_account": "acc-pipeline", "actor": "user", "published": false,
		"created_at": time.Date(2026, 10, 20, 15, 0, sequence, 0, time.UTC).Format(time.RFC3339Nano),
	})

	return framed(4, body)
}

// lockedBuffer is a bytes.Buffer that the test can read while the process writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// consumerBinary builds cmd/consumer and returns its path.
func consumerBinary(t *testing.T) string {
	t.Helper()

	return buildBinary(t, "consumer")
}

// buildBinary builds ./cmd/<name> into a temporary directory.
func buildBinary(t *testing.T, name string) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/"+name)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return bin
}

type consumerProcess struct {
	cmd  *exec.Cmd
	out  *lockedBuffer
	done chan error
}

// startConsumer runs the real binary against the topic and the history database.
func startConsumer(t *testing.T, bin, brokers, topic, group, postgresDSN string) *consumerProcess {
	t.Helper()

	// Registered first, so it runs after the process is stopped: a group can only
	// be deleted once it has no members.
	t.Cleanup(func() { deleteGroup(t, brokers, group) })

	return startBinary(t, bin, []string{
		"POSTGRES_DSN=" + postgresDSN,
		"KAFKA_CLIENT_BROKERS=" + brokers,
		"KAFKA_TOPIC_OUTBOX=" + topic,
		"KAFKA_GROUP_ID=" + group,
		"KAFKA_AUTO_OFFSET_RESET=earliest",
	})
}

// deleteGroup removes a consumer group the test created. The broker refuses while
// the group still has members, so it tries for a few seconds.
func deleteGroup(t *testing.T, brokers, group string) {
	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Errorf("admin client to delete group %s: %v", group, err)
		return
	}
	defer admin.Close()

	var last error
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := admin.DeleteConsumerGroups(ctx, []string{group})
		cancel()
		if err == nil && len(res.ConsumerGroupResults) == 1 && res.ConsumerGroupResults[0].Error.Code() == ckafka.ErrNoError {
			return
		}
		last = err
		if err == nil && len(res.ConsumerGroupResults) == 1 {
			last = res.ConsumerGroupResults[0].Error
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Errorf("could not delete the consumer group %s: %v", group, last)
}

// startBinary runs one of the service's binaries from the repo root (where
// config.yaml is) with extra environment variables, and captures its output.
func startBinary(t *testing.T, bin string, extraEnv []string) *consumerProcess {
	t.Helper()

	out := &lockedBuffer{}
	cmd := exec.Command(bin)
	cmd.Dir = "../.."
	cmd.Env = append(append(os.Environ(),
		"ENVIRONMENT=production", // do not read a developer's .env
		"LOG_LEVEL=info",
	), extraEnv...)
	cmd.Stdout, cmd.Stderr = out, out
	require.NoError(t, cmd.Start())

	p := &consumerProcess{cmd: cmd, out: out, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		if t.Failed() {
			t.Logf("process output:\n%s", out.String())
		}
	})

	return p
}

// stop sends SIGTERM and requires a clean exit.
func (p *consumerProcess) stop(t *testing.T) {
	t.Helper()

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case err := <-p.done:
		require.NoError(t, err, "the process exits 0 on SIGTERM")
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("the process did not stop after SIGTERM")
	}
}

// eventually polls until check passes or the time is up.
func eventually(t *testing.T, timeout time.Duration, what string, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// assertCommittedEverything checks that, for each partition with records, the
// group's committed offset is the end of the partition.
func assertCommittedEverything(t *testing.T, brokers, topic, group string) {
	t.Helper()

	c, err := ckafka.NewConsumer(&ckafka.ConfigMap{"bootstrap.servers": brokers, "group.id": group, "enable.auto.commit": false})
	require.NoError(t, err)
	defer c.Close()

	partitions := []ckafka.TopicPartition{{Topic: &topic, Partition: 0}, {Topic: &topic, Partition: 1}, {Topic: &topic, Partition: 2}}
	committed, err := c.Committed(partitions, 10000)
	require.NoError(t, err)

	for i, tp := range committed {
		_, high, err := c.QueryWatermarkOffsets(topic, int32(i), 10000)
		require.NoError(t, err)
		if high == 0 {
			continue
		}
		assert.Equal(t, high, int64(tp.Offset), "partition %d: the committed offset is the end of the partition", i)
	}
}

// uniqueName returns a name that no other run uses.
func uniqueName(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}
