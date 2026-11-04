package history_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

// The pipeline tests need a Kafka cluster, and two of them a Debezium connector reading a Postgres. They start
// their own in Docker and touch nothing on the machine: no port of the host is fixed (each one is picked free
// at the moment), no name is shared with the local cluster, and every container carries a memory cap so a test
// run cannot starve whatever else the Docker VM is running.
//
// The infrastructure is heavy to start (Kafka, Schema Registry and Connect are three JVMs), so a run starts it
// once, on demand, and every test that needs it shares it. What a test could contaminate is not shared: each
// one makes its own topics, consumer groups, connectors, slots and conversation ids, all with unique names, and
// the history database each one reads is a Postgres of its own.
//
//	kafka only        TestPipelineTopic, ...StoresRecordsOfEveryShape..., the two outage tests
//	kafka + debezium  TestPipelineThroughDebezium, TestPipelineRecoversFromALostReplicationSlot
//
// A run that dies without reaching TestMain leaves its containers behind (Ryuk is off, because it needs the
// Docker socket mounted, which Colima does not give it). They carry the label conv-history-it:
//
//	docker rm -f $(docker ps -aq --filter label=conv-history-it)
const (
	kafkaImage    = "confluentinc/cp-kafka:7.5.3"
	registryImage = "confluentinc/cp-schema-registry:7.5.3"

	// connectImage is built from testdata/connect and kept, so only the first run builds it.
	connectRepo = "conv-history-it/connect"
	connectTag  = "7.5.3-debezium-2.5.4"

	mib = 1 << 20
)

// stack is what the pipeline tests run against. Every address is the one the test process uses; the names a
// container uses for another (kafka, schema-registry, db) are fixed by the network.
type stack struct {
	// brokers is the Kafka bootstrap address from the host.
	brokers string
	// connectURL is the Connect REST address from the host. Empty when only Kafka was started.
	connectURL string
	// agentDSN connects to the agent's Postgres, the one Debezium reads, from the host.
	agentDSN string
	// agentDB is what Connect connects to it with, on the network.
	agentDB agentDatabase
}

// agentDatabase is how a connector reaches the agent's Postgres from inside the network.
type agentDatabase struct{ host, user, password, name string }

// shared is the infrastructure of a run: started on first use by whichever test needs it, removed by TestMain.
var shared struct {
	mu        sync.Mutex
	net       *testcontainers.DockerNetwork
	kafka     *kafkaState
	debezium  *debeziumState
	terminate []func()
}

type kafkaState struct {
	brokers string
	err     error
}

type debeziumState struct {
	connectURL string
	agentDSN   string
	agentDB    agentDatabase
	err        error
}

// TestMain removes the shared infrastructure once every test has run.
func TestMain(m *testing.M) {
	code := m.Run()

	shared.mu.Lock()
	for i := len(shared.terminate) - 1; i >= 0; i-- {
		shared.terminate[i]()
	}
	if shared.net != nil {
		_ = shared.net.Remove(context.Background())
	}
	shared.mu.Unlock()

	os.Exit(code)
}

// requireKafka returns a Kafka cluster of this run, starting it if no test has yet. The test is skipped when
// Docker is not reachable.
func requireKafka(t *testing.T) stack {
	t.Helper()

	testsupport.RequireDocker(t)

	shared.mu.Lock()
	defer shared.mu.Unlock()

	state := startKafkaOnce()
	if state.err != nil {
		t.Fatalf("start the Kafka of this run: %v", state.err)
	}

	return stack{brokers: state.brokers}
}

// requireDebezium returns the Kafka cluster with Schema Registry, a Connect worker that has the Debezium
// PostgreSQL connector, and a Postgres with logical decoding and the agent's event store, starting what no
// test has yet.
func requireDebezium(t *testing.T) stack {
	t.Helper()

	s := requireKafka(t)

	shared.mu.Lock()
	defer shared.mu.Unlock()

	if shared.debezium == nil {
		shared.debezium = startDebezium()
	}
	if shared.debezium.err != nil {
		t.Fatalf("start Debezium for this run: %v", shared.debezium.err)
	}
	s.connectURL, s.agentDSN, s.agentDB = shared.debezium.connectURL, shared.debezium.agentDSN, shared.debezium.agentDB

	return s
}

// startKafkaOnce starts the network and the broker the first time it is called. The caller holds shared.mu.
func startKafkaOnce() *kafkaState {
	if shared.kafka != nil {
		return shared.kafka
	}

	state := &kafkaState{}
	shared.kafka = state

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	nw, err := network.New(ctx, network.WithLabels(map[string]string{testsupport.Label: "network"}))
	if err != nil {
		state.err = fmt.Errorf("create the network: %w", err)

		return state
	}
	shared.net = nw

	state.brokers, state.err = startKafka(ctx, nw.Name)

	return state
}

// startKafka starts one broker that is its own KRaft controller, so no ZooKeeper is needed. Containers on the
// network reach it as kafka:29092. The host reaches it through a port picked free right now, which the broker
// must advertise: a client gets back the address of the listener it connected to, and an address it cannot reach
// fails after the metadata lookup.
func startKafka(ctx context.Context, networkName string) (string, error) {
	port, err := testsupport.FreePort()
	if err != nil {
		return "", err
	}
	// A KRaft cluster id is a UUID in URL-safe base64 without padding.
	id := uuid.New()
	clusterID := base64.RawURLEncoding.EncodeToString(id[:])

	req := testcontainers.ContainerRequest{
		Image:          kafkaImage,
		ExposedPorts:   []string{"9092/tcp"},
		Labels:         map[string]string{testsupport.Label: "kafka"},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"kafka"}},
		Env: map[string]string{
			"CLUSTER_ID":                                     clusterID,
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENERS":                                "INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("INTERNAL://kafka:29092,EXTERNAL://127.0.0.1:%d", port),
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
			"CONFLUENT_SUPPORT_METRICS_ENABLE":               "false",
			"KAFKA_HEAP_OPTS":                                "-Xms128m -Xmx400m",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{"9092/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(port)}}}
			hc.Memory = 900 * mib
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(3 * time.Minute),
	}

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return "", fmt.Errorf("start kafka: %w", err)
	}
	shared.terminate = append(shared.terminate, func() { _ = started.Terminate(context.Background()) })

	brokers := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForBroker(ctx, brokers); err != nil {
		return "", err
	}

	return brokers, nil
}

// waitForBroker returns once a client on the host gets the metadata of the cluster through the advertised address.
func waitForBroker(ctx context.Context, brokers string) error {
	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer admin.Close()

	var last error
	for range 60 {
		if _, last = admin.GetMetadata(nil, false, 3000); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return fmt.Errorf("kafka at %s does not answer: %w", brokers, last)
}

// startDebezium starts the agent's Postgres with logical decoding and its event store, Schema Registry, and a
// Connect worker with the Debezium PostgreSQL connector. The caller holds shared.mu and the broker is up.
func startDebezium() *debeziumState {
	state := &debeziumState{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	networkName := shared.net.Name

	// The agent's database. The cluster's has wal_level=logical for the same reason: Debezium reads the WAL.
	agent, terminate, err := testsupport.RunPostgres(ctx,
		testsupport.WithArgs("postgres", "-c", "wal_level=logical"),
		testsupport.OnNetwork(networkName, "db"),
		testsupport.WithMemory(256*mib))
	if err != nil {
		state.err = fmt.Errorf("start the agent's postgres: %w", err)

		return state
	}
	shared.terminate = append(shared.terminate, terminate)

	if err := migrateAgent(ctx, agent.DSN); err != nil {
		state.err = err

		return state
	}

	registry, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          registryImage,
			ExposedPorts:   []string{"8081/tcp"},
			Labels:         map[string]string{testsupport.Label: "schema-registry"},
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{networkName: {"schema-registry"}},
			Env: map[string]string{
				"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
				"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "PLAINTEXT://kafka:29092",
				"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
				"SCHEMA_REGISTRY_HEAP_OPTS":                    "-Xms64m -Xmx200m",
			},
			HostConfigModifier: func(hc *container.HostConfig) { hc.Memory = 450 * mib },
			WaitingFor:         wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		state.err = fmt.Errorf("start schema registry: %w", err)

		return state
	}
	shared.terminate = append(shared.terminate, func() { _ = registry.Terminate(context.Background()) })

	connect, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    filepath.Join("testdata", "connect"),
				Dockerfile: "Dockerfile",
				Repo:       connectRepo,
				Tag:        connectTag,
				KeepImage:  true,
			},
			ExposedPorts:   []string{"8083/tcp"},
			Labels:         map[string]string{testsupport.Label: "connect"},
			Networks:       []string{networkName},
			NetworkAliases: map[string][]string{networkName: {"connect"}},
			Env: map[string]string{
				"CONNECT_BOOTSTRAP_SERVERS":                 "kafka:29092",
				"CONNECT_REST_ADVERTISED_HOST_NAME":         "connect",
				"CONNECT_REST_PORT":                         "8083",
				"CONNECT_GROUP_ID":                          "conv-history-it",
				"CONNECT_CONFIG_STORAGE_TOPIC":              "_connect-configs",
				"CONNECT_OFFSET_STORAGE_TOPIC":              "_connect-offsets",
				"CONNECT_STATUS_STORAGE_TOPIC":              "_connect-status",
				"CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR": "1",
				"CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR": "1",
				"CONNECT_STATUS_STORAGE_REPLICATION_FACTOR": "1",
				"CONNECT_KEY_CONVERTER":                     "org.apache.kafka.connect.json.JsonConverter",
				"CONNECT_VALUE_CONVERTER":                   "org.apache.kafka.connect.json.JsonConverter",
				"CONNECT_PLUGIN_PATH":                       "/usr/share/java,/usr/share/confluent-hub-components",
				"KAFKA_HEAP_OPTS":                           "-Xms128m -Xmx640m",
			},
			HostConfigModifier: func(hc *container.HostConfig) { hc.Memory = 1100 * mib },
			WaitingFor:         wait.ForHTTP("/connectors").WithPort("8083/tcp").WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		state.err = fmt.Errorf("start kafka connect: %w", err)

		return state
	}
	shared.terminate = append(shared.terminate, func() { _ = connect.Terminate(context.Background()) })

	url, err := connect.PortEndpoint(ctx, "8083/tcp", "http")
	if err != nil {
		state.err = fmt.Errorf("connect address: %w", err)

		return state
	}

	state.connectURL, state.agentDSN = url, agent.DSN
	state.agentDB = agentDatabase{host: "db", user: agent.User, password: agent.Password, name: agent.Database}

	return state
}

// defaultAgentMigrationsDir is where the testing repo keeps the migrations of the agent's event store, as a
// sibling checkout. AGENT_MIGRATIONS_DIR points elsewhere.
const defaultAgentMigrationsDir = "../../../ml-genai-agents-bs-testing/migrations/agent/conversation"

// migrateAgent creates the agent's conversation_events in a database, with the migrations the agent runs.
func migrateAgent(ctx context.Context, dsn string) error {
	dir := os.Getenv("AGENT_MIGRATIONS_DIR")
	if dir == "" {
		dir = defaultAgentMigrationsDir
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("the agent's migrations live in the testing repo; check it out beside this one or set AGENT_MIGRATIONS_DIR (%s): %v", dir, err)
	}
	sort.Strings(files)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	for _, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("run %s: %w", strings.TrimSuffix(filepath.Base(file), ".sql"), err)
		}
	}

	return nil
}
