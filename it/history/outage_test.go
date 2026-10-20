package history_test

import (
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

// outageFixture is a consumer binary on its own topic, with a history database that
// a test can stop and start again.
type outageFixture struct {
	pg       *testsupport.Postgres
	pool     *pgxpool.Pool
	brokers  string
	topic    string
	group    string
	bin      string
	prod     *producer
	sequence int
	consumer *consumerProcess
}

func newOutageFixture(t *testing.T) *outageFixture {
	t.Helper()

	s := requireKafka(t)
	pg := testsupport.StartPostgres(t)
	pool, err := pgxpool.New(context.Background(), pg.DSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	migrateUp(t, pool)

	f := &outageFixture{pg: pg, pool: pool, brokers: s.brokers, topic: uniqueName("outage-test"), group: uniqueName("outage-group"), bin: consumerBinary(t)}
	createTopic(t, f.brokers, f.topic)
	f.prod = newProducer(t, f.brokers)

	return f
}

// send writes n chat events of one conversation to partition 0, which keeps their
// order and makes the committed offset easy to read.
func (f *outageFixture) send(conversation string, n int) {
	for range n {
		f.sequence++
		f.prod.send(f.topic, 0, eventKey(conversation), eventValue(conversation, f.sequence, "UserMessageSent", map[string]any{"text": "hola"}))
	}
}

func (f *outageFixture) waitForInbox(t *testing.T, n int) {
	t.Helper()

	eventually(t, 120*time.Second, strconv.Itoa(n)+" rows in the inbox", func() bool {
		var got int
		return f.pool.QueryRow(context.Background(), "SELECT count(*) FROM conversation_inbox").Scan(&got) == nil && got >= n
	})
}

func (f *outageFixture) rows(t *testing.T) (inbox, messages int) {
	t.Helper()

	require.NoError(t, f.pool.QueryRow(context.Background(), "SELECT count(*) FROM conversation_inbox").Scan(&inbox))
	require.NoError(t, f.pool.QueryRow(context.Background(), "SELECT count(*) FROM conversation_messages").Scan(&messages))

	return inbox, messages
}

func summaryTotals(t *testing.T, output string) map[string]int {
	t.Helper()

	summary := regexp.MustCompile(`consumer stopped: ([^"]+)`).FindStringSubmatch(output)
	require.NotNil(t, summary, "the consumer logs its totals when it stops:\n%s", output)
	totals := map[string]int{}
	for _, m := range regexp.MustCompile(`(\w+)=(\d+)`).FindAllStringSubmatch(summary[1], -1) {
		totals[m[1]], _ = strconv.Atoi(m[2])
	}

	return totals
}

// TestPipelineRidesOutADatabaseOutageLongerThanThePollInterval: the consumer retries
// an event in place, which blocks polling. With a poll interval of 10 s, an outage of
// 15 s or more makes the broker take the partition away. Once the database is back
// the consumer must still store every event exactly once, the ones sent before, during
// and after the outage, and exit cleanly with every offset committed.
func TestPipelineRidesOutADatabaseOutageLongerThanThePollInterval(t *testing.T) {
	f := newOutageFixture(t)
	f.consumer = startConsumer(t, f.bin, f.brokers, f.topic, f.group, f.pg.DSN,
		"KAFKA_MAX_POLL_INTERVAL_MS=10000", "KAFKA_SESSION_TIMEOUT_MS=6000")
	conversation := uniqueName("outage")

	f.send(conversation, 5)
	f.waitForInbox(t, 5)

	f.pg.Stop(t)
	f.send(conversation, 5)
	// Retries wait 1, 2, 4 and 8 seconds, so the fifth failure comes 15 s after the first,
	// past the 10 s poll interval.
	f.consumer.waitForLog(t, "was not stored, trying again", 5, 90*time.Second)

	f.pg.Start(t)
	f.waitForInbox(t, 10)

	f.consumer.stop(t)
	inbox, messages := f.rows(t)
	totals := summaryTotals(t, f.consumer.out.String())
	t.Logf("after the outage: inbox=%d messages=%d totals=%v", inbox, messages, totals)

	assert.Equal(t, 10, inbox, "every event once, none lost, none doubled")
	assert.Equal(t, 10, messages)
	assert.Zero(t, totals["rejected"])
	assert.Zero(t, totals["malformed"])
	assert.GreaterOrEqual(t, totals["retries"], 5)
	assert.Equal(t, 10, totals["stored"], "ten events stored in all")
	// The event in flight when the outage began was stored after the broker had taken
	// the partition away. Its commit failed, and the broker handed it out again. Without
	// that redelivery the poll interval was not exceeded and this test proves nothing.
	assert.GreaterOrEqual(t, totals["duplicates"], 1, "the broker redelivered the uncommitted event and the inbox absorbed it")
	assertCommittedEverything(t, f.brokers, f.topic, f.group)
}

// TestPipelineStopsOnSIGTERMWhileTheDatabaseIsDown: a stop during an outage must not
// hang, must exit 0, and must not commit the event it could not store. A new consumer
// of the same group then stores it.
func TestPipelineStopsOnSIGTERMWhileTheDatabaseIsDown(t *testing.T) {
	f := newOutageFixture(t)
	f.consumer = startConsumer(t, f.bin, f.brokers, f.topic, f.group, f.pg.DSN)
	conversation := uniqueName("sigterm")

	f.send(conversation, 3)
	f.waitForInbox(t, 3)

	f.pg.Stop(t)
	f.send(conversation, 1)
	f.consumer.waitForLog(t, "was not stored, trying again", 1, 60*time.Second)

	started := time.Now()
	f.consumer.stop(t) // exit 0 within 30 s, or the test fails
	assert.Less(t, time.Since(started), 20*time.Second, "the retry wait does not delay the stop")

	totals := summaryTotals(t, f.consumer.out.String())
	assert.Equal(t, 3, totals["stored"])
	assert.GreaterOrEqual(t, totals["retries"], 1)
	committed, end := committedOffsets(t, f.brokers, f.topic, f.group, 0)
	assert.Equal(t, end-1, committed, "the event that was never stored is not committed")

	f.pg.Start(t)
	f.consumer = startConsumer(t, f.bin, f.brokers, f.topic, f.group, f.pg.DSN)
	f.waitForInbox(t, 4)
	f.consumer.stop(t)

	inbox, messages := f.rows(t)
	assert.Equal(t, 4, inbox, "the event left uncommitted is stored by the next consumer")
	assert.Equal(t, 4, messages)
	assertCommittedEverything(t, f.brokers, f.topic, f.group)
}
