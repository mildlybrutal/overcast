package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cricketv1 "github.com/mildlybrutal/overcast/gen/cricket/v1"
	"github.com/mildlybrutal/overcast/internal/models"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// fakeReader serves msgs in order, then blocks until ctx is cancelled.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

// fakeStore fails the first failFirst upserts, or every upsert if failFirst < 0.
type fakeStore struct {
	mu        sync.Mutex
	failFirst int
	calls     int
	stored    []string
}

func (s *fakeStore) UpsertMatch(_ context.Context, m models.MatchData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failFirst < 0 || s.calls <= s.failFirst {
		return errors.New("postgres unavailable")
	}
	s.stored = append(s.stored, m.ID)
	return nil
}

type fakeCache struct {
	mu        sync.Mutex
	published []string
	fail      bool
}

func (c *fakeCache) CacheLiveMatch(context.Context, models.MatchData) error {
	if c.fail {
		return errors.New("redis down")
	}
	return nil
}

func (c *fakeCache) PublishMatchUpdate(_ context.Context, m models.MatchData) error {
	if c.fail {
		return errors.New("redis down")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, m.ID)
	return nil
}

func matchMsg(t *testing.T, offset int64, id string) kafka.Message {
	t.Helper()
	b, err := proto.Marshal(&cricketv1.MatchData{Id: id, Name: "A vs B"})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Offset: offset, Key: []byte(id), Value: b}
}

func fastBackoff(t *testing.T) {
	t.Helper()
	oldInit, oldMax := initialBackoff, maxBackoff
	initialBackoff, maxBackoff = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { initialBackoff, maxBackoff = oldInit, oldMax })
}

// run starts the worker and stops it once cond holds (or fails after 2s).
func run(t *testing.T, w *ProcessingWorker, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Start(ctx); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestCommitsOnlyAfterPostgresWrite(t *testing.T) {
	fastBackoff(t)
	reader := &fakeReader{msgs: []kafka.Message{matchMsg(t, 1, "m1"), matchMsg(t, 2, "m2")}}
	store := &fakeStore{failFirst: 3} // m1 fails 3 times, then succeeds
	cache := &fakeCache{}

	run(t, newWorker(reader, store, cache), func() bool { return len(reader.commits()) == 2 })

	if got := reader.commits(); got[0] != 1 || got[1] != 2 {
		t.Fatalf("commits = %v, want [1 2]", got)
	}
	if len(store.stored) != 2 || store.stored[0] != "m1" {
		t.Fatalf("stored = %v, want m1 then m2 (no event skipped)", store.stored)
	}
	if len(cache.published) != 2 {
		t.Fatalf("published = %v, want both updates pushed live", cache.published)
	}
}

// If the service shuts down while Postgres is still failing, the offset must
// stay uncommitted so the message is redelivered after restart.
func TestShutdownDuringRetryDoesNotCommit(t *testing.T) {
	fastBackoff(t)
	reader := &fakeReader{msgs: []kafka.Message{matchMsg(t, 7, "m1")}}
	store := &fakeStore{failFirst: -1}

	run(t, newWorker(reader, store, &fakeCache{}), func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.calls >= 3
	})

	if got := reader.commits(); len(got) != 0 {
		t.Fatalf("committed %v while Postgres never accepted the write", got)
	}
}

func TestUndecodableMessageIsSkipped(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{
		{Offset: 1, Value: []byte{0xff, 0xff, 0xff}},
		matchMsg(t, 2, "m2"),
	}}
	store := &fakeStore{}

	run(t, newWorker(reader, store, &fakeCache{}), func() bool { return len(reader.commits()) == 2 })

	if len(store.stored) != 1 || store.stored[0] != "m2" {
		t.Fatalf("stored = %v, want only m2", store.stored)
	}
}

// Redis is derived state: an outage must not stop the stream or lose events.
func TestRedisFailureDoesNotBlockCommit(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{matchMsg(t, 1, "m1")}}
	store := &fakeStore{}

	run(t, newWorker(reader, store, &fakeCache{fail: true}), func() bool { return len(reader.commits()) == 1 })

	if len(store.stored) != 1 {
		t.Fatal("event not stored")
	}
}
