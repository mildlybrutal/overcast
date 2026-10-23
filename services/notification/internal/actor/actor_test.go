package actor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return mr, rdb
}

func shortIdle(t *testing.T) {
	t.Helper()
	old := IdleTimeout
	IdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { IdleTimeout = old })
}

func newClient() *Client {
	return &Client{ID: "c", Send: make(chan []byte, 8)}
}

// publish retries until the actor's Redis subscription is live.
func publish(t *testing.T, mr *miniredis.Miniredis, matchID, payload string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for mr.Publish("match:updates:"+matchID, payload) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("actor never subscribed to Redis")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func recv(t *testing.T, c *Client) ([]byte, bool) {
	t.Helper()
	select {
	case msg, ok := <-c.Send:
		return msg, ok
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting on client.Send")
		return nil, false
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Full lifecycle: join, receive a broadcast, leave, idle shutdown. Afterwards
// no actor goroutine or Redis subscription may be left running, checked while
// the Redis client is still open (closing it would hide a leaked subscriber).
func TestActorLifecycleLeaksNothing(t *testing.T) {
	shortIdle(t)
	mr, rdb := newTestRedis(t)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	baseline := goleak.IgnoreCurrent() // miniredis + pooled connection

	sup := NewSupervisor(context.Background(), rdb)
	c := newClient()
	sup.RouteClientJoin("m1", c)

	publish(t, mr, "m1", "score:1")
	if msg, _ := recv(t, c); string(msg) != "score:1" {
		t.Fatalf("got %q, want score:1", msg)
	}

	sup.RouteClientLeave("m1", c)
	if _, ok := recv(t, c); ok {
		t.Fatal("client.Send should be closed after leave")
	}

	waitFor(t, "idle actor to be removed", func() bool { return sup.actorCount() == 0 })
	if err := goleak.Find(baseline); err != nil {
		t.Fatal(err)
	}
}

// Sending to an actor that has stopped used to block forever (unbuffered
// mailbox, nobody reading). It must now return false immediately.
func TestSendToStoppedActorDoesNotBlock(t *testing.T) {
	_, rdb := newTestRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	act := NewMatchActor("m1", rdb, func(*MatchActor) {})
	go act.Start(ctx)
	cancel()
	<-act.Done()

	result := make(chan bool, 1)
	go func() { result <- act.Send(ActorMessage{Type: ClientLeave, Client: newClient()}) }()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("Send to a stopped actor reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("Send to a stopped actor blocked")
	}
}

// A client joining after the match's actor idled out gets a fresh actor that
// still delivers updates.
func TestJoinAfterIdleShutdownSpawnsFreshActor(t *testing.T) {
	shortIdle(t)
	mr, rdb := newTestRedis(t)
	sup := NewSupervisor(context.Background(), rdb)

	first := newClient()
	sup.RouteClientJoin("m1", first)
	sup.RouteClientLeave("m1", first)
	waitFor(t, "first actor to stop", func() bool { return sup.actorCount() == 0 })

	second := newClient()
	sup.RouteClientJoin("m1", second)
	publish(t, mr, "m1", "score:2")
	if msg, _ := recv(t, second); string(msg) != "score:2" {
		t.Fatalf("got %q, want score:2", msg)
	}
}

// When the actor stops it closes every client's Send channel, so the
// per-connection writer goroutines exit instead of waiting forever.
func TestStoppingActorClosesClientChannels(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	sup := NewSupervisor(ctx, rdb)

	clients := []*Client{newClient(), newClient(), newClient()}
	for _, c := range clients {
		sup.RouteClientJoin("m1", c)
	}
	cancel()

	for _, c := range clients {
		if _, ok := recv(t, c); ok {
			t.Fatal("expected closed channel after shutdown")
		}
	}

	// Joins after shutdown must not spin or block, and must close Send.
	late := newClient()
	sup.RouteClientJoin("m1", late)
	if _, ok := recv(t, late); ok {
		t.Fatal("late join after shutdown should get a closed channel")
	}
}

// A client whose buffer is full is dropped, and once the actor has no clients
// left it idles out like any other.
func TestSlowClientIsDropped(t *testing.T) {
	shortIdle(t)
	mr, rdb := newTestRedis(t)
	sup := NewSupervisor(context.Background(), rdb)

	slow := &Client{ID: "slow", Send: make(chan []byte, 1)}
	sup.RouteClientJoin("m1", slow)

	publish(t, mr, "m1", "a") // fills the 1-slot buffer
	waitFor(t, "first message buffered", func() bool { return len(slow.Send) == 1 })
	mr.Publish("match:updates:m1", "b") // buffer full: client dropped

	// Don't read yet: draining "a" early would make room for "b".
	waitFor(t, "actor to idle out after dropping its only client", func() bool { return sup.actorCount() == 0 })

	if msg, _ := recv(t, slow); string(msg) != "a" {
		t.Fatalf("got %q, want a", msg)
	}
	if _, ok := recv(t, slow); ok {
		t.Fatal("slow client's channel should be closed")
	}
}
