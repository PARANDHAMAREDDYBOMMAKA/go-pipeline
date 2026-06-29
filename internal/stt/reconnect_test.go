package stt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconnectResumesAfterDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inner := NewMockClient()
	var reconnects int32
	client := WithReconnect(inner, func() { atomic.AddInt32(&reconnects, 1) })

	stream, err := client.Open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()

	first := waitStream(t, inner, 1)
	first.Emit(Transcript{Text: "before", Final: true})
	if got := readText(t, stream); got != "before" {
		t.Fatalf("pre-drop text = %q", got)
	}

	first.Close()

	second := waitStream(t, inner, 2)
	if atomic.LoadInt32(&reconnects) == 0 {
		deadline := time.After(time.Second)
		for atomic.LoadInt32(&reconnects) == 0 {
			select {
			case <-deadline:
				t.Fatal("reconnect callback never fired")
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	second.Emit(Transcript{Text: "after", Final: true})
	if got := readText(t, stream); got != "after" {
		t.Fatalf("post-reconnect text = %q", got)
	}
}

func waitStream(t *testing.T, c *MockClient, n int) *MockStream {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		c.mu.Lock()
		count := len(c.streams)
		var s *MockStream
		if count >= n {
			s = c.streams[n-1]
		}
		c.mu.Unlock()
		if s != nil {
			return s
		}
		select {
		case <-deadline:
			t.Fatalf("stream #%d never opened (have %d)", n, count)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func readText(t *testing.T, s Stream) string {
	t.Helper()
	select {
	case ev := <-s.Events():
		return ev.Text
	case <-time.After(2 * time.Second):
		t.Fatal("no transcript received")
		return ""
	}
}
