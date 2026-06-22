package room

import (
	"context"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/transport"
)

func constFrame(v int16) media.PCM {
	n := media.SamplesPerFrame(media.BusSampleRate)
	s := make([]int16, n)
	for i := range s {
		s[i] = v
	}
	return media.PCM{Samples: s, SampleRate: media.BusSampleRate}
}

func (in *intake) peek() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.have
}

func waitIntake(t *testing.T, r *Room, id ParticipantID) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		r.mu.RLock()
		in := r.intakes[id]
		r.mu.RUnlock()
		if in != nil && in.peek() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("intake for %s never filled", id)
}

func TestRoomFullMeshSelfExclusion(t *testing.T) {
	r := NewRoom("t")
	defer r.Close()
	ctx := context.Background()

	lpA := transport.NewLoopback(4)
	lpB := transport.NewLoopback(4)
	lpC := transport.NewLoopback(4)

	srcA, sinkA, _ := lpA.Attach(ctx)
	srcB, sinkB, _ := lpB.Attach(ctx)
	srcC, sinkC, _ := lpC.Attach(ctx)

	a := NewParticipant("A", "A", KindHuman, srcA, sinkA)
	b := NewParticipant("B", "B", KindHuman, srcB, sinkB)
	c := NewParticipant("C", "C", KindHuman, srcC, sinkC)
	for _, p := range []*Participant{a, b, c} {
		if err := r.Join(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	lpA.Source().Feed(constFrame(100))
	lpB.Source().Feed(constFrame(50))
	lpC.Source().Feed(constFrame(20))

	waitIntake(t, r, "A")
	waitIntake(t, r, "B")
	waitIntake(t, r, "C")

	r.tick()

	want := map[ParticipantID]int16{"A": 70, "B": 120, "C": 150}
	for _, lp := range []struct {
		id   ParticipantID
		sink *transport.RecordSink
	}{{"A", lpA.Sink()}, {"B", lpB.Sink()}, {"C", lpC.Sink()}} {
		fr := lp.sink.Frames()
		if len(fr) != 1 {
			t.Fatalf("%s: got %d frames want 1", lp.id, len(fr))
		}
		if fr[0].Samples[0] != want[lp.id] {
			t.Fatalf("%s: mix=%d want %d (global-self)", lp.id, fr[0].Samples[0], want[lp.id])
		}
	}
}

func TestRoomSelectiveSubscription(t *testing.T) {
	r := NewRoom("t")
	defer r.Close()
	ctx := context.Background()

	lpA := transport.NewLoopback(4)
	lpB := transport.NewLoopback(4)
	lpC := transport.NewLoopback(4)
	srcA, sinkA, _ := lpA.Attach(ctx)
	srcB, sinkB, _ := lpB.Attach(ctx)
	srcC, sinkC, _ := lpC.Attach(ctx)

	a := NewParticipant("A", "A", KindAgent, srcA, sinkA)
	b := NewParticipant("B", "B", KindHuman, srcB, sinkB)
	c := NewParticipant("C", "C", KindHuman, srcC, sinkC)
	for _, p := range []*Participant{a, b, c} {
		if err := r.Join(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	if err := r.Subscribe("A", "B"); err != nil {
		t.Fatal(err)
	}

	lpA.Source().Feed(constFrame(1000))
	lpB.Source().Feed(constFrame(50))
	lpC.Source().Feed(constFrame(20))
	waitIntake(t, r, "A")
	waitIntake(t, r, "B")
	waitIntake(t, r, "C")

	r.tick()

	fa := lpA.Sink().Frames()
	if len(fa) != 1 {
		t.Fatalf("A frames=%d", len(fa))
	}
	if fa[0].Samples[0] != 50 {
		t.Fatalf("A hears only B: got %d want 50", fa[0].Samples[0])
	}
}

func TestRoomSilenceWhenNoFrame(t *testing.T) {
	r := NewRoom("t")
	defer r.Close()
	ctx := context.Background()

	lpA := transport.NewLoopback(4)
	srcA, sinkA, _ := lpA.Attach(ctx)
	a := NewParticipant("A", "A", KindHuman, srcA, sinkA)
	if err := r.Join(ctx, a); err != nil {
		t.Fatal(err)
	}
	r.tick()
	fr := lpA.Sink().Frames()
	if len(fr) != 1 {
		t.Fatalf("frames=%d want 1", len(fr))
	}
	for _, s := range fr[0].Samples {
		if s != 0 {
			t.Fatalf("expected silence, got %d", s)
		}
	}
}
