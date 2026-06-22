package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/transport"
)

func frame(seq uint32, v int16) media.PCM {
	n := media.SamplesPerFrame(media.BusSampleRate)
	s := make([]int16, n)
	for i := range s {
		s[i] = v
	}
	return media.PCM{Samples: s, SampleRate: media.BusSampleRate, Seq: seq}
}

func TestPipeForwardsInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := transport.NewFeedSource(16)
	out := transport.NewRecordSink()
	probe := NewProbe("test")

	go Pipe(ctx, in, out, probe)

	for i := 0; i < 5; i++ {
		in.Feed(frame(uint32(i), int16(100+i)))
	}

	deadline := time.After(time.Second)
	for {
		if len(out.Frames()) >= 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d frames forwarded", len(out.Frames()))
		case <-time.After(time.Millisecond):
		}
	}

	got := out.Frames()
	for i := 0; i < 5; i++ {
		if got[i].Seq != uint32(i) {
			t.Fatalf("frame %d out of order: seq=%d", i, got[i].Seq)
		}
	}

	name, count, avg, max := probe.Stats()
	if name != "test" || count < 5 {
		t.Fatalf("probe stats wrong: %s count=%d", name, count)
	}
	if max > 5*time.Millisecond {
		t.Fatalf("forward latency too high: avg=%s max=%s", avg, max)
	}
}
