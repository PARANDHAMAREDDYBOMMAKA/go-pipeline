package vad

import (
	"testing"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

func frame(v int16) media.PCM {
	n := media.SamplesPerFrame(media.BusSampleRate)
	s := make([]int16, n)
	for i := range s {
		s[i] = v
	}
	return media.PCM{Samples: s, SampleRate: media.BusSampleRate}
}

func TestEnergyVADStartAndHangover(t *testing.T) {
	v := NewEnergyVAD(500, 2, 3)

	if _, sp := v.Push(frame(0)); sp {
		t.Fatal("should be silent initially")
	}
	v.Push(frame(8000))
	changed, sp := v.Push(frame(8000))
	if !changed || !sp {
		t.Fatalf("should transition to speaking after startFrames")
	}

	v.Push(frame(0))
	v.Push(frame(0))
	changed, sp = v.Push(frame(0))
	if !changed || sp {
		t.Fatalf("should transition to silence after hangover, changed=%v sp=%v", changed, sp)
	}
}
