package media

import (
	"math"
	"testing"
)

func TestULawRoundTripBounded(t *testing.T) {
	maxErr := 0
	for s := -32000; s <= 32000; s += 7 {
		in := int16(s)
		out := DecodeULaw(EncodeULaw(in))
		e := int(out) - int(in)
		if e < 0 {
			e = -e
		}
		if e > maxErr {
			maxErr = e
		}
	}

	if maxErr > 1024 {
		t.Fatalf("μ-law round-trip error too large: %d", maxErr)
	}
}

func TestALawRoundTripBounded(t *testing.T) {
	maxErr := 0
	for s := -32000; s <= 32000; s += 7 {
		in := int16(s)
		out := DecodeALaw(EncodeALaw(in))
		e := int(out) - int(in)
		if e < 0 {
			e = -e
		}
		if e > maxErr {
			maxErr = e
		}
	}
	if maxErr > 1024 {
		t.Fatalf("A-law round-trip error too large: %d", maxErr)
	}
}

func TestSamplesPerFrame(t *testing.T) {
	cases := map[int]int{8000: 160, 16000: 320, 24000: 480, 48000: 960}
	for rate, want := range cases {
		if got := SamplesPerFrame(rate); got != want {
			t.Errorf("SamplesPerFrame(%d)=%d want %d", rate, got, want)
		}
	}
}

func TestResampleUpDownRoundTrip(t *testing.T) {
	const n = 320
	in := make([]int16, n)
	for i := range in {
		in[i] = int16(8000 * math.Sin(2*math.Pi*float64(i)/40))
	}
	up := Resample(in, 16000, 48000)
	if len(up) != n*3 {
		t.Fatalf("upsample len=%d want %d", len(up), n*3)
	}
	down := Resample(up, 48000, 16000)
	if len(down) != n {
		t.Fatalf("downsample len=%d want %d", len(down), n)
	}
}

func TestMixAccumLimitAndSelfExclusion(t *testing.T) {
	a := []int16{100, 200, 300}
	b := []int16{50, 60, 70}
	acc := make([]int32, 3)
	AccumInto(acc, a)
	AccumInto(acc, b)

	if acc[0] != 150 || acc[1] != 260 || acc[2] != 370 {
		t.Fatalf("AccumInto wrong: %v", acc)
	}

	selfA := make([]int32, 3)
	copy(selfA, acc)
	SubFrom(selfA, a)
	got := Limit(selfA)
	if got[0] != b[0] || got[1] != b[1] || got[2] != b[2] {
		t.Fatalf("self-exclusion wrong: got %v want %v", got, b)
	}
}

func TestLimitClips(t *testing.T) {
	acc := []int32{math.MaxInt16 + 5000, math.MinInt16 - 5000, 0}
	got := Limit(acc)
	if got[0] != math.MaxInt16 || got[1] != math.MinInt16 || got[2] != 0 {
		t.Fatalf("Limit clip wrong: %v", got)
	}
}
