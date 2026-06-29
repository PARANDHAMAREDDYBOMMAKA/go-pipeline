package obs

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	TurnLatency  = "turn_response_latency"
	LLMTTFT      = "llm_ttft"
	TTSTTFB      = "tts_ttfb"
	EndpointWait = "endpoint_wait"
)

const (
	STTReconnect = "stt_reconnect"
	LLMError     = "llm_error"
	TTSError     = "tts_error"
	LLMFallback  = "llm_fallback"
	TTSFallback  = "tts_fallback"
)

const maxSamples = 2048

type sample struct {
	buf   []time.Duration
	total int
}

func (s *sample) add(d time.Duration) {
	s.total++
	if len(s.buf) < maxSamples {
		s.buf = append(s.buf, d)
		return
	}
	s.buf[s.total%maxSamples] = d
}

type Metrics struct {
	mu sync.Mutex
	m  map[string]*sample
	c  map[string]int64
}

func New() *Metrics {
	return &Metrics{m: map[string]*sample{}, c: map[string]int64{}}
}

func (x *Metrics) Record(name string, d time.Duration) {
	if x == nil {
		return
	}
	x.mu.Lock()
	s := x.m[name]
	if s == nil {
		s = &sample{}
		x.m[name] = s
	}
	s.add(d)
	x.mu.Unlock()
}

func (x *Metrics) Inc(name string) {
	if x == nil {
		return
	}
	x.mu.Lock()
	x.c[name]++
	x.mu.Unlock()
}

func (x *Metrics) Count(name string) int64 {
	if x == nil {
		return 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.c[name]
}

type Stat struct {
	Name               string
	Count              int
	Avg, P50, P95, Max time.Duration
}

func (x *Metrics) Snapshot() []Stat {
	if x == nil {
		return nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	names := make([]string, 0, len(x.m))
	for n := range x.m {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Stat, 0, len(names))
	for _, n := range names {
		s := x.m[n]
		v := append([]time.Duration(nil), s.buf...)
		out = append(out, stat(n, v, s.total))
	}
	return out
}

func (x *Metrics) Counters() map[string]int64 {
	if x == nil {
		return nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make(map[string]int64, len(x.c))
	for k, v := range x.c {
		out[k] = v
	}
	return out
}

func (x *Metrics) Text() string {
	var b strings.Builder
	for _, s := range x.Snapshot() {
		fmt.Fprintf(&b, "%-22s count=%d avg=%s p50=%s p95=%s max=%s\n",
			s.Name, s.Count, r(s.Avg), r(s.P50), r(s.P95), r(s.Max))
	}
	counters := x.Counters()
	names := make([]string, 0, len(counters))
	for n := range counters {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(&b, "%-22s count=%d\n", n, counters[n])
	}
	return b.String()
}

func stat(name string, v []time.Duration, total int) Stat {
	n := len(v)
	if n == 0 {
		return Stat{Name: name, Count: total}
	}
	slices.Sort(v)
	var sum time.Duration
	for _, d := range v {
		sum += d
	}
	return Stat{
		Name:  name,
		Count: total,
		Avg:   sum / time.Duration(n),
		P50:   v[n*50/100],
		P95:   v[min(n*95/100, n-1)],
		Max:   v[n-1],
	}
}

func r(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
