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

type Metrics struct {
	mu sync.Mutex
	m  map[string][]time.Duration
}

func New() *Metrics { return &Metrics{m: map[string][]time.Duration{}} }

func (x *Metrics) Record(name string, d time.Duration) {
	if x == nil {
		return
	}
	x.mu.Lock()
	x.m[name] = append(x.m[name], d)
	x.mu.Unlock()
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
		v := append([]time.Duration(nil), x.m[n]...)
		out = append(out, stat(n, v))
	}
	return out
}

func (x *Metrics) Text() string {
	var b strings.Builder
	for _, s := range x.Snapshot() {
		fmt.Fprintf(&b, "%-22s count=%d avg=%s p50=%s p95=%s max=%s\n",
			s.Name, s.Count, r(s.Avg), r(s.P50), r(s.P95), r(s.Max))
	}
	return b.String()
}

func stat(name string, v []time.Duration) Stat {
	n := len(v)
	if n == 0 {
		return Stat{Name: name}
	}
	slices.Sort(v)
	var sum time.Duration
	for _, d := range v {
		sum += d
	}
	return Stat{
		Name:  name,
		Count: n,
		Avg:   sum / time.Duration(n),
		P50:   v[n*50/100],
		P95:   v[min(n*95/100, n-1)],
		Max:   v[n-1],
	}
}

func r(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
