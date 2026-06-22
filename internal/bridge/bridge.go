package bridge

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Probe struct {
	name  string
	count int64
	sumNs int64
	maxNs int64
}

func NewProbe(name string) *Probe { return &Probe{name: name} }

func (p *Probe) observe(d time.Duration) {
	if p == nil {
		return
	}
	ns := d.Nanoseconds()
	atomic.AddInt64(&p.count, 1)
	atomic.AddInt64(&p.sumNs, ns)
	for {
		cur := atomic.LoadInt64(&p.maxNs)
		if ns <= cur || atomic.CompareAndSwapInt64(&p.maxNs, cur, ns) {
			break
		}
	}
}

func (p *Probe) Stats() (name string, count int64, avg, max time.Duration) {
	if p == nil {
		return "", 0, 0, 0
	}
	c := atomic.LoadInt64(&p.count)
	sum := atomic.LoadInt64(&p.sumNs)
	mx := atomic.LoadInt64(&p.maxNs)
	var a time.Duration
	if c > 0 {
		a = time.Duration(sum / c)
	}
	return p.name, c, a, time.Duration(mx)
}

func Pipe(ctx context.Context, src media.MediaSource, sink media.MediaSink, probe *Probe) {
	frames := src.Frames()
	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-frames:
			if !ok {
				return
			}
			t0 := time.Now()
			_ = sink.Write(f)
			probe.observe(time.Since(t0))
		}
	}
}

type Endpoint interface {
	Source() media.MediaSource
	Sink() media.MediaSink
}

func Connect(ctx context.Context, a, b Endpoint, upProbe, downProbe *Probe) {
	go Pipe(ctx, a.Source(), b.Sink(), upProbe)
	go Pipe(ctx, b.Source(), a.Sink(), downProbe)
}
