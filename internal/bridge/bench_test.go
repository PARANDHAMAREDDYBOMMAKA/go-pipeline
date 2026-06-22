package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/transport"
)

func BenchmarkPipeForward(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := transport.NewFeedSource(2)
	out := transport.NewRecordSink()
	probe := NewProbe("bench")
	go Pipe(ctx, in, out, probe)

	f := frame(0, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Feed(f)
	}
	for len(out.Frames()) < 1 {
		time.Sleep(time.Microsecond)
	}
	b.StopTimer()

	_, count, avg, max := probe.Stats()
	b.ReportMetric(float64(avg.Nanoseconds()), "ns/forward-avg")
	b.ReportMetric(float64(max.Nanoseconds()), "ns/forward-max")
	_ = count
}
