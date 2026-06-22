package tts

import (
	"context"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type MockClient struct {
	FramesPerUtterance int
	Rate               int
}

func NewMockClient() *MockClient {
	return &MockClient{FramesPerUtterance: 5, Rate: media.BusSampleRate}
}

func (c *MockClient) Synthesize(ctx context.Context, text string, voice Voice) (Stream, error) {
	rate := c.Rate
	if rate == 0 {
		rate = media.BusSampleRate
	}
	n := media.SamplesPerFrame(rate)
	out := make(chan media.PCM, c.FramesPerUtterance)
	go func() {
		defer close(out)
		for i := 0; i < c.FramesPerUtterance; i++ {
			s := make([]int16, n)
			for j := range s {
				s[j] = int16(1000)
			}
			select {
			case <-ctx.Done():
				return
			case out <- media.PCM{Samples: s, SampleRate: rate}:
			}
		}
	}()
	return &mockStream{audio: out}, nil
}

type mockStream struct {
	audio chan media.PCM
}

func (s *mockStream) Audio() <-chan media.PCM { return s.audio }
func (s *mockStream) Close() error            { return nil }
