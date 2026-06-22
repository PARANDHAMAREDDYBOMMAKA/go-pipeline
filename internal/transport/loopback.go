package transport

import (
	"context"
	"sync"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Loopback struct {
	src  *FeedSource
	sink *RecordSink
}

func NewLoopback(buffer int) *Loopback {
	return &Loopback{
		src:  NewFeedSource(buffer),
		sink: NewRecordSink(),
	}
}

func (l *Loopback) Attach(ctx context.Context) (media.MediaSource, media.MediaSink, error) {
	return l.src, l.sink, nil
}

func (l *Loopback) Capabilities() Caps {
	return Caps{Codecs: []Codec{CodecRawPCM}, ClockRate: media.BusSampleRate}
}

func (l *Loopback) Close() error {
	l.src.Close()
	return l.sink.Close()
}

func (l *Loopback) Source() *FeedSource { return l.src }

func (l *Loopback) Sink() *RecordSink { return l.sink }

type FeedSource struct {
	frames chan media.PCM
	vad    chan media.VADEvent
	once   sync.Once
}

func NewFeedSource(buffer int) *FeedSource {
	if buffer < 1 {
		buffer = 1
	}
	return &FeedSource{
		frames: make(chan media.PCM, buffer),
		vad:    make(chan media.VADEvent, 8),
	}
}

func (s *FeedSource) Feed(f media.PCM) {
	select {
	case s.frames <- f:
	default:
		select {
		case <-s.frames:
		default:
		}
		select {
		case s.frames <- f:
		default:
		}
	}
}

func (s *FeedSource) MarkSpeech(speaking bool) {
	select {
	case s.vad <- media.VADEvent{Speaking: speaking}:
	default:
	}
}

func (s *FeedSource) Frames() <-chan media.PCM              { return s.frames }
func (s *FeedSource) SpeechActivity() <-chan media.VADEvent { return s.vad }

func (s *FeedSource) Close() error {
	s.once.Do(func() { close(s.frames) })
	return nil
}

type RecordSink struct {
	mu     sync.Mutex
	frames []media.PCM
	clears int
	closed bool
}

func NewRecordSink() *RecordSink { return &RecordSink{} }

func (s *RecordSink) Write(p media.PCM) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.frames = append(s.frames, p.Clone())
	return nil
}

func (s *RecordSink) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clears++
	return nil
}

func (s *RecordSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *RecordSink) Frames() []media.PCM {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]media.PCM, len(s.frames))
	copy(out, s.frames)
	return out
}

func (s *RecordSink) Clears() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clears
}
