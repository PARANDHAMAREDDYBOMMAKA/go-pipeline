package agent

import (
	"sync"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type micSink struct {
	frames chan media.PCM
	mu     sync.Mutex
	closed bool
}

func newMicSink(buffer int) *micSink {
	if buffer < 1 {
		buffer = 1
	}
	return &micSink{frames: make(chan media.PCM, buffer)}
}

func (s *micSink) Write(p media.PCM) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	select {
	case s.frames <- p:
	default:
		select {
		case <-s.frames:
		default:
		}
		select {
		case s.frames <- p:
		default:
		}
	}
	return nil
}

func (s *micSink) Clear() error { return nil }

func (s *micSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.frames)
	}
	return nil
}

type voiceSource struct {
	frames chan media.PCM
	vad    chan media.VADEvent
	once   sync.Once
}

func newVoiceSource(buffer int) *voiceSource {
	if buffer < 1 {
		buffer = 1
	}
	return &voiceSource{
		frames: make(chan media.PCM, buffer),
		vad:    make(chan media.VADEvent),
	}
}

func (s *voiceSource) Frames() <-chan media.PCM              { return s.frames }
func (s *voiceSource) SpeechActivity() <-chan media.VADEvent { return s.vad }

func (s *voiceSource) Close() error {
	s.once.Do(func() { close(s.frames) })
	return nil
}

func (s *voiceSource) drain() {
	for {
		select {
		case <-s.frames:
		default:
			return
		}
	}
}
