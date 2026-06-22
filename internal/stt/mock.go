package stt

import (
	"context"
	"sync"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type MockClient struct {
	mu      sync.Mutex
	streams []*MockStream
}

func NewMockClient() *MockClient { return &MockClient{} }

func (c *MockClient) Open(ctx context.Context) (Stream, error) {
	s := &MockStream{events: make(chan Transcript, 16)}
	c.mu.Lock()
	c.streams = append(c.streams, s)
	c.mu.Unlock()
	return s, nil
}

func (c *MockClient) Last() *MockStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.streams) == 0 {
		return nil
	}
	return c.streams[len(c.streams)-1]
}

type MockStream struct {
	mu       sync.Mutex
	events   chan Transcript
	sent     int
	closed   bool
	sendOnce sync.Once
}

func (s *MockStream) Send(p media.PCM) error {
	s.mu.Lock()
	s.sent++
	s.mu.Unlock()
	return nil
}

func (s *MockStream) Emit(t Transcript) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	s.events <- t
}

func (s *MockStream) SentFrames() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

func (s *MockStream) Events() <-chan Transcript { return s.events }

func (s *MockStream) CloseSend() error { return nil }

func (s *MockStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	return nil
}
