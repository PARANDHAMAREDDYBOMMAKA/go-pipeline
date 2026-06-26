package llm

import (
	"context"
	"sync"
)

type MockClient struct {
	mu       sync.Mutex
	tokens   []string
	delay    func()
	lastMsgs []Message
}

func NewMockClient(tokens []string) *MockClient {
	return &MockClient{tokens: tokens}
}

func (c *MockClient) SetDelay(f func()) { c.delay = f }

func (c *MockClient) LastMessages() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastMsgs
}

func (c *MockClient) Generate(ctx context.Context, req Request) (Stream, error) {
	c.mu.Lock()
	c.lastMsgs = req.Messages
	toks := c.tokens
	delay := c.delay
	c.mu.Unlock()

	out := make(chan Token, len(toks)+1)
	go func() {
		defer close(out)
		for _, t := range toks {
			if delay != nil {
				delay()
			}
			select {
			case <-ctx.Done():
				return
			case out <- Token{Text: t}:
			}
		}
		select {
		case <-ctx.Done():
		case out <- Token{Done: true}:
		}
	}()
	return &mockStream{tokens: out}, nil
}

type mockStream struct {
	tokens chan Token
}

func (s *mockStream) Tokens() <-chan Token { return s.tokens }
func (s *mockStream) Close() error         { return nil }
