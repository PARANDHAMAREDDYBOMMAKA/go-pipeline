package llm

import (
	"context"
	"sync"
)

type ScriptedResponse struct {
	Tokens    []string
	ToolCalls []ToolCall
}

type ScriptedClient struct {
	mu        sync.Mutex
	responses []ScriptedResponse
	calls     int
	requests  []Request
}

func NewScriptedClient(responses ...ScriptedResponse) *ScriptedClient {
	return &ScriptedClient{responses: responses}
}

func (c *ScriptedClient) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *ScriptedClient) Requests() []Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Request, len(c.requests))
	copy(out, c.requests)
	return out
}

func (c *ScriptedClient) Generate(ctx context.Context, req Request) (Stream, error) {
	c.mu.Lock()
	idx := c.calls
	c.calls++
	c.requests = append(c.requests, req)
	var resp ScriptedResponse
	if idx < len(c.responses) {
		resp = c.responses[idx]
	}
	c.mu.Unlock()

	out := make(chan Token, len(resp.Tokens)+len(resp.ToolCalls)+1)
	go func() {
		defer close(out)
		for _, t := range resp.Tokens {
			select {
			case <-ctx.Done():
				return
			case out <- Token{Text: t}:
			}
		}
		for i := range resp.ToolCalls {
			tc := resp.ToolCalls[i]
			select {
			case <-ctx.Done():
				return
			case out <- Token{ToolCall: &tc}:
			}
		}
		select {
		case <-ctx.Done():
		case out <- Token{Done: true}:
		}
	}()
	return &mockStream{tokens: out}, nil
}
