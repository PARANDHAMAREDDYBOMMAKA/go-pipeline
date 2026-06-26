package llm

import (
	"context"
	"encoding/json"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

type Message struct {
	Role       Role
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
	Name       string
}

type Tool struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

type Request struct {
	Messages    []Message
	Tools       []Tool
	Temperature *float64
	MaxTokens   int
}

type Token struct {
	Text     string
	ToolCall *ToolCall
	Done     bool
}

type Stream interface {
	Tokens() <-chan Token
	Close() error
}

type Client interface {
	Generate(ctx context.Context, req Request) (Stream, error)
}
