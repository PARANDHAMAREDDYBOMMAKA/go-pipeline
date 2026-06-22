package llm

import "context"

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

type Message struct {
	Role    Role
	Content string
}

type Token struct {
	Text string
	Done bool
}

type Stream interface {
	Tokens() <-chan Token
	Close() error
}

type Client interface {
	Generate(ctx context.Context, messages []Message) (Stream, error)
}
