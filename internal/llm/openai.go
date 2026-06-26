package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/httpx"
)

type OpenAIClient struct {
	APIKey      string
	BaseURL     string
	Model       string
	Temperature *float64
	MaxTokens   int
	http        *http.Client
}

func NewOpenAIClient(apiKey, baseURL, model string) *OpenAIClient {
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &OpenAIClient{APIKey: apiKey, BaseURL: strings.TrimRight(baseURL, "/"), Model: model, http: httpx.Shared()}
}

type oaMessage struct {
	Role       string       `json:"role"`
	Content    string       `json:"content"`
	ToolCalls  []oaToolCall `json:"tool_calls,omitempty"`
	ToolCallID string       `json:"tool_call_id,omitempty"`
	Name       string       `json:"name,omitempty"`
}

type oaToolCall struct {
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Index    int    `json:"index,omitempty"`
	Function struct {
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
	} `json:"function"`
}

type oaTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

type oaRequest struct {
	Model       string      `json:"model"`
	Messages    []oaMessage `json:"messages"`
	Stream      bool        `json:"stream"`
	Tools       []oaTool    `json:"tools,omitempty"`
	Temperature *float64    `json:"temperature,omitempty"`
	MaxTokens   int         `json:"max_tokens,omitempty"`
}

type oaChunk struct {
	Choices []struct {
		Delta struct {
			Content   string       `json:"content"`
			ToolCalls []oaToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

func (c *OpenAIClient) Generate(ctx context.Context, req Request) (Stream, error) {
	reqBody := oaRequest{Model: c.Model, Stream: true, MaxTokens: c.MaxTokens, Temperature: c.Temperature}
	if req.Temperature != nil {
		reqBody.Temperature = req.Temperature
	}
	if req.MaxTokens > 0 {
		reqBody.MaxTokens = req.MaxTokens
	}
	for _, m := range req.Messages {
		om := oaMessage{Role: string(m.Role), Content: m.Content, ToolCallID: m.ToolCallID, Name: m.Name}
		for _, tc := range m.ToolCalls {
			var otc oaToolCall
			otc.ID = tc.ID
			otc.Type = "function"
			otc.Function.Name = tc.Name
			otc.Function.Arguments = tc.Arguments
			om.ToolCalls = append(om.ToolCalls, otc)
		}
		reqBody.Messages = append(reqBody.Messages, om)
	}
	for _, t := range req.Tools {
		var ot oaTool
		ot.Type = "function"
		ot.Function.Name = t.Name
		ot.Function.Description = t.Description
		ot.Function.Parameters = t.Parameters
		reqBody.Tools = append(reqBody.Tools, ot)
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("openai: status %d", resp.StatusCode)
	}

	out := make(chan Token, 64)
	s := &openaiStream{tokens: out, resp: resp}
	go s.read(ctx)
	return s, nil
}

type openaiStream struct {
	tokens chan Token
	resp   *http.Response
}

func (s *openaiStream) read(ctx context.Context) {
	defer close(s.tokens)
	defer s.resp.Body.Close()

	pending := map[int]*ToolCall{}
	var order []int

	emit := func(t Token) bool {
		select {
		case <-ctx.Done():
			return false
		case s.tokens <- t:
			return true
		}
	}

	scanner := bufio.NewScanner(s.resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		var ch oaChunk
		if json.Unmarshal([]byte(payload), &ch) != nil || len(ch.Choices) == 0 {
			continue
		}
		choice := ch.Choices[0]
		for _, tc := range choice.Delta.ToolCalls {
			acc, ok := pending[tc.Index]
			if !ok {
				acc = &ToolCall{}
				pending[tc.Index] = acc
				order = append(order, tc.Index)
			}
			if tc.ID != "" {
				acc.ID = tc.ID
			}
			if tc.Function.Name != "" {
				acc.Name = tc.Function.Name
			}
			acc.Arguments += tc.Function.Arguments
		}
		if choice.Delta.Content != "" {
			if !emit(Token{Text: choice.Delta.Content}) {
				return
			}
		}
		if choice.FinishReason != nil && *choice.FinishReason == "tool_calls" {
			break
		}
	}

	for _, idx := range order {
		tc := pending[idx]
		if tc.Name == "" {
			continue
		}
		if !emit(Token{ToolCall: tc}) {
			return
		}
	}
	emit(Token{Done: true})
}

func (s *openaiStream) Tokens() <-chan Token { return s.tokens }
func (s *openaiStream) Close() error {
	s.resp.Body.Close()
	return nil
}
