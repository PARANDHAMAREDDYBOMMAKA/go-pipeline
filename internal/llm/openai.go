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
	APIKey  string
	BaseURL string
	Model   string
	http    *http.Client
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
	Role    string `json:"role"`
	Content string `json:"content"`
}

type oaRequest struct {
	Model    string      `json:"model"`
	Messages []oaMessage `json:"messages"`
	Stream   bool        `json:"stream"`
}

type oaChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

func (c *OpenAIClient) Generate(ctx context.Context, messages []Message) (Stream, error) {
	reqBody := oaRequest{Model: c.Model, Stream: true}
	for _, m := range messages {
		reqBody.Messages = append(reqBody.Messages, oaMessage{Role: string(m.Role), Content: m.Content})
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.http.Do(req)
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
		text := ch.Choices[0].Delta.Content
		if text == "" {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case s.tokens <- Token{Text: text}:
		}
	}
	select {
	case <-ctx.Done():
	case s.tokens <- Token{Done: true}:
	}
}

func (s *openaiStream) Tokens() <-chan Token { return s.tokens }
func (s *openaiStream) Close() error {
	s.resp.Body.Close()
	return nil
}
