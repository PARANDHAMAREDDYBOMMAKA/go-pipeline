package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
)

func TestAgentToolCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sttc := stt.NewMockClient()
	llmc := llm.NewScriptedClient(
		llm.ScriptedResponse{ToolCalls: []llm.ToolCall{{ID: "call_1", Name: "get_time", Arguments: `{"tz":"utc"}`}}},
		llm.ScriptedResponse{Tokens: []string{"It is noon."}},
	)
	ttsc := tts.NewMockClient()

	var handled int32
	var gotArgs atomic.Value
	gotArgs.Store("")

	a := New(Config{
		SystemPrompt:      "bot",
		MinSentenceCh:     3,
		VADStartFrames:    2,
		VADHangoverFrames: 3,
		Tools:             []llm.Tool{{Name: "get_time", Description: "current time"}},
		ToolHandler: func(_ context.Context, name, args string) (string, error) {
			if name == "get_time" {
				atomic.AddInt32(&handled, 1)
				gotArgs.Store(args)
			}
			return "12:00 UTC", nil
		},
	}, sttc, llmc, ttsc)

	go func() { _ = a.Run(ctx) }()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.Source().Frames():
			}
		}
	}()

	sttStream := waitSTT(t, sttc)
	pushN(a, loud(0), 3)
	sttStream.Emit(stt.Transcript{Text: "what time is it", Final: true})
	pushN(a, quiet(), 4)

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&handled) == 0 {
		select {
		case <-deadline:
			t.Fatal("tool handler never fired")
		case <-time.After(2 * time.Millisecond):
		}
	}

	if got := gotArgs.Load().(string); got != `{"tz":"utc"}` {
		t.Fatalf("tool args mismatch: %q", got)
	}

	deadline = time.After(2 * time.Second)
	for {
		var hasToolCall, hasToolResult, hasFinal bool
		for _, m := range a.History() {
			if m.Role == llm.RoleAssistant && len(m.ToolCalls) == 1 && m.ToolCalls[0].Name == "get_time" {
				hasToolCall = true
			}
			if m.Role == llm.RoleTool && m.ToolCallID == "call_1" && m.Content == "12:00 UTC" {
				hasToolResult = true
			}
			if m.Role == llm.RoleAssistant && m.Content == "It is noon." {
				hasFinal = true
			}
		}
		if hasToolCall && hasToolResult && hasFinal {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("history missing tool flow: %+v", a.History())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if calls := llmc.Calls(); calls != 2 {
		t.Fatalf("expected 2 llm calls (tool + final), got %d", calls)
	}
	reqs := llmc.Requests()
	if len(reqs) == 0 || len(reqs[0].Tools) != 1 {
		t.Fatalf("tools not forwarded to llm: %+v", reqs)
	}
}

func TestAgentEndpointGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sttc := stt.NewMockClient()
	llmc := llm.NewMockClient([]string{"ok. "})
	ttsc := tts.NewMockClient()

	a := New(Config{
		SystemPrompt:      "bot",
		MinSentenceCh:     2,
		VADStartFrames:    2,
		VADHangoverFrames: 3,
		EndpointGraceMs:   80,
	}, sttc, llmc, ttsc)

	go func() { _ = a.Run(ctx) }()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.Source().Frames():
			}
		}
	}()

	sttStream := waitSTT(t, sttc)
	pushN(a, loud(0), 3)
	sttStream.Emit(stt.Transcript{Text: "hello there", Final: true})
	pushN(a, quiet(), 4)

	deadline := time.After(2 * time.Second)
	for {
		found := false
		for _, m := range a.History() {
			if m.Role == llm.RoleUser && m.Content == "hello there" {
				found = true
			}
		}
		if found {
			break
		}
		select {
		case <-deadline:
			t.Fatal("grace endpoint never committed the turn")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
