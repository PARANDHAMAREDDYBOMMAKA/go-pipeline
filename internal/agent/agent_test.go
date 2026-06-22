package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
)

func loud(n int) media.PCM {
	s := make([]int16, media.SamplesPerFrame(media.BusSampleRate))
	for i := range s {
		s[i] = 9000
	}
	return media.PCM{Samples: s, SampleRate: media.BusSampleRate}
}

func quiet() media.PCM {
	return media.PCM{Samples: make([]int16, media.SamplesPerFrame(media.BusSampleRate)), SampleRate: media.BusSampleRate}
}

func pushN(a *Agent, p media.PCM, n int) {
	for i := 0; i < n; i++ {
		_ = a.Sink().Write(p)
		time.Sleep(time.Millisecond)
	}
}

func waitSTT(t *testing.T, m *stt.MockClient) *stt.MockStream {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if s := m.Last(); s != nil {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stt stream never opened")
	return nil
}

func TestAgentCleanTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sttc := stt.NewMockClient()
	llmc := llm.NewMockClient([]string{"Hello there. ", "How can I help?"})
	ttsc := tts.NewMockClient()

	a := New(Config{
		SystemPrompt:      "you are a test bot",
		MinSentenceCh:     5,
		VADStartFrames:    2,
		VADHangoverFrames: 3,
	}, sttc, llmc, ttsc)

	go func() { _ = a.Run(ctx) }()

	sttStream := waitSTT(t, sttc)

	pushN(a, loud(0), 3)
	sttStream.Emit(stt.Transcript{Text: "hello", Final: true})
	pushN(a, quiet(), 4)

	got := 0
	timeout := time.After(2 * time.Second)
	for got < 3 {
		select {
		case <-a.Source().Frames():
			got++
		case <-timeout:
			t.Fatalf("only got %d synth frames", got)
		}
	}

	deadline := time.After(time.Second)
	for {
		h := a.History()
		hasUser, hasAssistant := false, false
		for _, m := range h {
			if m.Role == llm.RoleUser && m.Content == "hello" {
				hasUser = true
			}
			if m.Role == llm.RoleAssistant {
				hasAssistant = true
			}
		}
		if hasUser && hasAssistant {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("history missing turn: %+v", h)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestAgentBargeIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	toks := make([]string, 40)
	for i := range toks {
		toks[i] = "word. "
	}
	sttc := stt.NewMockClient()
	llmc := llm.NewMockClient(toks)
	llmc.SetDelay(func() { time.Sleep(15 * time.Millisecond) })
	ttsc := tts.NewMockClient()

	a := New(Config{
		SystemPrompt:      "bot",
		MinSentenceCh:     3,
		VADStartFrames:    2,
		VADHangoverFrames: 3,
	}, sttc, llmc, ttsc)

	var barged int32
	a.SetBargeInHook(func() { atomic.AddInt32(&barged, 1) })

	go func() { _ = a.Run(ctx) }()
	sttStream := waitSTT(t, sttc)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.Source().Frames():
			}
		}
	}()

	pushN(a, loud(0), 3)
	sttStream.Emit(stt.Transcript{Text: "tell me a long story", Final: true})
	pushN(a, quiet(), 4)

	for i := 0; i < 500; i++ {
		if a.State() == StateSpeaking {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if a.State() != StateSpeaking {
		t.Fatalf("agent never reached speaking, state=%s", a.State())
	}

	pushN(a, loud(0), 4)

	for i := 0; i < 1000; i++ {
		if atomic.LoadInt32(&barged) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&barged) == 0 {
		t.Fatal("barge-in hook never fired")
	}
}
