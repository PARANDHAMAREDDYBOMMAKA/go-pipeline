package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/agent"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/bridge"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/transport/telephony"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
	"github.com/coder/websocket"
)

func mulawMedia(v int16) []byte {
	n := media.SamplesPerFrame(8000)
	pcm := make([]int16, n)
	for i := range pcm {
		pcm[i] = v
	}
	payload := base64.StdEncoding.EncodeToString(media.EncodeULawFrame(pcm))
	b, _ := json.Marshal(map[string]any{
		"event": "media",
		"media": map[string]string{"track": "inbound", "payload": payload},
	})
	return b
}

func TestFullCallFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sttMock := stt.NewMockClient()
	longReply := make([]string, 30)
	for i := range longReply {
		longReply[i] = "word. "
	}
	llmMock := llm.NewMockClient(longReply)
	llmMock.SetDelay(func() { time.Sleep(12 * time.Millisecond) })
	ttsMock := tts.NewMockClient()

	var ag *agent.Agent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		callCtx, callCancel := context.WithCancel(ctx)
		defer callCancel()

		tw := telephony.NewTwilioTransport(conn)
		src, sink, _ := tw.Attach(callCtx)
		go func() { <-tw.Done(); callCancel() }()

		if _, err := tw.Meta(callCtx); err != nil {
			return
		}

		ag = agent.New(agent.Config{
			SystemPrompt:      "bot",
			FirstMessage:      "Hello there, how can I help?",
			MinSentenceCh:     4,
			VADStartFrames:    2,
			VADHangoverFrames: 3,
		}, sttMock, llmMock, ttsMock)
		ag.SetBargeInHook(func() { _ = sink.Clear() })

		go bridge.Pipe(callCtx, src, ag.Sink(), nil)
		go bridge.Pipe(callCtx, ag.Source(), sink, nil)
		_ = ag.Run(callCtx)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.Close(websocket.StatusNormalClosure, "")

	var mediaOut int64
	var clears int64
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var e struct {
				Event string `json:"event"`
			}
			if json.Unmarshal(data, &e) != nil {
				continue
			}
			switch e.Event {
			case "media":
				atomic.AddInt64(&mediaOut, 1)
			case "clear":
				atomic.AddInt64(&clears, 1)
			}
		}
	}()

	send := func(b []byte) { _ = conn.Write(ctx, websocket.MessageText, b) }

	send([]byte(`{"event":"connected","protocol":"Call","version":"1.0.0"}`))
	send([]byte(`{"event":"start","streamSid":"MZtest","start":{"streamSid":"MZtest","accountSid":"ACtest","callSid":"CAtest","tracks":["inbound"],"mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1},"customParameters":{"from":"+15550001111","to":"+15550002222"}}}`))

	t.Log("step 1: expect greeting audio flowing back to caller")
	waitFor(t, "greeting media", 3*time.Second, func() bool { return atomic.LoadInt64(&mediaOut) >= 1 })
	greetingFrames := atomic.LoadInt64(&mediaOut)

	t.Log("step 2: simulate a user turn (speak -> transcript -> silence endpoint)")
	sttStream := waitSTT(t, sttMock)
	for i := 0; i < 3; i++ {
		send(mulawMedia(9000))
		time.Sleep(3 * time.Millisecond)
	}
	sttStream.Emit(stt.Transcript{Text: "tell me a story", Final: true})
	for i := 0; i < 4; i++ {
		send(mulawMedia(0))
		time.Sleep(3 * time.Millisecond)
	}

	t.Log("step 3: expect the agent's reply audio flowing back")
	waitFor(t, "reply media", 4*time.Second, func() bool {
		return atomic.LoadInt64(&mediaOut) > greetingFrames+2
	})

	waitFor(t, "agent speaking", 2*time.Second, func() bool {
		return ag != nil && ag.State() == agent.StateSpeaking
	})

	t.Log("step 4: barge-in - caller talks over the agent, expect a Twilio 'clear'")
	for i := 0; i < 4; i++ {
		send(mulawMedia(9000))
		time.Sleep(3 * time.Millisecond)
	}
	waitFor(t, "barge-in clear", 3*time.Second, func() bool { return atomic.LoadInt64(&clears) >= 1 })

	t.Log("step 5: verify conversation history captured the turn")
	waitFor(t, "history", 2*time.Second, func() bool {
		if ag == nil {
			return false
		}
		hasUser, hasAssistant := false, false
		for _, m := range ag.History() {
			if m.Role == llm.RoleUser && m.Content == "tell me a story" {
				hasUser = true
			}
			if m.Role == llm.RoleAssistant && m.Content != "Hello there, how can I help?" {
				hasAssistant = true
			}
		}
		return hasUser && hasAssistant
	})

	t.Logf("flow ok: greeting+reply media frames=%d, barge-in clears=%d",
		atomic.LoadInt64(&mediaOut), atomic.LoadInt64(&clears))
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for: %s", what)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func waitSTT(t *testing.T, m *stt.MockClient) *stt.MockStream {
	t.Helper()
	for i := 0; i < 1500; i++ {
		if s := m.Last(); s != nil {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stt stream never opened")
	return nil
}
