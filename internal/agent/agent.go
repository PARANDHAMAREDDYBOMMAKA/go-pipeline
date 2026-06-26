package agent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/obs"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/vad"
)

type State int

const (
	StateIdle State = iota
	StateListening
	StateThinking
	StateSpeaking
)

func (s State) String() string {
	switch s {
	case StateListening:
		return "listening"
	case StateThinking:
		return "thinking"
	case StateSpeaking:
		return "speaking"
	default:
		return "idle"
	}
}

type ToolHandler func(ctx context.Context, name, args string) (string, error)

type Config struct {
	SystemPrompt      string
	FirstMessage      string
	Voice             tts.Voice
	MinSentenceCh     int
	SynthAhead        int
	BargeInFrames     int
	MicBuffer         int
	SynthBuffer       int
	VADThreshold      float64
	VADStartFrames    int
	VADHangoverFrames int
	EndpointGraceMs   int
	Temperature       *float64
	MaxTokens         int
	Tools             []llm.Tool
	ToolHandler       ToolHandler
	MaxToolIters      int
	Metrics           *obs.Metrics
}

func (c *Config) withDefaults() {
	if c.MinSentenceCh == 0 {
		c.MinSentenceCh = 12
	}
	if c.SynthAhead == 0 {
		c.SynthAhead = 2
	}
	if c.BargeInFrames == 0 {
		c.BargeInFrames = 3
	}
	if c.MicBuffer == 0 {
		c.MicBuffer = 8
	}
	if c.SynthBuffer == 0 {
		c.SynthBuffer = 50
	}
	if c.VADThreshold == 0 {
		c.VADThreshold = 500
	}
	if c.VADStartFrames == 0 {
		c.VADStartFrames = 2
	}
	if c.VADHangoverFrames == 0 {
		c.VADHangoverFrames = 25
	}
	if c.MaxToolIters == 0 {
		c.MaxToolIters = 4
	}
}

type Agent struct {
	cfg Config

	stt        stt.Client
	llm        llm.Client
	tts        tts.Client
	ttsSession tts.Session
	vad        vad.Detector

	sink *micSink
	src  *voiceSource

	turns      chan string
	userSpeech chan bool

	metrics *obs.Metrics

	mu              sync.Mutex
	state           State
	history         []llm.Message
	bargeIn         func()
	endpointPending bool
	committed       bool
	speechEndAt     time.Time
	turnStart       time.Time
	turnFirstAudio  bool
}

func New(cfg Config, sttc stt.Client, llmc llm.Client, ttsc tts.Client) *Agent {
	cfg.withDefaults()
	m := cfg.Metrics
	if m == nil {
		m = obs.New()
	}
	a := &Agent{
		cfg:        cfg,
		stt:        sttc,
		llm:        llmc,
		tts:        ttsc,
		metrics:    m,
		vad:        vad.NewEnergyVAD(cfg.VADThreshold, cfg.VADStartFrames, cfg.VADHangoverFrames),
		sink:       newMicSink(cfg.MicBuffer),
		src:        newVoiceSource(cfg.SynthBuffer),
		turns:      make(chan string, 4),
		userSpeech: make(chan bool, 16),
	}
	a.history = []llm.Message{{Role: llm.RoleSystem, Content: cfg.SystemPrompt}}
	if cfg.FirstMessage != "" {
		a.history = append(a.history, llm.Message{Role: llm.RoleAssistant, Content: cfg.FirstMessage})
	}
	return a
}

func (a *Agent) Source() media.MediaSource { return a.src }
func (a *Agent) Sink() media.MediaSink     { return a.sink }
func (a *Agent) Metrics() *obs.Metrics     { return a.metrics }

func (a *Agent) SetBargeInHook(f func()) {
	a.mu.Lock()
	a.bargeIn = f
	a.mu.Unlock()
}

func (a *Agent) State() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

func (a *Agent) setState(s State) {
	a.mu.Lock()
	a.state = s
	a.mu.Unlock()
}

func (a *Agent) History() []llm.Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]llm.Message, len(a.history))
	copy(out, a.history)
	return out
}

func (a *Agent) appendHistory(m llm.Message) {
	a.mu.Lock()
	a.history = append(a.history, m)
	a.mu.Unlock()
}

func (a *Agent) Run(ctx context.Context) error {
	sttStream, err := a.stt.Open(ctx)
	if err != nil {
		return err
	}
	defer sttStream.Close()

	if sc, ok := a.tts.(tts.SessionClient); ok {
		if sess, serr := sc.OpenSession(ctx, a.cfg.Voice); serr == nil {
			a.mu.Lock()
			a.ttsSession = sess
			a.mu.Unlock()
			defer sess.Close()
		}
	}

	a.setState(StateListening)
	transcripts := &transcriptBuffer{}

	go a.readTranscripts(ctx, sttStream, transcripts)
	go a.ingest(ctx, sttStream, transcripts)

	if first := a.firstMessage(); first != "" {
		a.speak(ctx, first)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case text := <-a.turns:
			if text == "" {
				continue
			}
			a.handleTurn(ctx, text)
			a.setState(StateListening)
		}
	}
}

func (a *Agent) firstMessage() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.history) >= 2 && a.history[1].Role == llm.RoleAssistant {
		return a.history[1].Content
	}
	return ""
}

func (a *Agent) ingest(ctx context.Context, sttStream stt.Stream, tb *transcriptBuffer) {
	frames := a.sink.frames
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-frames:
			if !ok {
				return
			}
			_ = sttStream.Send(p)
			changed, speaking := a.vad.Push(p)
			if changed {
				select {
				case a.userSpeech <- speaking:
				default:
				}
				if speaking {
					a.mu.Lock()
					a.endpointPending = false
					a.committed = false
					a.speechEndAt = time.Time{}
					a.mu.Unlock()
				} else {
					a.mu.Lock()
					a.speechEndAt = time.Now()
					a.mu.Unlock()
					a.scheduleEndpoint(tb)
				}
			}
		}
	}
}

func (a *Agent) readTranscripts(ctx context.Context, sttStream stt.Stream, tb *transcriptBuffer) {
	events := sttStream.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-events:
			if !ok {
				return
			}
			if t.Final && t.Text != "" {
				tb.append(t.Text)
			}
			if t.EndOfTurn {
				a.endpoint(tb)
			} else if t.Final {
				a.tryEndpoint(tb)
			}
		}
	}
}

func (a *Agent) scheduleEndpoint(tb *transcriptBuffer) {
	grace := a.cfg.EndpointGraceMs
	if grace <= 0 {
		a.endpoint(tb)
		return
	}
	a.mu.Lock()
	if a.committed {
		a.mu.Unlock()
		return
	}
	a.endpointPending = true
	a.mu.Unlock()
	time.AfterFunc(time.Duration(grace)*time.Millisecond, func() { a.tryEndpoint(tb) })
}

func (a *Agent) endpoint(tb *transcriptBuffer) {
	a.mu.Lock()
	if a.committed {
		a.mu.Unlock()
		return
	}
	a.endpointPending = true
	a.mu.Unlock()
	a.tryEndpoint(tb)
}

func (a *Agent) tryEndpoint(tb *transcriptBuffer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.endpointPending || a.committed {
		return
	}
	text := tb.take()
	if text == "" {
		return
	}
	a.endpointPending = false
	a.committed = true
	if !a.speechEndAt.IsZero() {
		a.metrics.Record(obs.EndpointWait, time.Since(a.speechEndAt))
	}
	select {
	case a.turns <- text:
	default:
	}
}

func (a *Agent) handleTurn(ctx context.Context, userText string) {
	a.setState(StateThinking)

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.watchBargeIn(turnCtx, cancel)

	a.mu.Lock()
	a.turnStart = time.Now()
	a.turnFirstAudio = false
	a.history = append(a.history, llm.Message{Role: llm.RoleUser, Content: userText})
	a.mu.Unlock()

	ahead := a.cfg.SynthAhead
	if ahead < 1 {
		ahead = 1
	}
	jobs := make(chan *ttsJob, ahead)
	done := make(chan struct{})
	go a.player(turnCtx, jobs, done)

	emit := func(frag string) bool {
		job := a.startSynth(turnCtx, frag)
		select {
		case jobs <- job:
			return true
		case <-turnCtx.Done():
			return false
		}
	}

	for iter := 0; iter < a.cfg.MaxToolIters; iter++ {
		req := llm.Request{
			Messages:    a.History(),
			Tools:       a.cfg.Tools,
			Temperature: a.cfg.Temperature,
			MaxTokens:   a.cfg.MaxTokens,
		}
		llmReqAt := time.Now()
		stream, err := a.llm.Generate(turnCtx, req)
		if err != nil {
			break
		}

		sent := llm.NewSentencizer(a.cfg.MinSentenceCh)
		var reply strings.Builder
		var toolCalls []llm.ToolCall
		ttftDone := false

		for tok := range stream.Tokens() {
			if tok.Done {
				break
			}
			if tok.ToolCall != nil {
				toolCalls = append(toolCalls, *tok.ToolCall)
				continue
			}
			if tok.Text == "" {
				continue
			}
			if !ttftDone {
				ttftDone = true
				a.metrics.Record(obs.LLMTTFT, time.Since(llmReqAt))
			}
			reply.WriteString(tok.Text)
			for _, frag := range sent.Push(tok.Text) {
				if !emit(frag) {
					break
				}
			}
			if turnCtx.Err() != nil {
				break
			}
		}
		stream.Close()
		if turnCtx.Err() == nil {
			if rem := sent.Flush(); rem != "" {
				emit(rem)
			}
		}

		replyText := reply.String()
		if len(toolCalls) > 0 && a.cfg.ToolHandler != nil && turnCtx.Err() == nil {
			a.appendHistory(llm.Message{Role: llm.RoleAssistant, Content: replyText, ToolCalls: toolCalls})
			for _, tc := range toolCalls {
				result, terr := a.cfg.ToolHandler(turnCtx, tc.Name, tc.Arguments)
				if terr != nil {
					result = "error: " + terr.Error()
				}
				a.appendHistory(llm.Message{Role: llm.RoleTool, Content: result, ToolCallID: tc.ID, Name: tc.Name})
			}
			continue
		}
		if replyText != "" {
			a.appendHistory(llm.Message{Role: llm.RoleAssistant, Content: replyText})
		}
		break
	}

	close(jobs)
	<-done
}

type ttsJob struct {
	out     <-chan media.PCM
	synthAt time.Time
}

func (a *Agent) startSynth(ctx context.Context, text string) *ttsJob {
	out := make(chan media.PCM, 64)
	job := &ttsJob{out: out, synthAt: time.Now()}
	go func() {
		defer close(out)
		a.mu.Lock()
		sess := a.ttsSession
		a.mu.Unlock()

		var st tts.Stream
		var err error
		if sess != nil {
			st, err = sess.Synthesize(ctx, text)
		} else {
			st, err = a.tts.Synthesize(ctx, text, a.cfg.Voice)
		}
		if err != nil {
			return
		}
		defer st.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-st.Audio():
				if !ok {
					return
				}
				if f.SampleRate != 0 && f.SampleRate != media.BusSampleRate {
					f = media.ResampleFrame(f, media.BusSampleRate)
				}
				select {
				case <-ctx.Done():
					return
				case out <- f:
				}
			}
		}
	}()
	return job
}

func (a *Agent) player(ctx context.Context, jobs <-chan *ttsJob, done chan<- struct{}) {
	defer close(done)
	for job := range jobs {
		a.drainJob(ctx, job)
	}
}

func (a *Agent) drainJob(ctx context.Context, job *ttsJob) {
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-job.out:
			if !ok {
				return
			}
			if first {
				first = false
				a.metrics.Record(obs.TTSTTFB, time.Since(job.synthAt))
				a.recordFirstAudio()
			}
			a.setState(StateSpeaking)
			select {
			case <-ctx.Done():
				return
			case a.src.frames <- f:
			}
		}
	}
}

func (a *Agent) speak(ctx context.Context, text string) {
	a.drainJob(ctx, a.startSynth(ctx, text))
}

func (a *Agent) watchBargeIn(turnCtx context.Context, cancel context.CancelFunc) {
	go func() {
		for {
			select {
			case <-turnCtx.Done():
				return
			case sp := <-a.userSpeech:
				if sp && a.State() == StateSpeaking {
					cancel()
					a.fireBargeIn()
					return
				}
			}
		}
	}()
}

func (a *Agent) recordFirstAudio() {
	a.mu.Lock()
	if a.turnFirstAudio || a.turnStart.IsZero() {
		a.mu.Unlock()
		return
	}
	a.turnFirstAudio = true
	start := a.turnStart
	a.mu.Unlock()
	a.metrics.Record(obs.TurnLatency, time.Since(start))
}

func (a *Agent) fireBargeIn() {
	a.src.drain()
	a.mu.Lock()
	hook := a.bargeIn
	a.mu.Unlock()
	if hook != nil {
		hook()
	}
}

type transcriptBuffer struct {
	mu   sync.Mutex
	text string
	at   time.Time
}

func (t *transcriptBuffer) append(s string) {
	t.mu.Lock()
	if t.text != "" {
		t.text += " "
	}
	t.text += s
	t.at = time.Now()
	t.mu.Unlock()
}

func (t *transcriptBuffer) take() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.text
	t.text = ""
	return s
}
