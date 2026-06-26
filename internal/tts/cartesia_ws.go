package tts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/coder/websocket"
)

type CartesiaWSClient struct {
	APIKey  string
	Model   string
	Version string
	Rate    int
}

func NewCartesiaWSClient(apiKey, model, version string, rate int) *CartesiaWSClient {
	if model == "" {
		model = "sonic-2"
	}
	if version == "" {
		version = "2024-06-10"
	}
	if rate <= 0 {
		rate = defaultCartesiaRate
	}
	return &CartesiaWSClient{APIKey: apiKey, Model: model, Version: version, Rate: rate}
}

func (c *CartesiaWSClient) Synthesize(ctx context.Context, text string, voice Voice) (Stream, error) {
	sess, err := c.OpenSession(ctx, voice)
	if err != nil {
		return nil, err
	}
	st, err := sess.Synthesize(ctx, text)
	if err != nil {
		sess.Close()
		return nil, err
	}
	return &closingStream{Stream: st, sess: sess}, nil
}

type closingStream struct {
	Stream
	sess Session
}

func (s *closingStream) Close() error {
	err := s.Stream.Close()
	s.sess.Close()
	return err
}

func (c *CartesiaWSClient) OpenSession(ctx context.Context, voice Voice) (Session, error) {
	q := url.Values{}
	q.Set("api_key", c.APIKey)
	q.Set("cartesia_version", c.Version)
	endpoint := "wss://api.cartesia.ai/tts/websocket?" + q.Encode()

	conn, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(8 << 20)

	s := &cartesiaSession{
		conn:   conn,
		client: c,
		voice:  voice,
		routes: map[string]chan media.PCM{},
	}
	go s.readPump(ctx)
	return s, nil
}

type cartesiaSession struct {
	conn   *websocket.Conn
	client *CartesiaWSClient
	voice  Voice

	mu     sync.Mutex
	routes map[string]chan media.PCM
	closed bool
	seq    uint64
}

type caWSRequest struct {
	ContextID    string         `json:"context_id"`
	ModelID      string         `json:"model_id"`
	Transcript   string         `json:"transcript"`
	Voice        caVoice        `json:"voice"`
	OutputFormat caOutputFormat `json:"output_format"`
	Language     string         `json:"language,omitempty"`
	Continue     bool           `json:"continue"`
}

type caWSCancel struct {
	ContextID string `json:"context_id"`
	Cancel    bool   `json:"cancel"`
}

type caWSResponse struct {
	ContextID string `json:"context_id"`
	Type      string `json:"type"`
	Data      string `json:"data"`
	Done      bool   `json:"done"`
}

func (s *cartesiaSession) readPump(ctx context.Context) {
	defer s.shutdown()
	for {
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			return
		}
		var r caWSResponse
		if json.Unmarshal(data, &r) != nil {
			continue
		}
		s.mu.Lock()
		ch := s.routes[r.ContextID]
		s.mu.Unlock()
		if ch == nil {
			continue
		}
		switch {
		case r.Type == "chunk" && r.Data != "":
			raw, derr := base64.StdEncoding.DecodeString(r.Data)
			if derr != nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- media.PCM{Samples: bytesToPCM16(raw), SampleRate: s.client.Rate}:
			}
		case r.Type == "done" || r.Type == "error" || r.Done:
			s.closeRoute(r.ContextID)
		}
	}
}

func (s *cartesiaSession) Synthesize(ctx context.Context, text string) (Stream, error) {
	model := s.voice.Model
	if model == "" {
		model = s.client.Model
	}
	id := "ctx-" + strconv.FormatUint(atomic.AddUint64(&s.seq, 1), 10)

	ch := make(chan media.PCM, 64)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("cartesia ws: session closed")
	}
	s.routes[id] = ch
	s.mu.Unlock()

	req := caWSRequest{
		ContextID:  id,
		ModelID:    model,
		Transcript: text,
		Voice:      caVoice{Mode: "id", ID: s.voice.ID},
		OutputFormat: caOutputFormat{
			Container:  "raw",
			Encoding:   "pcm_s16le",
			SampleRate: s.client.Rate,
		},
		Language: s.voice.Language,
	}
	body, err := json.Marshal(req)
	if err != nil {
		s.closeRoute(id)
		return nil, err
	}
	if err := s.conn.Write(ctx, websocket.MessageText, body); err != nil {
		s.closeRoute(id)
		return nil, err
	}
	return &cartesiaWSStream{audio: ch, sess: s, id: id}, nil
}

func (s *cartesiaSession) closeRoute(id string) {
	s.mu.Lock()
	if ch, ok := s.routes[id]; ok {
		delete(s.routes, id)
		close(ch)
	}
	s.mu.Unlock()
}

func (s *cartesiaSession) cancel(id string) {
	s.mu.Lock()
	_, active := s.routes[id]
	closed := s.closed
	s.mu.Unlock()
	if active && !closed {
		if body, err := json.Marshal(caWSCancel{ContextID: id, Cancel: true}); err == nil {
			_ = s.conn.Write(context.Background(), websocket.MessageText, body)
		}
	}
	s.closeRoute(id)
}

func (s *cartesiaSession) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	routes := s.routes
	s.routes = map[string]chan media.PCM{}
	s.mu.Unlock()
	for _, ch := range routes {
		close(ch)
	}
}

func (s *cartesiaSession) Close() error {
	s.shutdown()
	return s.conn.Close(websocket.StatusNormalClosure, "")
}

type cartesiaWSStream struct {
	audio <-chan media.PCM
	sess  *cartesiaSession
	id    string
}

func (s *cartesiaWSStream) Audio() <-chan media.PCM { return s.audio }
func (s *cartesiaWSStream) Close() error {
	s.sess.cancel(s.id)
	return nil
}
