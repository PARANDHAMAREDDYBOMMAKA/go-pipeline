package stt

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/coder/websocket"
)

type DeepgramClient struct {
	APIKey     string
	Model      string
	Lang       string
	EndpointMs int
}

func NewDeepgramClient(apiKey, model, lang string, endpointMs int) *DeepgramClient {
	if model == "" {
		model = "nova-2"
	}
	if lang == "" {
		lang = "en"
	}
	if endpointMs <= 0 {
		endpointMs = 300
	}
	return &DeepgramClient{APIKey: apiKey, Model: model, Lang: lang, EndpointMs: endpointMs}
}

func (c *DeepgramClient) Open(ctx context.Context) (Stream, error) {
	q := url.Values{}
	q.Set("encoding", "linear16")
	q.Set("sample_rate", fmt.Sprintf("%d", media.BusSampleRate))
	q.Set("channels", "1")
	q.Set("model", c.Model)
	q.Set("language", c.Lang)
	q.Set("interim_results", "true")
	q.Set("smart_format", "true")
	q.Set("endpointing", fmt.Sprintf("%d", c.EndpointMs))

	endpoint := "wss://api.deepgram.com/v1/listen?" + q.Encode()
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Token " + c.APIKey}},
	})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(1 << 20)

	s := &deepgramStream{conn: conn, events: make(chan Transcript, 32)}
	go s.read(ctx)
	return s, nil
}

type deepgramStream struct {
	conn      *websocket.Conn
	events    chan Transcript
	closeOnce sync.Once
}

type dgResult struct {
	Type    string `json:"type"`
	IsFinal bool   `json:"is_final"`
	Channel struct {
		Alternatives []struct {
			Transcript string  `json:"transcript"`
			Confidence float32 `json:"confidence"`
		} `json:"alternatives"`
	} `json:"channel"`
	SpeechFinal bool `json:"speech_final"`
}

func (s *deepgramStream) read(ctx context.Context) {
	defer close(s.events)
	for {
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			return
		}
		var r dgResult
		if json.Unmarshal(data, &r) != nil {
			continue
		}
		if r.Type != "Results" || len(r.Channel.Alternatives) == 0 {
			continue
		}
		alt := r.Channel.Alternatives[0]
		if alt.Transcript == "" {
			continue
		}
		select {
		case s.events <- Transcript{
			Text:       alt.Transcript,
			Final:      r.IsFinal,
			EndOfTurn:  r.SpeechFinal,
			Confidence: alt.Confidence,
		}:
		case <-ctx.Done():
			return
		}
	}
}

func (s *deepgramStream) Send(p media.PCM) error {
	buf := make([]byte, len(p.Samples)*2)
	for i, v := range p.Samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(v))
	}
	return s.conn.Write(context.Background(), websocket.MessageBinary, buf)
}

func (s *deepgramStream) Events() <-chan Transcript { return s.events }

func (s *deepgramStream) CloseSend() error {
	return s.conn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"CloseStream"}`))
}

func (s *deepgramStream) Close() error {
	s.closeOnce.Do(func() {
		_ = s.conn.Close(websocket.StatusNormalClosure, "")
	})
	return nil
}
