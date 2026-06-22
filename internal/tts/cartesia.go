package tts

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/httpx"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

const cartesiaRate = 24000

type CartesiaClient struct {
	APIKey  string
	Model   string
	Version string
	http    *http.Client
}

func NewCartesiaClient(apiKey, model, version string) *CartesiaClient {
	if model == "" {
		model = "sonic-2"
	}
	if version == "" {
		version = "2024-06-10"
	}
	return &CartesiaClient{APIKey: apiKey, Model: model, Version: version, http: httpx.Shared()}
}

type caVoice struct {
	Mode string `json:"mode"`
	ID   string `json:"id"`
}

type caOutputFormat struct {
	Container  string `json:"container"`
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
}

type caRequest struct {
	ModelID      string         `json:"model_id"`
	Transcript   string         `json:"transcript"`
	Voice        caVoice        `json:"voice"`
	OutputFormat caOutputFormat `json:"output_format"`
	Language     string         `json:"language,omitempty"`
}

type caEvent struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

func (c *CartesiaClient) Synthesize(ctx context.Context, text string, voice Voice) (Stream, error) {
	model := voice.Model
	if model == "" {
		model = c.Model
	}
	reqBody := caRequest{
		ModelID:    model,
		Transcript: text,
		Voice:      caVoice{Mode: "id", ID: voice.ID},
		OutputFormat: caOutputFormat{
			Container:  "raw",
			Encoding:   "pcm_s16le",
			SampleRate: cartesiaRate,
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.cartesia.ai/tts/sse", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Cartesia-Version", c.Version)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("cartesia: status %d", resp.StatusCode)
	}

	out := make(chan media.PCM, 64)
	s := &cartesiaStream{audio: out, resp: resp}
	go s.read(ctx)
	return s, nil
}

type cartesiaStream struct {
	audio chan media.PCM
	resp  *http.Response
}

func (s *cartesiaStream) read(ctx context.Context) {
	defer close(s.audio)
	defer s.resp.Body.Close()

	scanner := bufio.NewScanner(s.resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var ev caEvent
		if json.Unmarshal([]byte(payload), &ev) != nil {
			continue
		}
		if ev.Type == "done" {
			break
		}
		if ev.Type != "chunk" || ev.Data == "" {
			continue
		}
		raw, derr := base64.StdEncoding.DecodeString(ev.Data)
		if derr != nil {
			continue
		}
		pcm := bytesToPCM16(raw)
		select {
		case <-ctx.Done():
			return
		case s.audio <- media.PCM{Samples: pcm, SampleRate: cartesiaRate}:
		}
	}
	_ = scanner.Err()
}

func (s *cartesiaStream) Audio() <-chan media.PCM { return s.audio }
func (s *cartesiaStream) Close() error {
	s.resp.Body.Close()
	return nil
}

func bytesToPCM16(b []byte) []int16 {
	n := len(b) / 2
	out := make([]int16, n)
	for i := range n {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}
