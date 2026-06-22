package telephony

import (
	"encoding/base64"
	"encoding/json"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

const TwilioRate = 8000

type twEvent struct {
	Event     string   `json:"event"`
	StreamSid string   `json:"streamSid,omitempty"`
	Start     *twStart `json:"start,omitempty"`
	Media     *twMedia `json:"media,omitempty"`
	Stop      *twStop  `json:"stop,omitempty"`
	Dtmf      *twDtmf  `json:"dtmf,omitempty"`
}

type twStart struct {
	StreamSid        string            `json:"streamSid"`
	AccountSid       string            `json:"accountSid"`
	CallSid          string            `json:"callSid"`
	Tracks           []string          `json:"tracks"`
	MediaFormat      twMediaFormat     `json:"mediaFormat"`
	CustomParameters map[string]string `json:"customParameters"`
}

type twMediaFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sampleRate"`
	Channels   int    `json:"channels"`
}

type twMedia struct {
	Track     string `json:"track"`
	Chunk     string `json:"chunk"`
	Timestamp string `json:"timestamp"`
	Payload   string `json:"payload"`
}

type twStop struct {
	AccountSid string `json:"accountSid"`
	CallSid    string `json:"callSid"`
}

type twDtmf struct {
	Track string `json:"track"`
	Digit string `json:"digit"`
}

func parseInbound(data []byte) (twEvent, error) {
	var e twEvent
	err := json.Unmarshal(data, &e)
	return e, err
}

func decodePayload(payloadB64 string) ([]int16, error) {
	raw, err := base64.StdEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, err
	}
	pcm8k := media.DecodeULawFrame(raw)
	return media.Resample(pcm8k, TwilioRate, media.BusSampleRate), nil
}

func encodeOutbound(streamSid string, busPCM []int16) ([]byte, error) {
	pcm8k := media.Resample(busPCM, media.BusSampleRate, TwilioRate)
	ulaw := media.EncodeULawFrame(pcm8k)
	payload := base64.StdEncoding.EncodeToString(ulaw)
	return json.Marshal(twEvent{
		Event:     "media",
		StreamSid: streamSid,
		Media:     &twMedia{Payload: payload},
	})
}

func buildClear(streamSid string) ([]byte, error) {
	return json.Marshal(map[string]string{"event": "clear", "streamSid": streamSid})
}
