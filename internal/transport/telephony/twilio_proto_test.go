package telephony

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

func TestParseStart(t *testing.T) {
	raw := `{"event":"start","streamSid":"MZ123","start":{"streamSid":"MZ123","accountSid":"AC1","callSid":"CA1","tracks":["inbound"],"mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1},"customParameters":{"from":"+15550001111","to":"+15550002222"}}}`
	e, err := parseInbound([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if e.Event != "start" || e.Start == nil {
		t.Fatalf("bad parse: %+v", e)
	}
	if e.Start.CallSid != "CA1" || e.Start.CustomParameters["from"] != "+15550001111" {
		t.Fatalf("bad start fields: %+v", e.Start)
	}
}

func TestMediaPayloadRoundTrip(t *testing.T) {
	n := media.SamplesPerFrame(TwilioRate)
	pcm8k := make([]int16, n)
	for i := range pcm8k {
		pcm8k[i] = int16((i % 50) * 100)
	}
	payload := base64.StdEncoding.EncodeToString(media.EncodeULawFrame(pcm8k))

	in := `{"event":"media","streamSid":"MZ1","media":{"track":"inbound","payload":"` + payload + `"}}`
	e, err := parseInbound([]byte(in))
	if err != nil || e.Media == nil {
		t.Fatalf("parse media: %v %+v", err, e)
	}
	bus, err := decodePayload(e.Media.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(bus) != media.SamplesPerFrame(media.BusSampleRate) {
		t.Fatalf("decoded len=%d want %d", len(bus), media.SamplesPerFrame(media.BusSampleRate))
	}
}

func TestEncodeOutbound(t *testing.T) {
	bus := make([]int16, media.SamplesPerFrame(media.BusSampleRate))
	for i := range bus {
		bus[i] = 1000
	}
	msg, err := encodeOutbound("MZ9", bus)
	if err != nil {
		t.Fatal(err)
	}
	var e twEvent
	if err := json.Unmarshal(msg, &e); err != nil {
		t.Fatal(err)
	}
	if e.Event != "media" || e.StreamSid != "MZ9" || e.Media == nil || e.Media.Payload == "" {
		t.Fatalf("bad outbound: %s", msg)
	}
	ulaw, err := base64.StdEncoding.DecodeString(e.Media.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(ulaw) != media.SamplesPerFrame(TwilioRate) {
		t.Fatalf("outbound ulaw len=%d want %d", len(ulaw), media.SamplesPerFrame(TwilioRate))
	}
}

func TestBuildClear(t *testing.T) {
	msg, err := buildClear("MZ7")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	_ = json.Unmarshal(msg, &m)
	if m["event"] != "clear" || m["streamSid"] != "MZ7" {
		t.Fatalf("bad clear: %s", msg)
	}
}
