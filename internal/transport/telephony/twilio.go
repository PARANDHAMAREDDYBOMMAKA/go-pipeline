package telephony

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/transport"
	"github.com/coder/websocket"
)

type Meta struct {
	StreamSid  string
	CallSid    string
	AccountSid string
	From       string
	To         string
	Params     map[string]string
}

type DTMFEvent struct {
	Digit string
}

type TwilioTransport struct {
	conn *websocket.Conn

	src  *transport.FeedSource
	sink *twilioSink

	meta      atomic.Pointer[Meta]
	ready     chan struct{}
	readyOnce sync.Once

	dtmf chan DTMFEvent

	done      chan struct{}
	closeOnce sync.Once
}

func NewTwilioTransport(conn *websocket.Conn) *TwilioTransport {
	t := &TwilioTransport{
		conn:  conn,
		src:   transport.NewFeedSource(8),
		ready: make(chan struct{}),
		dtmf:  make(chan DTMFEvent, 8),
		done:  make(chan struct{}),
	}
	t.sink = &twilioSink{conn: conn, t: t}
	return t
}

func (t *TwilioTransport) Done() <-chan struct{} { return t.done }

func (t *TwilioTransport) Attach(ctx context.Context) (media.MediaSource, media.MediaSink, error) {
	go t.readLoop(ctx)
	return t.src, t.sink, nil
}

func (t *TwilioTransport) Capabilities() transport.Caps {
	return transport.Caps{Codecs: []transport.Codec{transport.CodecPCMU}, ClockRate: TwilioRate, DTMF: true}
}

func (t *TwilioTransport) DTMF() <-chan DTMFEvent { return t.dtmf }

func (t *TwilioTransport) Meta(ctx context.Context) (Meta, error) {
	select {
	case <-ctx.Done():
		return Meta{}, ctx.Err()
	case <-t.ready:
		if m := t.meta.Load(); m != nil {
			return *m, nil
		}
		return Meta{}, nil
	}
}

func (t *TwilioTransport) streamSid() string {
	if m := t.meta.Load(); m != nil {
		return m.StreamSid
	}
	return ""
}

func (t *TwilioTransport) readLoop(ctx context.Context) {
	defer t.Close()
	for {
		_, data, err := t.conn.Read(ctx)
		if err != nil {
			return
		}
		e, err := parseInbound(data)
		if err != nil {
			continue
		}
		switch e.Event {
		case "start":
			if e.Start != nil {
				m := &Meta{
					StreamSid:  firstNonEmpty(e.Start.StreamSid, e.StreamSid),
					CallSid:    e.Start.CallSid,
					AccountSid: e.Start.AccountSid,
					From:       e.Start.CustomParameters["from"],
					To:         e.Start.CustomParameters["to"],
					Params:     e.Start.CustomParameters,
				}
				t.meta.Store(m)
				t.readyOnce.Do(func() { close(t.ready) })
			}
		case "media":
			if e.Media != nil && e.Media.Payload != "" {
				bus, derr := decodePayload(e.Media.Payload)
				if derr == nil {
					t.src.Feed(media.PCM{Samples: bus, SampleRate: media.BusSampleRate})
				}
			}
		case "dtmf":
			if e.Dtmf != nil {
				select {
				case t.dtmf <- DTMFEvent{Digit: e.Dtmf.Digit}:
				default:
				}
			}
		case "stop":
			return
		}
	}
}

func (t *TwilioTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)
		_ = t.src.Close()
		_ = t.conn.Close(websocket.StatusNormalClosure, "")
		t.readyOnce.Do(func() { close(t.ready) })
	})
	return nil
}

type twilioSink struct {
	conn *websocket.Conn
	t    *TwilioTransport
}

func (s *twilioSink) Write(p media.PCM) error {
	sid := s.t.streamSid()
	if sid == "" {
		return nil
	}
	frame := p.Samples
	if p.SampleRate != 0 && p.SampleRate != media.BusSampleRate {
		frame = media.Resample(frame, p.SampleRate, media.BusSampleRate)
	}
	msg, err := encodeOutbound(sid, frame)
	if err != nil {
		return err
	}
	return s.conn.Write(context.Background(), websocket.MessageText, msg)
}

func (s *twilioSink) Clear() error {
	sid := s.t.streamSid()
	if sid == "" {
		return nil
	}
	msg, err := buildClear(sid)
	if err != nil {
		return err
	}
	return s.conn.Write(context.Background(), websocket.MessageText, msg)
}

func (s *twilioSink) Close() error { return nil }

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
