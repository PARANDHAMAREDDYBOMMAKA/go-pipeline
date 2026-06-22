package transport

import (
	"context"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Codec int

const (
	CodecPCMU Codec = iota
	CodecPCMA
	CodecOpus
	CodecRawPCM
)

type Caps struct {
	Codecs         []Codec
	ClockRate      int
	DTMF           bool
	CanRenegotiate bool
}

type Transport interface {
	Attach(ctx context.Context) (media.MediaSource, media.MediaSink, error)
	Capabilities() Caps
	Close() error
}
