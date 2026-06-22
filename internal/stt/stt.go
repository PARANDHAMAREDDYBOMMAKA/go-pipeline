package stt

import (
	"context"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Transcript struct {
	Text       string
	Final      bool
	Confidence float32
	PTS        time.Duration
}

type Stream interface {
	Send(media.PCM) error
	Events() <-chan Transcript
	CloseSend() error
	Close() error
}

type Client interface {
	Open(ctx context.Context) (Stream, error)
}
