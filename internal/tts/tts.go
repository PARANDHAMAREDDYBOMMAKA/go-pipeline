package tts

import (
	"context"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Voice struct {
	ID       string
	Model    string
	Language string
	Speed    string
}

type Stream interface {
	Audio() <-chan media.PCM
	Close() error
}

type Client interface {
	Synthesize(ctx context.Context, text string, voice Voice) (Stream, error)
}

type Session interface {
	Synthesize(ctx context.Context, text string) (Stream, error)
	Close() error
}

type SessionClient interface {
	OpenSession(ctx context.Context, voice Voice) (Session, error)
}
