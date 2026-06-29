package stt

import (
	"context"
	"sync"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

const (
	reconnectInitialBackoff = 200 * time.Millisecond
	reconnectMaxBackoff     = 5 * time.Second
)

type ReconnectClient struct {
	inner       Client
	onReconnect func()
}

func WithReconnect(inner Client, onReconnect func()) Client {
	return &ReconnectClient{inner: inner, onReconnect: onReconnect}
}

func (c *ReconnectClient) Open(ctx context.Context) (Stream, error) {
	first, err := c.inner.Open(ctx)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(ctx)
	r := &reconnectStream{
		client:      c.inner,
		onReconnect: c.onReconnect,
		ctx:         cctx,
		cancel:      cancel,
		events:      make(chan Transcript, 32),
		cur:         first,
	}
	go r.manage()
	return r, nil
}

type reconnectStream struct {
	client      Client
	onReconnect func()
	ctx         context.Context
	cancel      context.CancelFunc
	events      chan Transcript

	mu  sync.Mutex
	cur Stream
}

func (r *reconnectStream) current() Stream {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cur
}

func (r *reconnectStream) setCurrent(s Stream) {
	r.mu.Lock()
	r.cur = s
	r.mu.Unlock()
}

func (r *reconnectStream) manage() {
	defer close(r.events)
	backoff := reconnectInitialBackoff
	for {
		inner := r.current()
		if inner == nil {
			if r.ctx.Err() != nil {
				return
			}
			ni, err := r.client.Open(r.ctx)
			if err != nil {
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, reconnectMaxBackoff)
				continue
			}
			r.setCurrent(ni)
			inner = ni
			backoff = reconnectInitialBackoff
			if r.onReconnect != nil {
				r.onReconnect()
			}
		}
		r.pump(inner)
		_ = inner.Close()
		r.setCurrent(nil)
		if r.ctx.Err() != nil {
			return
		}
	}
}

func (r *reconnectStream) pump(inner Stream) {
	events := inner.Events()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			select {
			case r.events <- ev:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

func (r *reconnectStream) Send(p media.PCM) error {
	inner := r.current()
	if inner == nil {
		return nil
	}
	if err := inner.Send(p); err != nil {
		r.mu.Lock()
		if r.cur == inner {
			r.cur = nil
		}
		r.mu.Unlock()
		_ = inner.Close()
		return err
	}
	return nil
}

func (r *reconnectStream) Events() <-chan Transcript { return r.events }

func (r *reconnectStream) CloseSend() error {
	if inner := r.current(); inner != nil {
		return inner.CloseSend()
	}
	return nil
}

func (r *reconnectStream) Close() error {
	r.cancel()
	if inner := r.current(); inner != nil {
		return inner.Close()
	}
	return nil
}
