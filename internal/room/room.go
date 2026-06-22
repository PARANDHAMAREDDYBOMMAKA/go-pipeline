package room

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

var ErrClosed = errors.New("room: closed")

type Room struct {
	name         string
	frameSamples int

	mu          sync.RWMutex
	parts       map[ParticipantID]*Participant
	intakes     map[ParticipantID]*intake
	ingressDone map[ParticipantID]chan struct{}
	closed      bool

	tickN uint32

	stop     chan struct{}
	stopOnce sync.Once
}

func NewRoom(name string) *Room {
	return &Room{
		name:         name,
		frameSamples: media.SamplesPerFrame(media.BusSampleRate),
		parts:        make(map[ParticipantID]*Participant),
		intakes:      make(map[ParticipantID]*intake),
		ingressDone:  make(map[ParticipantID]chan struct{}),
		stop:         make(chan struct{}),
	}
}

func (r *Room) Name() string { return r.name }

func (r *Room) Join(ctx context.Context, p *Participant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, exists := r.parts[p.ID]; exists {
		return errors.New("room: participant already joined: " + string(p.ID))
	}
	in := &intake{}
	done := make(chan struct{})
	r.parts[p.ID] = p
	r.intakes[p.ID] = in
	r.ingressDone[p.ID] = done

	if p.Source != nil {
		go ingressLoop(ctx, done, p.Source, in)
	}
	return nil
}

func (r *Room) Leave(id ParticipantID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaveLocked(id)
}

func (r *Room) leaveLocked(id ParticipantID) error {
	if done, ok := r.ingressDone[id]; ok {
		close(done)
	}
	delete(r.parts, id)
	delete(r.intakes, id)
	delete(r.ingressDone, id)
	return nil
}

func (r *Room) Subscribe(sub, pub ParticipantID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.parts[sub]
	if !ok {
		return errors.New("room: unknown subscriber: " + string(sub))
	}
	if p.subs == nil {
		p.subs = make(map[ParticipantID]bool)
	}
	p.subs[pub] = true
	return nil
}

func (r *Room) Unsubscribe(sub, pub ParticipantID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.parts[sub]
	if !ok {
		return errors.New("room: unknown subscriber: " + string(sub))
	}
	if p.subs == nil {
		p.subs = make(map[ParticipantID]bool)
	}
	delete(p.subs, pub)
	return nil
}

func (r *Room) Participants() []*Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Participant, 0, len(r.parts))
	for _, p := range r.parts {
		out = append(out, p)
	}
	return out
}

func (r *Room) Run(ctx context.Context) {
	ticker := time.NewTicker(media.FrameMillis * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

func (r *Room) Close() error {
	r.stopOnce.Do(func() { close(r.stop) })
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	for id := range r.parts {
		if done, ok := r.ingressDone[id]; ok {
			close(done)
		}
		if p := r.parts[id]; p != nil && p.Sink != nil {
			_ = p.Sink.Close()
		}
	}
	r.parts = map[ParticipantID]*Participant{}
	r.intakes = map[ParticipantID]*intake{}
	r.ingressDone = map[ParticipantID]chan struct{}{}
	return nil
}

func (r *Room) tick() {
	r.mu.RLock()

	type member struct {
		p     *Participant
		frame []int16
	}
	members := make([]member, 0, len(r.parts))
	frames := make(map[ParticipantID][]int16, len(r.parts))
	for id, p := range r.parts {
		f := r.currentFrame(id)
		members = append(members, member{p: p, frame: f})
		frames[id] = f
	}
	r.mu.RUnlock()

	if len(members) == 0 {
		return
	}

	global := make([]int32, r.frameSamples)
	for _, m := range members {
		media.AccumInto(global, m.frame)
	}

	pts := time.Duration(r.tickN) * media.FrameMillis * time.Millisecond
	seq := r.tickN
	r.tickN++

	for _, m := range members {
		if m.p.Sink == nil {
			continue
		}
		var mixed []int16
		if m.p.subs == nil {

			acc := make([]int32, r.frameSamples)
			copy(acc, global)
			media.SubFrom(acc, m.frame)
			mixed = media.Limit(acc)
		} else {

			acc := make([]int32, r.frameSamples)
			for srcID, f := range frames {
				if m.p.hears(srcID) {
					media.AccumInto(acc, f)
				}
			}
			mixed = media.Limit(acc)
		}
		_ = m.p.Sink.Write(media.PCM{
			Samples:    mixed,
			SampleRate: media.BusSampleRate,
			PTS:        pts,
			Seq:        seq,
		})
	}
}

func (r *Room) currentFrame(id ParticipantID) []int16 {
	in := r.intakes[id]
	if in == nil {
		return make([]int16, r.frameSamples)
	}
	f, ok := in.pop()
	if !ok {
		return make([]int16, r.frameSamples)
	}
	return normalize(f.Samples, r.frameSamples)
}

func normalize(s []int16, n int) []int16 {
	if len(s) == n {
		return s
	}
	out := make([]int16, n)
	copy(out, s)
	return out
}

func ingressLoop(ctx context.Context, done chan struct{}, src media.MediaSource, in *intake) {
	frames := src.Frames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case f, ok := <-frames:
			if !ok {
				return
			}
			if f.SampleRate != 0 && f.SampleRate != media.BusSampleRate {
				f = media.ResampleFrame(f, media.BusSampleRate)
			}
			in.push(f)
		}
	}
}
