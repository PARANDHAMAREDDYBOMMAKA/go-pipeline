package media

import "time"

const BusSampleRate = 16000

const FrameMillis = 20

func SamplesPerFrame(sampleRate int) int {
	return sampleRate * FrameMillis / 1000
}

type PCM struct {
	Samples    []int16
	SampleRate int
	PTS        time.Duration
	Seq        uint32
}

func (p PCM) Clone() PCM {
	s := make([]int16, len(p.Samples))
	copy(s, p.Samples)
	p.Samples = s
	return p
}

type VADEvent struct {
	Speaking bool
	PTS      time.Duration
}

type MediaSource interface {
	Frames() <-chan PCM
	SpeechActivity() <-chan VADEvent
	Close() error
}

type MediaSink interface {
	Write(PCM) error
	Clear() error
	Close() error
}
