package vad

import (
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type Event struct {
	Speaking bool
	PTS      time.Duration
}

type Detector interface {
	Push(media.PCM) (changed bool, speaking bool)
	Speaking() bool
	Reset()
}

type EnergyVAD struct {
	threshold      float64
	startFrames    int
	hangoverFrames int

	speaking   bool
	activeRun  int
	silenceRun int
}

func NewEnergyVAD(threshold float64, startFrames, hangoverFrames int) *EnergyVAD {
	if threshold <= 0 {
		threshold = 500
	}
	if startFrames <= 0 {
		startFrames = 2
	}
	if hangoverFrames <= 0 {
		hangoverFrames = 25
	}
	return &EnergyVAD{threshold: threshold, startFrames: startFrames, hangoverFrames: hangoverFrames}
}

func (v *EnergyVAD) Push(p media.PCM) (bool, bool) {
	level := media.RMS(p.Samples)
	loud := level >= v.threshold

	if loud {
		v.activeRun++
		v.silenceRun = 0
	} else {
		v.silenceRun++
		v.activeRun = 0
	}

	prev := v.speaking
	if !v.speaking && v.activeRun >= v.startFrames {
		v.speaking = true
	} else if v.speaking && v.silenceRun >= v.hangoverFrames {
		v.speaking = false
	}
	return v.speaking != prev, v.speaking
}

func (v *EnergyVAD) Speaking() bool { return v.speaking }

func (v *EnergyVAD) Reset() {
	v.speaking = false
	v.activeRun = 0
	v.silenceRun = 0
}
