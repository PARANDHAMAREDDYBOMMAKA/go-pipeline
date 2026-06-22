package room

import "github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"

type ParticipantID string

type Kind int

const (
	KindHuman Kind = iota
	KindAgent
	KindSIP
	KindTelephony
)

func (k Kind) String() string {
	switch k {
	case KindHuman:
		return "human"
	case KindAgent:
		return "agent"
	case KindSIP:
		return "sip"
	case KindTelephony:
		return "telephony"
	default:
		return "unknown"
	}
}

type Participant struct {
	ID   ParticipantID
	Name string
	Kind Kind

	Source media.MediaSource
	Sink   media.MediaSink

	subs map[ParticipantID]bool
}

func NewParticipant(id ParticipantID, name string, kind Kind, src media.MediaSource, sink media.MediaSink) *Participant {
	return &Participant{ID: id, Name: name, Kind: kind, Source: src, Sink: sink}
}

func (p *Participant) hears(other ParticipantID) bool {
	if other == p.ID {
		return false
	}
	if p.subs == nil {
		return true
	}
	return p.subs[other]
}
