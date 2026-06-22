package room

import (
	"sync"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
)

type intake struct {
	mu      sync.Mutex
	frame   media.PCM
	have    bool
	dropped uint64
}

func (in *intake) push(f media.PCM) {
	in.mu.Lock()
	if in.have {
		in.dropped++
	}
	in.frame = f
	in.have = true
	in.mu.Unlock()
}

func (in *intake) pop() (media.PCM, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.have {
		return media.PCM{}, false
	}
	f := in.frame
	in.have = false
	return f, true
}

func (in *intake) drops() uint64 {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.dropped
}
