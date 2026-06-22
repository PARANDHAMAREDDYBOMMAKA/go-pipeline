package llm

import "strings"

type Sentencizer struct {
	buf      strings.Builder
	minChars int
}

func NewSentencizer(minChars int) *Sentencizer {
	if minChars <= 0 {
		minChars = 12
	}
	return &Sentencizer{minChars: minChars}
}

func isBoundary(r rune) bool {
	switch r {
	case '.', '!', '?', ';', ':', '\n':
		return true
	}
	return false
}

func (s *Sentencizer) Push(text string) []string {
	var out []string
	for _, r := range text {
		s.buf.WriteRune(r)
		if isBoundary(r) && s.buf.Len() >= s.minChars {
			frag := strings.TrimSpace(s.buf.String())
			if frag != "" {
				out = append(out, frag)
			}
			s.buf.Reset()
		}
	}
	return out
}

func (s *Sentencizer) Flush() string {
	frag := strings.TrimSpace(s.buf.String())
	s.buf.Reset()
	return frag
}
