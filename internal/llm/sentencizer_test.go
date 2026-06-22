package llm

import (
	"reflect"
	"testing"
)

func TestSentencizerEmitsAtBoundaries(t *testing.T) {
	s := NewSentencizer(5)
	var got []string
	got = append(got, s.Push("Hello there.")...)
	got = append(got, s.Push(" How are you")...)
	got = append(got, s.Push("? Fine")...)
	want := []string{"Hello there.", "How are you?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if rem := s.Flush(); rem != "Fine" {
		t.Fatalf("flush=%q want Fine", rem)
	}
}

func TestSentencizerHonorsMinChars(t *testing.T) {
	s := NewSentencizer(10)
	got := s.Push("Hi. ok.")
	if len(got) != 0 {
		t.Fatalf("expected no emit below minChars, got %v", got)
	}
	got = s.Push(" this is long enough.")
	if len(got) != 1 {
		t.Fatalf("expected one fragment, got %v", got)
	}
}
