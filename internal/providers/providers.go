package providers

import (
	"fmt"
	"sort"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/config"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
)

type (
	STTFactory   func(config.Config) (stt.Client, error)
	LLMFactory   func(config.Config) (llm.Client, error)
	TTSFactory   func(config.Config) (tts.Client, error)
	VoiceFactory func(config.Config) tts.Voice
)

var (
	sttReg   = map[string]STTFactory{}
	llmReg   = map[string]LLMFactory{}
	ttsReg   = map[string]TTSFactory{}
	voiceReg = map[string]VoiceFactory{}
)

func RegisterSTT(name string, f STTFactory) { sttReg[name] = f }
func RegisterLLM(name string, f LLMFactory) { llmReg[name] = f }
func RegisterTTS(name string, f TTSFactory, voice VoiceFactory) {
	ttsReg[name] = f
	if voice != nil {
		voiceReg[name] = voice
	}
}

func STT(cfg config.Config) (stt.Client, error) {
	f, ok := sttReg[cfg.STTProvider]
	if !ok {
		return nil, fmt.Errorf("stt: unknown provider %q (registered: %v)", cfg.STTProvider, keys(sttReg))
	}
	return f(cfg)
}

func LLM(cfg config.Config) (llm.Client, error) {
	f, ok := llmReg[cfg.LLMProvider]
	if !ok {
		return nil, fmt.Errorf("llm: unknown provider %q (registered: %v)", cfg.LLMProvider, keys(llmReg))
	}
	return f(cfg)
}

func TTS(cfg config.Config) (tts.Client, error) {
	f, ok := ttsReg[cfg.TTSProvider]
	if !ok {
		return nil, fmt.Errorf("tts: unknown provider %q (registered: %v)", cfg.TTSProvider, keys(ttsReg))
	}
	return f(cfg)
}

func Voice(cfg config.Config) tts.Voice {
	if f, ok := voiceReg[cfg.TTSProvider]; ok {
		return f(cfg)
	}
	return tts.Voice{}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
