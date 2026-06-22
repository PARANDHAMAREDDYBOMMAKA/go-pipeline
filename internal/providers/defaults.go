package providers

import (
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/config"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
)

func init() {
	RegisterSTT("deepgram", func(c config.Config) (stt.Client, error) {
		return stt.NewDeepgramClient(c.DeepgramAPIKey, c.DeepgramModel, c.DeepgramLang), nil
	})

	RegisterLLM("openai", func(c config.Config) (llm.Client, error) {
		return llm.NewOpenAIClient(c.OpenAIAPIKey, c.OpenAIBaseURL, c.OpenAIModel), nil
	})
	RegisterLLM("groq", func(c config.Config) (llm.Client, error) {
		return llm.NewOpenAIClient(c.GroqAPIKey, c.GroqBaseURL, c.GroqModel), nil
	})

	RegisterTTS("cartesia", func(c config.Config) (tts.Client, error) {
		return tts.NewCartesiaClient(c.CartesiaAPIKey, c.CartesiaModel, c.CartesiaVersion), nil
	}, func(c config.Config) tts.Voice {
		return tts.Voice{ID: c.CartesiaVoiceID, Model: c.CartesiaModel}
	})
}
