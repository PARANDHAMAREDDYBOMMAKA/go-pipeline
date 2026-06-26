package providers

import (
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/config"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/llm"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/stt"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/tts"
)

func init() {
	RegisterSTT("deepgram", func(c config.Config) (stt.Client, error) {
		return stt.NewDeepgramClient(c.DeepgramAPIKey, c.DeepgramModel, c.DeepgramLang, c.DeepgramEndpointMs), nil
	})

	RegisterLLM("openai", func(c config.Config) (llm.Client, error) {
		client := llm.NewOpenAIClient(c.OpenAIAPIKey, c.OpenAIBaseURL, c.OpenAIModel)
		applyLLMTuning(client, c)
		return client, nil
	})
	RegisterLLM("groq", func(c config.Config) (llm.Client, error) {
		client := llm.NewOpenAIClient(c.GroqAPIKey, c.GroqBaseURL, c.GroqModel)
		applyLLMTuning(client, c)
		return client, nil
	})

	cartesiaVoice := func(c config.Config) tts.Voice {
		return tts.Voice{ID: c.CartesiaVoiceID, Model: c.CartesiaModel, Language: c.CartesiaLanguage, Speed: c.CartesiaSpeed}
	}
	RegisterTTS("cartesia", func(c config.Config) (tts.Client, error) {
		return tts.NewCartesiaClient(c.CartesiaAPIKey, c.CartesiaModel, c.CartesiaVersion, c.CartesiaSampleRate), nil
	}, cartesiaVoice)
	RegisterTTS("cartesia_ws", func(c config.Config) (tts.Client, error) {
		return tts.NewCartesiaWSClient(c.CartesiaAPIKey, c.CartesiaModel, c.CartesiaVersion, c.CartesiaSampleRate), nil
	}, cartesiaVoice)
}

func applyLLMTuning(client *llm.OpenAIClient, c config.Config) {
	if c.LLMTemperature >= 0 {
		t := c.LLMTemperature
		client.Temperature = &t
	}
	if c.LLMMaxTokens > 0 {
		client.MaxTokens = c.LLMMaxTokens
	}
}
