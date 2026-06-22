package config

import "os"

type Config struct {
	Port string

	STTProvider string
	LLMProvider string
	TTSProvider string

	DeepgramAPIKey string
	DeepgramModel  string
	DeepgramLang   string

	OpenAIAPIKey  string
	OpenAIBaseURL string
	OpenAIModel   string

	GroqAPIKey  string
	GroqBaseURL string
	GroqModel   string

	CartesiaAPIKey  string
	CartesiaModel   string
	CartesiaVersion string
	CartesiaVoiceID string

	SystemPrompt string
	FirstMessage string

	PublicHost string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Load() Config {
	return Config{
		Port:            env("PORT", "8080"),
		STTProvider:     env("STT_PROVIDER", "deepgram"),
		LLMProvider:     env("LLM_PROVIDER", "openai"),
		TTSProvider:     env("TTS_PROVIDER", "cartesia"),
		DeepgramAPIKey:  os.Getenv("DEEPGRAM_API_KEY"),
		DeepgramModel:   env("DEEPGRAM_MODEL", "nova-2"),
		DeepgramLang:    env("DEEPGRAM_LANG", "en"),
		OpenAIAPIKey:    os.Getenv("OPENAI_API_KEY"),
		OpenAIBaseURL:   env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		OpenAIModel:     env("OPENAI_MODEL", "gpt-4o-mini"),
		GroqAPIKey:      os.Getenv("GROQ_API_KEY"),
		GroqBaseURL:     env("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
		GroqModel:       env("GROQ_MODEL", "llama-3.3-70b-versatile"),
		CartesiaAPIKey:  os.Getenv("CARTESIA_API_KEY"),
		CartesiaModel:   env("CARTESIA_MODEL", "sonic-2"),
		CartesiaVersion: env("CARTESIA_VERSION", "2024-06-10"),
		CartesiaVoiceID: env("CARTESIA_VOICE_ID", "71a7ad14-091c-4e8e-a314-022ece01c121"),
		SystemPrompt:    env("SYSTEM_PROMPT", "You are a helpful, concise voice assistant. Keep replies short and natural for speech."),
		FirstMessage:    env("FIRST_MESSAGE", "Hello! How can I help you today?"),
		PublicHost:      os.Getenv("PUBLIC_HOST"),
	}
}
