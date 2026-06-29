package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port string

	STTProvider string
	LLMProvider string
	TTSProvider string

	DeepgramAPIKey     string
	DeepgramModel      string
	DeepgramLang       string
	DeepgramEndpointMs int

	OpenAIAPIKey  string
	OpenAIBaseURL string
	OpenAIModel   string

	GroqAPIKey  string
	GroqBaseURL string
	GroqModel   string

	CartesiaAPIKey     string
	CartesiaModel      string
	CartesiaVersion    string
	CartesiaVoiceID    string
	CartesiaSampleRate int
	CartesiaLanguage   string
	CartesiaSpeed      string

	SystemPrompt string
	FirstMessage string

	LLMTemperature float64
	LLMMaxTokens   int

	MinSentenceChars  int
	SynthAhead        int
	BargeInFrames     int
	MicBuffer         int
	SynthBuffer       int
	VADThreshold      float64
	VADStartFrames    int
	VADHangoverFrames int
	EndpointGraceMs   int

	DialTimeoutMs       int
	LLMRetries          int
	TTSRetries          int
	LLMFallbackProvider string
	TTSFallbackProvider string

	PublicHost   string
	ProfilesFile string
	Profiles     map[string]Profile
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func Load() Config {
	c := Config{
		Port:                env("PORT", "8080"),
		STTProvider:         env("STT_PROVIDER", "deepgram"),
		LLMProvider:         env("LLM_PROVIDER", "openai"),
		TTSProvider:         env("TTS_PROVIDER", "cartesia"),
		DeepgramAPIKey:      os.Getenv("DEEPGRAM_API_KEY"),
		DeepgramModel:       env("DEEPGRAM_MODEL", "nova-2"),
		DeepgramLang:        env("DEEPGRAM_LANG", "en"),
		DeepgramEndpointMs:  envInt("DEEPGRAM_ENDPOINT_MS", 300),
		OpenAIAPIKey:        os.Getenv("OPENAI_API_KEY"),
		OpenAIBaseURL:       env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		OpenAIModel:         env("OPENAI_MODEL", "gpt-4o-mini"),
		GroqAPIKey:          os.Getenv("GROQ_API_KEY"),
		GroqBaseURL:         env("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
		GroqModel:           env("GROQ_MODEL", "llama-3.3-70b-versatile"),
		CartesiaAPIKey:      os.Getenv("CARTESIA_API_KEY"),
		CartesiaModel:       env("CARTESIA_MODEL", "sonic-2"),
		CartesiaVersion:     env("CARTESIA_VERSION", "2024-06-10"),
		CartesiaVoiceID:     env("CARTESIA_VOICE_ID", "71a7ad14-091c-4e8e-a314-022ece01c121"),
		CartesiaSampleRate:  envInt("CARTESIA_SAMPLE_RATE", 16000),
		CartesiaLanguage:    env("CARTESIA_LANGUAGE", ""),
		CartesiaSpeed:       env("CARTESIA_SPEED", ""),
		SystemPrompt:        env("SYSTEM_PROMPT", "You are a helpful, concise voice assistant. Keep replies short and natural for speech."),
		FirstMessage:        env("FIRST_MESSAGE", "Hello! How can I help you today?"),
		LLMTemperature:      envFloat("LLM_TEMPERATURE", -1),
		LLMMaxTokens:        envInt("LLM_MAX_TOKENS", 0),
		MinSentenceChars:    envInt("MIN_SENTENCE_CHARS", 12),
		SynthAhead:          envInt("SYNTH_AHEAD", 2),
		BargeInFrames:       envInt("BARGE_IN_FRAMES", 3),
		MicBuffer:           envInt("MIC_BUFFER", 8),
		SynthBuffer:         envInt("SYNTH_BUFFER", 50),
		VADThreshold:        envFloat("VAD_THRESHOLD", 500),
		VADStartFrames:      envInt("VAD_START_FRAMES", 2),
		VADHangoverFrames:   envInt("VAD_HANGOVER_FRAMES", 25),
		EndpointGraceMs:     envInt("ENDPOINT_GRACE_MS", 0),
		DialTimeoutMs:       envInt("DIAL_TIMEOUT_MS", 8000),
		LLMRetries:          envInt("LLM_RETRIES", 2),
		TTSRetries:          envInt("TTS_RETRIES", 2),
		LLMFallbackProvider: env("LLM_FALLBACK_PROVIDER", ""),
		TTSFallbackProvider: env("TTS_FALLBACK_PROVIDER", ""),
		PublicHost:          os.Getenv("PUBLIC_HOST"),
		ProfilesFile:        os.Getenv("PROFILES_FILE"),
	}
	if c.ProfilesFile != "" {
		if profiles, err := LoadProfiles(c.ProfilesFile); err == nil {
			c.Profiles = profiles
		}
	}
	return c
}
