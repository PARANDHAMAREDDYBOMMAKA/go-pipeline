package config

import (
	"encoding/json"
	"os"
)

type Profile struct {
	SystemPrompt string `json:"system_prompt"`
	FirstMessage string `json:"first_message"`

	STTProvider string `json:"stt_provider"`
	LLMProvider string `json:"llm_provider"`
	TTSProvider string `json:"tts_provider"`

	OpenAIModel   string `json:"openai_model"`
	GroqModel     string `json:"groq_model"`
	DeepgramModel string `json:"deepgram_model"`
	DeepgramLang  string `json:"deepgram_lang"`

	CartesiaModel    string `json:"cartesia_model"`
	CartesiaVoiceID  string `json:"cartesia_voice_id"`
	CartesiaLanguage string `json:"cartesia_language"`
	CartesiaSpeed    string `json:"cartesia_speed"`

	Temperature *float64 `json:"temperature"`
	MaxTokens   *int     `json:"max_tokens"`

	VADThreshold      *float64 `json:"vad_threshold"`
	VADStartFrames    *int     `json:"vad_start_frames"`
	VADHangoverFrames *int     `json:"vad_hangover_frames"`
	BargeInFrames     *int     `json:"barge_in_frames"`
	MinSentenceChars  *int     `json:"min_sentence_chars"`
	SynthAhead        *int     `json:"synth_ahead"`
	EndpointGraceMs   *int     `json:"endpoint_grace_ms"`
}

func LoadProfiles(path string) (map[string]Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]Profile
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c Config) apply(p Profile) Config {
	setStr(&c.SystemPrompt, p.SystemPrompt)
	setStr(&c.FirstMessage, p.FirstMessage)
	setStr(&c.STTProvider, p.STTProvider)
	setStr(&c.LLMProvider, p.LLMProvider)
	setStr(&c.TTSProvider, p.TTSProvider)
	setStr(&c.OpenAIModel, p.OpenAIModel)
	setStr(&c.GroqModel, p.GroqModel)
	setStr(&c.DeepgramModel, p.DeepgramModel)
	setStr(&c.DeepgramLang, p.DeepgramLang)
	setStr(&c.CartesiaModel, p.CartesiaModel)
	setStr(&c.CartesiaVoiceID, p.CartesiaVoiceID)
	setStr(&c.CartesiaLanguage, p.CartesiaLanguage)
	setStr(&c.CartesiaSpeed, p.CartesiaSpeed)
	if p.Temperature != nil {
		c.LLMTemperature = *p.Temperature
	}
	if p.MaxTokens != nil {
		c.LLMMaxTokens = *p.MaxTokens
	}
	if p.VADThreshold != nil {
		c.VADThreshold = *p.VADThreshold
	}
	if p.VADStartFrames != nil {
		c.VADStartFrames = *p.VADStartFrames
	}
	if p.VADHangoverFrames != nil {
		c.VADHangoverFrames = *p.VADHangoverFrames
	}
	if p.BargeInFrames != nil {
		c.BargeInFrames = *p.BargeInFrames
	}
	if p.MinSentenceChars != nil {
		c.MinSentenceChars = *p.MinSentenceChars
	}
	if p.SynthAhead != nil {
		c.SynthAhead = *p.SynthAhead
	}
	if p.EndpointGraceMs != nil {
		c.EndpointGraceMs = *p.EndpointGraceMs
	}
	return c
}

func (c Config) Resolve(params map[string]string) Config {
	out := c
	if params != nil {
		if name := params["profile"]; name != "" {
			if p, ok := c.Profiles[name]; ok {
				out = out.apply(p)
			}
		}
		setStr(&out.SystemPrompt, params["system_prompt"])
		setStr(&out.FirstMessage, params["first_message"])
		setStr(&out.STTProvider, params["stt_provider"])
		setStr(&out.LLMProvider, params["llm_provider"])
		setStr(&out.TTSProvider, params["tts_provider"])
		setStr(&out.OpenAIModel, params["llm_model"])
		setStr(&out.GroqModel, params["llm_model"])
		setStr(&out.CartesiaVoiceID, params["voice_id"])
		setStr(&out.CartesiaModel, params["tts_model"])
		setStr(&out.CartesiaLanguage, params["language"])
	}
	return out
}

func setStr(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}
