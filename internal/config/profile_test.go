package config

import "testing"

func TestResolveProfileAndParams(t *testing.T) {
	temp := 0.3
	base := Config{
		SystemPrompt:    "default",
		LLMProvider:     "openai",
		OpenAIModel:     "gpt-4o-mini",
		CartesiaVoiceID: "voice-default",
		LLMTemperature:  -1,
		Profiles: map[string]Profile{
			"sales": {
				SystemPrompt:    "you sell things",
				CartesiaVoiceID: "voice-sales",
				Temperature:     &temp,
			},
		},
	}

	got := base.Resolve(map[string]string{"profile": "sales"})
	if got.SystemPrompt != "you sell things" {
		t.Fatalf("profile system prompt not applied: %q", got.SystemPrompt)
	}
	if got.CartesiaVoiceID != "voice-sales" {
		t.Fatalf("profile voice not applied: %q", got.CartesiaVoiceID)
	}
	if got.LLMTemperature != 0.3 {
		t.Fatalf("profile temperature not applied: %v", got.LLMTemperature)
	}
	if base.SystemPrompt != "default" {
		t.Fatal("Resolve mutated the base config")
	}

	override := base.Resolve(map[string]string{
		"profile":       "sales",
		"voice_id":      "voice-call",
		"llm_model":     "gpt-4o",
		"system_prompt": "be terse",
	})
	if override.CartesiaVoiceID != "voice-call" {
		t.Fatalf("param voice override failed: %q", override.CartesiaVoiceID)
	}
	if override.OpenAIModel != "gpt-4o" {
		t.Fatalf("param model override failed: %q", override.OpenAIModel)
	}
	if override.SystemPrompt != "be terse" {
		t.Fatalf("param prompt override failed: %q", override.SystemPrompt)
	}

	none := base.Resolve(nil)
	if none.SystemPrompt != "default" {
		t.Fatalf("nil params should keep defaults: %q", none.SystemPrompt)
	}
}
