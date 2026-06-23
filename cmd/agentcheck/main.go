package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/agent"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/config"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/media"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/obs"
	"github.com/PARANDHAMAREDDYBOMMAKA/go-pipeline/internal/providers"
)

const userUtterance = "What is two plus two"

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	sttc, err := providers.STT(cfg)
	must("stt", err)
	llmc, err := providers.LLM(cfg)
	must("llm", err)
	ttsc, err := providers.TTS(cfg)
	must("tts", err)

	mic := synthesizeMic(ctx, cfg)
	if len(mic) == 0 {
		log.Fatal("could not synthesize mic audio")
	}
	fmt.Printf("mic audio ready: %dms @ %dHz\n", len(mic)*1000/media.BusSampleRate, media.BusSampleRate)

	m := obs.New()
	ag := agent.New(agent.Config{
		SystemPrompt:  "You are a terse voice assistant. One short sentence.",
		MinSentenceCh: 12,
		Voice:         providers.Voice(cfg),
		Metrics:       m,
	}, sttc, llmc, ttsc)

	go func() { _ = ag.Run(ctx) }()

	var agentFrames int
	var firstAudioAt time.Duration
	start := time.Now()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ag.Source().Frames():
				if !ok {
					return
				}
				if agentFrames == 0 {
					firstAudioAt = time.Since(start)
				}
				agentFrames++
			}
		}
	}()

	stopSilence := make(chan struct{})
	feed(ag, mic)
	go feedSilence(ag, stopSilence)

	deadline := time.After(20 * time.Second)
	for agentFrames < 5 {
		select {
		case <-deadline:
			log.Printf("timeout: only %d agent frames", agentFrames)
			goto report
		case <-time.After(20 * time.Millisecond):
		}
	}
report:
	close(stopSilence)
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("agent replied: frames=%d first_audio=%s reply=%q\n",
		agentFrames, firstAudioAt.Round(time.Millisecond), lastAssistant(ag))
	fmt.Println("--- metrics ---")
	fmt.Print(m.Text())
}

func synthesizeMic(ctx context.Context, cfg config.Config) []int16 {
	client, err := providers.TTS(cfg)
	if err != nil {
		return nil
	}
	stream, err := client.Synthesize(ctx, userUtterance, providers.Voice(cfg))
	if err != nil {
		return nil
	}
	defer stream.Close()
	var all []int16
	rate := media.BusSampleRate
	for f := range stream.Audio() {
		rate = f.SampleRate
		all = append(all, f.Samples...)
	}
	return media.Resample(all, rate, media.BusSampleRate)
}

func feed(ag *agent.Agent, audio []int16) {
	n := media.SamplesPerFrame(media.BusSampleRate)
	for i := 0; i < len(audio); i += n {
		end := min(i+n, len(audio))
		_ = ag.Sink().Write(media.PCM{Samples: audio[i:end], SampleRate: media.BusSampleRate})
		time.Sleep(media.FrameMillis * time.Millisecond)
	}
}

func feedSilence(ag *agent.Agent, stop <-chan struct{}) {
	n := media.SamplesPerFrame(media.BusSampleRate)
	for {
		select {
		case <-stop:
			return
		default:
			_ = ag.Sink().Write(media.PCM{Samples: make([]int16, n), SampleRate: media.BusSampleRate})
			time.Sleep(media.FrameMillis * time.Millisecond)
		}
	}
}

func lastAssistant(ag *agent.Agent) string {
	h := ag.History()
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].Role == "assistant" {
			return h[i].Content
		}
	}
	return ""
}

func must(stage string, err error) {
	if err != nil {
		log.Fatalf("%s provider: %v", stage, err)
	}
}
