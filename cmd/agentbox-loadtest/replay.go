package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type replayPrompt struct {
	ID     string `json:"id"`
	Group  string `json:"group"`
	Prompt string `json:"prompt"`
}

type replaySample struct {
	Kind    string       `json:"kind"`
	Role    string       `json:"role"`
	Index   int          `json:"index"`
	ID      string       `json:"id"`
	Group   string       `json:"group"`
	SHA256  string       `json:"request_sha256"`
	Outcome string       `json:"outcome"`
	Status  int          `json:"status"`
	TotalMS float64      `json:"total_ms"`
	TTFTMS  *float64     `json:"ttft_ms"`
	Usage   *openAIUsage `json:"usage"`
}

func runReplay(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	base := flags.String("base", "", "OpenAI base URL ending in /v1")
	corpus := flags.String("corpus", "", "fixed JSON array of id, group and prompt")
	model := flags.String("model", "", "model on this endpoint")
	role := flags.String("role", "", "direct or gateway")
	conc := flags.Int("concurrency", 1, "closed-loop concurrent requests")
	repeat := flags.Int("repeat", 1, "repetitions of the fixed corpus per run")
	maxTokens := flags.Int("max-tokens", 64, "output token budget")
	if err := flags.Parse(args); err != nil {
		return err
	}
	parsed, err := url.Parse(*base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(strings.TrimRight(parsed.Path, "/"), "/v1") ||
		*corpus == "" || *model == "" || (*role != "direct" && *role != "gateway") || *conc < 1 || *repeat < 1 || *maxTokens < 1 {
		return fmt.Errorf("replay requires --base URL/v1, --corpus, --model, --role direct|gateway and positive counts")
	}
	raw, err := os.ReadFile(*corpus)
	if err != nil {
		return err
	}
	var prompts []replayPrompt
	if err := json.Unmarshal(raw, &prompts); err != nil {
		return err
	}
	if len(prompts) == 0 {
		return fmt.Errorf("replay corpus is empty")
	}

	count := len(prompts) * *repeat
	responses := make([]result, count)
	digests := make([]string, count)
	token := os.Getenv("AGENTBOX_REPLAY_TOKEN")
	hc := newHTTPClient(*conc)
	defer hc.CloseIdleConnections()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	endpoint := directTarget(*base, "")
	start := time.Now()
	var wg sync.WaitGroup
	temperature := 0.0
	for w := range min(*conc, count) {
		wg.Go(func() {
			for i := w; i < count; i += *conc {
				prompt := prompts[i%len(prompts)]
				spec := reqSpec{Proto: OpenAI, Model: *model, Stream: true, MaxTokens: *maxTokens, Prompt: prompt.Prompt,
					Temperature: &temperature, Key: token}
				payload := spec.body()
				digests[i] = fmt.Sprintf("%x", sha256.Sum256(payload))
				responses[i] = sendBody(ctx, hc, endpoint, spec, payload)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	enc := json.NewEncoder(stdout)
	buckets := make(map[string][]result)
	usageSamples, inputTokens, outputTokens := 0, 0, 0
	for i, response := range responses {
		prompt := prompts[i%len(prompts)]
		buckets[prompt.Group] = append(buckets[prompt.Group], response)
		row := replaySample{Kind: "request", Role: *role, Index: i, ID: prompt.ID, Group: prompt.Group,
			SHA256: digests[i], Outcome: response.outcome(), Status: response.Status,
			TotalMS: durationMillis(response.Total), Usage: response.Usage}
		if response.TTFT > 0 {
			ttft := durationMillis(response.TTFT)
			row.TTFTMS = &ttft
		}
		if response.ok() && response.Usage != nil {
			usageSamples++
			inputTokens += response.Usage.Prompt
			outputTokens += response.Usage.Completion
		}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	aggregate := summarize(responses, elapsed)
	groups := make(map[string]summary, len(buckets))
	for group, samples := range buckets {
		groups[group] = summarize(samples, elapsed)
	}
	rec := map[string]any{
		"kind": "summary", "role": *role, "corpus_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)),
		"mode": "closed_loop", "concurrency": *conc, "repeat": *repeat, "model": *model, "max_tokens": *maxTokens,
		"temperature": temperature, "usage_samples": usageSamples, "latency": aggregate, "groups": groups,
	}
	// Publish request-window token throughput only when every successful request supplied usage.
	if usageSamples > 0 && usageSamples == aggregate.OK {
		rec["input_tokens_per_wall_second"] = round3(float64(inputTokens) / elapsed.Seconds())
		rec["output_tokens_per_wall_second"] = round3(float64(outputTokens) / elapsed.Seconds())
	}
	return enc.Encode(rec)
}

func durationMillis(d time.Duration) float64 {
	return round3(float64(d) / float64(time.Millisecond))
}
