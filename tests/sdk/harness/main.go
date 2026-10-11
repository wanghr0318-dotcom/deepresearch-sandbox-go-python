// Command harness serves the model API for the SDK compatibility tests (tests/sdk, model gateway design §10): the
// real internal/api handler (key authentication, errors in the caller's format) in front of internal/modelapi, on a
// fresh PostgreSQL database (quota holds, model_requests), with tests/e2e/fakeupstream as the provider. Test-only.
//
// It serves two model APIs:
//   - "openai": the provider is fakeupstream in OpenAI mode (/v1/chat/completions);
//   - "anthropic": the provider is fakeupstream in Anthropic mode (/v1/messages). The Gateway's primary route is
//     always OpenAI-format, so it points at a closed port; it fails before sending and the request fails over.
//
// Both serve logical model "m-1". A request with tools is answered with a get_weather tool call.
//
// It reads AGENTBOX_TEST_DATABASE_URL (an admin DSN: a database is created and dropped), prints one JSON line
// {"openai": URL, "anthropic": URL, "key": API key, "model": "m-1"} and serves until stdin is closed or a signal arrives.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/modelapi"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ownership"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

const model = "m-1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "harness:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	admin := os.Getenv("AGENTBOX_TEST_DATABASE_URL")
	if admin == "" {
		return errors.New("AGENTBOX_TEST_DATABASE_URL is not set")
	}
	dsn, drop, err := newDatabase(ctx, admin)
	if err != nil {
		return err
	}
	defer drop()
	st, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.InitializeInstallation(ctx, "sdk-harness", make([]byte, ownership.TokenSize)); err != nil {
		return err
	}
	if err := st.CompleteInstallation(ctx, "sdk-harness"); err != nil {
		return err
	}
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	u, err := st.CreateUser(ctx, "sdk", "sdk", "unused")
	if err != nil {
		return err
	}
	pepper := make([]byte, 32)
	_, _ = rand.Read(pepper)
	g, err := apikey.Generate(pepper)
	if err != nil {
		return err
	}
	if _, err := st.Tenancy().CreateKey(ctx, tenant.NewKey{ID: g.ID, UserID: u.ID, Name: "sdk", SecretHMAC: g.SecretHMAC,
		Scopes: tenant.Models}); err != nil {
		return err
	}

	fu := fakeupstream.New()
	defer fu.Close()
	fu.SetChatHook(func(body []byte) json.RawMessage {
		if !strings.Contains(string(body), `"tools"`) || strings.Contains(string(body), `"role":"tool"`) {
			return nil
		}
		return json.RawMessage(`{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]}`)
	})
	fu.SetMessagesHook(func(body []byte) json.RawMessage {
		if !strings.Contains(string(body), `"tools"`) || strings.Contains(string(body), `"tool_result"`) {
			return nil
		}
		return json.RawMessage(`[{"type":"text","text":"Let me check."},{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}]`)
	})
	verifier := apikey.NewVerifier(st.APIKeys(), pepper, nil)
	tmp, err := os.MkdirTemp("", "agentbox-sdk-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	blobs, err := blob.NewLocal(tmp)
	if err != nil {
		return err
	}
	limiter, streams := ratelimit.NewLocal(time.Now), ratelimit.NewConcurrency()
	out := map[string]string{"key": g.Full, "model": model}
	for _, mode := range []string{"openai", "anthropic"} {
		rt, err := routing(fu, mode)
		if err != nil {
			return err
		}
		mh := modelapi.New(modelapi.Config{Routing: func() *call.ModelRouting { return rt }, Models: []string{model},
			Holder: st.Quota(), Limits: st.Quota(), Limiter: limiter, Streams: streams, Store: st.ModelRequests()})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		h, err := api.New(api.Config{Store: st, Blobs: blobs, ListenAddr: ln.Addr().String(), Accounts: st,
			ResearchSpec: func(string) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
			Tenancy:      st.Tenancy(), APIKeys: verifier, Quota: st.Quota(), RateLimiter: limiter, ModelAPI: mh,
			Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))})
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: h}
		go func() { _ = srv.Serve(ln) }()
		defer func() { _ = srv.Close() }()
		out[mode] = "http://" + ln.Addr().String()
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	done := make(chan struct{})
	go func() {
		_, _ = bufio.NewReader(os.Stdin).ReadString(0) // until EOF
		close(done)
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	return nil
}

// routing builds the Gateway's model routes for a mode.
func routing(fu *fakeupstream.Server, mode string) (*call.ModelRouting, error) {
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	price := upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}
	cfg := upstream.ChatConfig{BaseURL: fu.ModelBaseURL(), Model: model, APIKey: "sk-fake", Pricing: price,
		HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0), StreamHTTP: d.HTTPClient(0, 0)}
	if mode == "anthropic" {
		cfg.BaseURL = "http://127.0.0.1:1/v1"
		cfg.Fallbacks = []upstream.ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: fu.URL(),
			APIKey: "sk-ant-fake", Pricing: price, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0), StreamHTTP: d.HTTPClient(0, 0)}}
	}
	c, err := call.New(call.Config{Store: nopStore{}, Blobs: nopBlobs{}, Events: nopEvents{}, Adapters: []upstream.Adapter{upstream.NewChat(cfg)}})
	if err != nil {
		return nil, err
	}
	return c.ModelRouting(), nil
}

// call.New requires these; the model routes never use them.
type nopStore struct{ call.Store }
type nopBlobs struct{ call.BlobStore }
type nopEvents struct{ call.HostEvents }

// newDatabase creates a fresh database next to admin's and returns its DSN and a function that drops it.
func newDatabase(ctx context.Context, admin string) (string, func(), error) {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_sdk_" + hex.EncodeToString(b)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", nil, err
	}
	u, err := url.Parse(admin)
	if err != nil {
		return "", nil, err
	}
	u.Path = "/" + name
	drop := func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	}
	return u.String(), drop, nil
}
