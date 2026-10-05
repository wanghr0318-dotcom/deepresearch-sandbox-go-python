package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// ChatConfig 配置 OpenAI 兼容的 chat adapter。
type ChatConfig struct {
	BaseURL          string // 例如 https://api.openai.com/v1；请求发往 BaseURL + "/chat/completions"
	Model            string // 唯一声明的模型
	APIKey           string // 来自宿主环境变量 AGENTBOX_MODEL_API_KEY；只放在 Authorization 头
	Pricing          Pricing
	MaxTokensDefault int          // max_tokens 缺省值；≤ 0 时取 1024
	HTTP             *http.Client // 须来自验证 Dialer.HTTPClient；nil 时用默认 Dialer（8 MiB）
}

type chatAdapter struct {
	cfg  ChatConfig
	http *http.Client
}

// NewChat 构造 OpenAI 兼容的 chat adapter（§9.3 子集：仅声明的模型、纯文本消息、非流式）。
func NewChat(cfg ChatConfig) Adapter {
	if cfg.MaxTokensDefault <= 0 {
		cfg.MaxTokensDefault = defaultMaxTokens
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &chatAdapter{cfg: cfg, http: defaultClient(cfg.HTTP, DefaultModelMaxBody)}
}

func (a *chatAdapter) Kind() Kind       { return KindChat }
func (a *chatAdapter) Provider() string { return "openai_compat" }
func (a *chatAdapter) Version() string  { return "openai_compat/1" }

// 允许的顶层字段与消息字段。n、logprobs、max_completion_tokens 等会改变计量或语义的字段一律拒绝。
var (
	chatFields = []string{
		"model", "messages", "max_tokens", "temperature", "top_p", "stop", "stream",
		"tools", "tool_choice", "response_format", "seed", "presence_penalty", "frequency_penalty",
	}
	chatMessageFields = []string{"role", "content", "name", "tool_call_id", "tool_calls"}
)

// Resolve：stream 只能缺省或 false（删去）；messages 非空且每条 content 为字符串；model 缺省补配置模型、
// 显式时须等于配置模型；max_tokens 缺省补 MaxTokensDefault。补上的值进入 applied_defaults。
func (a *chatAdapter) Resolve(body []byte) ([]byte, map[string]any, error) {
	obj, e := decodeObject(body)
	if e != nil {
		return nil, nil, e
	}
	if e := checkFields(obj, chatFields...); e != nil {
		return nil, nil, e
	}
	defaults := map[string]any{}
	if raw, ok := obj["stream"]; ok {
		switch strings.TrimSpace(string(raw)) {
		case "false":
			delete(obj, "stream")
		case "true":
			return nil, nil, fatalf(http.StatusBadRequest, CodeUnsupportedField, "不支持 stream: true")
		default:
			return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "stream 须为布尔值")
		}
	}
	if e := checkMessages(obj["messages"]); e != nil {
		return nil, nil, e
	}
	if raw, ok := obj["model"]; ok {
		var m string
		if json.Unmarshal(raw, &m) != nil {
			return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "model 须为字符串")
		}
		if m != a.cfg.Model {
			return nil, nil, fatalf(http.StatusBadRequest, CodeUnsupportedModel, "模型 %q 未声明", m)
		}
	} else {
		obj["model"], _ = json.Marshal(a.cfg.Model)
		defaults["model"] = a.cfg.Model
	}
	if raw, ok := obj["max_tokens"]; ok {
		if _, e := positiveInt(raw, "max_tokens"); e != nil {
			return nil, nil, e
		}
	} else {
		obj["max_tokens"], _ = json.Marshal(a.cfg.MaxTokensDefault)
		defaults["max_tokens"] = a.cfg.MaxTokensDefault
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "无法编码请求")
	}
	return out, defaults, nil
}

func checkMessages(raw json.RawMessage) *Error {
	var msgs []map[string]json.RawMessage
	if raw == nil || json.Unmarshal(raw, &msgs) != nil || len(msgs) == 0 {
		return fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages 须为非空数组")
	}
	for i, m := range msgs {
		if m == nil {
			return fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages[%d] 须为对象", i)
		}
		if e := checkFields(m, chatMessageFields...); e != nil {
			return e
		}
		var role string
		if json.Unmarshal(m["role"], &role) != nil || role == "" {
			return fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages[%d].role 须为字符串", i)
		}
		var content string
		c, ok := m["content"]
		if !ok || !bytes.HasPrefix(bytes.TrimSpace(c), []byte(`"`)) || json.Unmarshal(c, &content) != nil {
			return fatalf(http.StatusBadRequest, CodeUnsupportedField, "messages[%d].content 须为字符串（仅支持纯文本消息）", i)
		}
	}
	return nil
}

// chatInputs 从规范化请求中取出 input_estimate 与 max_tokens。
// input_estimate = ceil((bytes(messages) + bytes(tools)) / 4)：按每 4 字节一个 token 保守估计，含工具定义（§9.6）。
func chatInputs(resolved []byte) (inputEst, maxTokens int64, e *Error) {
	obj, e := decodeObject(resolved)
	if e != nil {
		return 0, 0, e
	}
	if maxTokens, e = positiveInt(obj["max_tokens"], "max_tokens"); e != nil {
		return 0, 0, e
	}
	n := int64(len(obj["messages"]) + len(obj["tools"]))
	return (n + 3) / 4, maxTokens, nil
}

// Estimate = input_estimate × 输入单价 + max_tokens × 输出单价（单价按每百万 token 微美元，各项向上取整）。
func (a *chatAdapter) Estimate(resolved []byte) (int64, error) {
	in, maxTok, e := chatInputs(resolved)
	if e != nil {
		return 0, e
	}
	ci, err := mulCeil(in, a.cfg.Pricing.InputMicroPerMTok, microPerMillionTokens)
	if err != nil {
		return 0, err
	}
	co, err := mulCeil(maxTok, a.cfg.Pricing.OutputMicroPerMTok, microPerMillionTokens)
	if err != nil {
		return 0, err
	}
	if ci > (1<<63-1)-co {
		return 0, errors.New("估算溢出")
	}
	return ci + co, nil
}

type chatReply struct {
	ID    string `json:"id"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// Do 发出非流式请求。2xx 且可解析 → ok（Body 为上游原文）；上游未返回 usage 时按保守估算计量
// （input_estimate、max_tokens）；2xx 但无法解析 → unknown（已发出、用量无法确认）。
func (a *chatAdapter) Do(ctx context.Context, resolved []byte) (Response, *Error) {
	in, maxTok, e := chatInputs(resolved)
	if e != nil {
		return Response{}, e
	}
	req, err := http.NewRequest(http.MethodPost, a.cfg.BaseURL+"/chat/completions", bytes.NewReader(resolved))
	if err != nil {
		return Response{}, newErr(OutcomeFatal, http.StatusBadGateway, CodeInvalidURL, errors.New("模型上游地址不合法"))
	}
	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set("Accept", contentTypeJSON)
	req.Header.Set("User-Agent", userAgent)
	if a.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	r, e := exchange(ctx, a.http, req, true)
	if e != nil {
		return Response{RetryAfter: r.retryAfter}, e
	}
	var reply chatReply
	if err := json.Unmarshal(r.body, &reply); err != nil {
		return Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("上游响应不是合法 JSON"))
	}
	usage := Usage{Requests: 1, ResponseBytes: int64(len(r.body)), InputTokens: in, OutputTokens: maxTok}
	if reply.Usage != nil {
		usage.InputTokens, usage.OutputTokens = reply.Usage.PromptTokens, reply.Usage.CompletionTokens
	}
	id := r.resp.Header.Get("X-Request-Id")
	if id == "" {
		id = reply.ID
	}
	return Response{Body: r.body, Usage: usage, UpstreamRequestID: id}, nil
}
