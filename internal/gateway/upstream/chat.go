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
	BaseURL string   // 例如 https://api.openai.com/v1；请求发往 BaseURL + "/chat/completions"
	Model   string   // 默认模型：请求未给 model 时使用（记入 applied_defaults）；总在白名单中
	Models  []string // 声明的模型白名单（§9.3）；空时只有 Model
	APIKey  string   // 来自宿主环境变量 AGENTBOX_MODEL_API_KEY；只放在 Authorization 头
	Pricing Pricing  // 默认价格表
	// PricingByModel 是按模型的价格表；缺项的模型用 Pricing。
	PricingByModel   map[string]Pricing
	MaxTokensDefault int          // max_tokens 缺省值；≤ 0 时取 1024（不超过 MaxTokensCap）
	MaxTokensCap     int          // max_tokens 上限，超出者截断（§9.6）；≤ 0 时取 4096
	HTTP             *http.Client // 须来自验证 Dialer.HTTPClient；nil 时用默认 Dialer（8 MiB）
	// PrimaryName 是主供应商的路由名（Router.Routes()[0]）；空时为 "primary"。
	PrimaryName string
	// Fallbacks 是按顺序的后备供应商（模型降级链）；空时只有主供应商，行为与引入降级链之前完全相同。
	// 非空时（链模式）2xx 但没有非空 choices 数组的响应按 unknown / upstream_bad_response 处理（可转下一供应商）。
	Fallbacks []ChatRoute
}

// ChatRoute 是降级链中的一个后备供应商（OpenAI 兼容）。
type ChatRoute struct {
	Name    string // 路由名（进入 call_tries.provider 与日志）
	BaseURL string // 请求发往 BaseURL + "/chat/completions"
	APIKey  string // 来自宿主环境变量（配置给出变量名）；只放在 Authorization 头
	// Models 把逻辑模型（声明的白名单中的名字）映射为该供应商的模型名；不在其中的逻辑模型不由它提供。
	// 空时以同名提供全部声明的模型。
	Models         map[string]string
	Pricing        Pricing            // 该供应商的默认价格表
	PricingByModel map[string]Pricing // 按逻辑模型的价格表；缺项用 Pricing
	HTTP           *http.Client       // 须来自验证 Dialer.HTTPClient；nil 时用默认 Dialer（8 MiB）
}

// PricingFor 返回模型的价格表：PricingByModel 中有该模型时取它，否则取默认 Pricing。
func (c ChatConfig) PricingFor(model string) Pricing {
	if p, ok := c.PricingByModel[model]; ok {
		return p
	}
	return c.Pricing
}

type chatAdapter struct {
	cfg      ChatConfig
	allowed  map[string]bool // Model ∪ Models
	http     *http.Client
	routes   []chatRoute // 下标 0 为主供应商（由 cfg 构造）
	strictOK bool        // 链模式：2xx 须有非空 choices
}

// chatRoute 是一个路由的执行参数。主供应商（下标 0）的 models 为 nil（逻辑模型名原样发出），价格取 cfg.PricingFor。
type chatRoute struct {
	name, baseURL, apiKey string
	models                map[string]string
	pricing               Pricing
	pricingByModel        map[string]Pricing
	http                  *http.Client
}

// NewChat 构造 OpenAI 兼容的 chat adapter（§9.3 子集：仅声明的模型、纯文本消息、非流式）。
func NewChat(cfg ChatConfig) Adapter {
	allowed := map[string]bool{cfg.Model: true}
	for _, m := range cfg.Models {
		allowed[m] = true
	}
	if cfg.MaxTokensCap <= 0 {
		cfg.MaxTokensCap = defaultMaxTokensCap
	}
	if cfg.MaxTokensDefault <= 0 {
		cfg.MaxTokensDefault = defaultMaxTokens
	}
	cfg.MaxTokensDefault = min(cfg.MaxTokensDefault, cfg.MaxTokensCap)
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.PrimaryName == "" {
		cfg.PrimaryName = "primary"
	}
	a := &chatAdapter{cfg: cfg, allowed: allowed, http: defaultClient(cfg.HTTP, DefaultModelMaxBody), strictOK: len(cfg.Fallbacks) > 0}
	a.routes = append(a.routes, chatRoute{name: cfg.PrimaryName, baseURL: cfg.BaseURL, apiKey: cfg.APIKey, http: a.http})
	for _, r := range cfg.Fallbacks {
		a.routes = append(a.routes, chatRoute{name: r.Name, baseURL: strings.TrimRight(r.BaseURL, "/"), apiKey: r.APIKey,
			models: r.Models, pricing: r.Pricing, pricingByModel: r.PricingByModel, http: defaultClient(r.HTTP, DefaultModelMaxBody)})
	}
	return a
}

// Routes 实现 Router：主供应商在前，后备供应商按配置顺序。
func (a *chatAdapter) Routes() []string {
	out := make([]string, len(a.routes))
	for i, r := range a.routes {
		out[i] = r.name
	}
	return out
}

// Serves 实现 Router：主供应商提供全部声明的模型；后备供应商提供 Models 中的逻辑模型（Models 为空时全部）。
func (a *chatAdapter) Serves(route int, model string) bool {
	if route < 0 || route >= len(a.routes) || !a.allowed[model] {
		return false
	}
	if route == 0 || len(a.routes[route].models) == 0 {
		return true
	}
	_, ok := a.routes[route].models[model]
	return ok
}

// PricingOn 实现 Router：主供应商为 cfg.PricingFor；后备供应商为其按模型的价格表，缺项用其默认价格表。
func (a *chatAdapter) PricingOn(route int, model string) Pricing {
	if route <= 0 || route >= len(a.routes) {
		return a.cfg.PricingFor(model)
	}
	r := a.routes[route]
	if p, ok := r.pricingByModel[model]; ok {
		return p
	}
	return r.pricing
}

// providerModel 返回路由对逻辑模型使用的模型名（主供应商与未映射时原样）。
func (r chatRoute) providerModel(model string) string {
	if m, ok := r.models[model]; ok && m != "" {
		return m
	}
	return model
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
	chatMessageFields = []string{"role", "content", "name", "tool_call_id", "tool_calls", "reasoning_content"}
)

// Resolve：stream 只能缺省或 false（删去）；messages 非空且每条 content 为字符串；model 缺省补默认模型、
// 显式时须在声明的白名单中（否则 unsupported_model）；max_tokens 缺省补 MaxTokensDefault，超过 MaxTokensCap 时截断。补上或截断的值进入 applied_defaults。
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
		if !a.allowed[m] {
			return nil, nil, fatalf(http.StatusBadRequest, CodeUnsupportedModel, "模型 %q 未声明", m)
		}
	} else {
		obj["model"], _ = json.Marshal(a.cfg.Model)
		defaults["model"] = a.cfg.Model
	}
	if raw, ok := obj["max_tokens"]; ok {
		n, e := positiveInt(raw, "max_tokens")
		if e != nil {
			return nil, nil, e
		}
		if n > int64(a.cfg.MaxTokensCap) {
			// 截断到上限并记入 applied_defaults，使指纹反映生效值。
			obj["max_tokens"], _ = json.Marshal(a.cfg.MaxTokensCap)
			defaults["max_tokens"] = a.cfg.MaxTokensCap
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
		if e := checkAssistantFields(i, role, m); e != nil {
			return e
		}
		c, ok := m["content"]
		if role == "assistant" && (!ok || isJSONNull(c)) && hasToolCalls(m) {
			continue // 只发起工具调用的 assistant 消息：content 可缺省或为 null
		}
		if !ok || !isJSONString(c) {
			return fatalf(http.StatusBadRequest, CodeUnsupportedField, "messages[%d].content 须为字符串（仅支持纯文本消息）", i)
		}
	}
	return nil
}

// checkAssistantFields 是推理模型的多轮工具调用（契约 H）：reasoning_content 只出现在 assistant 消息中且为字符串；
// assistant 的 tool_calls 须为数组。content 可为空串（由调用方的字符串检查接受）。
func checkAssistantFields(i int, role string, m map[string]json.RawMessage) *Error {
	if rc, ok := m["reasoning_content"]; ok {
		if role != "assistant" {
			return fatalf(http.StatusBadRequest, CodeUnsupportedField, "messages[%d].reasoning_content 只用于 assistant 消息", i)
		}
		if !isJSONString(rc) && !isJSONNull(rc) {
			return fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages[%d].reasoning_content 须为字符串", i)
		}
	}
	if tc, ok := m["tool_calls"]; ok && role == "assistant" {
		var calls []json.RawMessage
		if !isJSONNull(tc) && json.Unmarshal(tc, &calls) != nil {
			return fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages[%d].tool_calls 须为数组", i)
		}
	}
	return nil
}

// nonEmptyArray 报告 raw 是否为非空 JSON 数组。
func nonEmptyArray(raw json.RawMessage) bool {
	var xs []json.RawMessage
	return json.Unmarshal(raw, &xs) == nil && len(xs) > 0
}

func hasToolCalls(m map[string]json.RawMessage) bool {
	var calls []json.RawMessage
	return json.Unmarshal(m["tool_calls"], &calls) == nil && len(calls) > 0
}

func isJSONString(raw json.RawMessage) bool {
	var s string
	return bytes.HasPrefix(bytes.TrimSpace(raw), []byte(`"`)) && json.Unmarshal(raw, &s) == nil
}

func isJSONNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

// chatInputs 从规范化请求中取出 input_estimate、max_tokens 与（解析后的）model。
// input_estimate = ceil((bytes(messages) + bytes(tools)) / 4)：按每 4 字节一个 token 保守估计，含工具定义（§9.6）。
func chatInputs(resolved []byte) (inputEst, maxTokens int64, model string, e *Error) {
	obj, e := decodeObject(resolved)
	if e != nil {
		return 0, 0, "", e
	}
	if maxTokens, e = positiveInt(obj["max_tokens"], "max_tokens"); e != nil {
		return 0, 0, "", e
	}
	if json.Unmarshal(obj["model"], &model) != nil {
		return 0, 0, "", fatalf(http.StatusBadRequest, CodeInvalidRequest, "model 须为字符串")
	}
	n := int64(len(obj["messages"]) + len(obj["tools"]))
	return (n + 3) / 4, maxTokens, model, nil
}

// Estimate = input_estimate × 输入单价 + max_tokens × 输出单价（单价按每百万 token 微美元，各项向上取整）。
// 单价取解析后模型的价格表（PricingFor）。
func (a *chatAdapter) Estimate(resolved []byte) (int64, error) { return a.EstimateOn(0, resolved) }

// EstimateOn 实现 Router：同 Estimate，单价取该路由的价格表（PricingOn）。
func (a *chatAdapter) EstimateOn(route int, resolved []byte) (int64, error) {
	in, maxTok, model, e := chatInputs(resolved)
	if e != nil {
		return 0, e
	}
	p := a.PricingOn(route, model)
	ci, err := mulCeil(in, p.InputMicroPerMTok, microPerMillionTokens)
	if err != nil {
		return 0, err
	}
	co, err := mulCeil(maxTok, p.OutputMicroPerMTok, microPerMillionTokens)
	if err != nil {
		return 0, err
	}
	if ci > (1<<63-1)-co {
		return 0, errors.New("估算溢出")
	}
	return ci + co, nil
}

type chatReply struct {
	ID      string          `json:"id"`
	Choices json.RawMessage `json:"choices"` // 只在链模式下检查（非空数组）
	Usage   *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// Do 发出非流式请求。2xx 且可解析 → ok（Body 为上游原文）；上游未返回 usage 时按保守估算计量
// （input_estimate、max_tokens）；2xx 但无法解析 → unknown（已发出、用量无法确认）。
func (a *chatAdapter) Do(ctx context.Context, resolved []byte) (Response, *Error) {
	return a.DoOn(ctx, 0, resolved)
}

// DoOn 实现 Router：向该路由发出请求（请求体的 model 换成该供应商的模型名，其余不变）。链模式下 2xx 但没有
// 非空 choices 的响应为 unknown / upstream_bad_response（已发出、可能已计费；Coordinator 可转下一供应商）。
func (a *chatAdapter) DoOn(ctx context.Context, route int, resolved []byte) (Response, *Error) {
	if route < 0 || route >= len(a.routes) {
		return Response{}, fatalf(http.StatusBadGateway, CodeUnsupportedProvider, "路由 %d 不存在", route)
	}
	rt := a.routes[route]
	in, maxTok, model, e := chatInputs(resolved)
	if e != nil {
		return Response{}, e
	}
	if pm := rt.providerModel(model); pm != model {
		obj, e := decodeObject(resolved)
		if e != nil {
			return Response{}, e
		}
		obj["model"], _ = json.Marshal(pm)
		out, err := json.Marshal(obj)
		if err != nil {
			return Response{}, fatalf(http.StatusBadRequest, CodeInvalidRequest, "无法编码请求")
		}
		resolved = out
	}
	req, err := http.NewRequest(http.MethodPost, rt.baseURL+"/chat/completions", bytes.NewReader(resolved))
	if err != nil {
		return Response{}, newErr(OutcomeFatal, http.StatusBadGateway, CodeInvalidURL, errors.New("模型上游地址不合法"))
	}
	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set("Accept", contentTypeJSON)
	req.Header.Set("User-Agent", userAgent)
	if rt.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+rt.apiKey)
	}
	r, e := exchange(ctx, rt.http, req, true)
	if e != nil {
		return Response{RetryAfter: r.retryAfter}, e
	}
	var reply chatReply
	if err := json.Unmarshal(r.body, &reply); err != nil {
		return Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("上游响应不是合法 JSON"))
	}
	if a.strictOK && !nonEmptyArray(reply.Choices) {
		return Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("上游响应没有 choices"))
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
