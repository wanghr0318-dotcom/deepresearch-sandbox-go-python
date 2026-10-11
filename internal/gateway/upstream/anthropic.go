package upstream

// Anthropic 格式的供应商（模型网关设计 §7.3、§7.6）：路由的 protocol 为 anthropic 时，请求发往 BaseURL + "/v1/messages"，
// Key 放在 x-api-key 头，并带 anthropic-version。沙箱 Worker 仍发送 OpenAI 格式：DoOn 把规范化的 OpenAI 请求经
// llmproto 转为 Anthropic 格式，再把回复转回 OpenAI 格式，Worker 看到的形状与 OpenAI 供应商相同。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// DefaultAnthropicVersion 是未配置 anthropic_version 时发送的 anthropic-version 头。
const DefaultAnthropicVersion = "2023-06-01"

// anthropicMessagesPath 是 Anthropic Messages API 相对 BaseURL 的路径（BaseURL 不含 /v1，例如 https://api.anthropic.com）。
const anthropicMessagesPath = "/v1/messages"

// endpoint 是路由的对话接口地址。
func (r chatRoute) endpoint() string {
	if r.protocol == llmproto.ProtocolAnthropic {
		return r.baseURL + anthropicMessagesPath
	}
	return r.baseURL + "/chat/completions"
}

// newRequest 构造发往路由的 POST 请求：按协议放置 Key（Authorization: Bearer 或 x-api-key + anthropic-version）。
func (r chatRoute) newRequest(body []byte, stream bool) (*http.Request, *Error) {
	req, err := http.NewRequest(http.MethodPost, r.endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, newErr(OutcomeFatal, http.StatusBadGateway, CodeInvalidURL, errors.New("模型上游地址不合法"))
	}
	req.Header.Set("Content-Type", contentTypeJSON)
	if stream {
		req.Header.Set("Accept", llmproto.SSEContentType)
	} else {
		req.Header.Set("Accept", contentTypeJSON)
	}
	req.Header.Set("User-Agent", userAgent)
	if r.protocol == llmproto.ProtocolAnthropic {
		req.Header.Set("Anthropic-Version", r.anthropicVersion)
		if r.apiKey != "" {
			req.Header.Set("X-Api-Key", r.apiKey)
		}
	} else if r.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.apiKey)
	}
	return req, nil
}

// conversionError 把 llmproto 的转换错误归为 fatal：不能转换的字段为 400 unsupported_field，其余为 400 invalid_request。
func conversionError(err error) *Error {
	var uf *llmproto.UnsupportedFieldError
	if errors.As(err, &uf) {
		return newErr(OutcomeFatal, http.StatusBadRequest, CodeUnsupportedField, err)
	}
	return newErr(OutcomeFatal, http.StatusBadRequest, CodeInvalidRequest, err)
}

// stripReasoning 删去 assistant 消息的 reasoning_content：它是 OpenAI 兼容推理模型的多轮字段（没有签名），
// 不是合法的 Anthropic thinking 块，转换时不发送（Anthropic 模型不需要之前轮次的推理）。
func stripReasoning(resolved []byte) ([]byte, *Error) {
	obj, e := decodeObject(resolved)
	if e != nil {
		return nil, e
	}
	var msgs []map[string]json.RawMessage
	if json.Unmarshal(obj["messages"], &msgs) != nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "messages 须为数组")
	}
	changed := false
	for _, m := range msgs {
		if _, ok := m["reasoning_content"]; ok {
			delete(m, "reasoning_content")
			changed = true
		}
	}
	if !changed {
		return resolved, nil
	}
	raw, err := json.Marshal(msgs)
	if err != nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "无法编码请求")
	}
	obj["messages"] = raw
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "无法编码请求")
	}
	return out, nil
}

// sandboxToAnthropic 把规范化的 OpenAI 请求（Resolve 的结果）转为发往 Anthropic 路由的请求体，模型换成该供应商的模型名。
func (r chatRoute) sandboxToAnthropic(resolved []byte, model string) ([]byte, *Error) {
	body, e := stripReasoning(resolved)
	if e != nil {
		return nil, e
	}
	req, err := llmproto.DecodeOpenAIRequest(body)
	if err != nil {
		return nil, conversionError(err)
	}
	req.Model = r.providerModel(model)
	enc, err := llmproto.EncodeRequest(req, llmproto.ProtocolAnthropic)
	if err != nil {
		return nil, conversionError(err)
	}
	return enc.Body, nil
}

// convertible 报告规范化的 OpenAI 请求能否在该路由上执行（OpenAI 路由总能；Anthropic 路由须能转换）。
func (r chatRoute) convertible(resolved []byte, model string) bool {
	if r.protocol != llmproto.ProtocolAnthropic {
		return true
	}
	_, e := r.sandboxToAnthropic(resolved, model)
	return e == nil
}

// doAnthropic 是 DoOn 在 Anthropic 路由上的执行：转换请求、发出、把回复转回 OpenAI 格式。用量取回复的 usage
// （InputTokens 为全部输入，含缓存读取与写入）；没有 usage 时按保守估算。回复无法解析为 unknown / upstream_bad_response。
func (a *chatAdapter) doAnthropic(ctx context.Context, rt chatRoute, resolved []byte, in, maxTok int64, model string) (Response, *Error) {
	body, e := rt.sandboxToAnthropic(resolved, model)
	if e != nil {
		return Response{}, e
	}
	req, e := rt.newRequest(body, false)
	if e != nil {
		return Response{}, e
	}
	r, e := exchange(ctx, rt.http, req, true)
	if e != nil {
		return Response{RetryAfter: r.retryAfter}, e
	}
	reply, err := llmproto.DecodeAnthropicResponse(r.body)
	if err != nil {
		return Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("上游响应不是合法的 Anthropic 消息"))
	}
	if reply.Created == 0 {
		reply.Created = time.Now().Unix()
	}
	out, err := llmproto.EncodeOpenAIResponse(reply)
	if err != nil {
		return Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("无法把上游响应转为 OpenAI 格式"))
	}
	usage := Usage{Requests: 1, ResponseBytes: int64(len(r.body)), InputTokens: in, OutputTokens: maxTok}
	if reply.HasUsage {
		usage = UsageFrom(reply.Usage)
		usage.Requests, usage.ResponseBytes = 1, int64(len(r.body))
	}
	id := r.resp.Header.Get("Request-Id")
	if id == "" {
		id = reply.ID
	}
	return Response{Body: out, Usage: usage, UpstreamRequestID: id}, nil
}

// UsageFrom 把 llmproto 的用量（各项互不包含）转为 Gateway 的用量（InputTokens 含缓存读取与写入）。
func UsageFrom(u llmproto.Usage) Usage {
	return Usage{InputTokens: u.PromptTotal(), OutputTokens: u.OutputTokens, CacheReadTokens: u.CacheReadTokens,
		CacheWriteTokens: u.CacheWriteTokens}
}
