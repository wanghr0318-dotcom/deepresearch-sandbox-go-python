"""模型网关演示（scripts/demo-gateway.sh 第 3 步）：以未修改的官方 openai 与 anthropic Python SDK 调用模型 API。

由 demo-gateway.sh 以 `uv run --project tests/sdk` 运行（tests/sdk 的锁定依赖）。环境变量：
  AGENTBOX_DEMO_BASE  server 根地址（例如 http://127.0.0.1:18480）
  AGENTBOX_DEMO_KEY   models scope 的平台 key（只从环境变量读取，不打印）
逻辑模型 m-oa 由 OpenAI 格式的供应商提供，m-an 由 Anthropic 格式的供应商提供；两个供应商都是 fake upstream。
每个调用打印一行：SDK → 供应商格式、模型、回复摘要、用量。任何断言失败以非零状态退出。
"""

import json
import os
import sys

import anthropic
import openai

BASE = os.environ["AGENTBOX_DEMO_BASE"].rstrip("/")
KEY = os.environ["AGENTBOX_DEMO_KEY"]

WEATHER_OA = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Current weather of a city",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
    },
}
WEATHER_AN = {
    "name": "get_weather",
    "description": "Current weather of a city",
    "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
}
PROVIDER = {"m-oa": "OpenAI 供应商", "m-an": "Anthropic 供应商"}


def line(sdk: str, model: str, what: str, detail: str) -> None:
    print(f"  {sdk:<9} → {PROVIDER[model]:<14} {model:<5} {what:<10} {detail}")


def openai_calls(model: str) -> None:
    c = openai.OpenAI(base_url=BASE + "/v1", api_key=KEY, max_retries=0)
    msgs = [{"role": "user", "content": "用一句话介绍你自己"}]
    r = c.chat.completions.create(model=model, messages=msgs, max_tokens=64)
    assert r.choices[0].message.content, r
    line(
        "openai",
        model,
        "非流式",
        f"{r.choices[0].message.content!r} usage={r.usage.prompt_tokens}+{r.usage.completion_tokens}",
    )

    parts, usage = [], None
    stream = c.chat.completions.create(
        model=model, messages=msgs, max_tokens=64, stream=True, stream_options={"include_usage": True}
    )
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            parts.append(chunk.choices[0].delta.content)
        if chunk.usage:
            usage = chunk.usage
    assert parts and usage, (parts, usage)
    line(
        "openai",
        model,
        "流式",
        f"{len(parts)} 个内容 chunk {''.join(parts)!r} usage={usage.prompt_tokens}+{usage.completion_tokens}",
    )

    q = [{"role": "user", "content": "巴黎现在天气如何？"}]
    r = c.chat.completions.create(model=model, messages=q, tools=[WEATHER_OA], max_tokens=64)
    call = r.choices[0].message.tool_calls[0]
    assert r.choices[0].finish_reason == "tool_calls" and call.function.name == "get_weather", r
    line(
        "openai",
        model,
        "工具调用",
        f"{call.function.name}({call.function.arguments}) finish_reason={r.choices[0].finish_reason}",
    )
    q += [
        {"role": "assistant", "content": None, "tool_calls": [call.model_dump()]},
        {"role": "tool", "tool_call_id": call.id, "content": json.dumps({"city": "Paris", "temp_c": 18})},
    ]
    r = c.chat.completions.create(model=model, messages=q, tools=[WEATHER_OA], max_tokens=64)
    assert r.choices[0].message.content, r
    line("openai", model, "工具结果", f"{r.choices[0].message.content!r}")


def anthropic_calls(model: str) -> None:
    c = anthropic.Anthropic(base_url=BASE, api_key=KEY, max_retries=0)
    msgs = [{"role": "user", "content": "用一句话介绍你自己"}]
    r = c.messages.create(model=model, max_tokens=64, messages=msgs)
    text = "".join(b.text for b in r.content if b.type == "text")
    assert text, r
    line("anthropic", model, "非流式", f"{text!r} usage={r.usage.input_tokens}+{r.usage.output_tokens}")

    with c.messages.stream(model=model, max_tokens=64, messages=msgs) as s:
        parts = list(s.text_stream)
        final = s.get_final_message()
    assert parts and final.stop_reason == "end_turn", final
    line("anthropic", model, "流式", f"{len(parts)} 个 text_delta {''.join(parts)!r} stop_reason={final.stop_reason}")

    q = [{"role": "user", "content": "巴黎现在天气如何？"}]
    r = c.messages.create(model=model, max_tokens=64, messages=q, tools=[WEATHER_AN])
    use = next(b for b in r.content if b.type == "tool_use")
    assert r.stop_reason == "tool_use" and use.name == "get_weather", r
    line("anthropic", model, "工具调用", f"{use.name}({json.dumps(use.input)}) stop_reason={r.stop_reason}")
    q += [
        {"role": "assistant", "content": r.content},  # 文档中的写法：把回复的内容块原样放回
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": use.id, "content": '{"temp_c": 18}'}]},
    ]
    r = c.messages.create(model=model, max_tokens=64, messages=q, tools=[WEATHER_AN])
    text = "".join(b.text for b in r.content if b.type == "text")
    assert text, r
    line("anthropic", model, "工具结果", f"{text!r}")


def main() -> int:
    print(f"  openai {openai.__version__}，anthropic {anthropic.__version__}（未修改的官方 SDK）")
    openai_calls("m-oa")  # 同格式：原样转发
    openai_calls("m-an")  # OpenAI 客户端 → Anthropic 供应商：转换
    anthropic_calls("m-an")  # 同格式：原样转发
    anthropic_calls("m-oa")  # Anthropic 客户端 → OpenAI 供应商：转换
    return 0


if __name__ == "__main__":
    sys.exit(main())
