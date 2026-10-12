"""The official openai and anthropic Python SDKs, unmodified, against the model API (model gateway design §10).

Each SDK runs against both provider formats (fakeupstream in OpenAI mode and in Anthropic mode): non-stream,
stream and tool calls, plus authentication errors and the model list. 2 SDKs x 2 provider formats.
"""

import json

import anthropic
import openai
import pytest

PROVIDERS = ["openai", "anthropic"]
WEATHER_OPENAI = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Current weather of a city",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
    },
}
WEATHER_ANTHROPIC = {
    "name": "get_weather",
    "description": "Current weather of a city",
    "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
}


def openai_client(h, provider, key=None):
    return openai.OpenAI(base_url=h[provider] + "/v1", api_key=key or h["key"], max_retries=0)


def anthropic_client(h, provider, key=None):
    return anthropic.Anthropic(base_url=h[provider], api_key=key or h["key"], max_retries=0)


# ---- openai SDK ----


@pytest.mark.parametrize("provider", PROVIDERS)
def test_openai_non_stream(harness, provider):
    r = openai_client(harness, provider).chat.completions.create(
        model=harness["model"], messages=[{"role": "user", "content": "hello"}], max_tokens=64
    )
    assert r.choices[0].message.role == "assistant"
    assert r.choices[0].message.content.startswith("fake 回复")
    assert r.choices[0].finish_reason == "stop"
    assert r.usage.prompt_tokens > 0 and r.usage.completion_tokens > 0
    assert r.created > 0


@pytest.mark.parametrize("provider", PROVIDERS)
def test_openai_stream(harness, provider):
    stream = openai_client(harness, provider).chat.completions.create(
        model=harness["model"],
        messages=[{"role": "user", "content": "hello"}],
        max_tokens=64,
        stream=True,
        stream_options={"include_usage": True},
    )
    text, finish, usage = "", None, None
    for chunk in stream:
        if chunk.usage:
            usage = chunk.usage
        for c in chunk.choices:
            text += c.delta.content or ""
            finish = c.finish_reason or finish
    assert text.startswith("fake 回复")
    assert finish == "stop"
    assert usage is not None and usage.completion_tokens > 0


@pytest.mark.parametrize("provider", PROVIDERS)
def test_openai_tool_calls(harness, provider):
    client = openai_client(harness, provider)
    messages = [{"role": "user", "content": "What is the weather in Paris?"}]
    r = client.chat.completions.create(model=harness["model"], messages=messages, tools=[WEATHER_OPENAI], max_tokens=64)
    assert r.choices[0].finish_reason == "tool_calls"
    call = r.choices[0].message.tool_calls[0]
    assert call.function.name == "get_weather"
    assert json.loads(call.function.arguments) == {"city": "Paris"}

    # The streamed tool call assembles to the same call.
    stream = client.chat.completions.create(
        model=harness["model"], messages=messages, tools=[WEATHER_OPENAI], max_tokens=64, stream=True
    )
    name, args, finish = "", "", None
    for chunk in stream:
        for c in chunk.choices:
            for tc in c.delta.tool_calls or []:
                name += tc.function.name or ""
                args += tc.function.arguments or ""
            finish = c.finish_reason or finish
    assert name == "get_weather" and json.loads(args) == {"city": "Paris"} and finish == "tool_calls"

    # The tool result goes back and the model answers with text.
    messages += [
        r.choices[0].message.model_dump(exclude_none=True),
        {"role": "tool", "tool_call_id": call.id, "content": "sunny"},
    ]
    r = client.chat.completions.create(model=harness["model"], messages=messages, tools=[WEATHER_OPENAI], max_tokens=64)
    assert r.choices[0].finish_reason == "stop" and r.choices[0].message.content


@pytest.mark.parametrize("provider", PROVIDERS)
def test_openai_errors_and_models(harness, provider):
    with pytest.raises(openai.AuthenticationError) as e:
        openai_client(harness, provider, key="abx_live_k_AAAAAAAAAAAAAAAA_x").chat.completions.create(
            model=harness["model"], messages=[{"role": "user", "content": "hi"}]
        )
    assert e.value.code == "invalid_api_key"
    with pytest.raises(openai.NotFoundError):
        openai_client(harness, provider).chat.completions.create(
            model="no-such-model", messages=[{"role": "user", "content": "hi"}]
        )
    assert [m.id for m in openai_client(harness, provider).models.list()] == [
        harness["model"],
        harness["failing_model"],
    ]


# ---- anthropic SDK ----


@pytest.mark.parametrize("provider", PROVIDERS)
def test_anthropic_non_stream(harness, provider):
    m = anthropic_client(harness, provider).messages.create(
        model=harness["model"], max_tokens=64, messages=[{"role": "user", "content": "hello"}]
    )
    assert m.type == "message" and m.role == "assistant"
    assert m.content[0].type == "text" and m.content[0].text.startswith("fake 回复")
    assert m.stop_reason == "end_turn"
    assert m.usage.input_tokens > 0 and m.usage.output_tokens > 0


@pytest.mark.parametrize("provider", PROVIDERS)
def test_anthropic_stream(harness, provider):
    with anthropic_client(harness, provider).messages.stream(
        model=harness["model"], max_tokens=64, messages=[{"role": "user", "content": "hello"}]
    ) as stream:
        text = "".join(stream.text_stream)
        final = stream.get_final_message()
    assert text.startswith("fake 回复")
    assert final.stop_reason == "end_turn" and final.usage.output_tokens > 0


@pytest.mark.parametrize("provider", PROVIDERS)
def test_anthropic_tool_calls(harness, provider):
    client = anthropic_client(harness, provider)
    messages = [{"role": "user", "content": "What is the weather in Paris?"}]
    m = client.messages.create(model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=messages)
    assert m.stop_reason == "tool_use"
    use = next(b for b in m.content if b.type == "tool_use")
    assert use.name == "get_weather" and use.input == {"city": "Paris"}

    with client.messages.stream(
        model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=messages
    ) as stream:
        final = stream.get_final_message()
    streamed = next(b for b in final.content if b.type == "tool_use")
    assert streamed.name == "get_weather" and streamed.input == {"city": "Paris"} and final.stop_reason == "tool_use"

    messages += [
        {"role": "assistant", "content": [b.model_dump(exclude_none=True) for b in m.content]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": use.id, "content": "sunny"}]},
    ]
    m = client.messages.create(model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=messages)
    assert m.stop_reason == "end_turn" and m.content[0].text


@pytest.mark.parametrize("provider", PROVIDERS)
def test_anthropic_errors_and_models(harness, provider):
    with pytest.raises(anthropic.AuthenticationError) as e:
        anthropic_client(harness, provider, key="abx_live_k_AAAAAAAAAAAAAAAA_x").messages.create(
            model=harness["model"], max_tokens=16, messages=[{"role": "user", "content": "hi"}]
        )
    assert e.value.body["error"]["type"] == "authentication_error"
    assert [m.id for m in anthropic_client(harness, provider).models.list()] == [
        harness["model"],
        harness["failing_model"],
    ]


# ---- review round 1 (m6): more of the conversions most likely to drift ----


@pytest.mark.parametrize("provider", PROVIDERS)
def test_openai_stream_without_usage(harness, provider):
    stream = openai_client(harness, provider).chat.completions.create(
        model=harness["model"], messages=[{"role": "user", "content": "hello"}], max_tokens=64, stream=True
    )
    chunks = list(stream)
    assert all(c.usage is None for c in chunks)  # the usage chunk the gateway asked for is not passed on
    assert all(c.choices for c in chunks)
    assert "".join(c.choices[0].delta.content or "" for c in chunks).startswith("fake 回复")


@pytest.mark.parametrize("provider", PROVIDERS)
def test_mid_stream_error(harness, provider):
    stream = openai_client(harness, provider).chat.completions.create(
        model=harness["failing_model"], messages=[{"role": "user", "content": "hello"}], max_tokens=64, stream=True
    )
    with pytest.raises(openai.APIError):
        for _ in stream:
            pass
    with (
        pytest.raises(anthropic.APIError),
        anthropic_client(harness, provider).messages.stream(
            model=harness["failing_model"], max_tokens=64, messages=[{"role": "user", "content": "hello"}]
        ) as s,
    ):
        for _ in s.text_stream:
            pass


@pytest.mark.parametrize("provider", PROVIDERS)
def test_parallel_tool_calls(harness, provider):
    ask = [{"role": "user", "content": "Weather in Paris and Tokyo?"}]
    r = openai_client(harness, provider).chat.completions.create(
        model=harness["model"], messages=ask, tools=[WEATHER_OPENAI], max_tokens=64
    )
    calls = r.choices[0].message.tool_calls
    assert [json.loads(c.function.arguments)["city"] for c in calls] == ["Paris", "Tokyo"]
    assert len({c.id for c in calls}) == 2
    stream = openai_client(harness, provider).chat.completions.create(
        model=harness["model"], messages=ask, tools=[WEATHER_OPENAI], max_tokens=64, stream=True
    )
    args = {}
    for chunk in stream:
        for c in chunk.choices:
            for tc in c.delta.tool_calls or []:
                args[tc.index] = args.get(tc.index, "") + (tc.function.arguments or "")
    assert [json.loads(args[i])["city"] for i in sorted(args)] == ["Paris", "Tokyo"]

    m = anthropic_client(harness, provider).messages.create(
        model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=ask
    )
    assert [b.input["city"] for b in m.content if b.type == "tool_use"] == ["Paris", "Tokyo"]
    with anthropic_client(harness, provider).messages.stream(
        model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=ask
    ) as s:
        final = s.get_final_message()
    assert [b.input["city"] for b in final.content if b.type == "tool_use"] == ["Paris", "Tokyo"]


@pytest.mark.parametrize("provider", PROVIDERS)
def test_tool_choice(harness, provider):
    ask = [{"role": "user", "content": "What is the weather in Paris?"}]
    r = openai_client(harness, provider).chat.completions.create(
        model=harness["model"],
        messages=ask,
        tools=[WEATHER_OPENAI],
        tool_choice={"type": "function", "function": {"name": "get_weather"}},
        max_tokens=64,
    )
    assert r.choices[0].message.tool_calls[0].function.name == "get_weather"
    m = anthropic_client(harness, provider).messages.create(
        model=harness["model"],
        max_tokens=64,
        tools=[WEATHER_ANTHROPIC],
        tool_choice={"type": "tool", "name": "get_weather"},
        messages=ask,
    )
    assert any(b.type == "tool_use" and b.name == "get_weather" for b in m.content)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_system_prompt(harness, provider):
    r = openai_client(harness, provider).chat.completions.create(
        model=harness["model"],
        messages=[{"role": "system", "content": "Answer in French."}, {"role": "user", "content": "hello"}],
        max_tokens=64,
    )
    assert r.choices[0].message.content == "Bonjour"
    m = anthropic_client(harness, provider).messages.create(
        model=harness["model"],
        max_tokens=64,
        system="Answer in French.",
        messages=[{"role": "user", "content": "hello"}],
    )
    assert m.content[0].text == "Bonjour"


@pytest.mark.parametrize("provider", PROVIDERS)
def test_model_dump_history(harness, provider):
    """A previous response sent back with plain model_dump() (null fields included, e.g. anthropic caller: null,
    openai parsed / refusal / annotations: null) is accepted in both formats."""
    oc = openai_client(harness, provider)
    messages = [{"role": "user", "content": "What is the weather in Paris?"}]
    r = oc.chat.completions.create(model=harness["model"], messages=messages, tools=[WEATHER_OPENAI], max_tokens=64)
    call = r.choices[0].message.tool_calls[0]
    messages += [r.choices[0].message.model_dump(), {"role": "tool", "tool_call_id": call.id, "content": "sunny"}]
    r = oc.chat.completions.create(model=harness["model"], messages=messages, tools=[WEATHER_OPENAI], max_tokens=64)
    assert r.choices[0].finish_reason == "stop" and r.choices[0].message.content

    ac = anthropic_client(harness, provider)
    messages = [{"role": "user", "content": "What is the weather in Paris?"}]
    m = ac.messages.create(model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=messages)
    use = next(b for b in m.content if b.type == "tool_use")
    messages += [
        {"role": "assistant", "content": [b.model_dump() for b in m.content]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": use.id, "content": "sunny"}]},
    ]
    m = ac.messages.create(model=harness["model"], max_tokens=64, tools=[WEATHER_ANTHROPIC], messages=messages)
    assert m.stop_reason == "end_turn" and m.content[0].text
