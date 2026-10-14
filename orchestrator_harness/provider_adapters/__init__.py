"""Registered provider launcher bindings.

Each subpackage mirrors one shipped adapter catalog
(``adapters/<provider-id>/harness/launcher_binding.py``) and is loaded by the
controller through ``importlib``. Receipt selection is shared here so the
registered bindings remain byte-identical to the shipped catalog.
"""

import math
from typing import Any


_TOKEN_FIELDS = {
    "codex": frozenset({
        "input_tokens", "cached_input_tokens", "cache_write_input_tokens",
        "output_tokens", "reasoning_output_tokens", "total_tokens",
    }),
    "claude-code": frozenset({
        "input_tokens", "output_tokens", "cache_creation_input_tokens",
        "cache_read_input_tokens",
    }),
    "qwen-code": frozenset({
        "input_tokens", "output_tokens", "cache_read_input_tokens",
        "cache_creation_input_tokens", "thoughts_token_count",
    }),
}
_CLAUDE_MODEL_TOKENS = frozenset({
    "inputTokens", "outputTokens", "cacheReadInputTokens", "cacheCreationInputTokens",
})
_IDENTITY_FIELDS = (
    "subtype", "uuid", "id", "turn_id", "turnId", "thread_id", "threadId",
    "session_id", "sessionId", "model", "parent_tool_use_id",
)


def _token_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _cost(value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, ValueError):
        return False


def _identity(value: Any) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


def has_native_counter(observation: dict[str, Any]) -> bool:
    """Only sanitized provider token and cost fields establish observation."""
    return bool(
        observation["usage"]
        or observation.get("modelUsage")
        or "total_cost_usd" in observation
    )


def usage_observation(event: dict[str, Any], provider_id: str) -> dict[str, Any] | None:
    """Keep native counts and identity; leave interpretation to the joined store.

    A result rollup may include earlier message counts, and cache/reasoning
    counters may be included in input/output. This interface never totals them.
    """
    event_type = event.get("type")
    if not isinstance(event_type, str):
        raise ValueError("native event type must be a string")
    if event_type not in {
        "codex": {"turn.completed", "turn.failed", "turn.cancelled"},
        "claude-code": {"assistant", "result"},
        "qwen-code": {"assistant", "result"},
    }.get(provider_id, set()):
        return None
    message = event.get("message")
    nested = message if isinstance(message, dict) else {}
    usage = (
        nested.get("usage")
        if event_type == "assistant"
        else event.get("usage")
    )
    has_generic_usage = isinstance(usage, dict)
    safe_usage = {
        key: value for key, value in (usage.items() if has_generic_usage else ())
        if key in _TOKEN_FIELDS[provider_id] and _token_count(value)
    }
    model_usage: dict[str, dict[str, int | float]] = {}
    if provider_id == "claude-code" and event_type == "result":
        raw_models = event.get("modelUsage")
        if isinstance(raw_models, dict):
            for model, counters in list(raw_models.items())[:32]:
                if not _identity(model) or not isinstance(counters, dict):
                    continue
                safe_counters = {
                    key: value for key, value in counters.items()
                    if (key in _CLAUDE_MODEL_TOKENS and _token_count(value))
                    or (key == "costUSD" and _cost(value))
                }
                if safe_counters:
                    model_usage[model] = safe_counters
    source_cost = event.get("total_cost_usd")
    has_cost = (
        provider_id == "claude-code"
        and event_type == "result"
        and _cost(source_cost)
    )
    if not has_generic_usage and not model_usage and not has_cost:
        return None
    observation = {
        "event_type": event_type,
        "usage": safe_usage,
    }
    if model_usage:
        observation["modelUsage"] = model_usage
    if has_cost:
        observation["total_cost_usd"] = source_cost
    for key in _IDENTITY_FIELDS:
        if _identity(event.get(key)):
            observation[key] = event[key]
    if isinstance(event.get("is_error"), bool):
        observation["is_error"] = event["is_error"]
    for key, target in (
        ("id", "message_id"),
        ("model", "message_model"),
        ("stop_reason", "message_stop_reason"),
    ):
        if _identity(nested.get(key)):
            observation[target] = nested[key]
    return observation
