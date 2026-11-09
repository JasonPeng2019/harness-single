"""Extract Claude's terminal whole-tree modelUsage rollup without double counting."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def extract_claude_usage(
    path: Path, *, completed: bool = True, segment_index: int | None = None
) -> dict[str, Any]:
    # Lazy import keeps this companion usable from a packaged scripts directory.
    from token_ledger import LedgerError, nonnegative_int

    if not path.is_file():
        return {"status": "missing_transcript", "transcript_sha256": None}
    digest, segment_digest = hashlib.sha256(), hashlib.sha256()
    session: str | None = None
    segment_session: str | None = None
    segment = terminals = records = 0
    last_result_index: int | None = None
    previous_counters: dict[str, dict[str, int | None]] | None = None
    segment_has_terminal = False
    segment_bound = False
    selected: dict[str, Any] | None = None
    selected_line: int | None = None
    final_error = False
    with path.open("rb") as stream:
        for number, raw in enumerate(stream, 1):
            digest.update(raw)
            if not raw.strip():
                continue
            try:
                event = json.loads(raw.decode("utf-8"))
            except (UnicodeError, ValueError) as exc:
                if not completed and not raw.endswith(b"\n"):
                    break
                raise LedgerError(
                    f"malformed Claude transcript at {path}:{number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise LedgerError(
                    f"Claude transcript record must be an object at {path}:{number}"
                )
            # Nested tool events can contain their own result usage. The top-level
            # modelUsage terminal is the inclusive ROOT + built-in-agent rollup.
            nested = event.get("parent_tool_use_id") is not None
            if (
                event.get("type") == "system"
                and event.get("subtype") == "init"
                and not nested
            ):
                init_session = event.get("session_id")
                # Claude can repeat init after a builtin task notification before
                # it emits the first terminal result. That is still one process
                # segment. An init after a terminal retains the historical
                # appended-resume boundary, even when the session ID is reused.
                repeated_live_init = segment > 0 and not segment_has_terminal
                if repeated_live_init:
                    if not isinstance(init_session, str) or not init_session:
                        raise LedgerError(
                            f"repeated Claude init has no session binding: {path}:{number}"
                        )
                    if init_session != segment_session:
                        raise LedgerError(
                            f"multiple Claude session IDs in one segment: {path}"
                        )
                else:
                    segment += 1
                    segment_session = (
                        init_session
                        if isinstance(init_session, str) and init_session
                        else None
                    )
                    segment_has_terminal = False
                    last_result_index = None
                    previous_counters = None
                if segment_index is None or segment == segment_index:
                    if not repeated_live_init:
                        selected = None
                        terminals = 0
                        session = None
                        selected_line = None
                        final_error = False
                        segment_bound = False
                    if isinstance(init_session, str) and init_session:
                        session = init_session
                        segment_bound = True
            if segment_index is not None and segment != segment_index:
                if event.get("type") == "result" and not nested:
                    segment_has_terminal = True
                continue
            segment_digest.update(raw)
            records += 1
            candidate = event.get("session_id")
            if not nested and isinstance(candidate, str) and candidate:
                if session is not None and candidate != session:
                    raise LedgerError(
                        f"multiple Claude session IDs in one segment: {path}"
                    )
                session = candidate
            if event.get("type") != "result" or nested:
                continue
            result_index = event.get("result_index")
            if isinstance(result_index, bool) or (
                result_index is not None
                and (not isinstance(result_index, int) or result_index < 0)
            ):
                raise LedgerError(f"invalid Claude result_index at {path}:{number}")
            if terminals:
                if (
                    result_index is None
                    or last_result_index is None
                    or result_index != last_result_index + 1
                ):
                    raise LedgerError(
                        f"ambiguous multiple terminal Claude results in one provider segment: {path}"
                    )
                origin = event.get("origin")
                delta = event.get("model_usage_since_last_result")
                if (
                    not isinstance(origin, dict)
                    or origin.get("kind") != "task-notification"
                    or not isinstance(delta, dict)
                ):
                    raise LedgerError(
                        f"indexed Claude terminal lacks task-notification progression evidence: {path}:{number}"
                    )
                if selected is None:
                    raise LedgerError(
                        f"indexed Claude terminal progression follows a result without a usable rollup: {path}:{number}"
                    )
            elif result_index is not None and result_index != 0:
                raise LedgerError(
                    f"Claude terminal result progression does not start at index 0: {path}:{number}"
                )
            terminals += 1
            segment_has_terminal = True
            last_result_index = result_index
            if not segment_bound:
                raise LedgerError(
                    f"Claude terminal result has no top-level init session binding: {path}:{number}"
                )
            selected_line = number
            final_error = final_error or (
                event.get("is_error") is True or event.get("subtype") != "success"
            )
            models = event.get("modelUsage")
            if not isinstance(models, dict) or not models:
                # result.usage excludes built-in agents and is not a safe fallback.
                selected = None
                continue
            input_total = output_total = 0
            cache_read: list[int | None] = []
            cache_create: list[int | None] = []
            safe_models: dict[str, Any] = {}
            for model, counters in models.items():
                if (
                    not isinstance(model, str)
                    or not model
                    or not isinstance(counters, dict)
                ):
                    raise LedgerError("invalid Claude modelUsage entry")
                inp = nonnegative_int(
                    counters.get("inputTokens"), "inputTokens", required=True
                )
                out = nonnegative_int(
                    counters.get("outputTokens"), "outputTokens", required=True
                )
                read = nonnegative_int(
                    counters.get("cacheReadInputTokens"), "cacheReadInputTokens"
                )
                create = nonnegative_int(
                    counters.get("cacheCreationInputTokens"), "cacheCreationInputTokens"
                )
                input_total += inp
                output_total += out
                cache_read.append(read)
                cache_create.append(create)
                safe_models[model] = {
                    "input_tokens": inp,
                    "output_tokens": out,
                    "cached_input_tokens": read,
                    "cache_creation_input_tokens": create,
                    "usage_sha256": hashlib.sha256(
                        json.dumps(
                            counters, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8")
                    ).hexdigest(),
                }
            current_counters = {
                model: {
                    "input_tokens": counters["input_tokens"],
                    "output_tokens": counters["output_tokens"],
                    "cached_input_tokens": counters["cached_input_tokens"],
                    "cache_creation_input_tokens": counters[
                        "cache_creation_input_tokens"
                    ],
                }
                for model, counters in safe_models.items()
            }
            if previous_counters is not None:
                for model, prior in previous_counters.items():
                    current = current_counters.get(model)
                    if current is None:
                        raise LedgerError(
                            f"Claude indexed terminal dropped model {model}: {path}:{number}"
                        )
                    for key, prior_value in prior.items():
                        current_value = current[key]
                        if prior_value is not None and current_value is not None:
                            if current_value < prior_value:
                                raise LedgerError(
                                    f"Claude indexed terminal counter decreased for {model}.{key}: {path}:{number}"
                                )
                        elif prior_value != current_value:
                            raise LedgerError(
                                f"Claude indexed terminal counter availability changed for {model}.{key}: {path}:{number}"
                            )
            previous_counters = current_counters
            selected = {
                "input_tokens": input_total,
                "output_tokens": output_total,
                "cached_input_tokens": sum(cache_read)
                if all(v is not None for v in cache_read)
                else None,
                "cache_creation_input_tokens": sum(cache_create)
                if all(v is not None for v in cache_create)
                else None,
                "reasoning_output_tokens": None,
                "reported_total_tokens": None,
                "derived_input_plus_output_tokens": input_total + output_total,
                "comparison_total_tokens": input_total + output_total,
                "comparison_total_source": "claude_modelUsage_uncached_input_plus_output",
                "comparison_metric": "claude_uncached_input_plus_output",
                "usage_scope": "terminal_whole_tree_including_builtin_subagents",
                "model_usage": safe_models,
            }
    status = (
        "ok"
        if selected is not None
        else "in_flight"
        if not completed
        else "missing_usage"
    )
    if segment_index is not None and segment < segment_index:
        status = "in_flight" if not completed else "missing_segment"
    if final_error:
        status = "terminal_error_usage_unverified"
    return {
        "status": status,
        "thread_id": session,
        "selected_line": selected_line,
        "terminal_records": terminals,
        "json_records": records,
        "transcript_sha256": digest.hexdigest(),
        "transcript_segment_sha256": segment_digest.hexdigest(),
        **(selected or {}),
    }
