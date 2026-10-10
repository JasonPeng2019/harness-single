"""Original project sessions must remain valid paired tool conversations."""

import json

import pytest

from tests.fixtures.project_data import project_agent_session


@pytest.mark.parametrize(
    "session,owner",
    [
        ("project_parser", "agent_parser"),
        ("project_math", "agent_math"),
        ("project_backend_reset", "agent_backend"),
        ("project_backend_increment", "agent_backend"),
        ("project_backend_validation", "agent_backend"),
    ],
)
def test_generated_agent_session_has_complete_tool_pairs(session, owner):
    data = project_agent_session(session, owner)
    assert data["everos_session_id"] == session
    seen, pending = set(), set()
    last_timestamp = 0
    for message in data["messages"]:
        assert message["timestamp"] > last_timestamp
        last_timestamp = message["timestamp"]
        if message.get("tool_calls"):
            assert message["role"] == "assistant"
            assert message["sender_id"] == owner
            for call in message["tool_calls"]:
                assert call["id"] not in seen
                seen.add(call["id"])
                pending.add(call["id"])
                assert call["type"] == "function"
                assert call["function"]["name"]
                assert isinstance(json.loads(call["function"]["arguments"]), dict)
        if message["role"] == "tool":
            assert message.get("tool_call_id") in pending
            pending.remove(message["tool_call_id"])
        else:
            assert not message.get("tool_call_id")
    assert seen, "session must cover tool conversations"
    assert not pending, "every assistant call must have one tool result"
