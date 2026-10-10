"""Small original project scenarios for memory API and persistence tests.

Data is generated here rather than borrowed from an evaluation corpus. Keyword
tests use placeholder vectors; live vector tests embed the original story text.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta


def project_search_rows() -> dict[str, list[dict]]:
    rows = {"episode": [], "atomic_fact": [], "user_profile": [], "foresight": []}
    for owner in ("alex", "sam"):
        for index in range(8):
            stamp = datetime(2026, 5, 8, tzinfo=UTC) + timedelta(days=index * 10)
            entry = f"project_episode_{owner}_{index}"
            text = (
                f"Alex and Sam review project deployment {index}. "
                "The build support team checks code, tests, and release documentation. "
                "Deployment reviews use staged changes and continuous integration."
            )
            base = {
                "owner_id": owner,
                "owner_type": "user",
                "app_id": "default",
                "project_id": "default",
                "session_id": f"project_chat_{owner}",
                "timestamp": stamp.isoformat(),
                "sender_ids": ["alex", "sam"],
                "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
            rows["episode"].append(
                {
                    **base,
                    "id": f"{owner}_{entry}",
                    "entry_id": entry,
                    "parent_type": "memcell",
                    "parent_id": f"project_memcell_{owner}_{index}",
                    "subject": f"Project deployment review {index}",
                    "summary": text,
                    "episode": text,
                    "episode_tokens": text.lower(),
                    "md_path": f"users/{owner}/episodes/episode-{stamp.date()}.md",
                    "vector": [0.0] * 1024,
                    "subject_vector": [0.0] * 1024,
                }
            )
            fact = (
                f"Alex and Sam reviewed project deployment {index} using build support."
            )
            rows["atomic_fact"].append(
                {
                    **base,
                    "id": f"{owner}_project_fact_{index}",
                    "entry_id": f"project_fact_{owner}_{index}",
                    "parent_type": "episode",
                    "parent_id": entry,
                    "fact": fact,
                    "fact_tokens": fact.lower(),
                    "md_path": f"users/{owner}/.atomic_facts/fact-{stamp.date()}.md",
                    "vector": [0.0] * 1024,
                }
            )
        rows["user_profile"].append(
            {
                "id": owner,
                "owner_id": owner,
                "owner_type": "user",
                "app_id": "default",
                "project_id": "default",
                "summary": f"{owner} reviews project deployments.",
                "explicit_info_json": json.dumps(
                    [{"category": "Work", "description": "Build support"}]
                ),
                "implicit_traits_json": "[]",
                "profile_timestamp_ms": 1782864000000,
                "md_path": f"users/{owner}/user.md",
                "content_sha256": f"project-profile-{owner}",
            }
        )
    return rows


def project_conversation() -> dict:
    topics = ("release planning", "incident review", "test coverage", "documentation")
    batches = []
    start = datetime(2026, 1, 5, tzinfo=UTC)
    for index, topic in enumerate(topics):
        statements = (
            f"Alex: Today we are reviewing {topic} for our project. "
            "I own the deployment checklist.",
            f"Sam: I will maintain the {topic} notes "
            "and send them for code review tomorrow.",
            f"Alex: The {topic} checklist requires a clean build "
            "and a passing regression suite.",
            f"Sam: I completed the {topic} follow-up and committed "
            "the changes with test evidence.",
            "Alex: I prefer small commits and Python unittest "
            "for these maintenance tasks.",
            "Sam: Next week we will update the release guide "
            "after verifying the staging environment.",
        )
        messages = [
            {
                "sender_id": "alex" if offset % 2 == 0 else "sam",
                "role": "user",
                "timestamp": int(
                    (start + timedelta(days=index, seconds=offset * 30)).timestamp()
                    * 1000
                ),
                "content": text,
            }
            for offset, text in enumerate(statements)
        ]
        batches.append(
            {"scenario": topic, "message_count": len(messages), "messages": messages}
        )
    return {
        "everos_session_id": "project_maintenance_chat",
        "speakers": ["alex", "sam"],
        "batches": batches,
        "total_batches": len(batches),
        "total_messages": sum(batch["message_count"] for batch in batches),
    }


def project_agent_session(session_id: str, agent_id: str) -> dict:
    messages = []
    turns = (
        (
            "user",
            "Fix our project counter so reset clears the value. "
            "Add regression coverage.",
        ),
        (
            "assistant",
            "I inspected counter.py and found reset retains the old value. "
            "I will add a failing test.",
        ),
        ("tool", "python -m unittest: FAIL test_reset; expected zero, got three."),
        (
            "assistant",
            "I set the internal value to zero inside reset, "
            "preserving increment behavior.",
        ),
        ("tool", "python -m unittest: reset and increment regression tests PASS."),
        (
            "assistant",
            "The implementation and tests are committed. Reusable rule: "
            "test state transitions before changing stateful code.",
        ),
    )
    for round_index in range(3):
        for turn_index, (role, content) in enumerate(turns):
            sender = (
                agent_id
                if role == "assistant"
                else "tool_runner"
                if role == "tool"
                else "project_user"
            )
            message = {
                "sender_id": sender,
                "role": role,
                "timestamp": 1767571200000 + len(messages) * 1000,
                "content": (
                    f"Maintenance round {round_index + 1} for {session_id}: {content}"
                ),
            }
            if role == "assistant" and turn_index in (1, 3):
                message["tool_calls"] = [
                    {
                        "id": f"call_{session_id}_{round_index}_{turn_index}",
                        "type": "function",
                        "function": {
                            "name": "exec_command",
                            "arguments": json.dumps({"cmd": "python -m unittest"}),
                        },
                    }
                ]
            elif role == "tool":
                message["tool_call_id"] = messages[-1]["tool_calls"][0]["id"]
            messages.append(message)
    return {
        "everos_session_id": session_id,
        "everos_agent_sender_id": agent_id,
        "messages": messages,
    }
