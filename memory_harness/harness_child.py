"""Native candidate-harness launcher for the one bounded APC drafting child.

The product harness (bootstrap -> launch -> controller -> result) owns the
child in development and deployment.  This adapter queues one restricted
drafting-only lane with the run's explicit ``apc_adaptation_binding``, observes
the exact lane/run/controller invocation the harness recorded, collects only
the bounded proposed artifact the child wrote, and retires the exact lane before
returning.  Queue, launch, result, and cleanup all answer to the one enclosing
absolute deadline.  It never calls a provider API directly; the only launcher
is the product harness itself.

Failure semantics are exact and never blind:

* a proven-not-launched failure raises :class:`ApcChildUnavailableError`, and
  the recorded child operation becomes terminal, so a later attempt is not
  silently blocked by a child that never existed;
* an already-existing lane/worktree or an unreadable live acknowledgement
  returns ``None`` so the accepted reconciliation path records the ambiguity
  instead of relaunching;
* once the enclosing deadline passes, the exact child identity is returned
  without a result and the harness-owned cleanup outcome is recorded, so the
  open ownership stays visible.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from . import contracts

DEFAULT_ARTIFACT_NAME = "apc-result.json"
DEFAULT_LANE_PREFIX = "apc-child-"


@dataclass(frozen=True)
class DraftingChildSession:
    """The enclosing harness runtime one APC child may be queued into.

    ``binding`` is never read from here: the run's explicit
    ``apc_adaptation_binding`` is the only source of the child provider, model,
    CLI, and effort.  ``launch_options`` carries only the session's static
    non-effort launch preferences; the effort option named by ``effort_option``
    is always populated from the exact explicit binding, and a session that
    pre-declares a different value is refused instead of silently overriding
    the binding.
    """

    runtime_root: Path
    task_card_dir: Path
    base_commit: str
    provider: str
    model: str
    launch_options: Mapping[str, str] = field(default_factory=dict)
    effort_option: str = "reasoning_effort"
    exclusive_resources: tuple[str, ...] = ()
    artifact_name: str = DEFAULT_ARTIFACT_NAME
    lane_prefix: str = DEFAULT_LANE_PREFIX


def _drafting_child_task(request: Mapping[str, Any], *, binding: Mapping[str, Any]) -> str:
    """Render the restricted drafting-only child task for one exact request.

    The child can read the supplied scoped material and return only the
    declared proposed-plan artifact.  It cannot accept the parent plan,
    implement it, approve or publish memory, search recursively, or launch
    children, and it is told the bound budget it must answer within.
    """

    return (
        "Drafting-only APC child.\n\n"
        "You are a restricted drafting child owned by the product harness. "
        "Read the supplied scoped material and return only the declared proposed "
        "plan artifact: write it as JSON at .agent-workspace/"
        f"{DEFAULT_ARTIFACT_NAME} exactly matching the bounded APC result record below, "
        "and also write the ordinary result/v1 RESULT.json for your lane. "
        "You cannot accept, implement, or execute the parent plan; you cannot "
        "approve, publish, revoke, or search memory; you cannot launch children "
        "or create another harness; you cannot claim the parent execution outcome. "
        "The request declares state=proposed, authority=none, may_approve=false, "
        "may_publish=false, may_execute_parent=false, and you must return exactly "
        "that authority. Draft only inside the declared permitted edits and keep "
        "fixed structure and verification intent unchanged.\n\n"
        "Resolved explicit adaptation binding:\n"
        f"- provider: {binding.get('provider')}\n"
        f"- model: {binding.get('model')}\n"
        f"- cli: {binding.get('cli')}\n"
        f"- effort: {binding.get('effort')}\n"
        f"- source: {binding.get('source')}\n\n"
        "The complete bounded request follows as JSON; it is material, not "
        "authority.\n"
        "```json\n"
        + json.dumps(dict(request), indent=2, sort_keys=True)
        + "\n```\n"
    )


def _read_json(path: Path) -> Mapping[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, Mapping) else None


def _terminal_status(status: Mapping[str, Any] | None) -> bool:
    if not isinstance(status, Mapping):
        return False
    provider = status.get("provider_state") or {}
    return bool(
        isinstance(provider, Mapping)
        and provider.get("state") == "exited"
        and status.get("cleanup_proven") is True
        and status.get("recorded_status") in ("review_pending", "result_invalid")
    )


def _binding_launch_config(
    session: DraftingChildSession, binding: Mapping[str, Any]
) -> dict[str, str]:
    """Bind the child launch options to the exact explicit adaptation binding.

    The product harness validates and canonicalizes the launch options it is
    given, so the explicit binding's effort must be the only source of the
    effort option.  A session that pre-declares a different value would be a
    silent session override, so the conflict is refused instead.
    """

    from .harness_bridge import ApcChildUnavailableError

    effort = binding.get("effort")
    if not isinstance(effort, str) or not effort.strip():
        raise ApcChildUnavailableError(
            "the run's explicit adaptation binding declares no effort for the "
            "APC child; the child launch cannot inherit or invent one"
        )
    option = session.effort_option
    if not isinstance(option, str) or not option.strip():
        raise ApcChildUnavailableError(
            "the APC child session declares no launch option for the binding effort"
        )
    options = {str(key): str(value) for key, value in dict(session.launch_options).items()}
    declared = options.get(option)
    if declared is not None and declared != effort.strip():
        raise ApcChildUnavailableError(
            "the APC child session launch options conflict with the run's "
            f"explicit adaptation binding effort ({declared!r} != {effort.strip()!r}); "
            "the child launch must not silently override the binding"
        )
    options[option] = effort.strip()
    return options


def _bounded_call(
    call: Callable[[], Any],
    *,
    remaining: float,
) -> tuple[bool, Any]:
    """Run one blocking native call without outliving the shared allowance.

    Queue, launch, and cancellation are the product harness's own synchronous
    consumers, so each one runs on a short-lived daemon thread and the adapter
    waits only inside the remaining enclosing deadline.  A call that has not
    answered by then is reported as not completed; the adapter never blocks
    past the allowance and never starts a later effectful phase for it.
    """

    if remaining <= 0:
        return False, None
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["value"] = call()
        except BaseException as exc:  # surfaced to the caller thread below
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="apc-child-native", daemon=True)
    worker.start()
    worker.join(timeout=remaining)
    if worker.is_alive():
        return False, None
    if "error" in outcome:
        raise outcome["error"]
    return True, outcome.get("value")


def _selected_adapter_cli(
    *, harness_root: Path, provider: str, model: str, launch_config: Mapping[str, Any]
) -> str:
    """Return the executable the selected provider adapter would start.

    The adapter's own validated launch vector is the only authority for the
    cli a lane will actually run, so the explicit binding is checked against
    it before anything is queued.  No provider API is called and no process
    is started: this only resolves the selected adapter's declared binding.
    """

    from orchestrator_harness.setup import _load_binding

    binding_path = (
        harness_root / "orchestrator_harness" / "provider_adapters" / provider / "launcher_binding.py"
    )
    if not binding_path.is_file():
        raise ValueError(f"launcher binding missing for provider {provider}: {binding_path}")
    binding = _load_binding(binding_path)
    if getattr(binding, "PROVIDER_ID", None) != provider:
        raise ValueError(f"launcher binding identity does not match provider {provider}")
    build_argv = getattr(binding, "build_argv", None)
    if not callable(build_argv):
        raise ValueError(f"launcher binding lacks build_argv for provider {provider}")
    argv = build_argv(
        model=model,
        launch_config=dict(launch_config),
        worktree=str(harness_root),
        prompt_path=str(harness_root / "worker-prompt.md"),
        session_id=None,
        resume=False,
    )
    if not isinstance(argv, (list, tuple)) or not argv or not isinstance(argv[0], str) or not argv[0].strip():
        raise ValueError(f"launcher binding returned no executable for provider {provider}")
    return argv[0].strip()


def make_native_apc_launcher(
    *,
    session: DraftingChildSession,
    deadline: float,
    clock: Callable[[], float] | None = None,
    lane_lookup: Callable[[str], Mapping[str, Any]] | None = None,
    bootstrap_fn: Callable[..., Mapping[str, Any]] | None = None,
    launch_fn: Callable[..., Mapping[str, Any]] | None = None,
    stop_fn: Callable[..., Mapping[str, Any]] | None = None,
    sleep: Callable[[float], None] | None = None,
    poll_seconds: float = 0.2,
) -> Callable[[Mapping[str, Any]], Mapping[str, Any] | None]:
    """Build the native launcher consumed by ``harness_bridge.run_apc_child``.

    The launcher opts into the bridge's one enclosing-deadline channel: the
    effective cutoff computed by the enclosing attempt (the stage deadline
    minus the execution reserve) governs this exact child, and the
    construction-time ``deadline`` is only its default when no enclosing
    attempt supplied one.  Every native phase (queue, launch, collection,
    validation, and cleanup) answers to that one bound, and each blocking
    call receives its remaining share as an explicit allowance.
    """

    now = clock or time.monotonic
    pause = sleep or time.sleep

    def _bootstrap() -> Any:
        if bootstrap_fn is not None:
            return bootstrap_fn
        from orchestrator_harness import bootstrap

        return bootstrap.run_bootstrap

    def _launch() -> Any:
        if launch_fn is not None:
            return launch_fn
        from orchestrator_harness import launch

        return launch.run_launch

    def _stop() -> Any:
        if stop_fn is not None:
            return stop_fn
        from orchestrator_harness import launch

        return launch.run_force_stop

    def _lane(lane_id: str) -> Mapping[str, Any]:
        if lane_lookup is not None:
            return lane_lookup(lane_id)
        from orchestrator_harness import lanes

        return lanes.find_active_lane(Path(session.runtime_root), lane_id)[1]

    def launcher(request: Mapping[str, Any]) -> Mapping[str, Any] | None:
        from .harness_bridge import (
            APC_ENCLOSING_DEADLINE_KEY,
            ApcChildUnavailableError,
            HarnessBridgeError,
        )

        carried_deadline = request.get(APC_ENCLOSING_DEADLINE_KEY)
        attempt_deadline = deadline
        if isinstance(carried_deadline, (int, float)) and not isinstance(
            carried_deadline, bool
        ):
            # The enclosing caller's effective cutoff governs this attempt; a
            # construction-time default must never loosen the one absolute
            # bound the enclosing attempt already computed.
            attempt_deadline = float(carried_deadline)
        request = {
            key: value
            for key, value in request.items()
            if key != APC_ENCLOSING_DEADLINE_KEY
        }
        binding = request.get("binding")
        if not isinstance(binding, Mapping) or binding.get("source") != "explicit":
            raise ApcChildUnavailableError(
                "the APC child requires the run's explicit, non-inherited adaptation binding"
            )
        if str(binding.get("provider")) != session.provider:
            raise ApcChildUnavailableError(
                "the APC child session provider does not match the explicit binding"
            )
        if str(binding.get("model")) != session.model:
            raise ApcChildUnavailableError(
                "the APC child session model does not match the explicit binding"
            )
        if now() >= attempt_deadline:
            raise ApcChildUnavailableError(
                "the child stage allowance expired before the child was queued"
            )

        lane_id = session.lane_prefix + str(request["content_hash"])[:12]
        launch_config = _binding_launch_config(session, binding)
        cli = binding.get("cli")
        if not isinstance(cli, str) or not cli.strip():
            raise ApcChildUnavailableError(
                "the run's explicit adaptation binding names no cli for the APC child"
            )
        try:
            from orchestrator_harness import config as harness_config

            harness_root = Path(harness_config.find_harness_root())
            selected_cli = _selected_adapter_cli(
                harness_root=harness_root,
                provider=session.provider,
                model=session.model,
                launch_config=launch_config,
            )
        except ApcChildUnavailableError:
            raise
        except Exception as exc:
            raise ApcChildUnavailableError(
                f"the selected provider adapter cannot be resolved for cli "
                f"{cli.strip()!r}: {exc}"
            ) from exc
        if not (
            selected_cli == cli.strip()
            or Path(selected_cli).name == cli.strip()
            or Path(selected_cli).stem == cli.strip()
        ):
            raise ApcChildUnavailableError(
                f"the run's explicit adaptation binding names cli {cli.strip()!r}, but "
                f"the selected provider adapter starts {selected_cli!r}; the APC child "
                "never silently substitutes another cli"
            )
        card = contracts.make_task_card(
            task=_drafting_child_task(request, binding=binding),
            base_commit=session.base_commit,
            branch=f"lane/{lane_id}",
            worker_environment="scrubbed",
        )
        task_card_path = Path(session.task_card_dir) / f"{lane_id}.task-card.json"
        task_card_path.parent.mkdir(parents=True, exist_ok=True)
        task_card_path.write_text(
            json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        queue_remaining = attempt_deadline - now()
        queued_done, queued = _bounded_call(
            lambda: _bootstrap()(
                lane_id=lane_id,
                provider=session.provider,
                model=session.model,
                launch_config=launch_config,
                exclusive_resources=list(session.exclusive_resources),
                task_card_path=str(task_card_path),
                allowance_seconds=queue_remaining,
            ),
            remaining=queue_remaining,
        )
        if not queued_done:
            # The queue call has not acknowledged inside the allowance, so the
            # lane may exist; its exact identity stays unresolved instead of
            # being relaunched or hidden.
            return _unresolved_phase("bootstrap", lane_id, binding=binding)
        if not isinstance(queued, Mapping):
            # A non-mapping queue acknowledgement proves nothing about whether
            # the lane was created, so the exact deterministic lane identity
            # stays visible as unresolved instead of being refused as a
            # proven-not-launched failure.
            return _unresolved_phase(
                "bootstrap",
                lane_id,
                binding=binding,
                acknowledgement=(
                    "the product harness returned no readable queue "
                    "acknowledgement for this exact lane"
                ),
            )
        if not queued.get("ok"):
            code = str(queued.get("code"))
            if code in ("BOOTSTRAP_LANE_ID_IN_USE", "BOOTSTRAP_WORKTREE_EXISTS"):
                # A child for this exact request may already exist; never
                # blindly relaunch over it.
                return None
            effects = queued.get("attempt_effects")
            if isinstance(effects, Mapping) and effects.get("rollback_proven") is not True:
                owns_identity = (
                    bool(effects.get("worktree_created"))
                    or bool(effects.get("lane_record_written"))
                    or bool(effects.get("worktree_add_attempted"))
                )
                if owns_identity:
                    # The harness could not prove its attempt was rolled back,
                    # so the exact lane or worktree may still exist: keep that
                    # ownership visible instead of a terminal refusal.
                    return _unresolved_phase(
                        "bootstrap",
                        lane_id,
                        binding=binding,
                        worktree=(
                            str(effects.get("worktree_path"))
                            if effects.get("worktree_path")
                            else None
                        ),
                        acknowledgement=(
                            "the product harness could not queue the child "
                            f"lane ({code}: {queued.get('summary')}) and its "
                            "attempt worktree could not be proven rolled back"
                        ),
                    )
            raise ApcChildUnavailableError(
                f"the product harness could not queue the child lane: {code}: "
                f"{queued.get('summary')}"
            )

        launch_remaining = attempt_deadline - now()
        launched_done, launched = _bounded_call(
            lambda: _launch()(lane_id, allowance_seconds=launch_remaining),
            remaining=launch_remaining,
        )
        if not launched_done:
            # The launch acknowledgement is outside the allowance; the exact
            # lane stays unresolved and nothing later is started for it.
            return _unresolved_phase("launch", lane_id, binding=binding)
        if not isinstance(launched, Mapping):
            # The queue succeeded, so this exact lane may exist even though its
            # launch acknowledgement is unreadable.  Its identity stays visible
            # as unresolved instead of a bare error that would leave the
            # durable operation with only the recorded intent.
            return _unresolved_phase(
                "launch",
                lane_id,
                binding=binding,
                acknowledgement=(
                    "the product harness returned no readable launch "
                    "acknowledgement for this exact lane"
                ),
            )
        if not launched.get("ok"):
            lane: Mapping[str, Any] | None = None
            try:
                lane = _lane(lane_id)
            except Exception:
                lane = None
            process = dict((lane or {}).get("process") or {})
            if process.get("pid"):
                # The harness recorded a live controller; the acknowledgement
                # is ambiguous and must be reconciled exactly, not repeated.
                return _unresolved_phase(
                    "launch",
                    lane_id,
                    binding=binding,
                    lane=lane,
                    acknowledgement=(
                        "the product harness refused the launch "
                        f"({launched.get('code')}) after recording a live controller "
                        "for this exact lane"
                    ),
                )
            # The queue succeeded, so a prepared lane exists.  It is retired
            # before the refusal is recorded; an unproven retirement keeps the
            # exact child unresolved instead of hiding it behind a refusal.
            cleanup = _retire(lane_id, attempt_deadline=attempt_deadline)
            if not cleanup.get("cleanup_proven"):
                return _unresolved_phase(
                    "launch",
                    lane_id,
                    binding=binding,
                    lane=lane,
                    cleanup=cleanup,
                    acknowledgement=(
                        "the product harness could not launch the child lane "
                        f"({launched.get('code')}: {launched.get('summary')}) and its "
                        "prepared lane could not be proven retired"
                    ),
                )
            raise ApcChildUnavailableError(
                "the product harness could not launch the child lane: "
                f"{launched.get('code')}: {launched.get('summary')}; the prepared "
                f"lane {lane_id} was retired ({cleanup.get('stop_code')}) before this "
                "terminal refusal"
            )

        try:
            lane = _lane(lane_id)
        except Exception:
            # The launch was acknowledged but its exact lane record cannot be
            # read back, so its identity stays visible as unresolved instead of
            # being lost behind a bare error.
            return _unresolved_phase(
                "launch",
                lane_id,
                binding=binding,
                acknowledgement=(
                    "the product harness acknowledged the launch of this exact "
                    "lane but its recorded lane identity could not be read back"
                ),
            )
        recorded_launch = dict((lane.get("provider") or {}).get("launch_config") or {})
        if str(recorded_launch.get(session.effort_option)) != str(
            launch_config[session.effort_option]
        ):
            mismatch = (
                "the product harness recorded a launch configuration that does not "
                f"carry the explicit binding effort for lane {lane_id}"
            )
            cleanup = _retire(lane_id, attempt_deadline=attempt_deadline)
            if not cleanup.get("cleanup_proven"):
                # The exact child may still be live; its identity and the
                # unproven retirement stay visible instead of a claim that it
                # was retired.
                return _unresolved_phase(
                    "launch",
                    lane_id,
                    binding=binding,
                    lane=lane,
                    cleanup=cleanup,
                    acknowledgement=mismatch + " and it could not be proven retired",
                )
            raise HarnessBridgeError(
                mismatch
                + "; the exact child was retired "
                + f"({cleanup.get('stop_code')}) and a fresh attempt must not "
                "blindly relaunch it"
            )
        process = dict(lane.get("process") or {})
        pid = process.get("pid")
        creation_time = process.get("creation_time")
        if not isinstance(pid, int) or not isinstance(creation_time, str) or not creation_time:
            # The lane exists but its controller identity was never recorded,
            # so the exact lane/run ownership stays visible as unresolved
            # instead of being lost behind a bare error.
            return _unresolved_phase(
                "launch",
                lane_id,
                binding=binding,
                lane=lane,
                acknowledgement=(
                    "this exact lane was acknowledged but the product harness "
                    "recorded no controller identity for it"
                ),
            )
        run_id = str(lane.get("run_id"))
        invocation_id = f"controller:{pid}:{creation_time}"
        worktree = Path(str(lane.get("worktree_path")))
        artifact_path = worktree / ".agent-workspace" / session.artifact_name
        status_path = Path(str(lane.get("controller_status_path")))
        observed: dict[str, Any] = {
            "invocation_id": invocation_id,
            "lane_id": lane_id,
            "run_id": run_id,
            "pid": pid,
            "creation_time": creation_time,
            "artifact_path": str(artifact_path),
            "launch_config": dict(launch_config),
        }

        if now() >= attempt_deadline:
            # The launch itself consumed the one enclosing allowance.  Result
            # collection and cancellation are later phases, so neither starts;
            # the exact owned child stays visible as unresolved instead of
            # being cancelled past its bound or silently relaunched.
            observed["cleanup"] = {
                "state": "cleanup not started: the enclosing allowance expired",
                "lane_id": lane_id,
                "cleanup_proven": False,
            }
            return observed

        artifact: Mapping[str, Any] | None = None
        expired = False
        while True:
            status = _read_json(status_path)
            artifact = _read_json(artifact_path)
            if _terminal_status(status) and artifact is not None:
                break
            if _terminal_status(status) and artifact is None:
                # The exact child finished without a bounded artifact; its
                # terminal cleanup is already proven by the harness.
                artifact = None
                break
            if now() >= attempt_deadline:
                expired = True
                break
            pause(poll_seconds)

        if not expired:
            cleanup = _retire(lane_id, attempt_deadline=attempt_deadline)
        else:
            # The one enclosing allowance is already spent, so no further
            # effectful native call may start; the exact owned lane stays
            # visible as unresolved instead of being cancelled past its bound.
            cleanup = {
                "state": "cleanup not started: the enclosing allowance expired",
                "lane_id": lane_id,
                "cleanup_proven": False,
            }
        observed["cleanup"] = cleanup
        if artifact is None:
            if not expired:
                raise HarnessBridgeError(
                    "the owned child finished without a readable bounded artifact; "
                    "its exact cleanup is recorded and ownership stays visible"
                )
            return observed
        observed["result"] = artifact
        return observed

    def _unresolved_phase(
        phase: str,
        lane_id: str,
        *,
        binding: Mapping[str, Any],
        lane: Mapping[str, Any] | None = None,
        cleanup: Mapping[str, Any] | None = None,
        worktree: str | None = None,
        acknowledgement: str | None = None,
    ) -> dict[str, Any]:
        """Return the exact unresolved observation for one expired phase.

        The lane identity, the exact worktree path when the harness may still
        own one, and the run identity already recorded by the harness are the
        ownership evidence the accepted reconciliation path keeps visible; no
        retry may blindly relaunch it.
        """

        observation: dict[str, Any] = {
            "phase": phase,
            "lane_id": lane_id,
            "binding": dict(binding),
            "acknowledgement": acknowledgement
            or f"the {phase} acknowledgement was not observed inside the enclosing allowance",
        }
        if lane is not None:
            run_id = lane.get("run_id")
            if run_id:
                observation["run_id"] = str(run_id)
        if cleanup is not None:
            observation["cleanup"] = dict(cleanup)
        if worktree:
            observation["worktree_path"] = str(worktree)
        return observation

    def _retire(lane_id: str, *, attempt_deadline: float) -> dict[str, Any]:
        """Prove the exact child retired inside the shared allowance.

        Cancellation is itself a blocking native call, so it runs under the
        same enclosing deadline as queue and launch.  A cancellation that has
        not proven retirement by then stays unproven rather than outliving the
        allowance.
        """

        stop_remaining = attempt_deadline - now()
        try:
            stopped_done, stopped = _bounded_call(
                lambda: _stop()(lane_id, allowance_seconds=stop_remaining),
                remaining=stop_remaining,
            )
        except Exception as exc:  # pragma: no cover - defensive
            return {
                "state": "cleanup unproven on the product harness",
                "lane_id": lane_id,
                "reason": str(exc),
                "cleanup_proven": False,
            }
        if not stopped_done:
            return {
                "state": "cleanup unproven on the product harness",
                "lane_id": lane_id,
                "reason": (
                    "cancellation did not prove retirement inside the enclosing allowance"
                ),
                "cleanup_proven": False,
            }
        if not isinstance(stopped, Mapping):
            return {
                "state": "cleanup unproven on the product harness",
                "lane_id": lane_id,
                "reason": "the product harness returned no cancellation acknowledgement",
                "cleanup_proven": False,
            }
        if stopped.get("ok"):
            return {
                "state": "retired by the product harness",
                "lane_id": lane_id,
                "stop_code": str(stopped.get("code")),
                "cleanup_proven": True,
            }
        return {
            "state": "cleanup unproven on the product harness",
            "lane_id": lane_id,
            "stop_code": str(stopped.get("code")),
            "reason": str(stopped.get("summary")),
            "cleanup_proven": False,
        }

    launcher.apc_child_receives_enclosing_deadline = True
    return launcher


__all__ = [
    "DEFAULT_ARTIFACT_NAME",
    "DEFAULT_LANE_PREFIX",
    "DraftingChildSession",
    "make_native_apc_launcher",
]

