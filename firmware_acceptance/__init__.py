"""Static, fail-closed acceptance materials for Firmware V2.

This package deliberately has no transport, process-launch, probe, serial, or RF code.
It validates the records which a controller must create before it can use its own MCP boundary.
"""

from .kit import AdmissionError, canonical_bound_operation, evaluate_call, finding_gate_fragment, validate_campaign_contract, validate_pinned_server, validate_seed_manifest, worker_environment

__all__ = ["AdmissionError", "canonical_bound_operation", "evaluate_call", "finding_gate_fragment", "validate_campaign_contract", "validate_pinned_server", "validate_seed_manifest", "worker_environment"]
