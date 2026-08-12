"""Static, fail-closed acceptance materials for Firmware V2.

This package deliberately has no transport, process-launch, probe, serial, or RF code.
It validates the records which a controller must create before it can use its own MCP boundary.
"""

from .kit import AdmissionError, canonical_bound_operation, canonical_raw_result_bytes, evaluate_call, finding_gate_fragment, raw_result_sha256, validate_campaign_contract, validate_pinned_server, validate_seed_manifest, worker_environment
from .controller import FirmwareAcceptanceController
from .campaign_pack import DEFAULT_CAMPAIGN_PACK, FirmwareCampaignPack, FirmwareOperation
from .capability_adapter import FirmwareHardwareAdapter, HardwareCapabilityAdapter

__all__ = ["AdmissionError", "DEFAULT_CAMPAIGN_PACK", "FirmwareAcceptanceController", "FirmwareCampaignPack", "FirmwareHardwareAdapter", "FirmwareOperation", "HardwareCapabilityAdapter", "canonical_bound_operation", "canonical_raw_result_bytes", "evaluate_call", "finding_gate_fragment", "raw_result_sha256", "validate_campaign_contract", "validate_pinned_server", "validate_seed_manifest", "worker_environment"]
