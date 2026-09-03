"""Source-local snapshot of every accepted outer-checklist audited gap.

This is intentionally independent of ``gap_map``. Refresh it only from the
authoritative checklist named below, one ``- [!] <ID>.`` row at a time.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InventoryAuthority:
    source: str
    sha256: str
    selection_rule: str
    audited_target: str


AUTHORITY = InventoryAuthority(
    source="master_planning/harness-v2-tier4/MASTER-SPEC-IMPLEMENTATION-CHECKLIST.md",
    sha256="b71418686644768f2d766705606cd98bb4d7acd3589f4b703f05785ee5ca3abf",
    selection_rule="Each exact '- [!] <ID>.' row in the accepted 82-gap checklist.",
    audited_target="harness-single 7d74fb64d5644d20f1d2697328fc2fb4929af09e",
)


AUDITED_GAP_IDS: tuple[str, ...] = (
    "A1", "A2", "A3", "A6", "A8", "A9", "A10", "A11", "A14", "A14a", "A14b", "A15", "A16",
    "B7", "B8", "B11", "B14", "C3", "C5", "C6", "C16", "C17", "D2", "D4", "D5", "D8", "D10", "D13", "D17", "D18",
    "E5", "E7", "E8", "E12", "F2", "F3", "F6", "F12", "F14a", "F14b",
    "G4a", "G10", "G11", "G12", "G12a", "G14a", "G14b", "G16", "G21", "G22",
    "H1", "H4", "H5", "H7", "H8", "H9", "H13", "H14", "H15", "H18", "H19", "H20", "H21", "H23", "H24", "H25", "H29", "H30", "H32",
    "I2a", "I3", "I5", "I6", "I8a", "I9", "I9a", "I9b", "I10", "I11", "I12", "I13", "I14",
)
