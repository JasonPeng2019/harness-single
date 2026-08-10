"""Compatibility import surface for the prompt-bundle contract."""

from .prompt_bundle import (
    PROMPT_BUNDLE_SCHEMA,
    PromptBundle,
    PromptBundleError,
    PromptComponent,
    bundle_from_record,
    compose_prompt_bundle,
    prompt_bundle_record_from_paths,
)

__all__ = [
    "PROMPT_BUNDLE_SCHEMA",
    "PromptBundle",
    "PromptBundleError",
    "PromptComponent",
    "bundle_from_record",
    "compose_prompt_bundle",
    "prompt_bundle_record_from_paths",
]
