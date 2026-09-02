"""Registered provider launcher bindings.

Each subpackage mirrors one shipped adapter catalog
(``adapters/<provider-id>/harness/launcher_binding.py``) and is loaded by the
controller through ``importlib``; the package markers are import-safe and
carry no side effects.
"""
