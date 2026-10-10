# Test fixtures

`project_data.py` generates original project chat, coding-session and memory-row
scenarios for general API/persistence regression tests. No external evaluation
corpus, borrowed task prompt, precomputed provider vectors or score threshold is
bundled. Keyword tests stay offline; already-live-marked vector tests embed their
synthetic stories using the operator-configured test provider. The opt-in slow
closed-loop search suite ingests the project conversation, closes the application,
reopens the persisted memory with a fresh lifespan, and queries its extracted facts.
