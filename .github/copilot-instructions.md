<!--
Guidance for AI coding agents working on the `drainage` repository.
Keep this short, actionable, and codebase-specific. Update when project layout
or important workflows change.
-->

# Copilot / AI Agent Instructions — drainage

Short goal: maintenance and feature work should keep the project Python-first
and Delta-on-ADLS focused (no S3/AWS or Iceberg reintroduction; Rust removed).

- Big picture
  - `drainage/` is the Python package. Primary modules to read first:
    - `drainage/__init__.py` — public API (analyze_table, analyze_delta_lake, print_health_report).
    - `drainage/adls_client.py` — ADLS access and authentication (uses `DefaultAzureCredential`, supports UAMI `client_id`).
    - `drainage/delta_lake.py` — Delta analyzer implementation (core analysis logic lives here).
    - `drainage/types.py` — dataclasses describing HealthReport and metrics.
  - Examples in `examples/` show intended usage patterns and UAMI examples.

- Important architectural constraints
  - Delta-only: Iceberg support has been removed. If `table_type` is "iceberg", code should raise NotImplementedError.
  - ADLS-first: All storage access should use ADLS SDK (`azure-identity`, `azure-storage-file-datalake`). Do not reintroduce S3 code or aws-sdk dependencies.
  - Keep synchronous public API: internal analyzers may be async, but `analyze_delta_lake` uses `asyncio` to provide sync API expected by tests and examples.

- Developer workflows & commands
  - Tests: run `pytest` from repo root. CI uses Python 3.11 in `.github/workflows/ci.yml`.
  - Local dev: `pip install -r requirements-dev.txt` then `pip install -e .` to work on package in-place.
  - Build: `python -m build` creates distributions used by the publish workflow.

- Conventions and patterns observed
  - Authentication: prefer `DefaultAzureCredential`; if a UAMI must be used, callers pass `client_id` into `ADLSClient`/analyze functions.
  - Public API stability: `analyze_table(path, table_type=None, client_id=None)` and `analyze_delta_lake(path, client_id=None)` are the primary entrypoints; keep their signatures stable.
  - Tests mock top-level functions (see `tests/test_drainage.py`); keep names and call signatures consistent to avoid breaking mocks.

- Integration points & cross-component communication
  - ADLS access: `adls_client.py` is the single place to interact with Azure storage. New features that need storage access should use or extend this client.
  - Examples under `examples/` demonstrate how the public API is used; mirror those patterns in new code.

- Files to read when assessing changes
  - `drainage/__init__.py`, `drainage/adls_client.py`, `drainage/delta_lake.py`, `drainage/types.py`
  - `tests/test_drainage.py` and `tests/conftest.py` for test expectations and fixtures
  - `pyproject.toml` and `.github/workflows/ci.yml` for packaging and CI requirements

- Safety checks for AI edits (must-pass checklist for patches)
  1. Do not add or reintroduce S3/AWS SDK imports or `s3://` paths. If changing docs, convert S3 examples to ADLS (`abfss://` or `https://<account>.dfs.core.windows.net/...`).
  2. Do not add Iceberg logic; prefer raising `NotImplementedError` and document migration notes.
  3. Update `pyproject.toml` if new runtime dependencies are added and ensure CI installs them.
  4. Run `pytest` locally and ensure tests pass before proposing a PR.

- Quick code snippets (copyable)
  - Analyze with UAMI:
    ```py
    import drainage
    report = drainage.analyze_delta_lake("abfss://fs@account.dfs.core.windows.net/table/", client_id="<uami-client-id>")
    ```

If anything in these instructions is unclear or you need more examples of existing patterns, ask for the specific area to expand (tests, auth flows, analyzer internals).

Do not include detailed thought process or analysis in your responses; only provide the requested code, instructions, or to ask for clarifications.
