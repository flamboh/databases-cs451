# Repository Guidelines

## Project Layout
Core engine modules live in `lstore/` (`db.py`, `table.py`, `page.py`, `query.py`, `index.py`, `transaction*.py`). Auto-grader entrypoint stays at repo root with `main.py`, milestone harnesses (`m1_tester.py`, `exam_tester_m*`) and ad-hoc notebooks/scripts. Keep new datasets or benchmarks in a dedicated folder (`benchmarks/`) to avoid polluting the root. Dependencies belong in `requirements.txt` (currently `colorama` plus optional local utilities).

## Assignment 1 Essentials
Deliver a single-threaded, in-memory L-Store that supports `create`, `insert`, `select`, `update`, `delete`, and `sum` over integer columns. Implement the public APIs in `db.py`, `table.py`, `query.py`, and `index.py`, preserving signatures used by graders. Base pages must be read-optimized, tail pages append-only; both use columnar storage with 4 KB physical pages and 64-bit integer fields. Maintain RIDs, a page-directory per table, and the primary-key index by default. Passing `python3 main.py` is mandatory for auto-grading, and teams can earn bonus credit for well-documented indexing or performance extensions.

## Build, Test, and Development Commands
Use macOS system Python 3. Create a virtualenv if desired, then run `python3 -m pip install -r requirements.txt`. Quick regression: `python3 main.py`. Milestone probes: `python3 m1_tester.py`, `python3 exam_tester_m2_part1.py`, etc. Add focused coverage with `python3 -m unittest lstore.table`. Capture timings when benchmarking indexes or merge policies.

## Coding Style & Data Model
Follow PEP 8 with 4-space indentation, explicit imports, and `CamelCase` classes (`Database`, `Index`) paired with `snake_case` helpers. Document tricky storage invariants with concise docstrings, especially around base/tail merges and page directories. Compile for syntax sanity via `python3 -m compileall lstore`.

## Testing Guidelines
Exercise the full CRUD+sum surface plus merge behavior; seed randomness for deterministic results. Name new tests `test_<feature>.py` beside the module under `lstore/`. Include assertions for RID stability, page splits, and primary-key uniqueness. Keep concurrency scenarios gated to later milestones but mark placeholders if you stub them out.

## Commit & PR Guidelines
Write focused commits with imperative subjects (`add tail merge`, `fix sum overflow`). Reference Canvas ticket numbers or Piazza threads when applicable. PRs should summarize scope, list commands executed with exit codes, and call out any generated artifacts or datasets. Provide follow-up tasks if further milestones are impacted.

## Agent Notes
State any auto-generated fixtures in PRs and keep changes inside the repository workspace unless explicitly approved.
