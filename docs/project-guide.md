# ASIS Project Guide

## Project Overview
ASIS: Automated Strategic Intelligence System. A local-first, low-resource intelligence pipeline using DuckDB, Gemini Flash, and Pragmatic Clean Architecture.

## System Architecture (Pragmatic Pipeline)
* **Flow**: `Adapter (I/O) -> Raw (Bronze) -> Core (Logic) -> Interim (Silver) -> AI Agent -> Processed (Gold) -> UI`
* **Constraint**: Single-process, Low-memory optimization (DuckDB + Parquet).

## Directory & Boundaries
* `src/core/`: **PURE LOGIC ONLY**. No imports of `duckdb`, `requests`, `streamlit`. No File I/O.
* `src/adapters/`: **ALL I/O**. Database connections, API clients, Scrapers.
* `src/pipelines/`: **Orchestration**. Scripts that connect Adapters to Core.
* `src/ui/`: Streamlit code. **MUST** use `duckdb.connect(..., read_only=True)`.
* `tests/`: Unit tests (mirrors `src/core`) and Integration tests.

## Risk-Based Testing Strategy
* **Level 1: High Risk (I/O, API, DB Writes)**
    * Action: MUST implement `--dry-run` flag (limit=5, mock API).
    * Verification: Run dry-run + check logs.
* **Level 2: Core Logic (Regex, Models)**
    * Action: MUST write `pytest` in `tests/unit/`.
    * Verification: Tests must pass green.
* **Level 3: UI/Config**
    * Action: Visual check accepted.

## Strict Naming Conventions
* **Files**: `snake_case.py`.
* **Classes**: PascalCase + Role Suffix.
    * ✅ `NewsFetcher`, `ScoreCalculator`, `DuckDBStorage`.
    * ❌ `NewsManager`, `DataProcessor`, `Helper`.
* **Variables**:
    * DataFrame -> `df_news`
    * List -> `urls`, `items`
    * DuckDB -> `conn`

## Development Workflow (RPCD)
1.  **Request**: User gives task.
2.  **Plan**: Create `docs/planning/YYYYMMDD_feature.md` using TEMPLATE.
3.  **Code**: Implement per plan. **Do NOT break `src/core` purity.**
4.  **Debug**: Use `src/scripts/verify_status.py` or `pytest`.

## Planning Template
# [Feature Name] Implementation Plan

**Risk Level**: (High / Medium / Low)
**Testing Strategy**: (Dry Run / Unit Test / Visual)

## 1. Goal
*Brief one-liner.*

## 2. Architecture Changes
* **New Files**: `src/adapters/...`
* **Modified**: ...
* **Data Impact**: (Schema changes?)

## 3. Implementation Steps
1.  [Test] Define Dry-Run logic or Unit Test.
2.  [Core] Implement Pure Logic.
3.  [Adapter] Implement I/O.
4.  [Verify] Run `verify_status.py`.

