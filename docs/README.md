# heimdall-asis
ASIS: Automated Strategic Intelligence System. A local-first, low-resource intelligence pipeline using DuckDB, Gemini Flash, and Pragmatic Clean Architecture.
# ASIS: Automated Strategic Intelligence System (MVP)

> **Status**: Pre-Alpha (Construction Phase)
> **Environment**: Local WSL2 | 1GB RAM Limit | DuckDB | Gemini Flash
> **Architecture**: Pragmatic Pipeline (Medallion Architecture)

## 📂 Project Context
This is a local-first intelligence system designed to ingest data from GDELT/DBnomics, filter noise via Regex/LLM, and visualize strategic signals.

**⚠️ AGENT INSTRUCTIONS:**
Before starting ANY task, you **MUST** read:
1.  `docs/project-guide.md` (Architecture, Naming, Risk Rules)
2.  `docs/planning/TEMPLATE.md` (Planning Format)

---

## 🗺️ System Architecture

```mermaid
graph LR
    subgraph "External"
        G[GDELT]
        D[DBnomics]
        LLM[Gemini API]
    end

    subgraph "ASIS Local Pipeline"
        direction TB
        Adapter[I/O Adapters]
        Raw[(Bronze: Raw Parquet)]
        Core[Pure Logic (Rules)]
        Interim[(Silver: DuckDB)]
        
        Adapter --> Raw
        Raw --> Core
        Core --> Interim
        Interim --> Adapter
        Adapter <--> LLM
    end

    subgraph "Presentation"
        Interim --> UI[Streamlit Dashboard]
    end