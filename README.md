# heimdall-asis

**ASIS: Automated Strategic Intelligence System**

A local-first MVP intelligence pipeline leveraging Poetry for dependency management and DuckDB for efficient data processing. Built with Pragmatic Clean Architecture principles, this system integrates Gemini Flash for AI-powered insights while maintaining low resource requirements.

## Features

- **Local-First**: All data processing and storage happens locally using DuckDB
- **Poetry-Managed**: Clean dependency management with Poetry
- **Clean Architecture**: Organized into core logic, adapters, pipelines, and UI layers
- **AI-Powered**: Integrates Google's Gemini Flash for intelligent analysis
- **Streamlit UI**: Interactive web interface for data exploration

## Project Structure

```
heimdall-asis/
├── src/
│   ├── core/       # Business logic only
│   ├── adapters/   # I/O and database interactions
│   ├── pipelines/  # Data processing scripts
│   └── ui/         # Streamlit interface
├── data/           # Local data storage (git-ignored)
├── logs/           # Application logs (git-ignored)
├── docs/           # Documentation
└── tests/          # Test suite
```

## Getting Started

### Prerequisites

- Python ^3.11
- Poetry

### Installation

```bash
# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### Usage

```bash
# Run the Streamlit UI
streamlit run src/ui/app.py
```

## Dependencies

- **duckdb**: Embedded analytical database
- **pandas**: Data manipulation and analysis
- **streamlit**: Web UI framework
- **loguru**: Simplified logging
- **google-generativeai**: Gemini AI integration
- **python-dotenv**: Environment variable management
- **pytest**: Testing framework
