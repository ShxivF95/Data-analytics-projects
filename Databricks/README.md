# Databricks Lakehouse Pipeline

This folder contains Databricks notebooks implementing a Bronze → Silver pipeline.

## Architecture
SQL Server → ADF → ADLS (CSV) → Databricks → Delta Lake

## Layers

### Bronze
- Raw ingestion from ADLS
- Append-only
- Minimal transformations

### Silver
- Cleaned and deduplicated data
- Delta format
- Analytics-ready

### Gold
- Business aggregates (future scope)

## Orchestration
- Azure Data Factory handles ingestion
- Databricks handles transformations
