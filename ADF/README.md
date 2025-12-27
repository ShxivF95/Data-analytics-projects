# Azure Data Factory 

## Purpose
This ADF project performs incremental ingestion of SQL Server tables
into Azure Data Lake Storage (ADLS) for downstream processing in Databricks.

## Key Features
- Metadata-driven pipelines
- Incremental & full load support
- Parameterized datasets and pipelines
- Audit logging
- Self-hosted Integration Runtime

## Flow
SQL Server → ADF → ADLS (CSV/Parquet) → Databricks Bronze
