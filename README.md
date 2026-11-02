# Azure Batch Data Pipeline

A portfolio-ready batch ETL reference project for turning raw operational trip records into validated, analytics-ready route metrics. The repository includes a cloud-independent implementation you can run locally, plus a concrete Azure deployment blueprint.

## What it does

The pipeline copies each input file to a Bronze layer, validates and standardizes records into Silver, and produces daily route-level KPIs in Gold. Invalid records are retained separately with their source row and a reason, rather than silently discarded.

Included metrics: trip count, average arrival delay in minutes, and on-time percentage (arrival no more than five minutes after schedule).

## Architecture

```text
CSV / REST source
       |
       v
Azure Data Factory (scheduled, parameterized batch)
       |
       v
ADLS Gen2: Bronze --> Databricks / PySpark: Silver --> Gold
                                                     |
                                                     v
                                          Azure Synapse Analytics
```

The local runner mirrors the transformations without requiring an Azure account:

```text
CSV --> data/bronze --> validated data/ silver --> daily route summary
```

## Quick start (local)

Requires Python 3.10 or newer; the local pipeline uses only the standard library.

```powershell
python -m pipeline run --input data\sample\trips.csv --output data\output
python -m unittest discover -s tests -v
```

The output directory contains:

- `bronze/trips.csv` — immutable copy of the input batch.
- `silver/trips.csv` — normalized records with calculated arrival delay.
- `silver/rejected.csv` — rejected source rows with a validation reason.
- `gold/route_service_daily.csv` — daily route KPIs.
- `manifest.json` — input/output counts, run timestamp, and batch identifier.

Re-running with the same input produces the same data outputs. A new manifest timestamp identifies each run.

## Input contract

| Column | Type | Requirement |
|---|---|---|
| `trip_id` | string | Required, unique within a batch |
| `service_date` | ISO date | Required (`YYYY-MM-DD`) |
| `route_id` | string | Required |
| `scheduled_departure`, `actual_departure` | local time | Required (`HH:MM`) |
| `scheduled_arrival`, `actual_arrival` | local time | Required (`HH:MM`) |
| `passenger_count` | integer | Required, zero or greater |

Overnight arrival times are supported: an arrival clock time earlier than its departure is treated as the following day. Duplicate IDs, malformed dates/times, missing required values, and negative passenger counts are rejected.

## Project layout

```text
pipeline/                 Standard-library local ETL and CLI
tests/                    Unit and end-to-end tests
data/sample/              Small synthetic input for a reproducible demo
jobs/                     Azure Databricks PySpark transformation
infra/azure/              Deployment design and pipeline contract
sql/synapse/              Gold table DDL
docs/PROJECT_PLAN.md      Detailed implementation phases and acceptance criteria
```

## Azure implementation plan

1. **Provisioning:** create resource group, ADLS Gen2 storage, Azure Data Factory, Azure Databricks, and Synapse workspace using infrastructure as code.
2. **Secure ingestion:** use a managed identity and Key Vault-backed linked services; parameterize source path, service date, and destination. Land each source file once in Bronze with a run ID.
3. **Transformation:** run the included Databricks job, enforce the input contract, quarantine invalid rows, and write partitioned Silver and Gold Delta tables.
4. **Serving:** load Gold into a Synapse dedicated or serverless SQL endpoint; apply least-privilege access and documented retention.
5. **Operations:** add ADF retry limits, run-level audit records, freshness/quality alerts, replay support, and cost monitoring.
6. **Production readiness:** add schema evolution policy, late-arriving data handling, PII review, load/performance tests, and CI validation before deployment.

The cloud steps are a deployment blueprint, not a claim that Azure resources have been provisioned. No credentials or paid services are required for the local demo.

## Data and privacy

The checked-in sample is synthetic and contains no personal data. Use a licensed, public dataset or an approved internal source before a real deployment; do not commit secrets or production exports.

## License

MIT. See [LICENSE](LICENSE).
