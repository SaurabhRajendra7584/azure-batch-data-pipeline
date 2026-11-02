# Project plan: Azure Batch Data Pipeline

## Objective

Demonstrate a dependable, observable batch data pipeline for public or operational transport records, from raw file landing through curated route metrics. The local implementation makes the transformations testable without Azure; the Azure design maps the same stages onto managed services.

## Users and decisions supported

- Operations teams compare route punctuality by service date.
- Analysts inspect delay and ridership trends from a governed Gold table.
- Data engineers can trace rejected records back to the exact input batch.

## Data flow and layer contracts

| Layer | Storage / format | Contract |
|---|---|---|
| Bronze | ADLS Gen2, date/run-partitioned CSV or original source file | Immutable source copy, ingestion timestamp, source URI, run ID |
| Silver | ADLS Gen2 / Delta | One validated row per trip; normalized IDs/date/times; delay and quality columns |
| Gold | ADLS Gen2 / Delta, then Synapse | One row per service date and route; trip count, average arrival delay, on-time rate |
| Quarantine | ADLS Gen2 | Original rejected values, source row, run ID, stable rejection reason |

## Quality and reliability rules

1. Validate required columns and types at the ingestion boundary.
2. Reject duplicate trip IDs and invalid dates, times, or negative passenger counts.
3. Preserve source evidence and explain each rejected row.
4. Treat arrival times earlier than departure as next-day arrivals.
5. Define on-time as arrival delay <= 5 minutes; calculate delay from schedule.
6. Make each batch replayable and idempotent by source/run key.
7. Record counts and run metadata; alert on failed runs, unusual rejection rate, or stale data.

The local runner implements rules 1–5 and writes a run manifest. Azure deployment work adds durable audit storage, alerts, and replay orchestration.

## Delivery roadmap

### Phase 1 — Local reference pipeline (included)

- [x] CLI to read CSV and produce Bronze, Silver, quarantine, and Gold outputs.
- [x] Typed validation, duplicate detection, overnight trips, deterministic aggregation.
- [x] Synthetic sample data and automated unit/end-to-end tests.

### Phase 2 — Azure foundations

- [ ] Provision ADLS Gen2, ADF, Databricks, and Synapse using Bicep or Terraform.
- [ ] Configure managed identity, RBAC, Key Vault, and private networking.
- [ ] Define storage lifecycle, naming, and partition conventions.

### Phase 3 — Orchestration and scalable transforms

- [ ] Implement parameterized ADF ingestion with retries, run IDs, and backfill support.
- [ ] Package the included PySpark job as a Databricks task writing Delta tables.
- [ ] Add schema evolution and late-arriving record policies.

### Phase 4 — Serving and operations

- [ ] Publish Gold metrics to Synapse and document analyst access.
- [ ] Add freshness, volume, quality, and failure alerts with actionable run links.
- [ ] Add cost, throughput, and recovery objectives; test replay and large batches.

### Phase 5 — Release readiness

- [ ] CI for unit tests, linting, dependency/security checks, and infrastructure validation.
- [ ] Run privacy/licensing review on the selected source dataset.
- [ ] Load-test representative volumes and document deployment/rollback procedures.

## Acceptance criteria

- A clean local run produces all four expected data outputs and a manifest.
- Every input row is accounted for as accepted or rejected.
- Unit tests cover invalid records, duplicate IDs, overnight journeys, and KPI boundaries.
- Cloud deployment provisions no public data access by default and stores credentials outside source control.
- Gold totals reconcile to accepted Silver rows for each route/day.

## Risks and open decisions

- Real source feeds may use stop-level rather than trip-level records; agree on a canonical grain before deployment.
- Time zones, daylight-saving transitions, and service-day conventions need source-specific rules.
- Choose Synapse serverless versus dedicated SQL based on latency, concurrency, and cost requirements.
- Confirm public dataset license and data retention requirements before ingesting real data.
