# Azure deployment contract

Provisioning and deployment are intentionally a planned follow-up; this repository does not claim cloud resources exist.

## Required resources

- ADLS Gen2 account with hierarchical namespace and private containers: `bronze`, `silver`, `gold`, `quarantine`.
- Azure Data Factory with a managed identity and parameterized batch pipeline.
- Azure Databricks workspace and job using `jobs/silver_transform.py`.
- Synapse workspace or serverless SQL endpoint for Gold consumption.
- Key Vault for any source credentials; managed identity/RBAC wherever supported.

## Orchestration contract

ADF parameters: `source_uri`, `service_date`, `run_id`, `silver_uri`, `quarantine_uri`, and `gold_uri`.
Each run should capture source URI, source row count, accepted/rejected counts, start/end timestamps, status, and error details. Retries must be bounded and writes idempotent by `run_id`; a backfill must never overwrite Bronze source evidence.

## Deployment checklist

1. Provision resources and private networking with Bicep or Terraform.
2. Grant least-privilege identities access to only the required containers and workspace.
3. Create linked services with managed identity; never embed credentials in pipeline JSON.
4. Deploy the ADF pipeline and Databricks job; parameterize environment-specific paths.
5. Create Synapse objects with `sql/synapse/01_gold_schema.sql`.
6. Configure alerts for failures, stale inputs, and abnormal rejection rates.
7. Validate recovery by replaying a prior batch and reconciling Silver and Gold counts.
