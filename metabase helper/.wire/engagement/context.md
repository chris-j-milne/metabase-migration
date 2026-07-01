---
engagement_name: "carwow_metabase"
client_name: "carwow-metabase"
created_date: "2026-07-01"
engagement_lead: "Chris"
repo_mode: "combined"

# If repo_mode is dedicated_delivery, provide client repo details:
client_repo:
  github_url: null
  local_path: null
  default_branch: "main"

docstore:
  provider: notion
  confluence:
    space_key: null
    parent_page_url: null
  notion:
    parent_page_url: "https://app.notion.com/p/carwow/Metabase-Migration-390895f83963809abbead7e6f2bcf097"
    parent_page_id: "390895f83963809abbead7e6f2bcf097"
---

# Engagement Context: carwow_metabase

**Client**: carwow-metabase
**Engagement Lead**: Chris
**Created**: 2026-07-01
**Repo mode**: combined

---

## Engagement Overview

Carwow is migrating its data warehouse from Snowflake to BigQuery. As part of this migration, all Metabase saved queries must be reviewed and either deprecated or updated to point at the new BigQuery environment. The engagement covers auditing, triaging, converting, and applying SQL changes across the full Metabase question library during a ~4-week parallel-run window where both Snowflake and BigQuery are live.

## Business Objectives

1. Audit all Metabase saved queries for BigQuery-incompatible Snowflake syntax
2. Triage and convert retained queries to work against BigQuery
3. Apply converted queries to Metabase once BigQuery tables are populated and data comparisons pass

## Key Stakeholders

| Name | Role | Responsibilities | Contact |
|------|------|------------------|---------|
| Chris Milne | Engagement Lead | Overall delivery, conversion, and apply | chris.milne@carwow.co.uk |

## Current State Architecture

Carwow runs Snowflake as its primary analytical warehouse, with Metabase as the BI layer. The migration to BigQuery is a like-for-like lift-and-shift (Phase 1). During the parallel-run window, both databases are live and Metabase queries must be migrated without disruption.

**Key systems**:
- Snowflake: current analytical warehouse (source)
- BigQuery: target warehouse (BigQuery DataPlayground connection in Metabase used for dry-run validation)
- Metabase: BI layer at https://carwow.metabaseapp.com — API access via `METABASE_API_KEY`

## Engagement Releases

| # | Release Name | Type | Status | Start | End |
|---|-------------|------|--------|-------|-----|
| 01 | metabase-migration | custom | In progress | 2026-07-01 | |

## SOW Reference

Statement of Work: `engagement/sow.md`

This engagement is scoped to the Metabase query migration component of the broader Snowflake → BigQuery data migration at Carwow.

## Working Agreements

- **Primary contact**: chris.milne@carwow.co.uk
- **Toolkit**: `metabase_toolkit.py` — single-file Python CLI for all audit/conversion tasks
- **Auth**: Metabase API key stored as `METABASE_API_KEY` env var
- **Validation**: BigQuery DataPlayground connection in Metabase used for dry-run syntax validation

## Client Repo Details

This engagement uses the combined client + delivery repo. The `.wire/` folder lives directly in the client's code repo.

## Notes

- Phase 1 is a like-for-like lift-and-shift — no schema redesign
- Deprecation candidates: queries not run in 6+ months that aren't powering active dashboards
- Schema mapping (Snowflake three-part refs → BigQuery project.dataset.table) in `schema_map.json`
- BigQuery validates syntax before resolving tables — syntax errors caught even when tables don't exist