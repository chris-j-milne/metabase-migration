---
project_id: "20260701"
project_name: "01-metabase-migration"
project_type: "custom"
client_name: "carwow-metabase"
created_date: "2026-07-01"
last_updated: "2026-07-01"
current_phase: "active"
custom_commands_path: ".wire/releases/01-metabase-migration/custom-commands"

jira:
  project_key: null
  structure: subtasks
  epic_key: null
  artifacts: {}

docstore:
  provider: notion
  confluence:
    cloud_id: null
    space_key: null
    parent_page_id: null
    artifacts: {}
  notion:
    parent_page_id: "390895f83963809abbead7e6f2bcf097"
    parent_page_url: "https://app.notion.com/p/carwow/Metabase-Migration-390895f83963809abbead7e6f2bcf097"
    artifacts: {}

# Custom artifact entries are added here by /wire:custom-release-define
# Each entry follows this schema:
#
# [artifact-key]:
#   custom: true
#   source_document: ""       # which SoW/plan doc this deliverable came from
#   generate: not_started     # not_started | complete | fail
#   validate: not_started     # not_started | complete | fail
#   review: not_started       # not_started | approved | changes_requested | blocked
#   file: null                # path to the generated artifact file
#   generated_date: null
#   generated_files: []
#   revision_history: []
artifacts: {}

notes:
  - "Custom release created: 2026-07-01"
  - "Source documents: CLAUDE.md (Metabase Migration Toolkit project context)"

blockers: []
---

# Project Status: 01-metabase-migration

**Client**: carwow-metabase
**Project ID**: 20260701
**Type**: Custom (project-scoped)
**Created**: 2026-07-01
**Last Updated**: 2026-07-01

## Current Phase: Active

## Next Action

Run the custom release define command to map deliverables and generate specs:

```
/wire:custom-define releases/01-metabase-migration
```

Or begin work directly using the existing toolkit:

```bash
python metabase_toolkit.py summary
python metabase_toolkit.py scan-keywords -o bq_issues.csv
python metabase_toolkit.py batch-convert --ids <id_list> -o converted/
```

## Artifact Status Summary

| Deliverable | Source Doc | Generate | Validate | Review | Ready |
|-------------|-----------|----------|----------|--------|-------|
<!-- Rows added by /wire:custom-release-define -->

**Legend**: ✅ Complete | 🔄 In Progress | ❌ Failed | ⏸️ Not Started | ⚠️ Blocked

## Scope

This release covers the full Metabase query migration lifecycle for Carwow's Snowflake → BigQuery migration:

| Phase | Description | Status |
|-------|-------------|--------|
| Inventory | Count and categorise all Metabase saved queries | ⏸️ Not Started |
| Keyword scan | Find queries using Snowflake-incompatible syntax | ⏸️ Not Started |
| Triage | Identify deprecation candidates vs. queries needing conversion | ⏸️ Not Started |
| Batch conversion | Convert SQL from Snowflake to BigQuery syntax; validate against BQ DataPlayground | ⏸️ Not Started |
| Data comparison | Compare Snowflake vs BigQuery outputs for converted queries | ⏸️ Not Started |
| Apply migration | Write converted SQL back to Metabase via API | ⏸️ Not Started |

## Conversion Tracking

Converted files live in `converted/` at the project root:

| Folder | Purpose |
|--------|---------|
| `converted/syntax_clean/` | Syntax valid, no caveats — ready to apply |
| `converted/syntax_clean_verify_data/` | Syntax valid but runtime risk flagged |
| `converted/syntax_errors/` | BQ rejected syntax — needs human fix |
| `converted/migration_status.csv` | Single source of truth for all question statuses |

## Notes

- Phase 1 is a like-for-like lift-and-shift — no schema redesign
- Schema mapping (Snowflake three-part refs → BQ project.dataset.table) in `schema_map.json`
- BigQuery DataPlayground connection used for dry-run syntax validation
- `apply-migration` requires `schema_map.json` to be fully populated before running

## Blockers

[None]

## Session History

| Date | Objective | Accomplished | Next Focus |
|------|-----------|--------------|------------|
| 2026-07-01 | Engagement setup | Created Wire engagement and release structure | Run /wire:custom-define or begin inventory |
