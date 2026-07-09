# Metabase Migration Toolkit — Project Context

## Purpose

This project audits Metabase saved queries ahead of a **Snowflake → BigQuery data migration** at Carwow. The migration happens in two phases; phase 1 is a like-for-like lift-and-shift. During a ~4-week window where both databases run in parallel, all Metabase queries must be reviewed and either deprecated or updated to point at BigQuery.

## Key Tasks

1. **Inventory** — Count all queries with breakdowns by folder, creator, and last-run date
2. **Keyword scanning** — Find queries using Snowflake syntax that BigQuery doesn't support
3. **Triage** — Identify queries that can be deprecated vs. those that need to be re-pointed or rewritten
4. **Fix** — Update retained queries to work against BigQuery (syntax fixes + new data locations)

## Metabase Environment

- **Instance URL:** `https://carwow.metabaseapp.com`
- **Auth:** API key (`x-api-key` header) — key stored as `METABASE_API_KEY` env var (see `metabase_toolkit.py`)
- **Access method:** SSO in browser; API key for programmatic access
- **Scale:** Unknown total query count — some folders have a few queries, others have sub-folders with hundreds

## Known Snowflake → BigQuery Incompatibilities

The full list of flagged keywords is maintained in `BQ_INCOMPATIBLE_KEYWORDS` in `metabase_toolkit.py`. The scanner strips comments and string literals before scanning, so commented-out keywords are not flagged.

**Supported by both — no change needed:**
- `QUALIFY` — BigQuery supports it natively
- `IFNULL` — BigQuery supports it natively
- `PARSE_JSON` — BigQuery supports it natively

**Require rewriting:**

| Snowflake | BigQuery equivalent |
|---|---|
| `ILIKE` | `LOWER(col) LIKE LOWER(pattern)` |
| `DATEADD(part, n, date)` | `DATE_ADD(date, INTERVAL n part)` |
| `DATEDIFF(part, a, b)` | `DATE_DIFF(b, a, part)` — arguments are **swapped**: Snowflake returns `b−a`; BigQuery `DATE_DIFF(x,y,part)` returns `x−y`, so swap to preserve sign |
| `NVL(a, b)` | `COALESCE(a, b)` |
| `DECODE(col, v1, r1, ...)` | `CASE WHEN` |
| `REGEXP_SUBSTR` | `REGEXP_EXTRACT` |
| `LATERAL FLATTEN` | `UNNEST` |
| `TRY_CAST` | `SAFE_CAST` |
| `STRTOK` | `SPLIT` |

**Snowflake-only alias self-references in SELECT** — Snowflake allows referencing an alias defined earlier in the same SELECT clause (e.g. `SELECT x * 2 AS double_x, double_x + 1 AS foo`). BigQuery does not. Flagged by the `has_alias_self_ref` column in the export.

## Project Files

- `metabase_toolkit.py` — Main CLI tool for all audit tasks (see commands below)

## Toolkit Commands

```bash
# Install dependencies
pip install requests tabulate

# Test connection
python metabase_toolkit.py connect

# High-level stats: total queries, by folder, by creator, by last-run age
python metabase_toolkit.py summary

# Find all queries with BigQuery-incompatible Snowflake syntax
python metabase_toolkit.py scan-keywords -o bq_issues.csv

# Scan for specific keywords only
python metabase_toolkit.py scan-keywords --keywords "QUALIFY,ILIKE,DATEADD"

# List queries, optionally filtered by folder
python metabase_toolkit.py list-queries
python metabase_toolkit.py list-queries --folder "Marketing"

# Export full inventory (including raw SQL) to CSV
python metabase_toolkit.py export-csv -o full_inventory.csv

# Print SQL for a specific query
python metabase_toolkit.py show-sql 12345
```

## Metabase API Notes

- All requests use the header `x-api-key: <key>`
- Base path: `/api/...`
- Saved questions are called **cards** in the API (`/api/card`)
- Collections are **folders** in the UI (`/api/collection`)
- `last_query_start` on a card gives the last execution timestamp

**dataset_query format (two variants — toolkit handles both):**

The API now returns the newer MBQL v2 format for most cards:
```
dataset_query.stages[0]['native']        # MBQL v2 (new) — lib/type = "mbql.stage/native"
dataset_query.native.query               # Old format — still returned by some cards
```
The `extract_sql()` and `is_native()` functions in `metabase_toolkit.py` handle both transparently.

**Writing SQL back to Metabase (`PUT /api/card/:id`)** — confirmed working. Minimal payload:
```python
requests.put(f"{METABASE_URL}/api/card/{card_id}",
             headers=api_headers(),
             json={"dataset_query": dq})
```
Where `dq` is the full `dataset_query` dict with the modified SQL written back into `dq['stages'][0]['native']` (or `dq['native']['query']` for old-format cards). Fetch the card first, mutate in place, PUT the whole `dataset_query` back — do not reconstruct it from scratch.

## What "Deprecate" Means

A query is a candidate for deprecation if:
- It hasn't been run in 6+ months, **and**
- It isn't powering a dashboard or embedded somewhere still in use

Metabase doesn't expose downstream usage (e.g. whether a query result is piped into Google Sheets) via the API, so deprecation decisions may need manual review or owner outreach.

## Manual "Convert Question" Workflow

When the user says **"Convert question N"**, fetch the card, apply Snowflake → BigQuery conversions manually, validate against the BigQuery Playground, and **always write these three files** to the project directory:

| File | Contents |
|---|---|
| `card{N}_orig_sql.txt` | The original SQL exactly as stored in Metabase |
| `card{N}_adj_sql.txt` | The converted BigQuery SQL |
| `card{N}_results.txt` | Validation summary: status, BQ error message (if any), list of changes made |

`card{N}_results.txt` should include:
- Question ID and name
- Validation status (`syntax_valid`, `syntax_error`, `success`, etc.)
- The raw BigQuery error message if there was one
- A bullet list of every change made (e.g. "DATE_TRUNC argument order reversed")
- Any manual review notes (e.g. alias self-references, LATERAL FLATTEN patterns)

During individual "Convert question N" sessions, files are written to the project root. During batch conversion they go into the folder structure below.

## Batch Conversion Output Structure

All converted files live under `converted/`, organised by dry-run outcome:

```
converted/
  syntax_clean/              # Syntax valid, no caveats — apply when data is ready
  syntax_clean_verify_data/  # Syntax valid but a runtime risk was flagged (e.g. type mismatch)
  syntax_errors/             # BigQuery rejected the syntax — needs human fix first
  migration_status.csv       # Single source of truth for all question statuses
```

Each subfolder contains the three standard files per question: `card{N}_orig_sql.txt`, `card{N}_adj_sql.txt`, `card{N}_results.txt`.

**Category assignment rules:**
- `syntax_clean` — BQ returned "project/table not found" with no caveats noted
- `syntax_clean_verify_data` — BQ returned "project/table not found" BUT `card{N}_results.txt` contains a "Notes" entry flagging a potential runtime risk (e.g. TIMESTAMP vs DATE column type, DIV0 → SAFE_DIVIDE semantic change)
- `syntax_errors` — BQ returned an actual syntax error that conversion could not resolve

**migration_status.csv columns:**

| Column | Values |
|---|---|
| `id` | Metabase question ID |
| `name` | Question name |
| `original_category` | `syntax_clean` / `syntax_clean_verify_data` / `syntax_errors` |
| `current_status` | `pending` / `data_check_passed` / `data_check_failed` / `human_review` / `applied` |
| `failure_reason` | Short description of what failed (empty if none) |
| `notes` | Any extra context for the reviewer |

Files are **never moved between folders** — `original_category` in the CSV is the permanent record of the dry-run result. `current_status` tracks progression through Phase 2.

## Phase 2: Data Comparison Workflow

Triggered once BigQuery tables are populated. For each question in `syntax_clean/` and `syntax_clean_verify_data/`:

1. Run original SQL against **Snowflake** via Metabase → capture result set
2. Run converted SQL against **BigQuery** via Metabase → capture result set
3. Compare with these rules:
   - Sort both result sets before comparing (row order may differ between engines)
   - **Row count must match exactly** — all Metabase queries are full refreshes (no incremental logic), so any row count difference is a real problem, not noise. Fail immediately if counts differ.
   - Float tolerance of ±0.0001 for computed numeric columns
   - SUM/COUNT of key numeric columns within ±0.01% (float rounding only)
   - Column-level value comparison for non-float columns
4. **Pass** → set `current_status = data_check_passed`; queue for `apply-migration`
5. **Fail** → set `current_status = data_check_failed`; write `card{N}_datadiff.txt` summarising the discrepancy for human review

**Timing:** Run comparisons between Snowflake refresh windows (Snowflake refreshes **4× daily**). Both queries should be run in the same window to avoid live-data drift causing false failures. Each window is ~6 hours; aim to run ~45 minutes after a refresh completes to let the pipeline settle.

**Human review loop for failures:**
1. Reviewer opens `card{N}_datadiff.txt` and `card{N}_adj_sql.txt`
2. Reviewer edits the SQL and hands the corrected version back
3. Re-run the comparison with the corrected SQL
4. Pass → `applied`; still fail → `human_review` with notes

**`apply-migration` step:** Writes `card{N}_adj_sql.txt` to Metabase via `PUT /api/card/:id`, updates `current_status = applied` in the CSV. Requires schema mapping to be populated (see below) — will refuse to run if any unmapped Snowflake-style three-part table references remain in the SQL.

## Schema Mapping

Snowflake uses three-part references (`database.schema.table`). BigQuery uses `project.dataset.table`. These are left unchanged during dry-run conversion; the mapping is applied at `apply-migration` time.

The mapping lives in `schema_map.json` in the project root. **Populate this when the mapping is confirmed** — the toolkit reads it at apply time and substitutes all matching prefixes in the converted SQL.

Known Snowflake schemas from queries seen so far (BQ targets TBD):

```json
{
  "CARWOW.SMC_OPERATIONS":                    "TBD",
  "UK.ENHANCED":                              "TBD",
  "carwow.marketing_performance_reporting":   "TBD"
}
```

As more queries are converted, new Snowflake schemas will be discovered and should be added here as placeholders. The `apply-migration` command will warn (but not fail) on any reference not in this file during dry-run; it will hard-fail during live apply.

## SQL Conversion Agent (planned)

The next phase is an AI agent that takes a question ID, attempts to convert its SQL from Snowflake to BigQuery syntax, and validates the result in dry-run mode against the **BigQuery DataPlayground** connection in Metabase.

**Approach:**
1. Fetch the question's SQL via the toolkit
2. Use an LLM to rewrite Snowflake-specific syntax to BigQuery equivalents (table names left as placeholders)
3. Submit the converted SQL to the BigQuery DataPlayground via `POST /api/dataset` using the BigQuery connection's database ID
4. Interpret the result:
   - **Syntax error** → conversion is incomplete; report what BigQuery rejected
   - **Table not found** → syntax is valid; query just needs re-pointing to real BQ tables
   - **Success** → fully valid against the DataPlayground tables

**Key constraints:**
- BigQuery validates syntax before resolving tables, so syntax errors are caught even when tables don't exist
- Column-level validation is only possible when real tables exist — column name differences between Snowflake and BigQuery schemas will not be caught in dry-run mode
- The agent is a first-pass triage tool; queries flagged with errors still need human review

**Goal:** Categorise each retained question as (a) syntax-clean and ready to re-point, (b) needs specific syntax fixes, or (c) needs human eyes for structural rewrites (e.g. complex alias self-references or non-trivial LATERAL FLATTEN patterns).

## Planned Toolkit Commands (Phase 2)

```bash
# Batch-convert a list of question IDs; files go into converted/ subfolders
# Reads schema_map.json but leaves unmapped references as-is during dry-run
python metabase_toolkit.py batch-convert --ids 37,23794,1234 -o converted/

# Compare Snowflake vs BigQuery outputs for a question; writes card{N}_datadiff.txt
python metabase_toolkit.py compare-outputs 37

# Apply converted SQL to Metabase (requires schema_map.json to be complete)
# --dry-run prints the payload without sending
python metabase_toolkit.py apply-migration 37 --dry-run
python metabase_toolkit.py apply-migration 37

# Apply all questions with current_status = data_check_passed
python metabase_toolkit.py apply-migration --batch -i converted/migration_status.csv
```

## Partner Context

**Rittman Analytics** are the delivery partner for the underlying data platform migration (dbt models, HiTouch/reverse ETL). Key facts relevant to this toolkit:

- They use the **Wire framework** (Claude Code plugin) for AI-assisted dbt generation and migration
- The migration is designed to be a **like-for-like lift-and-shift** with ~100% data accuracy
- **All field names will be consistent** between Snowflake and BigQuery — no column renaming to worry about
- Known accuracy exceptions (affect dbt layer only, not Metabase):
  - Incremental models where historical data has different logic to the current model definition
  - Comparisons that coincide with a Snowflake refresh window
- The blocker for `apply-migration` is getting the BigQuery project/dataset names from Rittman to populate `schema_map.json`

**Wire skills available in this session** — most relevant to this project:
- `wire:metabase-migration-generate/review/validate`
- `wire:equivalency-validate` — for data comparison workflow
- `wire:migration-specialist` agent

## Development Notes

- Python 3.8+ required
- The toolkit is intentionally a single-file script for portability
- **Write access to Metabase confirmed** — `PUT /api/card/:id` tested successfully. See API Notes above for correct payload format.
- The `_call_claude()` function uses the Anthropic API; to switch to another LLM swap that function only — the prompt and surrounding logic are model-agnostic
