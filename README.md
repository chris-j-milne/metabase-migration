# Metabase Migration Toolkit

A CLI toolkit for auditing and migrating Metabase saved questions from Snowflake to BigQuery at Carwow.

## Background

Carwow is migrating its data warehouse from Snowflake to BigQuery. The underlying dbt models and data pipelines are being migrated by Rittman Analytics. This toolkit handles the Metabase layer — auditing all saved questions, converting Snowflake-specific SQL syntax to BigQuery equivalents, and applying the changes once the BigQuery tables are live.

The migration runs in two phases:

1. **Phase 1 (current)** — Inventory, keyword scan, and SQL conversion. Both databases run in parallel. All Metabase questions are reviewed and either deprecated or converted.
2. **Phase 2** — Data validation and cutover. Converted questions are validated against live BigQuery data, then applied to Metabase.

## Requirements

- Python 3.8+
- `pip install requests tabulate`
- `METABASE_API_KEY` environment variable
- `ANTHROPIC_API_KEY` environment variable (required for `convert-sql` command only)

## Commands

### Connection & inventory

```bash
# Verify API connection
python3 metabase_toolkit.py connect

# High-level stats: question counts by folder, creator, last-run age
python3 metabase_toolkit.py summary

# List questions, optionally filtered by folder
python3 metabase_toolkit.py list-queries
python3 metabase_toolkit.py list-queries --folder "Marketing"

# Export full inventory to CSV (includes raw SQL for native queries)
python3 metabase_toolkit.py export-csv -o metabase_inventory.csv
```

### Keyword scanning

```bash
# Find all questions with BigQuery-incompatible Snowflake syntax
python3 metabase_toolkit.py scan-keywords -o bq_issues.csv

# Scan for specific keywords only
python3 metabase_toolkit.py scan-keywords --keywords "ILIKE,DATEADD,DATEDIFF"
```

### SQL conversion (Phase 1)

```bash
# Convert a single question (Snowflake → BigQuery syntax)
python3 metabase_toolkit.py convert-sql 12345

# Batch-convert a list of question IDs
python3 metabase_toolkit.py batch-convert --ids 37,23794,1234 -o converted/
```

Conversion output files per question:

| File | Contents |
|---|---|
| `card{N}_orig_sql.txt` | Original SQL as stored in Metabase |
| `card{N}_adj_sql.txt` | Converted BigQuery SQL |
| `card{N}_results.txt` | Validation summary: status, changes made, any caveats |

Converted files are categorised into `converted/syntax_clean/`, `converted/syntax_clean_verify_data/`, or `converted/syntax_errors/` based on the BigQuery dry-run result.

### SQL inspection

```bash
# Print the SQL for a specific question
python3 metabase_toolkit.py show-sql 12345
```

### Data comparison & apply *(requires BigQuery tables to be live)*

```bash
# Compare Snowflake vs BigQuery result sets for a single question
python3 metabase_toolkit.py compare-outputs 12345

# Apply final SQL to Metabase (single question)
python3 metabase_toolkit.py apply-migration 12345

# Apply all questions in ready_to_migrate/ (dry run first)
python3 metabase_toolkit.py apply-migration --dry-run
python3 metabase_toolkit.py apply-migration
```

## Project Files

```
metabase_toolkit.py          Main CLI tool
schema_map.json              Snowflake → BigQuery schema/dataset name mapping (populate before apply)
converted/                   Conversion output (created by run-plan)
  syntax_clean/              Syntax valid, no caveats
  syntax_clean_verify_data/  Syntax valid but a runtime risk was flagged
  syntax_errors/             BigQuery rejected the syntax — needs human fix
  data_check_passed/         Data comparison passed — card{N}_data_check.txt per question
  data_check_failed/         Data comparison failed — card{N}_data_check.txt + card{N}_datadiff.txt
  ready_to_migrate/          Final SQL with schema refs substituted — card{N}_final_sql.txt
  migration_status.csv       Single source of truth for all question statuses
```

## Schema Mapping

Table references are left as-is during conversion (Snowflake three-part names like `UK.ENHANCED.my_table`). The `schema_map.json` file maps Snowflake prefixes to BigQuery equivalents.

Schema substitution happens when a question passes the data comparison check — `_final_sql.txt` is written with prefixes already substituted. The `apply-migration` command then hard-fails if any `TBD` entry from `schema_map.json` still appears in the final SQL, as a guard against pushing half-mapped table names to production.

Populate `schema_map.json` once Rittman Analytics confirms the BigQuery project and dataset names.

## Snowflake → BigQuery Syntax Reference

| Snowflake | BigQuery |
|---|---|
| `ILIKE` | `LOWER(col) LIKE LOWER(pattern)` |
| `DATEADD(part, n, date)` | `DATE_ADD(date, INTERVAL n part)` |
| `DATEDIFF(part, a, b)` | `DATE_DIFF(a, b, part)` |
| `NVL(a, b)` | `COALESCE(a, b)` |
| `DECODE(col, v1, r1, ...)` | `CASE WHEN` |
| `REGEXP_SUBSTR` | `REGEXP_EXTRACT` |
| `LATERAL FLATTEN` | `UNNEST` |
| `TRY_CAST` | `SAFE_CAST` |
| `STRTOK` | `SPLIT` |

`QUALIFY`, `IFNULL`, and `PARSE_JSON` are supported by BigQuery natively — no change needed.

## Data Validation (Phase 3)

When comparing Snowflake and BigQuery result sets, checks are applied per column type:

| Column type | Checks |
|---|---|
| **Numeric** | SUM, MAX, and MIN must all match within ±1% |
| **Date / datetime** | MAX and MIN values must match exactly |
| **Text / categorical** | Null count and unique value count must match exactly |

Row count mismatch is a hard fail — all other checks are skipped if counts differ.

Run comparisons outside Snowflake refresh windows (Snowflake refreshes 4× daily; safe window opens ~45 min after each refresh).


## FULL MIGRATION PROCESS

### Phase 1 — Stakeholder Review

A summary sheet of all Metabase queries (as at 16-Jun) was created using the tools in this toolkit. Queries were prioritised based on number of views/runs — any query with no evidence of being viewed or run in the last 3 months was flagged as deprecate, while the rest were categorised by view counts into migrate, opt-in, and approval required.

Stakeholders are reviewing this list, flagging which queries need migrating (with any required approvals) and which can be deprecated.

The results are manually collated into an action CSV with two columns:

```
id,action
1234,migrate
5678,deprecate
9012,skip
```

Valid actions: `migrate`, `skip`, `deprecate`.

### Phase 2 — Dry Run

Run using:
```bash
python3 metabase_toolkit.py run-plan --actions <action_file>.csv --dry-run
```

Remove `--dry-run` to execute for real.

**For queries in the action CSV**, the specified action is followed exactly.

**For queries not in the CSV** (new queries created after the review period), the following defaults apply:
- `skip` — if the query is in a personal collection, or is a GUI query with no SQL
- `deprecate` — if the query is a native SQL query anywhere else

**Dependency check (GUI queries):** Before any changes are made, the tool checks that every GUI question being kept active has its entire source-card chain also kept active. If a GUI question depends on a query that will be deprecated, a warning is printed:

```
⚠  1 dependency warning(s):

  [csv:migrate] 5: My GUI question name
         depends on  3: Source query name  → will be deprecate
```

The tool continues regardless, so the recommended workflow is:

1. Run with `--dry-run` and review warnings
2. Update the action CSV to fix any broken dependencies (change the source query to `migrate`)
3. Repeat until "✓ No broken GUI dependencies found"
4. Run without `--dry-run`

**For queries actioned as `migrate`:** The query SQL is sent to Claude to be rewritten for BigQuery syntax. No table re-pointing is done at this stage. Three files are written per query:

| File | Contents |
|---|---|
| `card<id>_orig_sql.txt` | Original SQL as stored in Metabase |
| `card<id>_adj_sql.txt` | Syntax-converted BigQuery SQL |
| `card<id>_results.txt` | Summary of changes made and any caveats |

Files are saved into one of three folders under `converted/`:

| Folder | Meaning |
|---|---|
| `syntax_clean/` | Syntax valid, ready to re-point to BigQuery tables |
| `syntax_clean_verify_data/` | Syntax valid but a runtime risk was flagged (e.g. timestamp vs date type) |
| `syntax_errors/` | BigQuery rejected the syntax — needs human review |

A summary row for every processed query is written to `converted/migration_status.csv`.

**For queries actioned as `skip`:** Recorded in `migration_status.csv` with `current_status = skipped`. No changes made to Metabase.

**For queries actioned as `deprecate`:** The query is moved in Metabase to `Archive/<original folder path>` so it remains accessible but is clearly out of active use. Recorded in `migration_status.csv` with `current_status = deprecated`.

### Phase 3 — Data Comparison *(requires BigQuery tables to be live)*

Run a single question:
```bash
python3 metabase_toolkit.py compare-outputs 12345 --adj-db <bigquery_db_id>
```

Or process a specific set of questions (smart-routing: already converted → runs data check; not yet converted → converts first):
```bash
python3 metabase_toolkit.py run-plan --actions <action_file>.csv --ids 12345,67890 --adj-db <bigquery_db_id>
```

**For each question:** The original SQL is run against Snowflake and the adjusted SQL (from `card{N}_adj_sql.txt`) is run against BigQuery. Table names in the adjusted SQL are still Snowflake-format at this stage — BigQuery validates syntax and returns data because the Carwow tables exist in BigQuery under equivalent names. Schema prefix substitution happens in the next step.

Comparisons applied per column type (see Data Validation section above).

**If the check passes:**
- `converted/data_check_passed/card{N}_data_check.txt` — timestamped summary
- `converted/ready_to_migrate/card{N}_final_sql.txt` — adjusted SQL with Snowflake schema prefixes replaced using `schema_map.json`; any `TBD` entries are flagged as a warning
- `migration_status.csv` updated to `current_status = data_check_passed`

**If the check fails:**
- `converted/data_check_failed/card{N}_data_check.txt` — timestamped summary
- `converted/data_check_failed/card{N}_datadiff.txt` — column-level detail of what mismatched
- `migration_status.csv` updated to `current_status = data_check_failed`

Run comparisons outside Snowflake refresh windows (Snowflake refreshes 4× daily; safe window opens ~45 min after each refresh completes).

**GUI `via_table` queries** *(planned):* These connect directly to a database table rather than containing SQL. Migration requires remapping the Metabase-internal database, table, and field IDs to their BigQuery equivalents — no SQL rewrite needed. Not yet implemented.

### Phase 4 — Apply Migration

Once questions are in `ready_to_migrate/` with no `TBD` schema entries remaining:

```bash
# Dry run — see what would be pushed
python3 metabase_toolkit.py apply-migration --dry-run

# Apply a single question
python3 metabase_toolkit.py apply-migration 12345

# Apply all questions in ready_to_migrate/
python3 metabase_toolkit.py apply-migration
```

This fetches each card from Metabase, writes the final SQL into `dataset_query` in place, and PUTs it back via the API. On success, `migration_status.csv` is updated to `current_status = applied`.

**Guard:** The command hard-fails per question if any `schema_map.json` prefix with value `TBD` still appears in the final SQL. Populate `schema_map.json` with the confirmed BigQuery project/dataset names from Rittman Analytics before running this step.

---

### Ad Hoc Notes

Archived queries with active dependencies that need restoring for migration:
- **4301** — depended on by questions flagged for migration
- **4624** — depended on by questions flagged for migration

Possible migration needed:
- **17954** — currently listed as not needing migration, but depends on archived query 17690

Native Table GUI Queries needing database re-pointing.
18154, 29405, 7957, 7956, 23167, 23035, 21616, 10066, 5053, 6141, 6144, 26104, 4263, 4270, 4271, 25477, 14687, 14688, 14689, 14686, 6539, 16171, 23102