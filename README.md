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
- `METABASE_API_KEY` environment variable (or key set in `metabase_toolkit.py`)
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

### Phase 2 (coming — requires BigQuery tables to be live)

```bash
# Compare Snowflake vs BigQuery result sets for a question
python3 metabase_toolkit.py compare-outputs 12345

# Apply converted SQL to Metabase
python3 metabase_toolkit.py apply-migration 12345
python3 metabase_toolkit.py apply-migration --batch -i converted/migration_status.csv
```

## Project Files

```
metabase_toolkit.py          Main CLI tool
schema_map.json              Snowflake → BigQuery schema/dataset name mapping (populate before apply)
converted/                   Conversion output (created by batch-convert)
  syntax_clean/              Syntax valid, no caveats
  syntax_clean_verify_data/  Syntax valid but a runtime risk was flagged
  syntax_errors/             BigQuery rejected the syntax — needs human fix
  migration_status.csv       Single source of truth for all question statuses
```

## Schema Mapping

Table references are left as-is during conversion (Snowflake three-part names like `UK.ENHANCED.my_table`). The `schema_map.json` file maps Snowflake prefixes to BigQuery equivalents and is applied at `apply-migration` time.

Populate this file once Rittman Analytics confirms the BigQuery project and dataset names. The apply command will warn on any unmapped reference during dry-run, and hard-fail during live apply.

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

## Data Validation (Phase 2)

When comparing Snowflake and BigQuery result sets:
- **Row count must match exactly** — all Metabase queries are full refreshes
- Numeric column aggregates (SUM/COUNT) within ±0.01%
- Run comparisons outside Snowflake refresh windows (Snowflake refreshes 4× daily; safe window opens ~45 min after each refresh)
