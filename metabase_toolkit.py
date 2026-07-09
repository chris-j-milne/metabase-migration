"""
Metabase Migration Toolkit
==========================
Helps audit Metabase queries ahead of a Snowflake → BigQuery migration.

Usage:
    python metabase_toolkit.py --help
    python metabase_toolkit.py summary
    python metabase_toolkit.py scan-keywords
    python metabase_toolkit.py list-queries --folder "My Folder"
    python metabase_toolkit.py export-csv

Requirements:
    pip install requests tabulate

Configuration:
    Set METABASE_URL and METABASE_API_KEY as environment variables, or
    edit the defaults at the top of this file.
"""

import os
import sys
import json
import argparse
import csv
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timezone

# Load .env from the project directory (if present) before reading any env vars.
# This lets API keys live in .env rather than ~/.zshrc.
def _load_dotenv():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _key, _, _val = _line.partition("=")
            _key = _key.strip()
            _val = _val.strip().strip('"').strip("'")
            os.environ.setdefault(_key, _val)

_load_dotenv()

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: run  pip install requests tabulate")

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

try:
    import anthropic as _anthropic_lib
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# Accumulates token usage across all _call_claude() calls in this process.
_TOKEN_USAGE = {"input": 0, "output": 0, "calls": 0}

# ── Config ────────────────────────────────────────────────────────────────────

METABASE_URL = os.getenv("METABASE_URL", "https://carwow.metabaseapp.com")
METABASE_API_KEY = os.getenv("METABASE_API_KEY", "")

# BigQuery Playground connection (database ID 67 in this Metabase instance)
BQ_PLAYGROUND_DB_ID = 67

MIGRATION_STATUS_FIELDS = [
    "id", "name", "original_category", "current_status",
    "failure_reason", "notes", "original_collection_id", "action_source",
]

# Snowflake connection (database ID 6 in this Metabase instance)
SNOWFLAKE_DB_ID = 6


# Keywords that exist in Snowflake but are NOT supported (or differ) in BigQuery
BQ_INCOMPATIBLE_KEYWORDS = [
    #"QUALIFY",
    "ILIKE",
    "TRYCAST",
    "TRY_CAST",
    "TRY_TO_NUMBER",
    "TRY_TO_DATE",
    "TRY_TO_TIMESTAMP",
    "FLATTEN",
    "LATERAL FLATTEN",
    #"PARSE_JSON",          # Supported in BigQuery
    "ARRAY_CONSTRUCT",
    "OBJECT_CONSTRUCT",
    "GET_PATH",
    "STARTSWITH",          # Snowflake function (BQ uses STARTS_WITH)
    "ENDSWITH",            # Snowflake function (BQ uses ENDS_WITH)
    "ZEROIFNULL",
    "NULLIFZERO",
    #"IFNULL",             # Supported in BigQuery
    "NVL",                 # Snowflake alias for COALESCE
    "NVL2",
    "DECODE",              # Snowflake shorthand
    "PIVOT",               # Syntax differs
    "UNPIVOT",
    "SAMPLE",              # Syntax differs (BQ uses TABLESAMPLE)
    "GENERATOR",           # Snowflake-specific
    "SEQ1", "SEQ2", "SEQ4", "SEQ8",  # Snowflake sequences
    "SPLIT_TO_TABLE",
    "STRTOK_TO_ARRAY",
    "STRTOK",
    "REGEXP_SUBSTR",       # BQ equivalent is REGEXP_EXTRACT
    "REGEXP_REPLACE",      # Syntax differs slightly
    "DATE_FROM_PARTS",
    "TIME_FROM_PARTS",
    "TIMESTAMP_FROM_PARTS",
    "DATEADD",             # BQ uses DATE_ADD
    "DATEDIFF",            # BQ uses DATE_DIFF
    "TIMESTAMPADD",
    "TIMESTAMPDIFF",
    #"LAST_DAY",
    "YEAROFWEEK",
    "YEAROFWEEKISO",
    "IFF",
    "DIV0", 
    "DIV0NULL",
    "LISTAGG",
    "OBJECT_KEYS",
    "ARRAY_SIZE",
    "ARRAY_SLICE",
    "TO_VARCHAR",
    "TO_CHAR",
    "BOOLAND_AGG",
    "BOOLOR_AGG",
]

# ── API helpers ───────────────────────────────────────────────────────────────

# -- Just gets the authentication header that 
def api_headers():
    return {
        "x-api-key": METABASE_API_KEY,
        "Content-Type": "application/json",
    }

def extract_sql(card):
    """Extract raw SQL from a card, handling both old and new Metabase query formats."""
    dq = card.get("dataset_query", {})
    # New MBQL v2 format: dataset_query.stages[0].native
    stages = dq.get("stages", [])
    if stages:
        return stages[0].get("native", "") or ""
    # Old format: dataset_query.native.query
    return dq.get("native", {}).get("query", "") or ""

def is_native(card):
    """Return True if the card is a native SQL query."""
    if card.get("query_type") == "native":
        return True
    dq = card.get("dataset_query", {})
    if dq.get("type") == "native":
        return True
    stages = dq.get("stages", [])
    if stages and stages[0].get("lib/type") == "mbql.stage/native":
        return True
    return False




def get(path, params=None):
    url = f"{METABASE_URL.rstrip('/')}/api{path}"
    r = requests.get(url, headers=api_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def post_api(path, payload):
    url = f"{METABASE_URL.rstrip('/')}/api{path}"
    r = requests.post(url, headers=api_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def put_api(path, payload):
    url = f"{METABASE_URL.rstrip('/')}/api{path}"
    r = requests.put(url, headers=api_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def check_connection():
    try:
        user = get("/user/current")
        print(f"✓ Connected as: {user.get('first_name')} {user.get('last_name')} ({user.get('email')})")
        return True
    except requests.HTTPError as e:
        print(f"✗ Connection failed: {e}")
        return False

# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_all_cards():
    """Fetch every saved question/query from Metabase."""
    print("Fetching all queries (this may take a moment)...")
    data = get("/card", params={"f": "all"})
    # API returns a list directly
    if isinstance(data, list):
        return data
    # Some versions return {"data": [...], "total": N}
    return data.get("data", data)

def fetch_collections():
    """Fetch all collections (folders)."""
    data = get("/collection")
    if isinstance(data, list):
        return data
    return data.get("data", [])

def fetch_all_dashboards():
    """Fetch every dashboard, handling both paginated and list responses."""
    data = get("/dashboard", params={"f": "all"})
    dashboards = data if isinstance(data, list) else data.get("data", [])
    # Handle pagination if present
    if isinstance(data, dict) and "total" in data:
        total = data["total"]
        page_size = len(dashboards)
        offset = page_size
        while offset < total:
            page = get("/dashboard", params={"f": "all", "limit": page_size, "offset": offset})
            batch = page if isinstance(page, list) else page.get("data", [])
            if not batch:
                break
            dashboards.extend(batch)
            offset += len(batch)
    return dashboards

def fetch_dashboard_card_map():
    """Return {card_id: [dashboard_name, ...]} by fetching all dashboards."""
    print("Fetching dashboard memberships...")
    dashboards = fetch_all_dashboards()
    card_to_dashboards = defaultdict(list)
    for d in dashboards:
        detail = get(f"/dashboard/{d['id']}")
        for dc in detail.get("dashcards", []):
            card_id = dc.get("card_id")
            if card_id:
                card_to_dashboards[card_id].append(d["name"])
    print(f"  Found {len(dashboards)} dashboards")
    return card_to_dashboards

def build_collection_map(collections):
    """Return {id: full_path_string} including nested paths."""
    id_to_col = {c["id"]: c for c in collections}
    id_to_col["root"] = {"name": "Our analytics (root)", "location": "/"}

    def full_path(col_id):
        col = id_to_col.get(col_id)
        if not col:
            return str(col_id)
        location = col.get("location", "/")
        # location looks like "/123/456/" — resolve each segment
        parts = [p for p in location.strip("/").split("/") if p]
        names = []
        for pid in parts:
            try:
                parent = id_to_col.get(int(pid))
                names.append(parent["name"] if parent else pid)
            except (ValueError, TypeError):
                names.append(pid)
        names.append(col["name"])
        return " / ".join(names)

    return {col_id: full_path(col_id) for col_id in id_to_col}

def fetch_personal_collection_ids(collections):
    """Return set of collection IDs that belong to individual users (personal folders)."""
    return {c["id"] for c in collections if c.get("personal_owner_id")}

def _build_col_name_parent_map(collections):
    """Return {(name, parent_id_or_None): collection_id} for efficient lookup."""
    result = {}
    for col in collections:
        location = col.get("location", "/")
        parts = [p for p in location.strip("/").split("/") if p]
        parent_id = int(parts[-1]) if parts else None
        result[(col["name"], parent_id)] = col["id"]
    return result

def _find_or_create_collection(name, parent_id, name_parent_map, dry_run=False):
    """Find a collection by (name, parent_id), creating it if missing. Mutates name_parent_map."""
    key = (name, parent_id)
    if key in name_parent_map:
        return name_parent_map[key]
    if dry_run:
        fake_id = -(abs(hash(key)) % 1_000_000)
        name_parent_map[key] = fake_id
        return fake_id
    payload = {"name": name, "color": "#509ee3"}
    if parent_id is not None:
        payload["parent_id"] = parent_id
    result = post_api("/collection", payload)
    new_id = result["id"]
    name_parent_map[key] = new_id
    return new_id

def _get_archive_collection_id(original_collection_id, collections_by_id, name_parent_map, archive_cache, dry_run=False):
    """Return collection ID for Archive/<original path>, creating sub-collections as needed."""
    orig_col = collections_by_id.get(original_collection_id)
    archive_root_key = "__archive_root__"
    if archive_root_key not in archive_cache:
        archive_cache[archive_root_key] = _find_or_create_collection("Archive", None, name_parent_map, dry_run)

    if not orig_col:
        return archive_cache[archive_root_key]

    location = orig_col.get("location", "/")
    parts = [p for p in location.strip("/").split("/") if p]
    names = []
    for pid in parts:
        try:
            parent = collections_by_id.get(int(pid))
            names.append(parent["name"] if parent else pid)
        except (ValueError, TypeError):
            names.append(pid)
    names.append(orig_col["name"])

    current_parent_id = archive_cache[archive_root_key]
    path_key = "Archive"
    for name in names:
        path_key = f"{path_key}/{name}"
        if path_key not in archive_cache:
            archive_cache[path_key] = _find_or_create_collection(name, current_parent_id, name_parent_map, dry_run)
        current_parent_id = archive_cache[path_key]

    return current_parent_id

def _move_card_to_collection(card_id, collection_id, dry_run=False):
    """Move a card to a different collection."""
    if not dry_run:
        put_api(f"/card/{card_id}", {"collection_id": collection_id})

def _check_dependency_warnings(cards, action_map, personal_ids, existing_status):
    """Return warnings for GUI questions kept active whose source-card chain contains
    a card that will be deprecated or is missing (archived/deleted)."""
    cards_map = {c["id"]: c for c in cards}

    def effective_action(c):
        card_id = c["id"]
        if existing_status.get(str(card_id), {}).get("current_status") == "applied":
            return "already_applied"
        if card_id in action_map:
            return action_map[card_id]
        if not is_native(c):
            return "skip"
        col_id = c.get("collection_id") or "root"
        if col_id in personal_ids:
            return "skip"
        return "deprecate"

    def chain_deps(card, visited=None):
        if visited is None:
            visited = set()
        sc_id = _get_source_card_id(card)
        if sc_id is None or sc_id in visited:
            return []
        visited.add(sc_id)
        dep = cards_map.get(sc_id)
        if dep is None:
            return [(sc_id, f"<missing/archived card {sc_id}>", "missing")]
        return [(sc_id, dep.get("name", ""), effective_action(dep))] + chain_deps(dep, visited)

    warnings = []
    for c in cards:
        if is_native(c):
            continue
        if effective_action(c) in ("deprecate", "already_applied"):
            continue
        if _get_source_card_id(c) is None:
            continue  # via_table — no source-card chain
        for dep_id, dep_name, dep_action in chain_deps(c):
            if dep_action in ("deprecate", "missing"):
                warnings.append({
                    "card_id": c["id"],
                    "card_name": c.get("name", ""),
                    "card_action": effective_action(c),
                    "dep_id": dep_id,
                    "dep_name": dep_name,
                    "dep_action": dep_action,
                })
    return warnings


def fetch_table_map():
    """Return {table_id: 'schema.table_name'} for all tables in Metabase."""
    data = get("/table")
    tables = data if isinstance(data, list) else data.get("data", [])
    result = {}
    for t in tables:
        schema = t.get("schema") or ""
        name = t.get("name") or ""
        result[t["id"]] = f"{schema}.{name}".lower() if schema else name.lower()
    return result

def fetch_database_map():
    """Return {db_id: db_name} for all database connections in Metabase."""
    data = get("/database")
    dbs = data if isinstance(data, list) else data.get("data", [])
    return {d["id"]: d["name"] for d in dbs}

# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_summary(args):
    """Print a high-level summary: totals, by folder, by creator, by last-run age."""
    cards = fetch_all_cards()
    collections = fetch_collections()
    col_map = build_collection_map(collections)

    total = len(cards)
    print(f"\n{'='*60}")
    print(f"  TOTAL QUERIES: {total}")
    print(f"{'='*60}\n")

    # By collection
    by_folder = defaultdict(int)
    for c in cards:
        col_id = c.get("collection_id") or "root"
        folder = col_map.get(col_id, str(col_id))
        by_folder[folder] += 1

    print("── By Folder ─────────────────────────────────────────────")
    folder_rows = sorted(by_folder.items(), key=lambda x: -x[1])
    _print_table(["Folder", "Count"], folder_rows)

    # By creator
    by_creator = defaultdict(int)
    for c in cards:
        creator = c.get("creator", {})
        name = f"{creator.get('first_name','')} {creator.get('last_name','')}".strip() or "Unknown"
        by_creator[name] += 1

    print("\n── By Creator ────────────────────────────────────────────")
    creator_rows = sorted(by_creator.items(), key=lambda x: -x[1])
    _print_table(["Creator", "Count"], creator_rows[:30])
    if len(creator_rows) > 30:
        print(f"  ... and {len(creator_rows)-30} more creators")

    # By last-run age
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    buckets = {"Never run": 0, "> 1 year": 0, "6–12 months": 0,
               "1–6 months": 0, "< 1 month": 0}
    for c in cards:
        lr = c.get("last_used_at") or c.get("updated_at")
        if not lr:
            buckets["Never run"] += 1
            continue
        try:
            dt = datetime.fromisoformat(lr.replace("Z", "+00:00").replace("+00:00", ""))
            age_days = (now - dt).days
            if age_days > 365:
                buckets["> 1 year"] += 1
            elif age_days > 180:
                buckets["6–12 months"] += 1
            elif age_days > 30:
                buckets["1–6 months"] += 1
            else:
                buckets["< 1 month"] += 1
        except Exception:
            buckets["Never run"] += 1

    print("\n── By Last Run ───────────────────────────────────────────")
    _print_table(["Last Run", "Count"], list(buckets.items()))


def cmd_scan_keywords(args):
    """Scan all query SQL for BigQuery-incompatible Snowflake keywords."""
    cards = fetch_all_cards()
    collections = fetch_collections()
    col_map = build_collection_map(collections)

    keywords = [k.upper() for k in (args.keywords.split(",") if args.keywords else BQ_INCOMPATIBLE_KEYWORDS)]

    print(f"\nScanning {len(cards)} queries for {len(keywords)} keywords...\n")

    hits = []
    keyword_counts = defaultdict(int)
    query_hit_counts = defaultdict(int)

    for c in cards:
        if not is_native(c):
            continue
        sql = extract_sql(c)
        if not sql:
            continue

        sql_upper = sql.upper()
        found = []
        for kw in keywords:
            # Word-boundary match to avoid partial hits
            pattern = r'\b' + re.escape(kw) + r'\b'
            matches = re.findall(pattern, sql_upper)
            if matches:
                found.append((kw, len(matches)))
                keyword_counts[kw] += 1

        if found:
            col_id = c.get("collection_id") or "root"
            folder = col_map.get(col_id, str(col_id))
            creator = c.get("creator", {})
            creator_name = f"{creator.get('first_name','')} {creator.get('last_name','')}".strip()
            hits.append({
                "id": c["id"],
                "name": c.get("name", ""),
                "folder": folder,
                "creator": creator_name,
                "keywords": ", ".join(f"{kw}(×{n})" for kw, n in found),
                "url": f"{METABASE_URL}/question/{c['id']}",
            })
            query_hit_counts[c["id"]] = len(found)

    print(f"Found {len(hits)} queries with incompatible keywords\n")

    if hits:
        print("── Queries requiring attention ───────────────────────────")
        rows = [[h["id"], h["name"][:50], h["folder"][:40], h["creator"], h["keywords"][:60]] for h in hits]
        _print_table(["ID", "Name", "Folder", "Creator", "Keywords found"], rows)

        print("\n── Keyword frequency ─────────────────────────────────────")
        kw_rows = sorted(keyword_counts.items(), key=lambda x: -x[1])
        _print_table(["Keyword", "Queries affected"], kw_rows)

    if args.output:
        _write_hits_csv(hits, args.output)
        print(f"\nResults saved to: {args.output}")


def cmd_show_keywords(args):
    """Show Snowflake-incompatible keywords for a single question by ID."""
    card = get(f"/card/{args.id}")
    name = card.get("name", f"Card {args.id}")
    if not is_native(card):
        print(f"Query '{name}' (id={args.id}) is a GUI query — no raw SQL to scan.")
        return
    sql = extract_sql(card)
    if not sql:
        print(f"Query '{name}' (id={args.id}) has no SQL.")
        return

    keywords = [k.upper() for k in BQ_INCOMPATIBLE_KEYWORDS]
    sql_upper = sql.upper()
    found = []
    for kw in keywords:
        matches = re.findall(r'\b' + re.escape(kw) + r'\b', sql_upper)
        if matches:
            found.append((kw, len(matches)))

    print(f"\n── {name} (id={args.id}) ────────────────────────────────")
    if not found:
        print("No Snowflake-incompatible keywords found.")
        return
    found.sort(key=lambda x: -x[1])
    _print_table(["Keyword", "Occurrences"], found)
    print(f"\nTotal: {len(found)} distinct incompatible keyword(s), {sum(n for _, n in found)} total occurrence(s)")


def cmd_list_queries(args):
    """List all queries, optionally filtered by folder name."""
    cards = fetch_all_cards()
    collections = fetch_collections()
    col_map = build_collection_map(collections)

    rows = []
    for c in cards:
        col_id = c.get("collection_id") or "root"
        folder = col_map.get(col_id, str(col_id))

        if args.folder and args.folder.lower() not in folder.lower():
            continue

        creator = c.get("creator", {})
        creator_name = f"{creator.get('first_name','')} {creator.get('last_name','')}".strip()
        query_type = "native" if is_native(c) else "gui"
        last_run = c.get("last_used_at", "—")
        rows.append([c["id"], c.get("name","")[:60], folder[:40], creator_name, query_type, last_run])

    print(f"\n{len(rows)} queries found\n")
    _print_table(["ID", "Name", "Folder", "Creator", "Type", "Last Run"], rows)

    if args.output:
        with open(args.output, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ID", "Name", "Folder", "Creator", "Type", "Last Run"])
            w.writerows(rows)
        print(f"\nSaved to: {args.output}")


def cmd_export_csv(args):
    """Export query stats inventory to CSV (no SQL column), including monthly run counts."""
    cards = fetch_all_cards()
    collections = fetch_collections()
    col_map = build_collection_map(collections)
    dash_map = fetch_dashboard_card_map()

    months = _last_n_months(6)
    print(f"Fetching monthly question run counts ({months[0]} to {months[-1]})...")
    run_data = _fetch_monthly_question_runs(months)
    print("Fetching dashboard vs direct run breakdown...")
    source_data = _fetch_question_run_sources(months)
    print("Fetching unique user counts (6m)...")
    user_data = _fetch_question_unique_users(months)
    print("Fetching unique user counts (all time)...")
    user_data_total = _fetch_question_unique_users_total()
    print("Fetching total run counts (all time)...")
    total_runs_data = _fetch_question_total_runs()
    print("Fetching page view counts (6m)...")
    views_6m_data = _fetch_question_views_6m(months)
    print("Fetching last viewed timestamps from view log...")
    last_viewed_data = _fetch_question_last_viewed()
    print("Fetching table map for GUI query resolution...")
    table_map = fetch_table_map()
    print("Fetching database map...")
    db_map = fetch_database_map()
    print("Fetching last run status (error / zero rows)...")
    last_run_status = _fetch_last_run_status()

    all_cards_map = {c["id"]: c for c in cards}

    output_file = args.output or "metabase_inventory.csv"
    fieldnames = [
        "id", "name", "folder", "creator", "type", "gui_subtype", "database", "source_databases",
        "uses_uk", "uses_de", "uses_es",
        "created_at", "updated_at", "last_run", "last_any_run", "last_viewed_at", "view_count",
        "archived", "url", "public_url", "has_template_tags", "has_alias_self_ref",
        "source_card_id", "source_card_missing",
        "table_count", "tables", "snowflake_keywords", "snowflake_keyword_total", "snowflake_keyword_detail",
        "last_run_errored", "last_run_zero_rows",
        "dashboard_count", "dashboards",
        "total_runs", "total_runs_6m", "dashboard_runs_6m", "direct_runs_6m",
        "unique_users_total", "unique_users_6m",
        "total_views_6m",
    ] + months

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in cards:
            col_id = c.get("collection_id") or "root"
            folder = col_map.get(col_id, str(col_id))
            creator = c.get("creator", {})
            creator_name = f"{creator.get('first_name','')} {creator.get('last_name','')}".strip()
            query_type = "native" if is_native(c) else "gui"
            sql = extract_sql(c) if query_type == "native" else ""
            table_refs = list(dict.fromkeys(
                _extract_table_refs(sql) if query_type == "native" else _extract_gui_tables(c, table_map)
            ))
            db_name = db_map.get(c.get("dataset_query", {}).get("database"), "")
            source_dbs, uses_uk, uses_de, uses_es = _determine_source_databases(table_refs, db_name)
            dash_names = dash_map.get(c["id"], [])
            c_runs = run_data.get(c["id"], {})
            c_sources = source_data.get(c["id"], {"dashboard": 0, "direct": 0})
            row = {
                "id": c["id"],
                "name": c.get("name", ""),
                "folder": folder,
                "creator": creator_name,
                "type": query_type,
                "gui_subtype": _gui_subtype(c, all_cards_map) if query_type == "gui" else "",
                "database": db_name,
                "source_databases": source_dbs,
                "uses_uk": uses_uk,
                "uses_de": uses_de,
                "uses_es": uses_es,
                "created_at": c.get("created_at", ""),
                "updated_at": c.get("updated_at", ""),
                "last_run": c.get("last_used_at", ""),
                "last_any_run": c.get("last_query_start", ""),
                "last_viewed_at": last_viewed_data.get(c["id"], ""),
                "view_count": c.get("view_count", 0),
                "archived": c.get("archived", False),
                "url": f"{METABASE_URL}/question/{c['id']}",
                "public_url": f"{METABASE_URL}/public/question/{c['public_uuid']}" if c.get("public_uuid") else "",
                "has_template_tags": _has_template_tags(sql),
                "has_alias_self_ref": _has_alias_self_reference(sql),
                "source_card_id": (sc := _get_source_card_id(c)) or "",
                "source_card_missing": (sc is not None and sc not in all_cards_map) or "",
                "table_count": len(table_refs),
                "tables": ", ".join(table_refs),
                "snowflake_keywords": len(kw_hits := _scan_snowflake_keywords(sql)),
                "snowflake_keyword_total": sum(n for _, n in kw_hits),
                "snowflake_keyword_detail": ", ".join(f"{kw}:{n}" for kw, n in kw_hits),
                "last_run_errored": last_run_status.get(c["id"], {}).get("errored", ""),
                "last_run_zero_rows": (
                    last_run_status[c["id"]]["result_rows"] == 0
                    if c["id"] in last_run_status and last_run_status[c["id"]]["result_rows"] is not None
                    else ""
                ),
                "dashboard_count": len(dash_names),
                "dashboards": ", ".join(dash_names),
                "total_runs": total_runs_data.get(c["id"], 0),
                "total_runs_6m": sum(c_runs.values()),
                "dashboard_runs_6m": c_sources["dashboard"],
                "direct_runs_6m": c_sources["direct"],
                "unique_users_total": user_data_total.get(c["id"], 0),
                "unique_users_6m": user_data.get(c["id"], 0),
                "total_views_6m": views_6m_data.get(c["id"], 0),
            }
            for m in months:
                row[m] = c_runs.get(m, 0)
            writer.writerow(row)

    print(f"✓ Exported {len(cards)} queries to: {output_file}")


def cmd_export_dashboards(args):
    """Export dashboard inventory to CSV with monthly view counts."""
    dashboards = fetch_all_dashboards()

    collections = fetch_collections()
    col_map = build_collection_map(collections)

    months_6 = _last_n_months(6)
    months_3 = _last_n_months(3)

    print("Fetching real user IDs for authenticated/anonymous split...")
    real_user_ids = _fetch_real_user_ids()
    print(f"  {len(real_user_ids)} authenticated users found")

    print(f"Fetching monthly dashboard views ({months_6[0]} to {months_6[-1]}, authenticated only)...")
    view_data = _fetch_monthly_dashboard_views(months_6, real_user_ids)

    print("Fetching unique viewer counts...")
    viewer_data = _fetch_dashboard_unique_viewers(months_6)

    print("Fetching authenticated vs anonymous view split (6 months)...")
    auth_anon_6m = _fetch_dashboard_auth_anon_views(months_6, real_user_ids)
    print("Fetching authenticated vs anonymous view split (3 months)...")
    auth_anon_3m = _fetch_dashboard_auth_anon_views(months_3, real_user_ids)

    output_file = args.output or "metabase_dashboards.csv"
    fieldnames = [
        "id", "name", "folder", "creator",
        "created_at", "updated_at", "last_viewed_at",
        "view_count",
        "total_views_6m", "authenticated_views_6m", "anonymous_views_6m", "unique_viewers_6m",
        "total_views_3m", "authenticated_views_3m", "anonymous_views_3m",
    ] + months_6

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in dashboards:
            col_id = d.get("collection_id") or "root"
            folder = col_map.get(col_id, str(col_id))
            creator = d.get("creator", {})
            creator_name = f"{creator.get('first_name','')} {creator.get('last_name','')}".strip()
            d_views = view_data.get(d["id"], {})
            aa6 = auth_anon_6m.get(d["id"], {"auth": 0, "anon": 0})
            aa3 = auth_anon_3m.get(d["id"], {"auth": 0, "anon": 0})
            row = {
                "id": d["id"],
                "name": d.get("name", ""),
                "folder": folder,
                "creator": creator_name,
                "created_at": d.get("created_at", ""),
                "updated_at": d.get("updated_at", ""),
                "last_viewed_at": d.get("last_viewed_at", ""),
                "view_count": d.get("view_count", 0),
                "total_views_6m": sum(d_views.values()),
                "authenticated_views_6m": aa6["auth"],
                "anonymous_views_6m": aa6["anon"],
                "unique_viewers_6m": viewer_data.get(d["id"], 0),
                "total_views_3m": aa3["auth"],
                "authenticated_views_3m": aa3["auth"],
                "anonymous_views_3m": aa3["anon"],
            }
            for m in months_6:
                row[m] = d_views.get(m, 0)
            writer.writerow(row)

    print(f"✓ Exported {len(dashboards)} dashboards to: {output_file}")


def cmd_export_dashboard_questions(args):
    """Export a flat mapping of every dashboard → question pairing."""
    collections = fetch_collections()
    col_map = build_collection_map(collections)

    print("Fetching all dashboards and their questions...")
    dashboards = fetch_all_dashboards()

    # Also build a card name lookup from the cards endpoint
    cards = fetch_all_cards()
    card_name_map = {c["id"]: c.get("name", "") for c in cards}

    output_file = args.output or "metabase_dashboard_questions.csv"
    fieldnames = ["dashboard_id", "dashboard_name", "dashboard_folder", "question_id", "question_name"]
    row_count = 0

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in dashboards:
            col_id = d.get("collection_id") or "root"
            folder = col_map.get(col_id, str(col_id))
            detail = get(f"/dashboard/{d['id']}")
            for dc in detail.get("dashcards", []):
                card_id = dc.get("card_id")
                if card_id:
                    writer.writerow({
                        "dashboard_id": d["id"],
                        "dashboard_name": d.get("name", ""),
                        "dashboard_folder": folder,
                        "question_id": card_id,
                        "question_name": card_name_map.get(card_id, ""),
                    })
                    row_count += 1

    print(f"✓ Exported {row_count} dashboard-question pairings across {len(dashboards)} dashboards to: {output_file}")


def cmd_export_sql(args):
    """Export SQL for all native queries to a separate CSV."""
    cards = fetch_all_cards()

    output_file = args.output or "metabase_sql.csv"
    native_count = 0
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "sql"])
        writer.writeheader()
        for c in cards:
            if not is_native(c):
                continue
            sql = extract_sql(c)
            if not sql:
                continue
            writer.writerow({
                "id": c["id"],
                "name": c.get("name", ""),
                "sql": sql,
            })
            native_count += 1

    print(f"✓ Exported {native_count} SQL queries to: {output_file}")


def cmd_show_sql(args):
    """Print the SQL for a specific query ID, following source-card chains for GUI questions."""
    card = get(f"/card/{args.id}")
    name = card.get("name", "")
    if is_native(card):
        sql = extract_sql(card)
        print(f"\n── {name} (id={args.id}) ────────────────────────────────")
        print(sql)
        return
    # Follow source-card chain to find the underlying native SQL
    visited = [args.id]
    current = card
    while not is_native(current):
        sc_id = _get_source_card_id(current)
        if sc_id is None:
            current_name = current.get("name", "")
            print(f"Query '{current_name}' (id={visited[-1]}) is a GUI query built on a database table — no SQL to extract.")
            return
        if sc_id in visited:
            print(f"Circular source-card reference detected: {' → '.join(str(x) for x in visited)} → {sc_id}")
            return
        visited.append(sc_id)
        current = get(f"/card/{sc_id}")
    source_name = current.get("name", "")
    if len(visited) > 1:
        chain = " → ".join(str(x) for x in visited)
        print(f"\nNote: {name} (id={args.id}) is a GUI wrapper. Source chain: {chain}")
        print(f"SQL is from: {source_name} (id={visited[-1]})")
    sql = extract_sql(current)
    print(f"\n── {source_name} (id={visited[-1]}) ────────────────────────────────")
    print(sql)


def cmd_show_card(args):
    """Print the raw JSON for a specific card from the Metabase API."""
    card = get(f"/card/{args.id}")
    output = json.dumps(card, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"✓ Saved card {args.id} JSON to: {args.output}")
    else:
        print(output)


def cmd_show_runs(args):
    """Show run history for a specific question from v_query_log."""
    card = get(f"/card/{args.id}")
    name = card.get("name", f"Card {args.id}")

    # Fetch user list once so we can resolve user_id → name
    user_data = get("/user", params={"status": "all"})
    users = user_data if isinstance(user_data, list) else user_data.get("data", [])
    user_map = {u["id"]: f"{u.get('first_name','')} {u.get('last_name','')}".strip() for u in users}

    # Raw row query against v_query_log filtered by card_id
    payload = {
        "database": 13371337,
        "type": "query",
        "query": {
            "source-table": 3908,  # v_query_log
            "filter": ["=", ["field", 58180, {"base-type": "type/Integer"}], args.id],  # card_id
            "order-by": [["desc", ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}]]],  # started_at
        },
    }
    r = requests.post(f"{METABASE_URL}/api/dataset", headers=api_headers(), json=payload, timeout=60)
    data = r.json()
    if data.get("error"):
        print(f"Error querying run log: {data['error']}")
        return

    result = data.get("data", {})
    cols = [c["name"] for c in result.get("cols", [])]
    rows = result.get("rows", [])

    if not rows:
        print(f"\n── {name} (id={args.id}) ────────────────────────────────")
        print("No run history found in the query log.")
        return

    # Map column names to indices
    def col(name_):
        return cols.index(name_) if name_ in cols else None

    idx_started  = col("started_at")
    idx_user     = col("user_id")
    idx_dash     = col("dashboard_id")

    table_rows = []
    for row in rows:
        started  = row[idx_started][:19].replace("T", " ") if idx_started is not None and row[idx_started] else "—"
        user_id  = row[idx_user] if idx_user is not None else None
        dash_id  = row[idx_dash] if idx_dash is not None else None
        user_str = user_map.get(user_id, f"user_id:{user_id}") if user_id else "—"
        source   = f"dashboard (id:{dash_id})" if dash_id else "direct"
        table_rows.append([started, source, user_str])

    print(f"\n── {name} (id={args.id}) ────────────────────────────────")
    _print_table(["Started At", "Triggered By", "User"], table_rows)

    last_run = table_rows[0][0]
    print(f"\nTotal runs in log: {len(rows)}  |  Most recent: {last_run}")
    if len(rows) == 10000:
        print("Note: result capped at 10,000 rows — there may be older runs not shown.")


def cmd_show_dashboard_views(args):
    """Show raw view log entries for a specific dashboard from v_view_log."""
    dash = get(f"/dashboard/{args.id}")
    name = dash.get("name", f"Dashboard {args.id}")

    user_data = get("/user", params={"status": "all"})
    users = user_data if isinstance(user_data, list) else user_data.get("data", [])
    user_map = {u["id"]: f"{u.get('first_name','')} {u.get('last_name','')}".strip() for u in users}

    payload = {
        "database": 13371337,
        "type": "query",
        "query": {
            "source-table": 3918,  # v_view_log
            "filter": ["and",
                ["=", ["field", 58290, {"base-type": "type/Integer"}], args.id],   # entity_id
                ["=", ["field", 58294, {"base-type": "type/Text"}], "dashboard"],  # entity_type
            ],
            "order-by": [["desc", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}]]],  # timestamp
        },
    }
    r = requests.post(f"{METABASE_URL}/api/dataset", headers=api_headers(), json=payload, timeout=60)
    data = r.json()
    if data.get("error"):
        print(f"Error querying view log: {data['error']}")
        return

    result = data.get("data", {})
    cols = [c["name"] for c in result.get("cols", [])]
    rows = result.get("rows", [])

    print(f"\n── {name} (id={args.id}) ────────────────────────────────")
    print(f"Columns returned by v_view_log: {cols}\n")

    if not rows:
        print("No view history found in the view log.")
        return

    def col(name_):
        return cols.index(name_) if name_ in cols else None

    idx_ts      = col("timestamp") or col("viewed_on") or 0
    idx_user    = col("user_id")
    idx_type    = col("entity_type")
    idx_entity  = col("entity_id")

    table_rows = []
    for row in rows:
        ts        = row[idx_ts][:19].replace("T", " ") if row[idx_ts] else "—"
        user_id   = row[idx_user] if idx_user is not None else None
        user_str  = user_map.get(user_id, f"user_id:{user_id}") if user_id else "—"
        etype     = row[idx_type] if idx_type is not None else "—"
        eid       = row[idx_entity] if idx_entity is not None else "—"
        table_rows.append([ts, eid, etype, user_str])

    _print_table(["Timestamp", "entity_id", "entity_type", "User"], table_rows)
    print(f"\nTotal view rows: {len(rows)}")
    if len(rows) == 10000:
        print("Note: result capped at 10,000 rows.")


# ── Helpers ───────────────────────────────────────────────────────────────────

# SQL keywords to exclude from table reference extraction
_SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
    "CROSS", "ON", "AND", "OR", "NOT", "IN", "AS", "WITH", "UNION", "ALL",
    "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "DISTINCT", "CASE",
    "WHEN", "THEN", "ELSE", "END", "NULL", "IS", "BETWEEN", "LIKE", "EXISTS",
    "INSERT", "INTO", "UPDATE", "SET", "DELETE", "VALUES", "LATERAL", "OVER",
    "PARTITION", "ROWS", "RANGE", "PRECEDING", "FOLLOWING", "UNBOUNDED",
    "CURRENT", "ROW", "FILTER", "QUALIFY", "SAMPLE", "TABLESAMPLE",
}

def _split_select_clause(text):
    """Return a list of SELECT item strings from the text following a SELECT keyword.
    Stops at the first top-level FROM/WHERE/GROUP/ORDER/HAVING/LIMIT/UNION/INTERSECT/EXCEPT.
    Respects paren depth and CASE...END nesting so commas inside them are not treated as item separators.
    """
    TOKEN = re.compile(
        r"'[^']*'|\"[^\"]*\"|"   # string literals (skip their contents)
        r'\b(CASE|END|FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|INTERSECT|EXCEPT)\b|'
        r'[(),]|'
        r'[^\'\"(),\s]+|'         # unquoted words / operators
        r'\s+',                   # whitespace
        re.IGNORECASE | re.DOTALL,
    )
    _TERMINATORS = {'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'}

    items = []
    buf = []
    paren_depth = 0
    case_depth = 0

    for m in TOKEN.finditer(text):
        tok = m.group()
        kw = (m.group(1) or '').upper()

        if tok == '(':
            paren_depth += 1
            buf.append(tok)
        elif tok == ')':
            if paren_depth > 0:
                paren_depth -= 1
                buf.append(tok)
            else:
                # Closing paren that belongs to an outer context — we're done
                if buf:
                    items.append(''.join(buf).strip())
                return items
        elif kw == 'CASE':
            case_depth += 1
            buf.append(tok)
        elif kw == 'END':
            case_depth -= 1
            buf.append(tok)
        elif kw in _TERMINATORS and paren_depth == 0 and case_depth == 0:
            if buf:
                items.append(''.join(buf).strip())
            return items
        elif tok == ',' and paren_depth == 0 and case_depth == 0:
            if buf:
                items.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(tok)

    if buf:
        items.append(''.join(buf).strip())
    return items


def _has_alias_self_reference(sql):
    """Return True if the SQL contains a SELECT clause where one expression references
    an alias defined by another expression in the same SELECT list.

    Snowflake supports:  SELECT price * 2 AS double_price, double_price - cost AS margin
    BigQuery does not — double_price is not a real column at SELECT time.

    Only checks aliases of 3+ characters to avoid false positives from short tokens.
    """
    if not sql:
        return False

    sql_clean = re.sub(r'--[^\n]*', ' ', sql)
    sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)

    for sel_m in re.finditer(r'\bSELECT\b', sql_clean, re.IGNORECASE):
        items = _split_select_clause(sql_clean[sel_m.end():])
        if len(items) < 2:
            continue

        # Parse each item into (alias_upper | None, body_text)
        parsed = []
        for item in items:
            item = item.strip()
            m = re.search(r'\bAS\s+(\w+)\s*$', item, re.IGNORECASE)
            if m:
                alias = m.group(1).upper()
                body = item[:m.start()]
            else:
                alias = None
                body = item
            parsed.append((alias, body))

        # All explicit aliases in this SELECT (3+ chars to limit noise)
        aliases = {alias for alias, _ in parsed if alias and len(alias) >= 3}
        if not aliases:
            continue

        # For each item, check if any OTHER alias from this SELECT appears in its body
        for own_alias, body in parsed:
            # Strip string literals from body so we don't match inside quoted values
            body_clean = re.sub(r"'[^']*'", ' ', body)
            body_clean = re.sub(r'"[^"]*"', ' ', body_clean)

            for alias in aliases:
                if alias == own_alias:
                    continue
                # Word-boundary match; exclude "table.alias" or "alias_longer"
                if re.search(r'(?<![.\w])' + re.escape(alias) + r'(?![.\w])',
                             body_clean, re.IGNORECASE):
                    return True

    return False


def _has_template_tags(sql):
    """Return True if the SQL uses Metabase template tags: {{var}}, [[optional]], {{{raw}}}."""
    if not sql:
        return False
    return bool(re.search(r'\{\{|\[\[', sql))


def _extract_table_refs(sql):
    """Return a list of table references (with duplicates) found after FROM/JOIN, excluding CTE names.
    Handles quoted identifiers ("SCHEMA"."TABLE"), unquoted (schema.table), and mixed forms."""
    if not sql:
        return []
    # Strip comments
    sql_clean = re.sub(r'--[^\n]*', ' ', sql)
    sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)
    # Collect CTE names to exclude (unquoted only — quoted CTEs are rare)
    cte_names = {n.upper() for n in re.findall(r'\b(\w+)\s+as\s*\(', sql_clean, flags=re.IGNORECASE)}
    # Each identifier part is either "quoted" or an unquoted word
    _part = r'(?:"[^"]+"|[\w]+)'
    refs = re.findall(
        r'(?:FROM|JOIN)\s+(' + _part + r'(?:\.' + _part + r')*)',
        sql_clean, flags=re.IGNORECASE,
    )
    result = []
    for ref in refs:
        # Normalise: strip quotes from each part, join with dots, lowercase
        parts = [a or b for a, b in re.findall(r'"([^"]+)"|([\w]+)', ref)]
        if not parts:
            continue
        top = parts[-1].upper()
        if top not in _SQL_KEYWORDS and top not in cte_names:
            result.append('.'.join(parts).lower())
    return result

def _gui_subtype(card, all_cards_map):
    """Classify a GUI question by what it is built on.

    via_native_question  — source-card points to a native SQL question (SQL lives there)
    via_gui_question     — source-card points to another GUI/MBQL question
    via_missing_question — source-card ID not found in active cards (archived/deleted)
    via_table            — directly references a database table (source-table integer)
    unknown              — MBQL query with no recognisable source
    """
    sc_id = _get_source_card_id(card)
    if sc_id is not None:
        if sc_id not in all_cards_map:
            return "via_missing_question"
        return "via_native_question" if is_native(all_cards_map[sc_id]) else "via_gui_question"
    dq = card.get("dataset_query", {})
    stages = dq.get("stages", []) or [dq.get("query", {})]
    for stage in stages:
        if isinstance(stage.get("source-table"), int):
            return "via_table"
    return "unknown"


def _get_source_card_id(card):
    """Return the primary source card ID for a GUI query (int), or None if built on a table."""
    dq = card.get("dataset_query", {})
    stages = dq.get("stages", [])
    if not stages:
        query = dq.get("query", {})
        stages = [query] if query else []
    for stage in stages:
        src_card = stage.get("source-card")
        if isinstance(src_card, int):
            return src_card
        src = stage.get("source-table")
        if isinstance(src, str) and src.startswith("card__"):
            try:
                return int(src.split("__")[1])
            except (IndexError, ValueError):
                pass
    return None


def _extract_gui_tables(card, table_map):
    """Return list of table/question references for a GUI/MBQL query.
    Direct table references return 'schema.table'; source-card references return 'question:ID'."""
    dq = card.get("dataset_query", {})
    stages = dq.get("stages", [])
    if not stages:
        query = dq.get("query", {})
        stages = [query] if query else []

    refs = []
    for stage in stages:
        src = stage.get("source-table")
        if isinstance(src, int):
            refs.append(table_map.get(src, f"table_id:{src}"))
        elif isinstance(src, str) and src.startswith("card__"):
            # Old MBQL v1: source-table = "card__XXXX"
            refs.append(f"question:{src.split('__')[1]}")

        src_card = stage.get("source-card")
        if isinstance(src_card, int):
            refs.append(f"question:{src_card}")

        for join in stage.get("joins", []):
            jt = join.get("source-table")
            if isinstance(jt, int):
                refs.append(table_map.get(jt, f"table_id:{jt}"))
            elif isinstance(jt, str) and jt.startswith("card__"):
                refs.append(f"question:{jt.split('__')[1]}")
            jt_card = join.get("source-card")
            if isinstance(jt_card, int):
                refs.append(f"question:{jt_card}")

    return refs

def _connection_db_code(connection_name):
    """Extract a short DB code (uk/de/es) from a Metabase connection name, or return the lowercased name."""
    if not connection_name:
        return None
    tokens = re.split(r'[\s_\-]+', connection_name.strip().lower())
    for code in ('uk', 'de', 'es'):
        if code in tokens:
            return code
    return connection_name.lower().strip()

def _determine_source_databases(table_refs, connection_name):
    """Return (source_databases_str, uses_uk, uses_de, uses_es).

    3-part refs (db.schema.table): first part is the explicit database.
    2-part/1-part refs: database is implicitly the Metabase connection.
    question:/table_id: refs are skipped (database unknown).
    """
    databases = set()
    has_implicit = False

    for ref in table_refs:
        if ref.startswith('question:') or ref.startswith('table_id:'):
            continue
        parts = ref.split('.')
        if len(parts) >= 3:
            databases.add(parts[0].lower())
        else:
            has_implicit = True

    if has_implicit:
        code = _connection_db_code(connection_name)
        if code:
            databases.add(code)

    source_databases = ', '.join(sorted(databases))
    return source_databases, 'uk' in databases, 'de' in databases, 'es' in databases

def _scan_snowflake_keywords(sql):
    """Return a list of (keyword, count) pairs for each Snowflake keyword found in SQL, sorted by count desc.
    Strips comments and string literals before scanning so commented-out keywords are not flagged."""
    if not sql:
        return []
    # Strip single-line and block comments
    sql_clean = re.sub(r'--[^\n]*', ' ', sql)
    sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)
    # Strip string literals
    sql_clean = re.sub(r"'[^']*'", ' ', sql_clean)
    sql_upper = sql_clean.upper()
    found = []
    for kw in BQ_INCOMPATIBLE_KEYWORDS:
        matches = re.findall(r'\b' + re.escape(kw.upper()) + r'\b', sql_upper)
        if matches:
            found.append((kw, len(matches)))
    return sorted(found, key=lambda x: -x[1])

def _print_table(headers, rows):
    if HAS_TABULATE:
        print(tabulate(rows, headers=headers, tablefmt="simple"))
    else:
        # Fallback plain text
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print(fmt.format(*headers))
        print("  ".join("-" * w for w in col_widths))
        for row in rows:
            print(fmt.format(*[str(c) for c in row]))


def _last_n_months(n=6):
    """Return list of YYYY-MM strings for the last N months, oldest first."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    result = []
    year, month = now.year, now.month
    for _ in range(n):
        result.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(result))

def _month_range(month_str):
    """Return (start_date_str, end_date_str) for a YYYY-MM string."""
    year, month = int(month_str[:4]), int(month_str[5:7])
    next_month, next_year = month + 1, year
    if next_month > 12:
        next_month, next_year = 1, year + 1
    return f"{year:04d}-{month:02d}-01", f"{next_year:04d}-{next_month:02d}-01"

def _mbql(query):
    """Run an MBQL query against the internal analytics database."""
    payload = {"database": 13371337, "type": "query", "query": query}
    r = requests.post(f"{METABASE_URL}/api/dataset", headers=api_headers(), json=payload, timeout=60)
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"MBQL error: {data['error']}")
    return data.get("data", {}).get("rows", [])

def _fetch_monthly_dashboard_views(months, real_user_ids=None):
    """Return {dashboard_id: {YYYY-MM: count}} from v_view_log for given months.
    If real_user_ids is provided, only counts views from authenticated users."""
    start_date = months[0] + "-01"
    base_filter = ["and",
        [">=", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
        ["=",  ["field", 58294, {"base-type": "type/Text"}], "dashboard"],
    ]
    if real_user_ids:
        base_filter.append(["in", ["field", 58291, {"base-type": "type/Integer"}], *sorted(real_user_ids)])
    rows = _mbql({
        "source-table": 3918,  # v_view_log
        "aggregation": [["count"]],
        "breakout": [
            ["field", 58290, {"base-type": "type/Integer"}],   # entity_id
            ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ", "temporal-unit": "month"}],
        ],
        "filter": base_filter,
    })
    result = defaultdict(dict)
    for entity_id, month_ts, count in rows:
        month_key = month_ts[:7]
        if month_key in months:
            result[entity_id][month_key] = count
    return result

def _fetch_monthly_question_runs(months):
    """Return {card_id: {YYYY-MM: count}} from v_query_log, queried one month at a time."""
    result = defaultdict(dict)
    for month_str in months:
        start_date, end_date = _month_range(month_str)
        rows = _mbql({
            "source-table": 3908,  # v_query_log
            "aggregation": [["count"]],
            "breakout": [["field", 58180, {"base-type": "type/Integer"}]],  # card_id
            "filter": ["and",
                [">=", ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
                ["<",  ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}], end_date],
                ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
            ],
        })
        for card_id, count in rows:
            if card_id is not None:
                result[card_id][month_str] = count
        print(f"  {month_str}: {len(rows)} questions ran")
    return result

def _fetch_dashboard_unique_viewers(months):
    """Return {dashboard_id: unique_viewer_count} from v_view_log over the full months window."""
    start_date = months[0] + "-01"
    rows = _mbql({
        "source-table": 3918,  # v_view_log
        "aggregation": [["distinct", ["field", 58291, {"base-type": "type/Integer"}]]],  # user_id
        "breakout": [["field", 58290, {"base-type": "type/Integer"}]],  # entity_id
        "filter": ["and",
            [">=", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
            ["=",  ["field", 58294, {"base-type": "type/Text"}], "dashboard"],
        ],
    })
    return {entity_id: count for entity_id, count in rows}

def _fetch_real_user_ids():
    """Return a set of user IDs that have an email address (i.e. real authenticated accounts)."""
    user_data = get("/user", params={"status": "all"})
    users = user_data if isinstance(user_data, list) else user_data.get("data", [])
    return {u["id"] for u in users if u.get("email")}

def _fetch_dashboard_auth_anon_views(months, real_user_ids):
    """Return {dashboard_id: {'auth': count, 'anon': count}} for the given months window."""
    start_date = months[0] + "-01"
    base_filter = ["and",
        [">=", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
        ["=",  ["field", 58294, {"base-type": "type/Text"}], "dashboard"],
    ]
    result = defaultdict(lambda: {"auth": 0, "anon": 0})

    # Authenticated: user_id is in the set of real users
    if real_user_ids:
        auth_rows = _mbql({
            "source-table": 3918,
            "aggregation": [["count"]],
            "breakout": [["field", 58290, {"base-type": "type/Integer"}]],
            "filter": ["and", *base_filter[1:],
                ["in", ["field", 58291, {"base-type": "type/Integer"}], *sorted(real_user_ids)],
            ],
        })
        for entity_id, count in auth_rows:
            if entity_id is not None:
                result[entity_id]["auth"] = count

    # Total views — compute anon as total - auth
    total_rows = _mbql({
        "source-table": 3918,
        "aggregation": [["count"]],
        "breakout": [["field", 58290, {"base-type": "type/Integer"}]],
        "filter": base_filter,
    })
    for entity_id, count in total_rows:
        if entity_id is not None:
            result[entity_id]["anon"] = max(0, count - result[entity_id]["auth"])

    return result

def _fetch_question_run_sources(months):
    """Return {card_id: {'dashboard': count, 'direct': count}} for the full months window."""
    start_date = months[0] + "-01"
    base_filter = ["and",
        [">=", ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
        ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
    ]
    result = defaultdict(lambda: {"dashboard": 0, "direct": 0})
    # Dashboard-triggered runs (dashboard_id is not null)
    for card_id, count in _mbql({
        "source-table": 3908,
        "aggregation": [["count"]],
        "breakout": [["field", 58180, {"base-type": "type/Integer"}]],
        "filter": base_filter + [["not-null", ["field", 58195, {"base-type": "type/Integer"}]]],
    }):
        if card_id is not None:
            result[card_id]["dashboard"] = count
    # Direct runs (dashboard_id is null)
    for card_id, count in _mbql({
        "source-table": 3908,
        "aggregation": [["count"]],
        "breakout": [["field", 58180, {"base-type": "type/Integer"}]],
        "filter": base_filter + [["is-null", ["field", 58195, {"base-type": "type/Integer"}]]],
    }):
        if card_id is not None:
            result[card_id]["direct"] = count
    return result

def _fetch_question_unique_users(months):
    """Return {card_id: unique_user_count} over the full months window."""
    start_date = months[0] + "-01"
    rows = _mbql({
        "source-table": 3908,
        "aggregation": [["distinct", ["field", 58191, {"base-type": "type/Integer"}]]],  # user_id
        "breakout": [["field", 58180, {"base-type": "type/Integer"}]],  # card_id
        "filter": ["and",
            [">=", ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
            ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
        ],
    })
    return {card_id: count for card_id, count in rows if card_id is not None}

def _fetch_question_unique_users_total():
    """Return {card_id: unique_user_count} all time from v_query_log."""
    rows = _mbql({
        "source-table": 3908,
        "aggregation": [["distinct", ["field", 58191, {"base-type": "type/Integer"}]]],  # user_id
        "breakout": [["field", 58180, {"base-type": "type/Integer"}]],  # card_id
        "filter": ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
    })
    return {card_id: count for card_id, count in rows if card_id is not None}

def _fetch_question_total_runs():
    """Return {card_id: total_run_count} all time from v_query_log."""
    rows = _mbql({
        "source-table": 3908,
        "aggregation": [["count"]],
        "breakout": [["field", 58180, {"base-type": "type/Integer"}]],  # card_id
        "filter": ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
    })
    return {card_id: count for card_id, count in rows if card_id is not None}

def _fetch_last_run_status():
    """Return {card_id: {'errored': bool, 'result_rows': int|None}} for the most recent run of each card.

    Fetches rows from v_query_log ordered newest-first and takes the first occurrence
    of each card_id (i.e. its most recent run).
    """
    payload = {
        "database": 13371337,
        "type": "query",
        "query": {
            "source-table": 3908,
            "fields": [
                ["field", 58180, {"base-type": "type/Integer"}],             # card_id
                ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}], # started_at
                ["field", 58192, {"base-type": "type/Text"}],                # error
                ["field", 58187, {"base-type": "type/Integer"}],             # result_rows
            ],
            "filter": ["not-null", ["field", 58180, {"base-type": "type/Integer"}]],
            "order-by": [["desc", ["field", 58185, {"base-type": "type/DateTimeWithLocalTZ"}]]],
            "limit": 100000,
        },
    }
    r = requests.post(f"{METABASE_URL}/api/dataset", headers=api_headers(), json=payload, timeout=120)
    data = r.json()
    if data.get("error"):
        print(f"Warning: could not fetch last run status: {data['error']}")
        return {}
    result = data.get("data", {})
    cols = [c["name"] for c in result.get("cols", [])]
    rows = result.get("rows", [])
    try:
        idx_card  = cols.index("card_id")
        idx_error = cols.index("error")
        idx_rrows = cols.index("result_rows")
    except ValueError:
        return {}
    seen = {}
    for row in rows:
        card_id = row[idx_card]
        if card_id is not None and card_id not in seen:
            seen[card_id] = {
                "errored": row[idx_error] is not None,
                "result_rows": row[idx_rrows],
            }
    return seen


def _fetch_question_views_6m(months):
    """Return {card_id: view_count} for page opens in the last 6 months from v_view_log."""
    start_date = months[0] + "-01"
    rows = _mbql({
        "source-table": 3918,  # v_view_log
        "aggregation": [["count"]],
        "breakout": [["field", 58290, {"base-type": "type/Integer"}]],  # entity_id
        "filter": ["and",
            [">=", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}], start_date],
            ["=",  ["field", 58294, {"base-type": "type/Text"}], "card"],
        ],
    })
    return {entity_id: count for entity_id, count in rows if entity_id is not None}

def _fetch_question_last_viewed():
    """Return {card_id: ISO timestamp string} for the most recent view of each question from v_view_log."""
    rows = _mbql({
        "source-table": 3918,  # v_view_log
        "aggregation": [["max", ["field", 58289, {"base-type": "type/DateTimeWithLocalTZ"}]]],  # timestamp
        "breakout": [["field", 58290, {"base-type": "type/Integer"}]],  # entity_id
        "filter": ["=", ["field", 58294, {"base-type": "type/Text"}], "card"],
    })
    return {entity_id: ts for entity_id, ts in rows if entity_id is not None}

def _write_hits_csv(hits, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","name","folder","creator","keywords","url"])
        w.writeheader()
        w.writerows(hits)


# ── SQL conversion (Snowflake → BigQuery) ────────────────────────────────────

_CONVERSION_PROMPT = """\
You are a SQL migration expert. Convert the following Snowflake SQL to BigQuery SQL.

RULES:
- Keep all table names, schema names, database names, column names, and aliases EXACTLY as written
- Convert ONLY Snowflake-specific syntax; leave everything else unchanged
- Return ONLY the converted SQL — no explanation, no markdown fences, no comments

CONVERSIONS TO APPLY:
- col ILIKE 'pattern'  →  LOWER(col) LIKE LOWER('pattern')
- NVL(a, b)  →  COALESCE(a, b)
- NVL2(a, b, c)  →  IF(a IS NOT NULL, b, c)
- ZEROIFNULL(x)  →  COALESCE(x, 0)
- NULLIFZERO(x)  →  NULLIF(x, 0)
- IFF(cond, a, b)  →  IF(cond, a, b)
- DIV0(a, b)  →  SAFE_DIVIDE(a, b)
- DIV0NULL(a, b)  →  IF(b = 0, NULL, a / b)
- DECODE(col, v1, r1, v2, r2, default)  →  CASE WHEN col=v1 THEN r1 WHEN col=v2 THEN r2 ELSE default END
- TRY_CAST(x AS type) / TRYCAST  →  SAFE_CAST(x AS type)
- TO_VARCHAR(x) / TO_CHAR(x)  →  CAST(x AS STRING)  (use FORMAT_DATE/FORMAT_TIMESTAMP if a format string is present)
- STARTSWITH(str, prefix)  →  STARTS_WITH(str, prefix)
- ENDSWITH(str, suffix)  →  ENDS_WITH(str, suffix)
- DATEADD(part, n, date)  →  DATE_ADD(date, INTERVAL n part)  — use DATETIME_ADD or TIMESTAMP_ADD if the value is a datetime/timestamp
- DATEDIFF(part, a, b)  →  DATE_DIFF(b, a, part)  — SWAP argument order: Snowflake returns b−a; BigQuery DATE_DIFF(x,y,part) returns x−y, so swap to preserve sign. Use DATETIME_DIFF or TIMESTAMP_DIFF accordingly (same swap applies)
- TIMESTAMPADD(part, n, ts)  →  TIMESTAMP_ADD(ts, INTERVAL n part)
- TIMESTAMPDIFF(part, a, b)  →  TIMESTAMP_DIFF(b, a, part)  — SWAP argument order (same reason as DATEDIFF above)
- REGEXP_SUBSTR(str, pattern)  →  REGEXP_EXTRACT(str, pattern)
- REGEXP_SUBSTR(str, pattern, pos, occ, flags, group)  →  REGEXP_EXTRACT with appropriate adjustments
- STRTOK(str, delim, n)  →  SPLIT(str, delim)[ORDINAL(n)]
- STRTOK_TO_ARRAY(str, delim)  →  SPLIT(str, delim)
- SPLIT_TO_TABLE(str, delim)  →  CROSS JOIN UNNEST(SPLIT(str, delim)) AS item
- LATERAL FLATTEN(input => arr)  →  CROSS JOIN UNNEST(arr) AS item  (restructure query as needed)
- ARRAY_CONSTRUCT(a, b, c)  →  [a, b, c]
- ARRAY_SIZE(arr)  →  ARRAY_LENGTH(arr)
- DATE_FROM_PARTS(y, m, d)  →  DATE(y, m, d)
- TIME_FROM_PARTS(h, m, s)  →  TIME(h, m, s)
- TIMESTAMP_FROM_PARTS(y, m, d, h, min, s)  →  DATETIME(y, m, d, h, min, s)
- LISTAGG(col, delim) WITHIN GROUP (ORDER BY ...)  →  STRING_AGG(col, delim ORDER BY ...)
- BOOLAND_AGG(x)  →  LOGICAL_AND(x)
- BOOLOR_AGG(x)  →  LOGICAL_OR(x)
- value::TYPE  →  CAST(value AS TYPE)  (e.g. x::DATE → CAST(x AS DATE))
- SELECT-clause alias self-reference (alias defined in one SELECT expression, referenced in another in the same SELECT)
  → wrap the inner SELECT in a subquery so the alias resolves
- Double-quoted identifiers e.g. "SCHEMA"."TABLE" or "DB"."SCHEMA"."TABLE"
  → strip the double quotes: SCHEMA.TABLE or DB.SCHEMA.TABLE
  (Snowflake uses double quotes for identifier quoting; BigQuery uses backticks — but since these are plain alphanumeric names, just remove the quotes entirely)

DO NOT CHANGE (BigQuery supports these natively):
- QUALIFY
- IFNULL
- PARSE_JSON
- All table, schema, database, and column names

SQL TO CONVERT:
{sql}"""


_CONVERSION_PROMPT_FULL = """\
You are a SQL migration expert. Convert the following Snowflake SQL to BigQuery SQL.

RULES:
- Keep all table names, schema names, database names, column names, and aliases EXACTLY as written
- Convert ONLY Snowflake-specific syntax; leave everything else unchanged
- Return your response in EXACTLY this format — no other text outside the tags:

<SQL>
{{converted SQL here}}
</SQL>
<CHANGES>
{{one bullet per syntax change applied, format: "- description"; write "- None" if no changes were needed}}
</CHANGES>
<NOTES>
{{one bullet per runtime risk the reviewer should verify; write "- None" if there are no risks}}
Flag these specific risks if you applied them:
- DIV0 or DIV0NULL replaced with SAFE_DIVIDE: returns NULL on division by zero instead of 0
- LATERAL FLATTEN rewritten as UNNEST: verify output row count matches original
- Alias self-references resolved via subquery: verify query semantics are preserved
- TRY_CAST on date/timestamp columns: type mismatch may surface at runtime when real tables exist
</NOTES>

CONVERSIONS TO APPLY:
- col ILIKE 'pattern'  →  LOWER(col) LIKE LOWER('pattern')
- NVL(a, b)  →  COALESCE(a, b)
- NVL2(a, b, c)  →  IF(a IS NOT NULL, b, c)
- ZEROIFNULL(x)  →  COALESCE(x, 0)
- NULLIFZERO(x)  →  NULLIF(x, 0)
- IFF(cond, a, b)  →  IF(cond, a, b)
- DIV0(a, b)  →  SAFE_DIVIDE(a, b)
- DIV0NULL(a, b)  →  IF(b = 0, NULL, a / b)
- DECODE(col, v1, r1, v2, r2, default)  →  CASE WHEN col=v1 THEN r1 WHEN col=v2 THEN r2 ELSE default END
- TRY_CAST(x AS type) / TRYCAST  →  SAFE_CAST(x AS type)
- TO_VARCHAR(x) / TO_CHAR(x)  →  CAST(x AS STRING)  (use FORMAT_DATE/FORMAT_TIMESTAMP if a format string is present)
- STARTSWITH(str, prefix)  →  STARTS_WITH(str, prefix)
- ENDSWITH(str, suffix)  →  ENDS_WITH(str, suffix)
- DATEADD(part, n, date)  →  DATE_ADD(date, INTERVAL n part)  — use DATETIME_ADD or TIMESTAMP_ADD if the value is a datetime/timestamp
- DATEDIFF(part, a, b)  →  DATE_DIFF(b, a, part)  — SWAP argument order: Snowflake returns b−a; BigQuery DATE_DIFF(x,y,part) returns x−y, so swap to preserve sign. Use DATETIME_DIFF or TIMESTAMP_DIFF accordingly (same swap applies)
- TIMESTAMPADD(part, n, ts)  →  TIMESTAMP_ADD(ts, INTERVAL n part)
- TIMESTAMPDIFF(part, a, b)  →  TIMESTAMP_DIFF(b, a, part)  — SWAP argument order (same reason as DATEDIFF above)
- REGEXP_SUBSTR(str, pattern)  →  REGEXP_EXTRACT(str, pattern)
- REGEXP_SUBSTR(str, pattern, pos, occ, flags, group)  →  REGEXP_EXTRACT with appropriate adjustments
- STRTOK(str, delim, n)  →  SPLIT(str, delim)[ORDINAL(n)]
- STRTOK_TO_ARRAY(str, delim)  →  SPLIT(str, delim)
- SPLIT_TO_TABLE(str, delim)  →  CROSS JOIN UNNEST(SPLIT(str, delim)) AS item
- LATERAL FLATTEN(input => arr)  →  CROSS JOIN UNNEST(arr) AS item  (restructure query as needed)
- ARRAY_CONSTRUCT(a, b, c)  →  [a, b, c]
- ARRAY_SIZE(arr)  →  ARRAY_LENGTH(arr)
- DATE_FROM_PARTS(y, m, d)  →  DATE(y, m, d)
- TIME_FROM_PARTS(h, m, s)  →  TIME(h, m, s)
- TIMESTAMP_FROM_PARTS(y, m, d, h, min, s)  →  DATETIME(y, m, d, h, min, s)
- LISTAGG(col, delim) WITHIN GROUP (ORDER BY ...)  →  STRING_AGG(col, delim ORDER BY ...)
- BOOLAND_AGG(x)  →  LOGICAL_AND(x)
- BOOLOR_AGG(x)  →  LOGICAL_OR(x)
- value::TYPE  →  CAST(value AS TYPE)  (e.g. x::DATE → CAST(x AS DATE))
- SELECT-clause alias self-reference (alias defined in one SELECT expression, referenced in another in the same SELECT)
  → wrap the inner SELECT in a subquery so the alias resolves
- Double-quoted identifiers e.g. "SCHEMA"."TABLE" or "DB"."SCHEMA"."TABLE"
  → strip the double quotes: SCHEMA.TABLE or DB.SCHEMA.TABLE
  (Snowflake uses double quotes for identifier quoting; BigQuery uses backticks — but since these are plain alphanumeric names, just remove the quotes entirely)

DO NOT CHANGE (BigQuery supports these natively):
- QUALIFY
- IFNULL
- PARSE_JSON
- All table, schema, database, and column names

SQL TO CONVERT:
{sql}"""


def _call_claude(prompt):
    """Call Claude and return the text response.

    Priority:
      1. anthropic SDK (if installed) + ANTHROPIC_API_KEY env var
      2. Raw HTTP               + ANTHROPIC_API_KEY env var
      3. `claude -p` CLI        (uses your Claude Code subscription — no API key needed)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        import tempfile
        try:
            # Pass prompt as positional arg; run from /tmp to skip project CLAUDE.md/Wire context
            result = subprocess.run(
                ["claude", "-p", prompt],
                capture_output=True, text=True, timeout=120,
                cwd=tempfile.gettempdir(),
            )
        except FileNotFoundError:
            raise RuntimeError(
                "Neither ANTHROPIC_API_KEY nor the `claude` CLI was found. "
                "Set ANTHROPIC_API_KEY or install Claude Code."
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                "claude CLI timed out after 120 s. "
                "Set ANTHROPIC_API_KEY (console.anthropic.com) for a faster direct API path."
            )
        if result.returncode != 0:
            raise RuntimeError(f"claude CLI error: {result.stderr.strip()}")
        _TOKEN_USAGE["calls"] += 1
        return result.stdout.strip()
    if HAS_ANTHROPIC:
        client = _anthropic_lib.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        _TOKEN_USAGE["input"]  += msg.usage.input_tokens
        _TOKEN_USAGE["output"] += msg.usage.output_tokens
        _TOKEN_USAGE["calls"]  += 1
        return msg.content[0].text
    # Fallback: raw HTTP
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    usage = data.get("usage", {})
    _TOKEN_USAGE["input"]  += usage.get("input_tokens", 0)
    _TOKEN_USAGE["output"] += usage.get("output_tokens", 0)
    _TOKEN_USAGE["calls"]  += 1
    return data["content"][0]["text"]


def _convert_sql_to_bigquery(sql):
    """Return BigQuery SQL converted from Snowflake SQL using Claude."""
    return _convert_sql_to_bigquery_full(sql)["sql"]


def _convert_sql_to_bigquery_full(sql):
    """Convert Snowflake SQL to BigQuery using Claude; return {sql, changes, notes}."""
    response = _call_claude(_CONVERSION_PROMPT_FULL.format(sql=sql)).strip()

    sql_match = re.search(r'<SQL>\s*(.*?)\s*</SQL>', response, re.DOTALL)
    changes_match = re.search(r'<CHANGES>\s*(.*?)\s*</CHANGES>', response, re.DOTALL)
    notes_match = re.search(r'<NOTES>\s*(.*?)\s*</NOTES>', response, re.DOTALL)

    converted_sql = sql_match.group(1).strip() if sql_match else response

    def _parse_bullets(text):
        result = []
        for line in (text or "").strip().splitlines():
            line = line.strip().lstrip("-").strip()
            if line and line.lower() != "none":
                result.append(line)
        return result

    return {
        "sql": converted_sql,
        "changes": _parse_bullets(changes_match.group(1) if changes_match else ""),
        "notes": _parse_bullets(notes_match.group(1) if notes_match else ""),
    }


def _make_results_text(card_id, name, bq_result, changes, notes):
    lines = [
        f"Question ID: {card_id}",
        f"Question Name: {name}",
        "",
        f"Validation status: {bq_result['status']}",
    ]
    if bq_result.get("error"):
        lines += [f"BigQuery response: {bq_result['error']}", ""]
    else:
        lines.append("")
    lines.append("Changes made:")
    for c in (changes or ["None"]):
        lines.append(f"  - {c}")
    if notes:
        lines += ["", "Notes:"]
        for n in notes:
            lines.append(f"  - {n}")
    lines.append("")
    return "\n".join(lines)


def _determine_category(bq_result, notes):
    if bq_result["status"] in ("syntax_error", "column_error", "other_error", "api_error"):
        return "syntax_errors"
    return "syntax_clean_verify_data" if notes else "syntax_clean"


def _load_migration_status(base_dir="converted"):
    """Return {str(card_id): row_dict} from migration_status.csv, or {} if not found."""
    csv_path = os.path.join(base_dir, "migration_status.csv")
    if not os.path.exists(csv_path):
        return {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        return {row["id"]: row for row in csv.DictReader(f)}

def _save_migration_status(rows_by_id, base_dir="converted"):
    """Write migration_status.csv from {str(card_id): row_dict}."""
    os.makedirs(base_dir, exist_ok=True)
    csv_path = os.path.join(base_dir, "migration_status.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MIGRATION_STATUS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_by_id.values())

def _write_card_files(card_id, orig_sql, adj_sql, results_text, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for fname, content in [
        (f"card{card_id}_orig_sql.txt", orig_sql),
        (f"card{card_id}_adj_sql.txt", adj_sql),
        (f"card{card_id}_results.txt", results_text),
    ]:
        with open(os.path.join(output_dir, fname), "w", encoding="utf-8") as f:
            f.write(content)


def _convert_and_write_card(c, dirs, schema_map_path=None):
    """Convert a card's SQL to BigQuery, write three output files, return a status_row dict."""
    card_id = c["id"]
    name = c.get("name", f"Card {card_id}")
    orig_sql = extract_sql(c)
    sql_clean = re.sub(r'\[\[.*?\]\]', '', orig_sql, flags=re.DOTALL)
    sql_clean = re.sub(r'\{\{[^}]+\}\}', 'NULL', sql_clean)

    try:
        conv = _convert_sql_to_bigquery_full(sql_clean)
    except RuntimeError as e:
        bq_result = {"status": "api_error", "error": str(e)}
        results_text = _make_results_text(card_id, name, bq_result, [], [])
        _write_card_files(card_id, orig_sql, sql_clean, results_text, dirs["syntax_errors"])
        _print_conversion_result(bq_result, prefix="    ")
        return {
            "id": card_id, "name": name,
            "original_category": "syntax_errors", "current_status": "pending",
            "failure_reason": f"Claude API error: {e}", "notes": "",
            "original_collection_id": "", "action_source": "",
        }

    conv["sql"] = _strip_double_quoted_identifiers(conv["sql"])
    schema_map = _load_schema_map(schema_map_path)
    validation_sql, unmapped = _apply_schema_map(conv["sql"], schema_map)
    if unmapped:
        print(f"    ⚠  {len(unmapped)} schema prefix(es) not in schema map (TBD or missing): {unmapped}")
    bq_result = _validate_on_bq_playground(validation_sql)
    category = _determine_category(bq_result, conv["notes"])
    results_text = _make_results_text(card_id, name, bq_result, conv["changes"], conv["notes"])
    _write_card_files(card_id, orig_sql, conv["sql"], results_text, dirs[category])
    _print_conversion_result(bq_result, prefix="    ")
    return {
        "id": card_id, "name": name,
        "original_category": category, "current_status": "pending",
        "failure_reason": bq_result.get("error", "") if category == "syntax_errors" else "",
        "notes": "; ".join(conv["notes"]),
        "original_collection_id": "", "action_source": "",
    }


def _strip_double_quoted_identifiers(sql):
    """Strip Snowflake-style double-quote identifier quoting from dotted references.

    Converts "SCHEMA"."TABLE" → SCHEMA.TABLE and "DB"."SCHEMA"."TABLE" → DB.SCHEMA.TABLE.
    Only removes quotes from identifiers adjacent to a dot, so standalone string literals
    in WHERE clauses are left untouched.
    """
    # "WORD". → WORD.  (quoted part before a dot)
    result = re.sub(r'"([A-Za-z_][A-Za-z0-9_]*)"\s*\.', r'\1.', sql)
    # ."WORD" → .WORD  (quoted part after a dot)
    result = re.sub(r'\.\s*"([A-Za-z_][A-Za-z0-9_]*)"', r'.\1', result)
    return result


def _validate_on_bq_playground(sql):
    """Submit SQL to the BigQuery Playground and return a categorised result dict.

    Possible statuses:
      'success'         — query ran (or table existed and returned rows)
      'syntax_valid'    — syntax OK but Snowflake tables not found in BQ (expected during migration)
      'syntax_error'    — BigQuery rejected the SQL syntax; conversion is incomplete
      'column_error'    — syntax OK, table found, but a column name is wrong
      'other_error'     — unexpected error
    """
    payload = {
        "database": BQ_PLAYGROUND_DB_ID,
        "type": "native",
        "native": {"query": sql},
    }
    r = requests.post(
        f"{METABASE_URL}/api/dataset",
        headers=api_headers(),
        json=payload,
        timeout=120,
    )
    data = r.json()
    error = data.get("error") or data.get("data", {}).get("error", "")
    if not error:
        rows = len(data.get("data", {}).get("rows", []))
        return {"status": "success", "rows": rows, "error": ""}

    e = error.lower()
    if any(p in e for p in ("syntax error", "unrecognized function", "unexpected keyword",
                             "expected end of input", "expected keyword", "invalid query")):
        return {"status": "syntax_error", "error": error}
    if any(p in e for p in ("not found: table", "not found: dataset", "not found: project",
                             "table not found", "dataset not found",
                             "has not enabled bigquery", "bigquery is not enabled",
                             "project not found", "invalid project")):
        return {"status": "syntax_valid", "error": error}
    if "unrecognized name" in e:
        return {"status": "column_error", "error": error}
    return {"status": "other_error", "error": error}


def cmd_convert_sql(args):
    """Convert one or all flagged questions from Snowflake to BigQuery SQL and validate on the Playground."""
    if args.batch or args.ids_csv:
        _cmd_convert_sql_batch(args)
    else:
        _cmd_convert_sql_single(args)


def _cmd_convert_sql_single(args):
    card = get(f"/card/{args.id}")
    name = card.get("name", f"Card {args.id}")
    print(f"\n── {name} (id={args.id})")

    if not is_native(card):
        print("Skipped: GUI query — no SQL to convert.")
        return

    orig_sql = extract_sql(card)
    if not orig_sql:
        print("Skipped: no SQL found.")
        return

    sql_clean = orig_sql
    if _has_template_tags(orig_sql):
        sql_clean = re.sub(r'\[\[.*?\]\]', '', orig_sql, flags=re.DOTALL)
        sql_clean = re.sub(r'\{\{[^}]+\}\}', 'NULL', sql_clean)
        print("Note: template tags replaced with NULL for validation.")

    print("Converting with Claude...")
    try:
        conv = _convert_sql_to_bigquery_full(sql_clean)
    except RuntimeError as e:
        print(f"Conversion failed: {e}")
        return

    if args.show_sql:
        print("\n── Original SQL:")
        print(sql_clean)
        print("\n── Converted SQL:")
        print(conv["sql"])

    print("Validating on BigQuery Playground...")
    schema_map = _load_schema_map()
    validation_sql, unmapped = _apply_schema_map(conv["sql"], schema_map)
    if unmapped:
        print(f"  ⚠  {len(unmapped)} schema prefix(es) not in schema_map.json (TBD or missing): {unmapped}")
    bq_result = _validate_on_bq_playground(validation_sql)
    _print_conversion_result(bq_result)

    results_text = _make_results_text(args.id, name, bq_result, conv["changes"], conv["notes"])
    _write_card_files(args.id, orig_sql, conv["sql"], results_text, ".")
    print(f"\nFiles written: card{args.id}_orig_sql.txt  card{args.id}_adj_sql.txt  card{args.id}_results.txt")


def _cmd_convert_sql_batch(args):
    """Convert all native questions with Snowflake keyword hits; write three files per question into converted/ subfolders."""
    base_dir = args.output or "converted"
    dirs = {
        "syntax_clean":             os.path.join(base_dir, "syntax_clean"),
        "syntax_clean_verify_data": os.path.join(base_dir, "syntax_clean_verify_data"),
        "syntax_errors":            os.path.join(base_dir, "syntax_errors"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)

    if args.ids_csv:
        print(f"Loading question IDs from {args.ids_csv}...")
        with open(args.ids_csv, newline="", encoding="utf-8") as f:
            ids = [int(row["id"]) for row in csv.DictReader(f) if row.get("id", "").strip()]
        print(f"Fetching {len(ids)} questions from Metabase...")
        flagged = []
        for qid in ids:
            try:
                card = get(f"/card/{qid}")
                if not is_native(card):
                    print(f"  Skipping {qid}: GUI question (no SQL)")
                    continue
                flagged.append(card)
            except Exception as e:
                print(f"  Skipping {qid}: {e}")
        print(f"{len(flagged)} native questions to convert.")
    else:
        print("Fetching all questions...")
        cards = fetch_all_cards()
        native_cards = [c for c in cards if is_native(c)]
        print(f"Found {len(native_cards)} native questions. Scanning for Snowflake keywords...")
        flagged = [c for c in native_cards
                   if _scan_snowflake_keywords(extract_sql(c)) or _has_alias_self_reference(extract_sql(c))]
        print(f"{len(flagged)} questions have Snowflake-incompatible syntax. Converting...")

    existing_status = _load_migration_status(base_dir)
    status_rows = []

    for i, c in enumerate(flagged, 1):
        print(f"  [{i}/{len(flagged)}] {c['id']}: {c.get('name', '')[:60]}")
        row = _convert_and_write_card(c, dirs, schema_map_path=getattr(args, "schema_map", None))
        existing = existing_status.get(str(c["id"]), {})
        row["original_collection_id"] = existing.get("original_collection_id", "")
        row["action_source"] = existing.get("action_source", "")
        status_rows.append(row)

    rows_by_id = dict(existing_status)
    for row in status_rows:
        rows_by_id[str(row["id"])] = row
    _save_migration_status(rows_by_id, base_dir)

    from collections import Counter
    cats = Counter(r["original_category"] for r in status_rows)
    print(f"\n── Batch complete → {base_dir}/")
    print(f"  syntax_clean:              {cats.get('syntax_clean', 0)}")
    print(f"  syntax_clean_verify_data:  {cats.get('syntax_clean_verify_data', 0)}")
    print(f"  syntax_errors:             {cats.get('syntax_errors', 0)}")
    print(f"  migration_status.csv:      {os.path.join(base_dir, 'migration_status.csv')}")


def _run_comparison_for_card(card_id, base_dir, adj_db, dry_run):
    """Run Phase 3 data comparison for a single already-converted card.
    Returns 'pass', 'fail', 'error', or 'skip' (dry_run)."""
    try:
        orig_file, adj_file, subfolder = _find_card_files(card_id, base_dir)
    except FileNotFoundError as e:
        print(f"    ✗ {e}")
        return "error"
    if dry_run:
        print(f"    [DRY RUN] would compare {orig_file} vs {adj_file}")
        return "skip"
    orig_sql = open(orig_file, encoding="utf-8").read()
    adj_sql  = open(adj_file,  encoding="utf-8").read()
    orig_data = _run_query(orig_sql, SNOWFLAKE_DB_ID)
    adj_data  = _run_query(adj_sql,  adj_db)
    comparison = _compare_result_sets(orig_data, adj_data)
    _write_data_check_file(card_id, comparison, base_dir)
    if comparison["status"] == "pass":
        print(f"    ✓ PASS — {comparison['detail']}")
        final_path, unmapped = _write_final_sql(card_id, adj_file, base_dir)
        if unmapped:
            print(f"    ⚠  Final SQL written but {len(unmapped)} schema prefix(es) still TBD: {unmapped}")
        else:
            print(f"    ✓ Final SQL ready: {final_path}")
        _update_migration_status(card_id, "data_check_passed", "", base_dir)
        return "pass"
    elif comparison["status"] == "fail":
        print(f"    ✗ FAIL — {comparison['detail']}")
        _write_datadiff(card_id, comparison, base_dir, subfolder)
        _update_migration_status(card_id, "data_check_failed", comparison["detail"], base_dir)
        return "fail"
    else:
        print(f"    ? ERROR — {comparison['detail']}")
        _update_migration_status(card_id, "data_check_failed", comparison["detail"], base_dir)
        return "error"


def cmd_run_plan(args):
    """Process Metabase questions using an action CSV + conditional defaults.
    If --ids is given, only those questions are processed; others are untouched.
    For questions that already have converted files, Phase 3 data comparison is run
    instead of reconverting."""
    base_dir = args.output or "converted"
    dry_run = args.dry_run
    ids_filter = None
    if getattr(args, "ids", None):
        try:
            ids_filter = {int(x.strip()) for x in args.ids.split(",") if x.strip()}
        except ValueError:
            print("Error: --ids must be a comma-separated list of integers")
            return
        print(f"Processing {len(ids_filter)} specific question(s): {sorted(ids_filter)}")

    # Load action CSV: {card_id (int): action}
    action_map = {}
    if args.actions:
        with open(args.actions, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    action_map[int(row["id"])] = row.get("action", "").strip().lower()
                except (ValueError, KeyError):
                    pass
        print(f"Loaded {len(action_map)} actions from {args.actions}")

    existing_status = _load_migration_status(base_dir)

    print("Fetching all questions...")
    cards = fetch_all_cards()
    if ids_filter:
        cards = [c for c in cards if c["id"] in ids_filter]
        if not cards:
            print("No matching questions found in Metabase.")
            return

    print("Fetching collections...")
    collections = fetch_collections()
    col_map = build_collection_map(collections)
    personal_ids = fetch_personal_collection_ids(collections)

    print("Checking GUI dependency chain integrity...")
    dep_warnings = _check_dependency_warnings(cards, action_map, personal_ids, existing_status)
    if dep_warnings:
        print(f"\n⚠  {len(dep_warnings)} dependency warning(s) — GUI questions kept active whose source will be deprecated:\n")
        seen = set()
        for w in dep_warnings:
            key = (w["card_id"], w["dep_id"])
            if key in seen:
                continue
            seen.add(key)
            src = "csv" if w["card_id"] in action_map else "default"
            print(f"  [{src}:{w['card_action']}] {w['card_id']}: {w['card_name'][:55]}")
            print(f"         depends on  {w['dep_id']}: {w['dep_name'][:55]}  → will be {w['dep_action']}")
        print()
    else:
        print("  ✓ No broken GUI dependencies found.\n")

    # Apply batch window AFTER dependency check (which always runs against the full set)
    offset = getattr(args, "offset", None) or 0
    limit  = getattr(args, "limit",  None)
    if offset or limit:
        total = len(cards)
        cards = cards[offset:]
        if limit:
            cards = cards[:limit]
        end = offset + len(cards)
        print(f"Batch window: cards {offset + 1}–{end} of {total} (--offset {offset} --limit {limit or 'none'})\n")

    dirs = {
        "syntax_clean":             os.path.join(base_dir, "syntax_clean"),
        "syntax_clean_verify_data": os.path.join(base_dir, "syntax_clean_verify_data"),
        "syntax_errors":            os.path.join(base_dir, "syntax_errors"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)

    rows_by_id = dict(existing_status)
    counts = {"migrate": 0, "skip": 0, "deprecate": 0, "already_applied": 0}

    for c in cards:
        card_id = c["id"]
        id_str = str(card_id)
        name = c.get("name", f"Card {card_id}")
        existing = existing_status.get(id_str, {})

        if existing.get("current_status") == "applied":
            counts["already_applied"] += 1
            continue

        original_collection_id = c.get("collection_id") or ""
        col_id = c.get("collection_id") or "root"
        folder = col_map.get(col_id, str(col_id))

        if card_id in action_map:
            action = action_map[card_id]
            action_source = "csv"
        elif not is_native(c):
            action = "skip"
            action_source = "default"
        elif col_id in personal_ids:
            action = "skip"
            action_source = "default"
        else:
            action = "deprecate"
            action_source = "default"

        if action == "skip":
            counts["skip"] += 1
            if dry_run:
                print(f"  [DRY RUN] skip      {card_id}: {name[:60]}")
            rows_by_id[id_str] = {
                "id": card_id, "name": name,
                "original_category": existing.get("original_category", ""),
                "current_status": "skipped",
                "failure_reason": existing.get("failure_reason", ""),
                "notes": existing.get("notes", ""),
                "original_collection_id": original_collection_id,
                "action_source": action_source,
            }

        elif action == "deprecate":
            counts["deprecate"] += 1
            if dry_run:
                print(f"  [DRY RUN] deprecate {card_id}: {name[:60]}  →  Archive/{folder}")
            else:
                print(f"  [staged]  deprecate {card_id}: {name[:60]}")
            rows_by_id[id_str] = {
                "id": card_id, "name": name,
                "original_category": existing.get("original_category", ""),
                "current_status": "pending_deprecation",
                "failure_reason": "",
                "notes": existing.get("notes", ""),
                "original_collection_id": original_collection_id,
                "action_source": action_source,
            }

        elif action == "migrate":
            if not is_native(c):
                counts["skip"] += 1
                if dry_run:
                    print(f"  [DRY RUN] skip (GUI) {card_id}: {name[:60]}")
                rows_by_id[id_str] = {
                    "id": card_id, "name": name,
                    "original_category": "",
                    "current_status": "skipped",
                    "failure_reason": "GUI question — no SQL to migrate",
                    "notes": "",
                    "original_collection_id": original_collection_id,
                    "action_source": action_source,
                }
                continue

            counts["migrate"] += 1
            # If converted files already exist, skip reconversion and go straight
            # to data comparison. This happens when --ids is used for incremental
            # migration after Phase 2 has already run.
            try:
                _find_card_files(card_id, base_dir)
                already_converted = True
            except FileNotFoundError:
                already_converted = False

            if already_converted:
                adj_db = getattr(args, "adj_db", None) or BQ_PLAYGROUND_DB_ID
                print(f"  [compare  {counts['migrate']}] {card_id}: {name[:60]}  (already converted — running data check)")
                _run_comparison_for_card(card_id, base_dir, adj_db, dry_run)
                rows_by_id[id_str] = existing or {
                    "id": card_id, "name": name,
                    "original_category": existing.get("original_category", ""),
                    "current_status": existing.get("current_status", "pending"),
                    "failure_reason": existing.get("failure_reason", ""),
                    "notes": existing.get("notes", ""),
                    "original_collection_id": original_collection_id,
                    "action_source": action_source,
                }
            elif dry_run:
                print(f"  [DRY RUN] migrate   {card_id}: {name[:60]}")
                rows_by_id[id_str] = existing or {
                    "id": card_id, "name": name,
                    "original_category": "", "current_status": "pending",
                    "failure_reason": "", "notes": "",
                    "original_collection_id": original_collection_id,
                    "action_source": action_source,
                }
            else:
                print(f"  [migrate  {counts['migrate']}] {card_id}: {name[:60]}")
                row = _convert_and_write_card(c, dirs, schema_map_path=getattr(args, "schema_map", None))
                row["original_collection_id"] = original_collection_id
                row["action_source"] = action_source
                rows_by_id[id_str] = row

    if not dry_run:
        _save_migration_status(rows_by_id, base_dir)

    print(f"\n── run-plan {'(dry run) ' if dry_run else ''}complete ──────────────────")
    print(f"  migrated:        {counts['migrate']}")
    print(f"  deprecated:      {counts['deprecate']}")
    print(f"  skipped:         {counts['skip']}")
    print(f"  already applied: {counts['already_applied']}")
    if dry_run:
        print("  (no changes written)")
        if dep_warnings:
            seen = set()
            unique_warnings = []
            for w in dep_warnings:
                key = (w["card_id"], w["dep_id"])
                if key not in seen:
                    seen.add(key)
                    unique_warnings.append(w)
            print(f"\n⚠  {len(unique_warnings)} dependency warning(s) — fix these in the action CSV before running live:\n")
            for w in unique_warnings:
                src = "csv" if w["card_id"] in action_map else "default"
                print(f"  [{src}:{w['card_action']}] {w['card_id']}: {w['card_name'][:55]}")
                print(f"         depends on  {w['dep_id']}: {w['dep_name'][:55]}  → will be {w['dep_action']}")
        else:
            print("  ✓ No broken GUI dependencies found.")

    if _TOKEN_USAGE["calls"] > 0:
        inp   = _TOKEN_USAGE["input"]
        out   = _TOKEN_USAGE["output"]
        calls = _TOKEN_USAGE["calls"]
        print(f"\n── Claude API usage ─────────────────────────")
        print(f"  API calls:     {calls}")
        if inp or out:
            cost = (inp / 1_000_000 * 3.00) + (out / 1_000_000 * 15.00)
            print(f"  Input tokens:  {inp:,}")
            print(f"  Output tokens: {out:,}")
            print(f"  Est. cost:     ${cost:.4f}  (Sonnet 4.6: $3/1M in, $15/1M out)")
        else:
            print("  (token counts unavailable — running via claude CLI)")


def _print_conversion_result(result, prefix=""):
    status = result["status"]
    error = result.get("error", "")
    if status == "success":
        print(f"{prefix}✓ Valid — query ran against Playground tables ({result.get('rows', 0)} rows)")
    elif status == "syntax_valid":
        print(f"{prefix}✓ Syntax valid — Snowflake tables not found in BQ yet (expected)")
        if error:
            print(f"{prefix}  {error[:200]}")
    elif status == "syntax_error":
        print(f"{prefix}✗ Syntax error — conversion incomplete, needs manual fix")
        if error:
            print(f"{prefix}  {error[:400]}")
    elif status == "column_error":
        print(f"{prefix}~ Column error — syntax OK but column name may differ between Snowflake and BQ")
        if error:
            print(f"{prefix}  {error[:200]}")
    else:
        print(f"{prefix}? Unexpected error")
        if error:
            print(f"{prefix}  {error[:200]}")


# ── Data comparison (Phase 2) ────────────────────────────────────────────────

def _find_card_files(card_id, base_dir="converted"):
    """Search all converted/ subfolders for the three card files; return (orig_path, adj_path, subfolder)."""
    for subfolder in ("syntax_clean", "syntax_clean_verify_data", "syntax_errors"):
        d = os.path.join(base_dir, subfolder)
        orig = os.path.join(d, f"card{card_id}_orig_sql.txt")
        adj  = os.path.join(d, f"card{card_id}_adj_sql.txt")
        if os.path.exists(orig) and os.path.exists(adj):
            return orig, adj, subfolder
    raise FileNotFoundError(
        f"No card files found for id {card_id} under {base_dir}/. "
        f"Run convert-sql {card_id} first."
    )


def _run_query(sql, db_id):
    """Execute SQL via Metabase /api/dataset and return {columns, rows, error}."""
    payload = {
        "database": db_id,
        "type": "native",
        "native": {"query": sql},
        "constraints": {"max-results": 10000},
    }
    try:
        r = requests.post(
            f"{METABASE_URL}/api/dataset",
            headers=api_headers(),
            json=payload,
            timeout=120,
        )
        data = r.json()
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}

    error = data.get("error") or data.get("data", {}).get("error", "")
    if error:
        return {"columns": [], "rows": [], "error": error}

    cols = [c["name"] for c in data.get("data", {}).get("cols", [])]
    rows = data.get("data", {}).get("rows", [])
    return {"columns": cols, "rows": rows, "error": ""}


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2})?")


def _col_type(values):
    """Classify a column's values as 'numeric', 'datetime', or 'text'."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "text"
    try:
        [float(v) for v in non_null]
        return "numeric"
    except (TypeError, ValueError):
        pass
    if all(isinstance(v, str) and _DATE_RE.match(v) for v in non_null):
        return "datetime"
    return "text"


def _pct_diff(a, b):
    """Percentage difference relative to the larger absolute value; 0 if both zero."""
    if a == 0 and b == 0:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b)) * 100


def _compare_result_sets(orig, adj):
    """Compare two result-set dicts using column-level statistics.

    Checks applied per column type:
    - numeric:   SUM, MAX, MIN within ±1%
    - datetime:  MAX and MIN must match exactly
    - text/other: null count and unique value count must match exactly
    Row count mismatch is an immediate hard fail.
    """
    if orig["error"] or adj["error"]:
        return {
            "status": "error",
            "detail": f"Query error — orig: {orig['error'] or 'ok'}  adj: {adj['error'] or 'ok'}",
            "orig_rows": 0, "adj_rows": 0, "mismatches": [],
        }

    orig_rows, adj_rows = orig["rows"], adj["rows"]
    orig_cols, adj_cols = orig["columns"], adj["columns"]

    issues = []
    if orig_cols != adj_cols:
        issues.append(f"Columns differ: orig={orig_cols} adj={adj_cols}")

    if len(orig_rows) != len(adj_rows):
        return {
            "status": "fail",
            "detail": f"Row count mismatch: orig={len(orig_rows)} adj={len(adj_rows)}",
            "orig_rows": len(orig_rows), "adj_rows": len(adj_rows), "mismatches": [],
        }

    if not orig_rows:
        return {
            "status": "pass", "detail": "Both result sets are empty",
            "orig_rows": 0, "adj_rows": 0, "mismatches": [],
        }

    mismatches = []
    for j, col in enumerate(orig_cols):
        o_vals = [row[j] if j < len(row) else None for row in orig_rows]
        a_vals = [row[j] if j < len(row) else None for row in adj_rows]
        ctype = _col_type(o_vals)

        if ctype == "numeric":
            o_nums = [float(v) for v in o_vals if v is not None]
            a_nums = [float(v) for v in a_vals if v is not None]
            if not o_nums and not a_nums:
                continue
            for stat, o_stat, a_stat in [
                ("SUM", sum(o_nums),  sum(a_nums)),
                ("MAX", max(o_nums),  max(a_nums)),
                ("MIN", min(o_nums),  min(a_nums)),
            ]:
                pct = _pct_diff(o_stat, a_stat)
                if pct > 1.0:
                    mismatches.append({
                        "col": col, "check": stat,
                        "orig": round(o_stat, 4), "adj": round(a_stat, 4),
                        "diff_pct": round(pct, 3),
                    })

        elif ctype == "datetime":
            o_dates = sorted(v for v in o_vals if v is not None)
            a_dates = sorted(v for v in a_vals if v is not None)
            if o_dates and a_dates:
                if o_dates[0] != a_dates[0]:
                    mismatches.append({"col": col, "check": "MIN date",
                                       "orig": o_dates[0], "adj": a_dates[0]})
                if o_dates[-1] != a_dates[-1]:
                    mismatches.append({"col": col, "check": "MAX date",
                                       "orig": o_dates[-1], "adj": a_dates[-1]})

        else:  # text / categorical
            o_nulls  = sum(1 for v in o_vals if v is None)
            a_nulls  = sum(1 for v in a_vals if v is None)
            o_unique = len({str(v) for v in o_vals if v is not None})
            a_unique = len({str(v) for v in a_vals if v is not None})
            if o_nulls != a_nulls:
                mismatches.append({"col": col, "check": "null count",
                                   "orig": o_nulls, "adj": a_nulls})
            if o_unique != a_unique:
                mismatches.append({"col": col, "check": "unique count",
                                   "orig": o_unique, "adj": a_unique})

    if mismatches or issues:
        detail = f"{len(mismatches)} column check(s) failed"
        if issues:
            detail += "; " + "; ".join(issues)
        return {
            "status": "fail", "detail": detail,
            "orig_rows": len(orig_rows), "adj_rows": len(adj_rows),
            "mismatches": mismatches,
        }

    return {
        "status": "pass",
        "detail": f"All column checks passed ({len(orig_rows)} rows, {len(orig_cols)} columns)",
        "orig_rows": len(orig_rows), "adj_rows": len(adj_rows), "mismatches": [],
    }


def _write_datadiff(card_id, comparison, base_dir, subfolder):
    path = os.path.join(base_dir, subfolder, f"card{card_id}_datadiff.txt")
    lines = [
        f"Question ID: {card_id}",
        f"Comparison status: {comparison['status']}",
        f"Detail: {comparison['detail']}",
        f"Original row count: {comparison.get('orig_rows', 'N/A')}",
        f"Adjusted row count: {comparison.get('adj_rows', 'N/A')}",
        "",
    ]
    mismatches = comparison.get("mismatches", [])
    if mismatches:
        lines.append(f"Column check failures ({len(mismatches)} total):")
        for m in mismatches:
            diff_str = f"  ({m['diff_pct']}% diff)" if "diff_pct" in m else ""
            lines.append(f"  col '{m['col']}' [{m['check']}]:  orig={m['orig']!r}  adj={m['adj']!r}{diff_str}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def _update_migration_status(card_id, new_status, failure_reason, base_dir="converted"):
    csv_path = os.path.join(base_dir, "migration_status.csv")
    if not os.path.exists(csv_path):
        return
    rows = []
    fieldnames = None
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if str(row.get("id")) == str(card_id):
                row["current_status"] = new_status
                row["failure_reason"] = failure_reason
            rows.append(row)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_schema_map(path=None):
    """Load a schema map JSON file. Defaults to schema_map.json alongside this script."""
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_map.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_")}


def _apply_schema_map(sql, schema_map):
    """Replace Snowflake db.schema prefixes with BigQuery project.dataset equivalents.
    Returns (new_sql, unmapped_prefixes)."""
    result = sql
    unmapped = []
    for snowflake_prefix, bq_prefix in sorted(schema_map.items(), key=lambda x: -len(x[0])):
        if bq_prefix == "TBD":
            if re.search(r'\b' + re.escape(snowflake_prefix) + r'\.', result, re.IGNORECASE):
                unmapped.append(snowflake_prefix)
        else:
            result = re.sub(
                r'\b' + re.escape(snowflake_prefix) + r'\.',
                bq_prefix + ".",
                result,
                flags=re.IGNORECASE,
            )
    return result, unmapped


def _write_data_check_file(card_id, comparison, base_dir="converted"):
    """Write card{N}_data_check.txt to data_check_passed/ or data_check_failed/."""
    status = comparison["status"]
    subdir = "data_check_passed" if status == "pass" else "data_check_failed"
    out_dir = os.path.join(base_dir, subdir)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"card{card_id}_data_check.txt")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"Question ID: {card_id}",
        f"Data check status: {status}",
        f"Timestamp: {ts}",
        f"Original row count: {comparison.get('orig_rows', 'N/A')}",
        f"Adjusted row count: {comparison.get('adj_rows', 'N/A')}",
        "",
    ]
    mismatches = comparison.get("mismatches", [])
    if status == "pass":
        lines.append(f"Column checks: all passed ({comparison.get('detail', '')})")
    elif mismatches:
        lines.append(f"Column check failures ({len(mismatches)} total):")
        for m in mismatches:
            diff_str = f"  ({m['diff_pct']}% diff)" if "diff_pct" in m else ""
            lines.append(f"  col '{m['col']}' [{m['check']}]:  orig={m['orig']!r}  adj={m['adj']!r}{diff_str}")
    else:
        lines.append(f"Detail: {comparison.get('detail', '')}")
    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def _write_final_sql(card_id, adj_file, base_dir="converted"):
    """Apply schema_map.json to adj SQL and write to ready_to_migrate/card{N}_final_sql.txt.
    Returns (path, unmapped_prefixes)."""
    adj_sql = open(adj_file, encoding="utf-8").read()
    schema_map = _load_schema_map()
    final_sql, unmapped = _apply_schema_map(adj_sql, schema_map)
    out_dir = os.path.join(base_dir, "ready_to_migrate")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"card{card_id}_final_sql.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(final_sql)
    return path, unmapped


def cmd_compare_outputs(args):
    """Run orig and adj SQL, compare result sets, update migration_status.csv."""
    card_id = args.id
    base_dir = args.base_dir or "converted"
    orig_db  = SNOWFLAKE_DB_ID
    adj_db   = args.adj_db if args.adj_db else BQ_PLAYGROUND_DB_ID

    try:
        orig_file, adj_file, subfolder = _find_card_files(card_id, base_dir)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    print(f"\n── Comparing card {card_id}  ({subfolder})")
    print(f"   Orig SQL : {orig_file}  →  db={orig_db} (Snowflake)")
    print(f"   Adj SQL  : {adj_file}  →  db={adj_db} {'(Snowflake — test mode)' if adj_db == SNOWFLAKE_DB_ID else '(BigQuery)'}")

    orig_sql = open(orig_file, encoding="utf-8").read()
    adj_sql  = orig_sql if args.test_mode else open(adj_file, encoding="utf-8").read()

    if args.test_mode:
        print("   (test mode — running orig SQL against both connections)")

    print("\nRunning original SQL...")
    orig_data = _run_query(orig_sql, orig_db)
    if orig_data["error"]:
        print(f"  ✗ Error: {orig_data['error'][:300]}")
    else:
        print(f"  ✓ {len(orig_data['rows'])} rows, {len(orig_data['columns'])} columns")

    print("Running adjusted SQL...")
    adj_data = _run_query(adj_sql, adj_db)
    if adj_data["error"]:
        print(f"  ✗ Error: {adj_data['error'][:300]}")
    else:
        print(f"  ✓ {len(adj_data['rows'])} rows, {len(adj_data['columns'])} columns")

    comparison = _compare_result_sets(orig_data, adj_data)

    if comparison["status"] == "pass":
        print(f"\n✓ PASS — {comparison['detail']}")
        data_check_path = _write_data_check_file(card_id, comparison, base_dir)
        print(f"  Data check written: {data_check_path}")
        final_path, unmapped = _write_final_sql(card_id, adj_file, base_dir)
        if unmapped:
            print(f"  ⚠  Final SQL written to {final_path} but {len(unmapped)} schema prefix(es) still TBD: {unmapped}")
        else:
            print(f"  Final SQL ready: {final_path}")
        _update_migration_status(card_id, "data_check_passed", "", base_dir)
    elif comparison["status"] == "fail":
        print(f"\n✗ FAIL — {comparison['detail']}")
        data_check_path = _write_data_check_file(card_id, comparison, base_dir)
        print(f"  Data check written: {data_check_path}")
        diff_path = _write_datadiff(card_id, comparison, base_dir, subfolder)
        print(f"  Datadiff written: {diff_path}")
        _update_migration_status(card_id, "data_check_failed", comparison["detail"], base_dir)
    else:
        print(f"\n? ERROR — {comparison['detail']}")
        _update_migration_status(card_id, "data_check_failed", comparison["detail"], base_dir)


def cmd_apply_deprecations(args):
    """Move all pending_deprecation cards to Archive/<original path> in Metabase."""
    base_dir = args.base_dir or "converted"
    dry_run = args.dry_run

    existing_status = _load_migration_status(base_dir)
    pending = {
        card_id: row for card_id, row in existing_status.items()
        if row.get("current_status") == "pending_deprecation"
    }

    if not pending:
        print("No questions with status 'pending_deprecation' found in migration_status.csv.")
        return

    print(f"Found {len(pending)} question(s) staged for deprecation.")
    print("Fetching collections...")
    collections = fetch_collections()
    collections_by_id = {c["id"]: c for c in collections}
    col_map = build_collection_map(collections)
    name_parent_map = _build_col_name_parent_map(collections)
    archive_cache = {}

    deprecated = 0
    failed = 0

    for id_str, row in sorted(pending.items(), key=lambda x: int(x[0])):
        card_id = int(id_str)
        name = row.get("name", f"Card {card_id}")
        original_collection_id = row.get("original_collection_id") or None
        if original_collection_id:
            original_collection_id = int(original_collection_id)
        folder = col_map.get(original_collection_id or "root", str(original_collection_id))

        archive_col_id = _get_archive_collection_id(
            original_collection_id,
            collections_by_id, name_parent_map, archive_cache, dry_run,
        )

        if dry_run:
            print(f"  [DRY RUN] deprecate {card_id}: {name[:60]}  →  Archive/{folder}")
            deprecated += 1
        else:
            try:
                _move_card_to_collection(card_id, archive_col_id)
                print(f"  deprecated {card_id}: {name[:60]}")
                _update_migration_status(card_id, "deprecated", "", base_dir)
                deprecated += 1
            except Exception as e:
                print(f"  ✗ {card_id}: move failed — {e}")
                failed += 1

    print(f"\n── apply-deprecations {'(dry run) ' if dry_run else ''}complete ──")
    print(f"  deprecated: {deprecated}")
    if failed:
        print(f"  failed:     {failed}")


def cmd_apply_migration(args):
    """Push final SQL from ready_to_migrate/ back to Metabase for one or all questions."""
    base_dir = args.base_dir or "converted"
    dry_run = args.dry_run
    ready_dir = os.path.join(base_dir, "ready_to_migrate")

    if args.id:
        card_ids = [args.id]
    else:
        if not os.path.isdir(ready_dir):
            print(f"No ready_to_migrate/ directory found under {base_dir}/")
            return
        pattern = re.compile(r'^card(\d+)_final_sql\.txt$')
        card_ids = []
        for fname in os.listdir(ready_dir):
            m = pattern.match(fname)
            if m:
                card_ids.append(int(m.group(1)))
        if not card_ids:
            print("No final SQL files found in ready_to_migrate/")
            return
        print(f"Found {len(card_ids)} question(s) ready to apply: {sorted(card_ids)}")

    schema_map = _load_schema_map()
    applied = 0
    failed = 0

    for card_id in sorted(card_ids):
        final_sql_path = os.path.join(ready_dir, f"card{card_id}_final_sql.txt")
        if not os.path.exists(final_sql_path):
            print(f"  ✗ {card_id}: no final SQL file at {final_sql_path}")
            failed += 1
            continue

        final_sql = open(final_sql_path, encoding="utf-8").read()

        # Hard-fail if any schema reference is still TBD
        tbd_refs = [
            prefix for prefix, target in schema_map.items()
            if target == "TBD"
            and re.search(r'\b' + re.escape(prefix) + r'\.', final_sql, re.IGNORECASE)
        ]
        if tbd_refs:
            print(f"  ✗ {card_id}: schema_map.json has unresolved TBD — cannot apply: {tbd_refs}")
            failed += 1
            continue

        adj_db = getattr(args, "adj_db", None)

        if dry_run:
            db_note = f"  database → {adj_db}" if adj_db else ""
            print(f"  [DRY RUN] {card_id}: would push SQL from {final_sql_path}{db_note}")
            applied += 1
            continue

        try:
            card = get(f"/card/{card_id}")
        except Exception as e:
            print(f"  ✗ {card_id}: failed to fetch card — {e}")
            failed += 1
            continue

        dq = card.get("dataset_query", {})
        stages = dq.get("stages", [])
        if stages:
            dq["stages"][0]["native"] = final_sql
        else:
            if "native" not in dq:
                dq["native"] = {}
            dq["native"]["query"] = final_sql

        if adj_db:
            dq["database"] = adj_db

        try:
            put_api(f"/card/{card_id}", {"dataset_query": dq})
            print(f"  ✓ {card_id}: applied to Metabase")
            _update_migration_status(card_id, "applied", "", base_dir)
            applied += 1
        except Exception as e:
            print(f"  ✗ {card_id}: PUT failed — {e}")
            failed += 1

    print(f"\n── apply-migration {'(dry run) ' if dry_run else ''}complete ──")
    print(f"  applied: {applied}")
    if failed:
        print(f"  failed:  {failed}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Metabase audit toolkit for Snowflake → BigQuery migration"
    )
    parser.add_argument("--url", help="Metabase base URL (overrides env/default)")
    parser.add_argument("--api-key", help="Metabase API key (overrides env/default)")

    sub = parser.add_subparsers(dest="command", required=True)

    # connect
    p = sub.add_parser("connect", help="Test API connection")

    # summary
    p = sub.add_parser("summary", help="Overall stats: total queries, by folder, creator, last-run age")

    # scan-keywords
    p = sub.add_parser("scan-keywords", help="Find Snowflake keywords incompatible with BigQuery")
    p.add_argument("--keywords", help="Comma-separated list of keywords to scan for (default: built-in list)")
    p.add_argument("--output", "-o", help="Save results to CSV file")

    # list-queries
    p = sub.add_parser("list-queries", help="List queries, optionally filtered by folder")
    p.add_argument("--folder", "-f", help="Filter by folder name (partial match)")
    p.add_argument("--output", "-o", help="Save to CSV")

    # export-csv
    p = sub.add_parser("export-csv", help="Export stats inventory to CSV (id, name, folder, creator, dates, view_count, tables, dashboards)")
    p.add_argument("--output", "-o", default="metabase_inventory.csv", help="Output filename")

    # export-sql
    p = sub.add_parser("export-sql", help="Export raw SQL for all native queries to CSV")
    p.add_argument("--output", "-o", default="metabase_sql.csv", help="Output filename")

    # export-dashboards
    p = sub.add_parser("export-dashboards", help="Export dashboard inventory with monthly view counts")
    p.add_argument("-o", "--output", help="Output CSV file (default: metabase_dashboards.csv)")

    # export-dashboard-questions
    p = sub.add_parser("export-dashboard-questions", help="Export flat mapping of every dashboard → question pairing")
    p.add_argument("-o", "--output", help="Output CSV file (default: metabase_dashboard_questions.csv)")

    # show-sql
    p = sub.add_parser("show-sql", help="Print SQL for a specific query by ID")
    p.add_argument("id", type=int, help="Metabase question/card ID")

    # show-keywords
    p = sub.add_parser("show-keywords", help="Show Snowflake-incompatible keywords for a specific query by ID")
    p.add_argument("id", type=int, help="Metabase question/card ID")

    # show-runs
    p = sub.add_parser("show-runs", help="Show run history for a specific query by ID")
    p.add_argument("id", type=int, help="Metabase question/card ID")

    # show-card
    p = sub.add_parser("show-card", help="Print raw JSON for a specific card from the API")
    p.add_argument("id", type=int, help="Metabase question/card ID")
    p.add_argument("-o", "--output", help="Save JSON to this file instead of printing")

    # show-dashboard-views
    p = sub.add_parser("show-dashboard-views", help="Show raw view log entries for a specific dashboard")
    p.add_argument("id", type=int, help="Metabase dashboard ID")

    p = sub.add_parser("convert-sql", help="Convert a question's SQL from Snowflake to BigQuery and validate on the Playground")
    p.add_argument("id", type=int, nargs="?", help="Question ID (omit with --batch)")
    p.add_argument("--batch", action="store_true", help="Convert questions in batch mode")
    p.add_argument("--ids-csv", metavar="FILE", help="CSV with an 'id' column — convert only those questions (batch mode)")
    p.add_argument("--show-sql", action="store_true", help="Print original and converted SQL")
    p.add_argument("-o", "--output", help="Output directory for batch mode (default: converted/)")

    # run-plan
    p = sub.add_parser("run-plan", help="Process all questions using an action CSV + conditional defaults")
    p.add_argument("--actions", metavar="FILE", help="CSV with 'id' and 'action' columns (migrate/skip/deprecate)")
    p.add_argument("--output", "-o", default="converted", help="Output directory (default: converted/)")
    p.add_argument("--dry-run", action="store_true", help="Print what would happen without making any changes")
    p.add_argument("--ids", metavar="ID,ID,...", help="Comma-separated question IDs to process (default: all questions)")
    p.add_argument("--limit", type=int, metavar="N", default=None,
                   help="Process at most N questions (useful for batched runs). Dependency check still runs against the full set.")
    p.add_argument("--offset", type=int, metavar="N", default=0,
                   help="Skip the first N questions before applying --limit (for processing subsequent batches)")
    p.add_argument("--adj-db", type=int, default=None,
                   help=f"Database ID for adjusted SQL in data comparison (default: {BQ_PLAYGROUND_DB_ID} BigQuery)")
    p.add_argument("--schema-map", metavar="FILE", default=None,
                   help="Schema map JSON file for BQ validation (default: schema_map.json). "
                        "Use schema_map_dummy.json during Phase 1 before real BQ names are known.")

    # compare-outputs
    p = sub.add_parser("compare-outputs", help="Compare Snowflake vs BigQuery result sets for a converted question")
    p.add_argument("id", type=int, help="Question ID")
    p.add_argument("--base-dir", default="converted", help="Base directory containing converted/ subfolders (default: converted)")
    p.add_argument("--adj-db", type=int, default=None,
                   help=f"Database ID for adjusted SQL (default: {BQ_PLAYGROUND_DB_ID} BigQuery). "
                        f"Pass {SNOWFLAKE_DB_ID} to run both against Snowflake for testing.")
    p.add_argument("--test-mode", action="store_true",
                   help="Run orig SQL against both connections — validates the comparison framework without needing real BigQuery data")

    # apply-deprecations
    p = sub.add_parser("apply-deprecations", help="Move all pending_deprecation questions to Archive/ in Metabase")
    p.add_argument("--base-dir", default="converted", help="Base directory containing migration_status.csv (default: converted)")
    p.add_argument("--dry-run", action="store_true", help="Print what would be moved without making changes")

    # apply-migration
    p = sub.add_parser("apply-migration", help="Push final SQL from ready_to_migrate/ to Metabase")
    p.add_argument("id", type=int, nargs="?", help="Question ID (omit to apply all in ready_to_migrate/)")
    p.add_argument("--base-dir", default="converted", help="Base directory (default: converted)")
    p.add_argument("--dry-run", action="store_true", help="Print what would be applied without making changes")
    p.add_argument("--adj-db", type=int, metavar="DB_ID", default=None,
                   help="Metabase database ID for BigQuery — updates dataset_query.database alongside the SQL")

    args = parser.parse_args()

    # Apply overrides
    global METABASE_URL, METABASE_API_KEY
    if args.url:
        METABASE_URL = args.url
    if hasattr(args, "api_key") and args.api_key:
        METABASE_API_KEY = args.api_key

    if args.command == "connect":
        check_connection()
    elif args.command == "summary":
        if check_connection():
            cmd_summary(args)
    elif args.command == "scan-keywords":
        if check_connection():
            cmd_scan_keywords(args)
    elif args.command == "list-queries":
        if check_connection():
            cmd_list_queries(args)
    elif args.command == "export-csv":
        if check_connection():
            cmd_export_csv(args)
    elif args.command == "export-sql":
        if check_connection():
            cmd_export_sql(args)
    elif args.command == "export-dashboards":
        if check_connection():
            cmd_export_dashboards(args)
    elif args.command == "export-dashboard-questions":
        if check_connection():
            cmd_export_dashboard_questions(args)
    elif args.command == "show-sql":
        if check_connection():
            cmd_show_sql(args)
    elif args.command == "show-keywords":
        if check_connection():
            cmd_show_keywords(args)
    elif args.command == "show-runs":
        if check_connection():
            cmd_show_runs(args)
    elif args.command == "show-card":
        if check_connection():
            cmd_show_card(args)
    elif args.command == "show-dashboard-views":
        if check_connection():
            cmd_show_dashboard_views(args)
    elif args.command == "convert-sql":
        if check_connection():
            cmd_convert_sql(args)
    elif args.command == "run-plan":
        if check_connection():
            cmd_run_plan(args)
    elif args.command == "compare-outputs":
        if check_connection():
            cmd_compare_outputs(args)
    elif args.command == "apply-deprecations":
        if check_connection():
            cmd_apply_deprecations(args)
    elif args.command == "apply-migration":
        if check_connection():
            cmd_apply_migration(args)


if __name__ == "__main__":
    main()
