"""
Generate a full dashboard inventory CSV from Metabase.
Outputs: dashboard_inventory.csv

Usage:
    python generate_dashboard_report.py

Requirements:
    pip install requests
"""

import requests
import csv
import json
import time
import os

METABASE_URL = os.getenv("METABASE_URL", "https://carwow.metabaseapp.com")
METABASE_API_KEY = os.getenv("METABASE_API_KEY", "mb_iLGg0UGD+PY96M/DHJauBL+cQn3tDVUxyxq4pJ4WGNE=")

HEADERS = {
    "x-api-key": METABASE_API_KEY,
    "Content-Type": "application/json",
}

OUTPUT_FILE = "dashboard_inventory.csv"


def get(path, params=None):
    r = requests.get(f"{METABASE_URL}/api{path}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def build_collection_map():
    """Return {collection_id: full_path} for all collections."""
    collections = get("/collection")
    if isinstance(collections, dict):
        collections = collections.get("data", [])

    id_to_col = {c["id"]: c for c in collections}
    id_to_col["root"] = {"name": "Our analytics (root)", "location": "/"}

    def full_path(col_id):
        col = id_to_col.get(col_id)
        if not col:
            return str(col_id)
        location = col.get("location", "/")
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


def main():
    print("Fetching collections...")
    col_map = build_collection_map()

    print("Fetching dashboard list...")
    data = get("/dashboard", params={"f": "all"})
    if isinstance(data, dict):
        dashboards = data.get("data", [])
    else:
        dashboards = data

    # Filter out archived
    dashboards = [d for d in dashboards if not d.get("archived", False)]
    total = len(dashboards)
    print(f"Found {total} dashboards. Fetching details (creator + view count)...")

    rows = []
    for i, d in enumerate(dashboards, 1):
        dash_id = d["id"]
        print(f"  [{i}/{total}] Dashboard {dash_id}: {d.get('name','')[:50]}")

        creator_name = ""
        view_count = ""

        try:
            detail = get(f"/dashboard/{dash_id}")

            # Creator
            creator = detail.get("creator") or detail.get("created_by") or {}
            first = creator.get("first_name", "")
            last = creator.get("last_name", "")
            creator_name = f"{first} {last}".strip() or creator.get("email", "Unknown")

            # View count — Metabase stores this on the dashboard object
            view_count = detail.get("view_count", "")

        except Exception as e:
            print(f"    Warning: could not fetch detail for {dash_id}: {e}")

        col_id = d.get("collection_id") or "root"
        folder = col_map.get(col_id, str(col_id))

        rows.append({
            "ID": dash_id,
            "Name": d.get("name", ""),
            "Owner": creator_name,
            "Folder": folder,
            "Views (all time)": view_count,
            "Created": d.get("created_at", "")[:10] if d.get("created_at") else "",
            "Last Updated": d.get("updated_at", "")[:10] if d.get("updated_at") else "",
            "URL": f"{METABASE_URL}/dashboard/{dash_id}",
        })

        # Gentle rate limiting
        time.sleep(0.1)

    # Sort by folder then name
    rows.sort(key=lambda r: (r["Folder"], r["Name"]))

    print(f"\nWriting {len(rows)} rows to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "Name", "Owner", "Folder", "Views (all time)", "Created", "Last Updated", "URL"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Done! Saved to {OUTPUT_FILE}")

    # Quick summary
    print("\n── Summary ──────────────────────────────────────────")
    owners = {}
    for r in rows:
        owners[r["Owner"]] = owners.get(r["Owner"], 0) + 1
    print(f"Total dashboards: {len(rows)}")
    print(f"Unique owners: {len(owners)}")
    top_owners = sorted(owners.items(), key=lambda x: -x[1])[:5]
    print("Top 5 creators:")
    for name, count in top_owners:
        print(f"  {name}: {count}")


if __name__ == "__main__":
    main()
