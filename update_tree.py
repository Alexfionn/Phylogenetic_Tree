#!/usr/bin/env python3
# coding: utf-8
"""
update_tree.py - verbose block-update variant

Schreibt immer:
  data/species.json
  data/tree.mmd

Versucht dann, NOTION_BLOCK_ID zu aktualisieren (mermaid code).
Gibt viele Debug-Infos in stdout (wichtig für Action-Logs).
"""

import os
import json
import re
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional

from notion_client import Client
from notion_client.errors import APIResponseError

# ---- Konfiguration: Property-Namen (an deine DB anpassen falls nötig) ----
RANK_KEYS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]

# ---- Pfade ----
DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
MERMAID_FILE = os.path.join(DATA_DIR, "tree.mmd")

# ---- Env / Secrets ----
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_BLOCK_ID = os.getenv("NOTION_BLOCK_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1", "true", "yes")

# Sanity: minimal env
if not NOTION_TOKEN:
    raise SystemExit("ERROR: NOTION_TOKEN fehlt.")
if not NOTION_DATABASE_ID:
    raise SystemExit("ERROR: NOTION_DATABASE_ID fehlt.")
if not NOTION_BLOCK_ID:
    raise SystemExit("ERROR: NOTION_BLOCK_ID fehlt.")

# ---- Notion Client ----
notion = Client(auth=NOTION_TOKEN)

# ---- Helpers ----
def normalize_id(maybe: Optional[str]) -> Optional[str]:
    if not maybe:
        return None
    s = maybe.strip()
    # If the id is a url or has fragment/query, try to extract the pure 32-hex id
    if "#" in s:
        s = s.split("#")[-1]
    if "?" in s:
        s = s.split("?")[0]
    s = s.replace("-", "")
    m = re.search(r'([0-9a-fA-F]{32})', s)
    return m.group(1) if m else None

NOTION_DATABASE_ID = normalize_id(NOTION_DATABASE_ID)
NOTION_BLOCK_ID = normalize_id(NOTION_BLOCK_ID)

def pretty_preview(s: Optional[str]) -> str:
    if not s:
        return "<none>"
    return f"{s[:4]}...{s[-4:]} (len={len(s)})"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_id(name: str) -> str:
    h = hashlib.sha1(name.encode("utf8")).hexdigest()[:10]
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in name)
    return f"n_{cleaned[:20]}_{h}"

def normalize_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return " ".join(s.strip().split()) or None

# ---- Notion operations ----
def query_all_database(database_id: str) -> List[Dict[str, Any]]:
    print(f"[INFO] Querying database {pretty_preview(database_id)} ...")
    results = []
    start_cursor = None
    while True:
        kwargs = {"database_id": database_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor
        resp = notion.databases.query(**kwargs)
        results.extend(resp.get("results", []))
        if resp.get("has_more"):
            start_cursor = resp.get("next_cursor")
        else:
            break
    return results

def extract_row_properties(page: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = page.get("properties", {})
    row: Dict[str, Optional[str]] = {}
    for key in RANK_KEYS:
        p = props.get(key)
        value = None
        if p:
            t = p.get("type")
            if t == "title":
                value = "".join([x.get("plain_text", "") for x in p.get("title", [])]) or None
            elif t == "rich_text":
                value = "".join([x.get("plain_text", "") for x in p.get("rich_text", [])]) or None
            elif t == "select":
                sel = p.get("select")
                value = sel.get("name") if sel else None
            elif t == "multi_select":
                arr = p.get("multi_select", [])
                value = arr[0]["name"] if arr else None
            else:
                # fallback try common keys
                if isinstance(p, dict):
                    for candidate in ("name", "plain_text", "text"):
                        if candidate in p and isinstance(p[candidate], str):
                            value = p[candidate]
                            break
        row[key.lower()] = normalize_name(value)
    row["_notion_page_id"] = page.get("id")
    return row

def deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        key = tuple([r.get(k.lower()) or "" for k in RANK_KEYS])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def build_tree(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    tree: Dict[str, Any] = {}
    for r in rows:
        node = tree
        for rank in RANK_KEYS:
            v = r.get(rank.lower())
            if not v:
                break
            if v not in node:
                node[v] = {}
            node = node[v]
    return tree

def render_mermaid(tree: Dict[str, Any], graph_dir="TD") -> str:
    lines = [f"%% Generated {datetime.utcnow().isoformat()}Z", f"graph {graph_dir}"]
    def walk(sub: Dict[str, Any], parent: Optional[str] = None):
        for name, child in sorted(sub.items(), key=lambda x: x[0].lower()):
            nid = safe_id(name)
            label = name.replace('"', '\\"')
            lines.append(f'{nid}["{label}"]')
            if parent:
                lines.append(f"{safe_id(parent)} --> {nid}")
            if child:
                walk(child, name)
    if not tree:
        return "\n".join(lines + ["%% (empty)"])
    for name, subtree in sorted(tree.items(), key=lambda x: x[0].lower()):
        lines.append(f'{safe_id(name)}["{name}"]')
        if subtree:
            walk(subtree, name)
    return "\n".join(lines)

def retrieve_block(block_id: str) -> Optional[Dict[str, Any]]:
    try:
        b = notion.blocks.retrieve(block_id=block_id)
        return b
    except APIResponseError as e:
        print("[ERROR] notion.blocks.retrieve failed:", getattr(e, "message", str(e)))
        return None

def update_code_block(block_id: str, mermaid_text: str) -> bool:
    print(f"[INFO] Retrieving block {pretty_preview(block_id)} ...")
    b = retrieve_block(block_id)
    if not b:
        print("[ERROR] Konnte Block nicht abrufen (siehe oben).")
        return False
    btype = b.get("type")
    print(f"[INFO] Block type from API: {btype}")
    if btype != "code":
        print("[ERROR] Block ist kein 'code' Block. Bitte setze NOTION_BLOCK_ID auf einen code-block.")
        print("Block preview keys:", list(b.keys())[:12])
        return False
    # Prepare payload: use 'rich_text' which is accepted by recent Notion API SDKs for code blocks
    payload = {
        "code": {
            "rich_text": [{"type": "text", "text": {"content": mermaid_text}}],
            "language": "mermaid"
        }
    }
    try:
        notion.blocks.update(block_id=block_id, **payload)
        print("[OK] Block aktualisiert.")
        return True
    except APIResponseError as e:
        print("[ERROR] notion.blocks.update failed:", getattr(e, "message", str(e)))
        return False
    except Exception as e:
        print("[ERROR] Unexpected error during update:", e)
        return False

# ---- Files write ----
def write_files(rows: List[Dict[str, Any]], mermaid: str):
    ensure = os.path.dirname(SPECIES_JSON)
    if ensure:
        os.makedirs(ensure, exist_ok=True)
    with open(SPECIES_JSON, "w", encoding="utf8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    with open(MERMAID_FILE, "w", encoding="utf8") as f:
        f.write(mermaid)
    print(f"[INFO] Wrote {SPECIES_JSON} ({len(rows)} rows) and {MERMAID_FILE}")

def attempt_commit_and_push():
    if not COMMIT_BACK:
        print("[INFO] COMMIT_BACK=false -> skipping commit/push.")
        return
    print("[INFO] COMMIT_BACK=true -> attempting to commit and push data/ changes.")
    # configure git and push
    os.system("git config user.email 'github-actions[bot]@users.noreply.github.com'")
    os.system("git config user.name 'github-actions[bot]'")
    os.system("git add data || true")
    os.system("git commit -m 'Auto update species data' || echo 'no changes to commit'")
    repo = os.getenv("GITHUB_REPOSITORY")
    branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
    if repo and os.getenv("GITHUB_TOKEN"):
        remote = f"https://x-access-token:{os.getenv('GITHUB_TOKEN')}@github.com/{repo}.git"
        os.system(f"git remote set-url origin {remote}")
        os.system(f"git push origin {branch} || echo 'push failed'")
    else:
        print("[WARN] Repo or GITHUB_TOKEN missing, push skipped.")

# ---- Main ----
def main():
    print("=== update_tree.py start ===")
    print("[DEBUG] NOTION_DATABASE_ID:", pretty_preview(NOTION_DATABASE_ID))
    print("[DEBUG] NOTION_BLOCK_ID   :", pretty_preview(NOTION_BLOCK_ID))
    # quick check: can we read database meta?
    try:
        meta = notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
        print("[INFO] Successfully retrieved database metadata. Title preview:",
              meta.get("title", [])[:1])
    except APIResponseError as e:
        print("[ERROR] Cannot retrieve database metadata:", getattr(e, "message", str(e)))
        print("→ Bitte prüfen: ist NOTION_DATABASE_ID korrekt und ist die Integration eingeladen?")
        # still continue to try querying (it will likely fail)
    # query rows
    try:
        pages = query_all_database(NOTION_DATABASE_ID)
    except Exception as e:
        print("[ERROR] query_all_database failed:", e)
        pages = []
    print(f"[INFO] Got {len(pages)} pages from DB")
    rows = [extract_row_properties(p) for p in pages]
    rows = deduplicate_rows(rows)
    print(f"[INFO] {len(rows)} rows after deduplication")
    tree = build_tree(rows)
    mermaid = render_mermaid(tree, graph_dir="TD")
    # write files always
    write_files(rows, mermaid)
    # Debug: print first 20 lines of mermaid
    print("=== Mermaid preview (first 20 lines) ===")
    for i, l in enumerate(mermaid.splitlines()):
        if i >= 20:
            break
        print(l)
    print("=== end preview ===")
    # attempt Notion update
    ok = update_code_block(NOTION_BLOCK_ID, mermaid)
    if not ok:
        print("[WARN] Notion update failed or skipped. Check logs above.")
    # commit/push optionally
    attempt_commit_and_push()
    print("=== Script finished ===")

if __name__ == "__main__":
    main()