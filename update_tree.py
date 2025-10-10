#!/usr/bin/env python3
"""
update_tree.py

Was es macht:
- Liest alle Rows aus einer Notion-Database (jede Zeile = 1 beobachtete Spezies)
- Reinigt / normalisiert / dedupliziert
- Speichert eine persistente JSON-Datei unter data/species.json
- Baut daraus einen Mermaid-Graphen und speichert data/tree.mmd
- Aktualisiert auf einer Ziel-Notion-Page den vorhandenen Mermaid-Codeblock
  (löscht vorhandene mermaid-code blocks und appended einen neuen)
- Optional: commit der data/ dateien in das repo (benötigt write-access via GITHUB_TOKEN)

Benötigte Python-Pakete:
pip install notion-client requests python-dotenv
"""

import os
import json
import time
import hashlib
from typing import List, Dict, Any, Optional
from notion_client import Client
import requests
from datetime import datetime

# --- Konfiguration (falls nötig anpassen) ---
# Property-Namen in deiner Notion-Database (exact match)
RANK_KEYS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]
# Pfade
DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
MERMAID_FILE = os.path.join(DATA_DIR, "tree.mmd")
# Kleines Limit für Paginierung (Notion liefert paginiert)
PAGE_SIZE = 100

# --- Umgebungsvariablen / Secrets ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1","true","yes")

if not NOTION_TOKEN or not NOTION_DATABASE_ID or not NOTION_PAGE_ID:
    raise SystemExit("Bitte setze NOTION_TOKEN, NOTION_DATABASE_ID und NOTION_PAGE_ID als Umgebungsvariablen / Secrets.")

# --- Helper / Notion client ---
notion = Client(auth=NOTION_TOKEN)

def query_all_database(database_id: str) -> List[Dict[str, Any]]:
    """Liest alle Seiten/Zeilen der Database (paginierend)."""
    results = []
    start_cursor = None
    while True:
        resp = notion.databases.query(
            **{
                "database_id": database_id,
                "page_size": PAGE_SIZE,
                **({"start_cursor": start_cursor} if start_cursor else {})
            }
        )
        results.extend(resp.get("results", []))
        if resp.get("has_more"):
            start_cursor = resp.get("next_cursor")
        else:
            break
    return results

def extract_row_properties(page) -> Dict[str, Optional[str]]:
    """Extrahiert RANK_KEYS aus einer Notion-Page row. Gibt dict rank->value (oder None)."""
    props = page.get("properties", {})
    row = {}
    for key in RANK_KEYS:
        p = props.get(key)
        value = None
        if not p:
            row[key.lower()] = None
            continue
        t = p.get("type")
        # unterstütze Text, title, select, rich_text
        if t == "title":
            # title: list of rich text
            title_arr = p.get("title", [])
            value = "".join([x.get("plain_text", "") for x in title_arr]).strip() or None
        elif t == "rich_text":
            value = "".join([x.get("plain_text","") for x in p.get("rich_text", [])]).strip() or None
        elif t == "select":
            sel = p.get("select")
            value = sel.get("name") if sel else None
        elif t == "multi_select":
            arr = p.get("multi_select", [])
            value = arr[0]["name"] if arr else None
        elif t == "title":
            value = "".join([x.get("plain_text","") for x in p.get("title", [])]) or None
        elif t == "rich_text":
            value = "".join([x.get("plain_text","") for x in p.get("rich_text", [])]) or None
        elif t == "people":
            value = None
        else:
            # generic fallback: try 'rich_text' or 'plain_text' properties maybe as text
            # Notion's property types vary; we handle common ones above.
            try:
                # some types hold a 'name' or 'text' key
                value = str(p.get(t) or "").strip()
            except Exception:
                value = None
        if isinstance(value, str):
            value = value.strip() or None
        row[key.lower()] = value
    # also add the Notion page id and title if present
    # title fallback: try "Name" property or first available
    name_prop = page.get("properties", {}).get("Name")
    if name_prop and name_prop.get("type") == "title":
        row["name"] = "".join([x.get("plain_text","") for x in name_prop.get("title", [])]).strip() or None
    else:
        row["name"] = None
    row["_notion_page_id"] = page.get("id")
    row["_raw"] = page
    return row

# --- Cleaning & Normalizing ---
def normalize_name(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip()
    # einfache Normalisierung: collapse whitespace, capitalize genus/species properly?
    # Wir lassen die Groß-/Kleinschreibung so wie eingegeben, aber entfernen doppelte Räume:
    s = " ".join(s.split())
    return s or None

def deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Entfernt exakte Duplikate (gleiche komplette Rang-Kette)"""
    seen = set()
    out = []
    for r in rows:
        key = tuple([normalize_name(r.get(k.lower())) or "" for k in RANK_KEYS])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# --- Build nested tree dict ---
def build_tree(rows: List[Dict[str, Any]]):
    tree = {}
    for r in rows:
        node = tree
        for rank in RANK_KEYS:
            value = normalize_name(r.get(rank.lower()))
            if not value:
                break
            if value not in node:
                node[value] = {}
            node = node[value]
    return tree

# --- Mermaid rendering ---
def safe_id(name: str) -> str:
    # stable id for node names
    h = hashlib.sha1(name.encode("utf8")).hexdigest()[:10]
    # remove problematic characters
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in name)
    return f"node_{cleaned[:20]}_{h}"

def render_mermaid(tree: Dict[str, Any], graph_dir: str = "TD") -> str:
    lines = [f"%% Mermaid generated on {datetime.utcnow().isoformat()}Z", f"graph {graph_dir}"]
    def walk(subtree, parent_name: Optional[str] = None):
        for name, child in sorted(subtree.items(), key=lambda x: x[0].lower()):
            nid = safe_id(name)
            # define node with label (use quoted label)
            label = name.replace('"', '\\"')
            lines.append(f'{nid}["{label}"]')
            if parent_name:
                lines.append(f"{safe_id(parent_name)} --> {nid}")
            # recurse
            if child:
                walk(child, name)
    # create a pseudo-root to connect top-level kingdoms (so graph is connected)
    top_names = list(tree.keys())
    if not top_names:
        return "\n".join(lines + ["%% (empty tree)"])
    # Create nodes and edges
    # Add top-level nodes
    for name, child in sorted(tree.items(), key=lambda x: x[0].lower()):
        nid = safe_id(name)
        lines.append(f'{nid}["{name}"]')
        # walk children
        if child:
            walk(child, name)
    return "\n".join(lines)

# --- Notion codeblock update (delete existing mermaid codeblocks and append new) ---
def get_page_children(page_id: str) -> List[Dict[str, Any]]:
    children = []
    start_cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=page_id, page_size=100, start_cursor=start_cursor) if start_cursor else notion.blocks.children.list(block_id=page_id, page_size=100)
        children.extend(resp.get("results", []))
        if resp.get("has_more"):
            start_cursor = resp.get("next_cursor")
        else:
            break
    return children

def delete_block(block_id: str):
    """Notion hat kein hard-delete endpoint für blocks; wir 'archive' pages but for blocks we can 'delete' via update(archived=True) on block for blocks that are page types.
       Workaround: replace by appending new children and optionally update blocks to empty text.
       Simpler: call notion.blocks.delete for block_id (note: may not be supported for all block types); fallback: update to empty content.
    """
    try:
        notion.blocks.delete(block_id=block_id)
        return True
    except Exception:
        try:
            # fallback: patch to empty paragraph
            notion.blocks.update(block_id=block_id, **{"paragraph": {"text": []}})
            return True
        except Exception:
            return False

def find_and_remove_mermaid_blocks(page_id: str) -> None:
    # find code blocks with language 'mermaid' among children and delete them
    children = get_page_children(page_id)
    for c in children:
        if c.get("type") == "code":
            code = c.get("code", {})
            lang = code.get("language")
            if lang and lang.lower() == "mermaid":
                bid = c.get("id")
                print("Deleting old mermaid code block:", bid)
                delete_block(bid)

def append_mermaid_block(page_id: str, mermaid_text: str) -> bool:
    block = {
        "object": "block",
        "type": "code",
        "code": {
            "caption": [{"type":"text","text":{"content":"Auto-generated Mermaid diagram"}}],
            "language": "mermaid",
            "text": [{"type":"text","text":{"content": mermaid_text}}]
        }
    }
    try:
        notion.blocks.children.append(block_id=page_id, children=[block])
        return True
    except Exception as e:
        print("Error appending mermaid block:", e)
        return False

# --- persist files locally and optionally commit to repo ---
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def write_files(species_rows: List[Dict[str, Any]], mermaid_text: str):
    ensure_dir(DATA_DIR)
    # Save species json (compact)
    with open(SPECIES_JSON, "w", encoding="utf8") as f:
        json.dump(species_rows, f, ensure_ascii=False, indent=2)
    # Save mermaid
    with open(MERMAID_FILE, "w", encoding="utf8") as f:
        f.write(mermaid_text)

def git_commit_and_push(files: List[str], message: str = "Update species data and mermaid"):
    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN fehlt, commit skipped")
        return
    # Lightweight method: use git CLI (Actions runner has git). Ensure we are in repo root.
    os.system("git config user.email 'github-actions[bot]@users.noreply.github.com'")
    os.system("git config user.name 'github-actions[bot]'")
    for p in files:
        os.system(f"git add {p}")
    os.system(f"git commit -m \"{message}\" || echo 'no changes to commit'")
    # push using token: set remote with token
    # remote is origin; we'll push to the same branch as GH_ACTIONS provided (GITHUB_REF)
    branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
    # create auth remote
    repo = os.getenv("GITHUB_REPOSITORY")
    if repo:
        remote_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
        os.system(f"git remote set-url origin {remote_url}")
        os.system(f"git push origin {branch} || echo 'push failed'")

# --- Main flow ---
def main():
    print("Query Notion database...")
    pages = query_all_database(NOTION_DATABASE_ID)
    print(f"Got {len(pages)} pages")
    rows = [extract_row_properties(p) for p in pages]
    # normalize
    for r in rows:
        for k in RANK_KEYS:
            rn = k.lower()
            r[rn] = normalize_name(r.get(rn))
    # deduplicate
    rows = deduplicate_rows(rows)
    print(f"{len(rows)} rows after deduplication")
    # build tree
    tree = build_tree(rows)
    mermaid = render_mermaid(tree, graph_dir="TD")
    # write data files
    write_files(rows, mermaid)
    print(f"Wrote {SPECIES_JSON} and {MERMAID_FILE}")
    # update Notion page: remove old mermaid blocks and append new one
    print("Removing old mermaid blocks (if any)...")
    find_and_remove_mermaid_blocks(NOTION_PAGE_ID)
    print("Appending new mermaid block...")
    ok = append_mermaid_block(NOTION_PAGE_ID, mermaid)
    if ok:
        print("Notion updated successfully.")
    else:
        print("Failed to update Notion.")
    # optionally commit files back to repo
    if COMMIT_BACK:
        print("Committing data files back to repo...")
        git_commit_and_push([SPECIES_JSON, MERMAID_FILE], message=f"Auto-update species {datetime.utcnow().isoformat()}Z")
    print("Done.")

if __name__ == "__main__":
    main()
