#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_tree.py

Version: block-id variant

Was es macht:
- Liest alle Zeilen aus einer Notion-Database (jede Zeile = 1 Spezies)
- Reinigt / normalisiert / dedupliziert die Daten
- Speichert eine persistente JSON-Datei unter data/species.json
- Erzeugt Mermaid-Quelltext unter data/tree.mmd
- Aktualisiert **einen vorhandenen Code-Block** in Notion (Block-ID) und ersetzt seinen Inhalt
  durch das neue Mermaid (prüft vorher, ob Block ein 'code' Block ist)
- Optional: commit/ push der data/ Dateien (COMMIT_BACK)

Erforderliche Umgebungsvariablen (als GitHub Secrets/Env):
- NOTION_TOKEN            (Integration token)
- NOTION_DATABASE_ID      (Database ID mit den Spezies)
- NOTION_BLOCK_ID         (die ID des Code-Blocks, den du ersetzen willst)
- GITHUB_TOKEN            (optional, für push zurück ins Repo)
- COMMIT_BACK             (optional: "true"/"false")

Benötigte Pakete:
  pip install notion-client requests python-dotenv
"""

import os
import json
import hashlib
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from notion_client import Client
from notion_client.errors import APIResponseError

# -------------------------
# Konfiguration / Schema
# -------------------------
# Name der Ränge, exakt wie in deiner Notion-Database-Properties (case-sensitive)
RANK_KEYS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]

# Pfade
DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
MERMAID_FILE = os.path.join(DATA_DIR, "tree.mmd")

# Notion-Paginierung
PAGE_SIZE = 100

# -------------------------
# Env / Secrets lesen
# -------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_BLOCK_ID = os.getenv("NOTION_BLOCK_ID")  # <-- Wir verwenden die BLOCK ID hier
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1", "true", "yes")

if not NOTION_TOKEN:
    raise SystemExit("Fehler: NOTION_TOKEN fehlt. Setze das Secret/Env.")
if not NOTION_DATABASE_ID:
    raise SystemExit("Fehler: NOTION_DATABASE_ID fehlt. Setze das Secret/Env.")
if not NOTION_BLOCK_ID:
    raise SystemExit("Fehler: NOTION_BLOCK_ID fehlt. Setze das Secret/Env (die ID des Codeblocks, nicht die Seite).")

# -------------------------
# Notion Client
# -------------------------
notion = Client(auth=NOTION_TOKEN)

# -------------------------
# Hilfsfunktionen: ID Normalisierung
# -------------------------
def normalize_id(maybe_id_or_link: Optional[str]) -> Optional[str]:
    """
    Extrahiert aus einem kopierten Notion-Link oder roher ID eine saubere 32-hex ID (ohne '-').
    Wenn ein Fragment (#...) vorhanden ist, wird der Fragment-Teil (Block) bevorzugt.
    """
    if not maybe_id_or_link:
        return None
    s = maybe_id_or_link.strip()
    # If there's a #fragment, prefer fragment (block link)
    if "#" in s:
        s = s.split("#")[-1]
    # remove query params if a full url was pasted
    if "?" in s:
        s = s.split("?")[0]
    # find 32 hex chars
    m = re.search(r'([0-9a-fA-F]{32})', s)
    if m:
        return m.group(1)
    # find UUID with hyphens
    m = re.search(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})', s)
    if m:
        return m.group(1).replace('-', '')
    # fallback: if it's only hex/hyphen-ish, normalize
    clean = s.replace('-', '')
    if re.fullmatch(r'[0-9a-fA-F]{32}', clean):
        return clean
    return None

NOTION_DATABASE_ID = normalize_id(NOTION_DATABASE_ID)
NOTION_BLOCK_ID = normalize_id(NOTION_BLOCK_ID)

if not NOTION_DATABASE_ID:
    raise SystemExit("Ungültige NOTION_DATABASE_ID. Bitte die reine Database-ID (32 hex chars) als Secret setzen.")
if not NOTION_BLOCK_ID:
    raise SystemExit("Ungültige NOTION_BLOCK_ID. Bitte die reine Block-ID (32 hex chars) als Secret setzen.")

# -------------------------
# Notion: Database lesen (paginierend)
# -------------------------
def query_all_database(database_id: str) -> List[Dict[str, Any]]:
    results = []
    start_cursor = None
    while True:
        kwargs = {"database_id": database_id, "page_size": PAGE_SIZE}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor
        resp = notion.databases.query(**kwargs)
        results.extend(resp.get("results", []))
        if resp.get("has_more"):
            start_cursor = resp.get("next_cursor")
        else:
            break
    return results

# -------------------------
# Properties extrahieren
# -------------------------
def extract_row_properties(page: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = page.get("properties", {})
    row: Dict[str, Optional[str]] = {}
    for key in RANK_KEYS:
        p = props.get(key)
        value = None
        if not p:
            row[key.lower()] = None
            continue
        t = p.get("type")
        # häufige Typen: title, rich_text, select, multi_select
        if t == "title":
            title_arr = p.get("title", [])
            value = "".join([x.get("plain_text", "") for x in title_arr]).strip() or None
        elif t == "rich_text":
            value = "".join([x.get("plain_text", "") for x in p.get("rich_text", [])]).strip() or None
        elif t == "select":
            sel = p.get("select")
            value = sel.get("name") if sel else None
        elif t == "multi_select":
            arr = p.get("multi_select", [])
            value = arr[0]["name"] if arr else None
        else:
            # fallback: try reading 'name' or plain string
            try:
                # Some types might embed a 'name' field
                if isinstance(p, dict) and "name" in p:
                    value = p.get("name")
            except Exception:
                value = None
        if isinstance(value, str):
            value = value.strip() or None
        row[key.lower()] = value
    # Title fallback
    name_prop = props.get("Name") or props.get("Title") or None
    if name_prop and name_prop.get("type") == "title":
        row["name"] = "".join([x.get("plain_text", "") for x in name_prop.get("title", [])]).strip() or None
    else:
        row["name"] = None
    row["_notion_page_id"] = page.get("id")
    row["_raw"] = page
    return row

# -------------------------
# Normalisierung / Dedup
# -------------------------
def normalize_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(s.strip().split())
    return s or None

def deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        key = tuple([normalize_name(r.get(k.lower())) or "" for k in RANK_KEYS])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# -------------------------
# Build nested tree
# -------------------------
def build_tree(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    tree: Dict[str, Any] = {}
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

# -------------------------
# Mermaid Rendering
# -------------------------
def safe_id(name: str) -> str:
    h = hashlib.sha1(name.encode("utf8")).hexdigest()[:10]
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in name)
    return f"n_{cleaned[:25]}_{h}"

def render_mermaid(tree: Dict[str, Any], graph_dir: str = "TD") -> str:
    header = f"%% Mermaid generated on {datetime.utcnow().isoformat()}Z"
    lines = [header, f"graph {graph_dir}"]
    def walk(subtree, parent_name: Optional[str] = None):
        for name, child in sorted(subtree.items(), key=lambda x: x[0].lower()):
            nid = safe_id(name)
            label = name.replace('"', '\\"')
            lines.append(f'{nid}["{label}"]')
            if parent_name:
                lines.append(f"{safe_id(parent_name)} --> {nid}")
            if child:
                walk(child, name)
    if not tree:
        return "\n".join(lines + ["%% (empty tree)"])
    # Add top-level and descend
    for name, subtree in sorted(tree.items(), key=lambda x: x[0].lower()):
        nid = safe_id(name)
        lines.append(f'{nid}["{name}"]')
        if subtree:
            walk(subtree, name)
    return "\n".join(lines)

# -------------------------
# File IO / commit
# -------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_files(species_rows: List[Dict[str, Any]], mermaid_text: str):
    ensure_dir(DATA_DIR)
    with open(SPECIES_JSON, "w", encoding="utf8") as f:
        json.dump(species_rows, f, ensure_ascii=False, indent=2)
    with open(MERMAID_FILE, "w", encoding="utf8") as f:
        f.write(mermaid_text)

def git_commit_and_push(files: List[str], message: str = "Auto update species data"):
    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN fehlt; Commit übersprungen.")
        return
    # Lightweight: use git CLI available in Actions runner
    os.system("git config user.email 'github-actions[bot]@users.noreply.github.com'")
    os.system("git config user.name 'github-actions[bot]'")
    for p in files:
        os.system(f"git add {p}")
    os.system(f'git commit -m "{message}" || echo "no changes to commit"')
    repo = os.getenv("GITHUB_REPOSITORY")
    branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
    if repo:
        remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
        os.system(f"git remote set-url origin {remote}")
        os.system(f"git push origin {branch} || echo 'push failed'")

# -------------------------
# Notion: Update a given code block
# -------------------------
def retrieve_block(block_id: str) -> Dict[str, Any]:
    norm = normalize_id(block_id)
    if not norm:
        raise SystemExit(f"Ungültige Block-ID: {block_id!r}")
    try:
        b = notion.blocks.retrieve(block_id=norm)
        return b
    except APIResponseError as e:
        print("Notion API Error beim retrieve block:", getattr(e, "message", str(e)))
        raise

def update_code_block(block_id: str, mermaid_text: str) -> bool:
    norm = normalize_id(block_id)
    if not norm:
        print("Ungültige Block-ID beim update.")
        return False
    # retrieve & verify type
    try:
        block = retrieve_block(norm)
    except Exception as e:
        print("Fehler beim Abrufen des Blocks:", e)
        return False
    btype = block.get("type")
    if btype != "code":
        print(f"Der Block ({norm}) ist kein 'code' Block, sondern type='{btype}'.")
        print("Bitte setze NOTION_BLOCK_ID auf die ID eines 'code' Blocks (Copy link to block -> ID nach #).")
        return False
    # Prepare code payload: Notion expects code: { text: [...], language: "mermaid" }
    code_payload = {
        "code": {
            "text": [{"type": "text", "text": {"content": mermaid_text}}],
            "language": "mermaid"
        }
    }
    try:
        notion.blocks.update(block_id=norm, **code_payload)
        print("Mermaid-Codeblock erfolgreich aktualisiert.")
        return True
    except APIResponseError as e:
        print("Fehler beim Update des Blocks:", getattr(e, "message", str(e)))
        return False
    except Exception as e:
        print("Unbekannter Fehler beim Update:", e)
        return False

# -------------------------
# Main Flow
# -------------------------
def main():
    print("Query Notion database...")
    pages = query_all_database(NOTION_DATABASE_ID)
    print(f"Got {len(pages)} pages")
    rows = [extract_row_properties(p) for p in pages]
    # normalize all rank fields
    for r in rows:
        for k in RANK_KEYS:
            rn = k.lower()
            r[rn] = normalize_name(r.get(rn))
    rows = deduplicate_rows(rows)
    print(f"{len(rows)} rows after deduplication")
    tree = build_tree(rows)
    mermaid = render_mermaid(tree, graph_dir="TD")
    write_files(rows, mermaid)
    print(f"Wrote {SPECIES_JSON} and {MERMAID_FILE}")

    # Update the provided code block directly
    print("Updating the provided NOTION_BLOCK_ID with new mermaid text...")
    ok = update_code_block(NOTION_BLOCK_ID, mermaid)
    if not ok:
        print("Update fehlgeschlagen. Keine Änderungen an Notion vorgenommen.")
    else:
        print("Notion block updated.")

    # Optionally commit data files back to repo
    if COMMIT_BACK:
        print("COMMIT_BACK ist true -> versuche zu committen und pushen...")
        git_commit_and_push([SPECIES_JSON, MERMAID_FILE], message=f"Auto-update species {datetime.utcnow().isoformat()}Z")

    print("Done.")

if __name__ == "__main__":
    main()
