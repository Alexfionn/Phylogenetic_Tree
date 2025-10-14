#!/usr/bin/env python3
# coding: utf-8
"""
update_tree.py - rewritten full version

Behavior:
 - Reads rows from a Notion database (columns are taxonomic ranks).
 - Supports many intermediate ranks (Domain .. Subspecies). All are optional.
 - Builds a hierarchical tree attaching each present rank under the nearest filled ancestor.
 - Writes data/species.json and data/tree.mmd (mermaid).
 - Attempts to update a Notion code block (language=mermaid).
 - Verbose logging intended for GitHub Actions output.
"""

from __future__ import annotations
import os
import re
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

try:
    from notion_client import Client
    from notion_client.errors import APIResponseError
except Exception:
    # If running locally without the SDK installed, we'll still be able to dry-run.
    Client = None  # type: ignore
    APIResponseError = Exception  # type: ignore

# ---- Configuration: extended rank keys (optional) ----
RANK_KEYS: List[str] = [
    "Domain",
    "Kingdom",
    "Subkingdom",
    "Infrakingdom",
    "Superphylum",
    "Phylum",
    "Subphylum",
    "Infraphylum",
    "Superclass",
    "Class",
    "Subclass",
    "Infraclass",
    "Superorder",
    "Order",
    "Suborder",
    "Infraorder",
    "Superfamily",
    "Family",
    "Subfamily",
    "Tribe",
    "Subtribe",
    "Genus",
    "Subgenus",
    "Species",
    "Subspecies",
]

# ---- Paths ----
DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
MERMAID_FILE = os.path.join(DATA_DIR, "tree.mmd")

# ---- Env / Secrets ----
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_BLOCK_ID = os.getenv("NOTION_BLOCK_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1", "true", "yes")

# Minimal sanity checks (fail fast so GH Actions show clear logs)
if not NOTION_TOKEN:
    raise SystemExit("ERROR: NOTION_TOKEN fehlt.")
if not NOTION_DATABASE_ID:
    raise SystemExit("ERROR: NOTION_DATABASE_ID fehlt.")
if not NOTION_BLOCK_ID:
    raise SystemExit("ERROR: NOTION_BLOCK_ID fehlt.")

# ---- Notion client ----
notion = Client(auth=NOTION_TOKEN) if Client is not None else None

# ---- Helpers ----
def normalize_id(maybe: Optional[str]) -> Optional[str]:
    if not maybe:
        return None
    s = maybe.strip()
    # Accept URLs like https://www.notion.so/.../xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    if "#" in s:
        s = s.split("#")[-1]
    if "?" in s:
        s = s.split("?")[0]
    s = s.replace("-", "")
    m = re.search(r"([0-9a-fA-F]{32})", s)
    return m.group(1) if m else None

NOTION_DATABASE_ID = normalize_id(NOTION_DATABASE_ID) or NOTION_DATABASE_ID
NOTION_BLOCK_ID = normalize_id(NOTION_BLOCK_ID) or NOTION_BLOCK_ID

def pretty_preview(s: Optional[str]) -> str:
    if not s:
        return "<none>"
    return f"{s[:4]}...{s[-4:]} (len={len(s)})"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_id_for(rank: str, name: str) -> str:
    """
    Generate a deterministic safe id that is unique per (rank,name).
    This avoids collisions if the same taxon name appears at different ranks.
    """
    base = f"{rank}:{name}"
    h = hashlib.sha1(base.encode("utf8")).hexdigest()[:12]
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in name)[:30]
    rclean = "".join(ch if ch.isalnum() else "_" for ch in rank)[:10]
    return f"n_{rclean}_{cleaned}_{h}"

def normalize_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s2 = " ".join(s.strip().split())
    return s2 or None

# ---- Notion operations ----
def query_all_database(database_id: str) -> List[Dict[str, Any]]:
    print(f"[INFO] Querying database {pretty_preview(database_id)} ...")
    if notion is None:
        raise RuntimeError("Notion SDK not available in this environment.")
    results: List[Dict[str, Any]] = []
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
    """
    Extracts all configured ranks from a page's properties.
    Returns a dict with lowercased rank keys and the original Notion page id.
    """
    props = page.get("properties", {}) or {}
    row: Dict[str, Optional[str]] = {}
    for key in RANK_KEYS:
        p = props.get(key)
        value: Optional[str] = None
        if p:
            t = p.get("type")
            # The Notion schema may vary; try common shapes
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
                # fallback: try to find common keys
                if isinstance(p, dict):
                    for candidate in ("name", "plain_text", "text"):
                        if candidate in p and isinstance(p[candidate], str):
                            value = p[candidate]
                            break
        row[key.lower()] = normalize_name(value)
    row["_notion_page_id"] = page.get("id")
    return row

def deduplicate_rows(rows: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    """
    Remove exact-duplicate taxonomic rows (based on the tuple of all configured ranks).
    """
    seen = set()
    out: List[Dict[str, Optional[str]]] = []
    for r in rows:
        key = tuple(r.get(k.lower()) or "" for k in RANK_KEYS)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# Tree structure:
# Each node keyed by (rank, name) string key; stores children dict.
# We internally use a dict-of-dicts structure.
NodeKey = str  # formatted as "Rank:Name"
Tree = Dict[NodeKey, Dict[str, Any]]

def node_key(rank: str, name: str) -> NodeKey:
    return f"{rank}:{name}"

def build_tree(rows: List[Dict[str, Optional[str]]]) -> Dict[str, Any]:
    """
    Build a tree where each named taxon becomes a node (rank+name).
    Each row causes a chain of nodes to be created for all present ranks,
    but nodes are attached under the nearest previous (higher) rank present in that row.
    """
    # root will be a dict mapping node_key -> children
    root: Dict[NodeKey, Dict[str, Any]] = {"__root__": {}}
    # map to hold direct children dicts for each node_key (so we can add children easily)
    children_map: Dict[NodeKey, Dict[str, Any]] = { "__root__": root["__root__"] }

    for r in rows:
        last_parent = "__root__"
        # iterate ranks in order; whenever a rank has a value, create/ensure node under last_parent
        for rank in RANK_KEYS:
            val = r.get(rank.lower())
            if not val:
                # skip empty ranks but keep last_parent unchanged
                continue
            k = node_key(rank, val)
            # if k not present in children_map -> create
            if k not in children_map:
                children_map[k] = {}
            # attach k under last_parent's children if not already attached
            parent_children = children_map[last_parent]
            if k not in parent_children:
                parent_children[k] = children_map[k]
            # move down
            last_parent = k
    return root["__root__"]

def render_mermaid(tree: Dict[str, Any], graph_dir="TD", show_rank: bool = False) -> str:
    """
    Render the internal tree (dict of node_key->children dict) to mermaid.
    Uses safe ids and labels nodes with the taxon name only (optionally with rank).
    """
    lines: List[str] = [f"%% Generated {datetime.utcnow().isoformat()}Z", f"graph {graph_dir}"]

    created_nodes: set = set()

    def split_key(k: NodeKey) -> Tuple[str, str]:
        # k formatted as "Rank:Name"
        if ":" in k:
            rank, name = k.split(":", 1)
        else:
            rank, name = ("", k)
        return rank, name

    def walk(sub: Dict[str, Any], parent_key: Optional[NodeKey] = None):
        for k, child in sorted(sub.items(), key=lambda it: split_key(it[0])[1].lower()):
            rank, name = split_key(k)
            nid = safe_id_for(rank, name)
            # label is name, optionally append rank in parentheses
            label = name if not show_rank else f"{name} ({rank})"
            if k not in created_nodes:
                # create node line
                # escape quotes in label
                safe_label = label.replace('"', '\\"')
                lines.append(f'{nid}["{safe_label}"]')
                created_nodes.add(k)
            if parent_key:
                parent_rank, parent_name = split_key(parent_key)
                parent_nid = safe_id_for(parent_rank, parent_name)
                lines.append(f"{parent_nid} --> {nid}")
            # recurse
            if child:
                walk(child, k)

    # top-level nodes are keys of tree (which is a dict mapping node_key->children)
    if not tree:
        lines.append("%% (empty)")
        return "\n".join(lines)

    walk(tree, None)
    return "\n".join(lines)

def retrieve_block(block_id: str) -> Optional[Dict[str, Any]]:
    try:
        if notion is None:
            raise RuntimeError("Notion client not available")
        b = notion.blocks.retrieve(block_id=block_id)
        return b
    except APIResponseError as e:
        print("[ERROR] notion.blocks.retrieve failed:", getattr(e, "message", str(e)))
        return None
    except Exception as e:
        print("[ERROR] notion.blocks.retrieve exception:", e)
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
    # Notion code blocks accept 'rich_text' or 'text' depending on SDK/version.
    # Try 'code': {'rich_text': [...], 'language': 'mermaid'}
    payload = {
        "code": {
            "rich_text": [{"type": "text", "text": {"content": mermaid_text}}],
            "language": "mermaid",
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
def write_files(rows: List[Dict[str, Optional[str]]], mermaid: str):
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
    # quick DB metadata check
    try:
        if notion is not None:
            meta = notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
            print("[INFO] Successfully retrieved database metadata. Title preview:",
                  meta.get("title", [])[:1])
        else:
            print("[WARN] Notion client not available; running in dry mode.")
    except APIResponseError as e:
        print("[ERROR] Cannot retrieve database metadata:", getattr(e, "message", str(e)))
        print("→ Bitte prüfen: ist NOTION_DATABASE_ID korrekt und ist die Integration eingeladen?")
    except Exception as e:
        print("[WARN] Exception retrieving database metadata:", e)

    # query rows
    try:
        pages = query_all_database(NOTION_DATABASE_ID) if notion is not None else []
    except Exception as e:
        print("[ERROR] query_all_database failed:", e)
        pages = []
    print(f"[INFO] Got {len(pages)} pages from DB")

    rows = [extract_row_properties(p) for p in pages]
    rows = deduplicate_rows(rows)
    print(f"[INFO] {len(rows)} rows after deduplication")

    tree = build_tree(rows)
    mermaid = render_mermaid(tree, graph_dir="TD", show_rank=False)

    # always write files
    write_files(rows, mermaid)

    # mermaid preview
    print("=== Mermaid preview (first 40 lines) ===")
    for i, l in enumerate(mermaid.splitlines()):
        if i >= 40:
            break
        print(l)
    print("=== end preview ===")

    ok = False
    try:
        ok = update_code_block(NOTION_BLOCK_ID, mermaid)
    except Exception as e:
        print("[ERROR] update_code_block raised exception:", e)
        ok = False

    if not ok:
        print("[WARN] Notion update failed or skipped. Check logs above.")

    attempt_commit_and_push()
    print("=== Script finished ===")

if __name__ == "__main__":
    main()