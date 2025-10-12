#!/usr/bin/env python3
# coding: utf-8
"""
update_tree.py - enhanced: rank-labels, styling, Wikipedia links + wiki cache
- Generates data/species.json and data/tree.mmd
- Searches Wikipedia for each taxon name (cached in data/wiki_cache.json)
- Styles phylum (light gray) and species (dark green) with Mermaid classDef
- Adds click links to Wikipedia (and Notion page if available for species)
- Updates a Notion 'code' block by block id with the Mermaid text
"""

import os, json, re, hashlib, time, urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from notion_client import Client
from notion_client.errors import APIResponseError

# ---- Configuration ----
RANK_KEYS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]

DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
MERMAID_FILE = os.path.join(DATA_DIR, "tree.mmd")
WIKI_CACHE_FILE = os.path.join(DATA_DIR, "wiki_cache.json")

# environment
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_BLOCK_ID = os.getenv("NOTION_BLOCK_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1", "true", "yes")

if not NOTION_TOKEN:
    raise SystemExit("ERROR: NOTION_TOKEN fehlt.")
if not NOTION_DATABASE_ID:
    raise SystemExit("ERROR: NOTION_DATABASE_ID fehlt.")
if not NOTION_BLOCK_ID:
    raise SystemExit("ERROR: NOTION_BLOCK_ID fehlt.")

notion = Client(auth=NOTION_TOKEN)

# ---- Helpers ----
def normalize_id(maybe: Optional[str]) -> Optional[str]:
    if not maybe: return None
    s = maybe.strip()
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
    if not s: return "<none>"
    return f"{s[:4]}...{s[-4:]} (len={len(s)})"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_id_for(rank: Optional[str], name: str) -> str:
    # stable id using both rank and name
    base = f"{(rank or '').strip()}||{name.strip()}"
    h = hashlib.sha1(base.encode("utf8")).hexdigest()[:10]
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in base)
    return f"n_{cleaned[:32]}_{h}"

def normalize_name(s: Optional[str]) -> Optional[str]:
    if not s: return None
    return " ".join(s.strip().split()) or None

# ---- Wiki caching & search ----
def load_wiki_cache() -> Dict[str, Optional[str]]:
    ensure_dir(DATA_DIR)
    if os.path.exists(WIKI_CACHE_FILE):
        try:
            with open(WIKI_CACHE_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_wiki_cache(cache: Dict[str, Optional[str]]):
    ensure_dir(DATA_DIR)
    with open(WIKI_CACHE_FILE, "w", encoding="utf8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

WIKI_CACHE = load_wiki_cache()

def wikipedia_search_url(name: str, rank_hint: Optional[str]=None) -> Optional[str]:
    key = f"{(rank_hint or '')}||{name}"
    if key in WIKI_CACHE:
        return WIKI_CACHE.get(key)
    query = name
    if rank_hint:
        query = f"{name} {rank_hint}"
    api = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": query,
        "srlimit": 5,
        "srprop": ""
    }
    try:
        r = requests.get(api, params=params, timeout=10, headers={"User-Agent":"phylo-bot/1.0 (contact)"})
        r.raise_for_status()
        data = r.json()
        hits = data.get("query", {}).get("search", [])
        if hits:
            title = None
            lower_name = name.lower()
            # prefer exact-match title (without parentheses)
            for h in hits:
                t = h.get("title","")
                if t.lower().split(" (")[0] == lower_name:
                    title = t
                    break
            if not title:
                title = hits[0].get("title")
            if title:
                url_title = title.replace(" ", "_")
                url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(url_title)}"
                WIKI_CACHE[key] = url
                save_wiki_cache(WIKI_CACHE)
                time.sleep(0.2)
                return url
    except Exception:
        # network hiccup: don't crash; store None to avoid repeated tries too quickly
        pass
    WIKI_CACHE[key] = None
    save_wiki_cache(WIKI_CACHE)
    return None

# ---- Notion DB access ----
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
    row = {}
    for key in RANK_KEYS:
        p = props.get(key)
        value = None
        if p:
            t = p.get("type")
            if t == "title":
                value = "".join([x.get("plain_text","") for x in p.get("title",[])]) or None
            elif t == "rich_text":
                value = "".join([x.get("plain_text","") for x in p.get("rich_text",[])]) or None
            elif t == "select":
                sel = p.get("select")
                value = sel.get("name") if sel else None
            elif t == "multi_select":
                arr = p.get("multi_select",[])
                value = arr[0]["name"] if arr else None
            else:
                if isinstance(p, dict):
                    for candidate in ("name","plain_text","text"):
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
    tree = {}
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

# ---- Mermaid rendering with classes + clicks ----
def render_mermaid_with_links(tree: Dict[str, Any], rows: List[Dict[str, Any]], graph_dir="TD") -> str:
    """
    Builds a mermaid string with:
     - nodes labeled as "Rank: Name" for higher ranks and Binomial for species
     - classDefs for phylum (light gray) and species (dark green)
     - click handlers linking to Wikipedia (if found) and Notion page for species if available
    """
    species_page_by_name = {}
    for r in rows:
        spec = r.get("species")
        genus = r.get("genus")
        if spec and genus:
            binom = f"{genus} {spec}"
            pid = r.get("_notion_page_id")
            if pid:
                species_page_by_name[binom] = pid

    lines: List[str] = [f"%% Generated {datetime.utcnow().isoformat()}Z", f"graph {graph_dir}"]
    class_nodes = {"phylum": [], "species": []}
    click_lines: List[str] = []
    created_nodes = set()

    def node_label_for(rank: Optional[str], node_name: str) -> str:
        if rank and rank.lower() != "species":
            return f"{rank}: {node_name}"
        else:
            return node_name

    def create_node(rank: Optional[str], node_name: str):
        nid = safe_id_for(rank, node_name)
        if nid in created_nodes:
            return nid
        label = node_label_for(rank, node_name).replace('"', '\\"')
        lines.append(f'{nid}["{label}"]')
        created_nodes.add(nid)
        if rank and rank.lower() == "phylum":
            class_nodes["phylum"].append(nid)
        if rank and rank.lower() == "species":
            class_nodes["species"].append(nid)
        # links
        wiki = wikipedia_search_url(node_name, rank_hint=rank)
        if rank and rank.lower() == "species":
            if node_name in species_page_by_name:
                pid = species_page_by_name[node_name]
                notion_link = f"https://www.notion.so/{pid}"
                click_lines.append(f'click {nid} "{notion_link}" "Open Notion page"')
                return nid
        if wiki:
            click_lines.append(f'click {nid} "{wiki}" "Open Wikipedia"')
        return nid

    def walk(subtree: Dict[str, Any], rank_index: int = 0, parent_info: Optional[Dict[str,str]] = None):
        # parent_info: {"rank": rank_str, "name": name}
        for name, child in sorted(subtree.items(), key=lambda x: x[0].lower()):
            rank = RANK_KEYS[rank_index] if rank_index < len(RANK_KEYS) else None
            # if species and parent_info holds genus, create binomial
            if rank and rank.lower() == "species":
                if parent_info and parent_info.get("rank","").lower() == "genus":
                    node_name = f"{parent_info.get('name')} {name}"
                else:
                    node_name = name
            else:
                node_name = name
            nid = create_node(rank, node_name)
            # connect to parent if present
            if parent_info:
                parent_rank = parent_info.get("rank")
                parent_name = parent_info.get("name")
                parent_nid = safe_id_for(parent_rank, parent_name)
                lines.append(f"{parent_nid} --> {nid}")
            # recurse
            if child:
                walk(child, rank_index=rank_index+1, parent_info={"rank": rank or "", "name": node_name})

    walk(tree, rank_index=0, parent_info=None)

    # Styling
    lines.append("")
    lines.append("%% Styling")
    lines.append("classDef phylum fill:#f0f0f0,stroke:#666,stroke-width:1px;")
    lines.append("classDef species fill:#064e2a,stroke:#022a15,color:#ffffff,stroke-width:1px;")
    if class_nodes["phylum"]:
        lines.append("class " + ",".join(class_nodes["phylum"]) + " phylum;")
    if class_nodes["species"]:
        lines.append("class " + ",".join(class_nodes["species"]) + " species;")

    lines.append("")
    lines.append("%% Click links")
    lines.extend(click_lines)

    return "\n".join(lines)

# ---- Notion block update ----
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
        print("[ERROR] Konnte Block nicht abrufen.")
        return False
    btype = b.get("type")
    print(f"[INFO] Block type from API: {btype}")
    if btype != "code":
        print("[ERROR] Block ist kein 'code' Block.")
        return False

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
    except Exception as e:
        print("[ERROR] notion.blocks.update failed:", e)
        return False

# ---- Files write & commit ----
def write_files(rows, mermaid):
    ensure_dir(DATA_DIR)
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

    try:
        meta = notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
        print("[INFO] Successfully retrieved database metadata. Title preview:",
              meta.get("title", [])[:1])
    except APIResponseError as e:
        print("[ERROR] Cannot retrieve database metadata:", getattr(e, "message", str(e)))

    # Query rows
    try:
        pages = query_all_database(NOTION_DATABASE_ID)
    except Exception as e:
        print("[ERROR] query_all_database failed:", e)
        pages = []
    print(f"[INFO] Got {len(pages)} pages from DB")
    rows = [extract_row_properties(p) for p in pages]
    rows = deduplicate_rows(rows)
    print(f"[INFO] {len(rows)} rows after deduplication")

    # build tree + mermaid
    tree = build_tree(rows)
    mermaid = render_mermaid_with_links(tree, rows, graph_dir="TD")
    write_files(rows, mermaid)

    print("=== Mermaid preview (first 40 lines) ===")
    for i, l in enumerate(mermaid.splitlines()):
        if i >= 40: break
        print(l)
    print("=== end preview ===")

    ok = update_code_block(NOTION_BLOCK_ID, mermaid)
    if not ok:
        print("[WARN] Notion update failed or skipped. Check logs above.")

    attempt_commit_and_push()
    print("=== Script finished ===")

if __name__ == "__main__":
    main()
