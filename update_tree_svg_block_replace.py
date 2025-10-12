#!/usr/bin/env python3
# coding: utf-8
"""
update_tree_svg_block_replace.py

Erzeugt ein taxonomy-basierendes Newick-Tree-SVG (wie zuvor) und ersetzt einen vorhandenen
Notion-Block (NOTION_BLOCK_ID) durch einen Image-Block, der auf die generierte SVG zeigt.

Wichtig:
- Wenn COMMIT_BACK=true: Script commit/pusht data/tree.svg ins Repo und versucht raw.githubusercontent URL zu verwenden.
- Wenn Repo privat oder du willst eigene URL nutzen: setze IMAGE_URL env var (öffentliche URL), dann wird diese verwendet.
"""

import os, json, re
from io import StringIO
from datetime import datetime
from typing import List, Dict, Any, Optional

# plotting / phylo
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from Bio import Phylo

# Notion client
from notion_client import Client
from notion_client.errors import APIResponseError

# -------------------------
# Konfiguration (anpassen falls nötig)
# -------------------------
RANK_KEYS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]

DATA_DIR = "data"
SPECIES_JSON = os.path.join(DATA_DIR, "species.json")
TREE_NWK = os.path.join(DATA_DIR, "tree.nwk")
TREE_SVG = os.path.join(DATA_DIR, "tree.svg")

# Env / Secrets
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_BLOCK_ID = os.getenv("NOTION_BLOCK_ID")     # der Block, den wir ersetzen wollen (muss existieren)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
COMMIT_BACK = os.getenv("COMMIT_BACK", "false").lower() in ("1","true","yes")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")  # owner/repo
GITHUB_REF = os.getenv("GITHUB_REF", "refs/heads/main")
# optional: set an explicit IMAGE_URL (publicly reachable) to use instead of raw.githubusercontent (useful for private repos)
IMAGE_URL_OVERRIDE = os.getenv("IMAGE_URL")

# minimal sanity
if not NOTION_TOKEN or not NOTION_DATABASE_ID:
    raise SystemExit("Fehler: Setze NOTION_TOKEN und NOTION_DATABASE_ID als Environment/Secrets.")
if not NOTION_BLOCK_ID:
    raise SystemExit("Fehler: Setze NOTION_BLOCK_ID (die Block-ID, die ersetzt werden soll).")

notion = Client(auth=NOTION_TOKEN)

# -------------------------
# Hilfsfunktionen
# -------------------------
def normalize_id(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip()
    if "#" in t:
        t = t.split("#")[-1]
    if "?" in t:
        t = t.split("?")[0]
    t = t.replace("-", "")
    m = re.search(r'([0-9a-fA-F]{32})', t)
    return m.group(1) if m else None

NOTION_DATABASE_ID = normalize_id(NOTION_DATABASE_ID)
NOTION_BLOCK_ID = normalize_id(NOTION_BLOCK_ID)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def pretty_preview(s: Optional[str]) -> str:
    if not s: return "<none>"
    return f"{s[:4]}...{s[-4:]} (len={len(s)})"

# -------------------------
# Notion DB lesen
# -------------------------
def query_all_database(database_id: str) -> List[Dict[str, Any]]:
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
                    for cand in ("name","plain_text","text"):
                        if cand in p and isinstance(p[cand], str):
                            value = p[cand]
                            break
        if isinstance(value, str):
            value = " ".join(value.strip().split())
        row[key.lower()] = value
    row["_notion_page_id"] = page.get("id")
    return row

def deduplicate_rows(rows):
    seen = set()
    out = []
    for r in rows:
        key = tuple([r.get(k.lower()) or "" for k in RANK_KEYS])
        if key in seen: continue
        seen.add(key)
        out.append(r)
    return out

# -------------------------
# Baumspezifikation -> Newick / Rendering
# -------------------------
def build_tree(rows):
    tree = {}
    for r in rows:
        node = tree
        for rank in RANK_KEYS:
            v = r.get(rank.lower())
            if not v: break
            if v not in node:
                node[v] = {}
            node = node[v]
    return tree

def esc_label(s: str) -> str:
    if re.search(r"[,\(\):;\s\'\"]", s):
        s2 = s.replace("'", "''")
        return f"'{s2}'"
    return s

def nested_to_newick(subtree, rank_index=0):
    parts = []
    for name, child in sorted(subtree.items(), key=lambda x: x[0].lower()):
        if child:
            inner = nested_to_newick(child, rank_index+1)
            rank = RANK_KEYS[rank_index] if rank_index < len(RANK_KEYS) else None
            if rank:
                label = f"{rank}:{name}"
            else:
                label = name
            parts.append(f"({inner}){esc_label(label)}")
        else:
            leaf = name.replace(" ", "_")
            parts.append(esc_label(leaf))
    return ",".join(parts)

def build_newick(tree):
    inner = nested_to_newick(tree, 0)
    return f"({inner});"

def render_newick_to_svg(newick_str, svg_path):
    # read string into tree
    tree = Phylo.read(StringIO(newick_str), "newick")
    nterm = len(tree.get_terminals())
    width = max(8, min(40, nterm * 0.2 + 5))
    height = max(6, min(200, nterm * 0.25 + 2))

    fig = plt.figure(figsize=(width, height), dpi=150)
    ax = fig.add_subplot(1,1,1)
    ax.set_axis_off()
    Phylo.draw(tree, axes=ax, do_show=False, show_confidence=False)
    ax.set_title("Taxonomy tree (taxonomy-based cladogram)", fontsize=14)
    ensure_dir(os.path.dirname(svg_path) or ".")
    fig.savefig(svg_path, format="svg", bbox_inches="tight")
    plt.close(fig)

# -------------------------
# Git commit/push helper (optional)
# -------------------------
def git_commit_and_push(file_paths: List[str], message: str = "Auto: update tree SVG"):
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        print("[WARN] GITHUB_TOKEN or GITHUB_REPOSITORY missing -> skip commit/push")
        return False
    os.system("git config user.email 'github-actions[bot]@users.noreply.github.com'")
    os.system("git config user.name 'github-actions[bot]'")
    for p in file_paths:
        os.system(f"git add {p} || true")
    os.system(f'git commit -m "{message}" || echo "no changes to commit"')
    branch = GITHUB_REF.split("/")[-1]
    remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{GITHUB_REPOSITORY}.git"
    os.system(f"git remote set-url origin {remote}")
    os.system(f"git push origin {branch} || echo 'push failed'")
    return True

def raw_github_url(path: str) -> Optional[str]:
    if not GITHUB_REPOSITORY:
        return None
    branch = GITHUB_REF.split("/")[-1]
    p = path.lstrip("/")
    return f"https://raw.githubusercontent.com/{GITHUB_REPOSITORY}/{branch}/{p}"

# -------------------------
# Notion: replace block with image
# -------------------------
def retrieve_block(block_id: str):
    try:
        return notion.blocks.retrieve(block_id=block_id)
    except APIResponseError as e:
        print("[ERROR] notion.blocks.retrieve failed:", getattr(e, "message", str(e)))
        return None

def replace_block_with_image(block_id: str, image_url: str, caption: str = "") -> bool:
    """
    Replaces the given block by deleting it and appending an image block under the same parent.
    Note: appended image will be at the end of the parent's children list.
    """
    b = retrieve_block(block_id)
    if not b:
        print("[ERROR] Could not retrieve block to replace.")
        return False
    parent = b.get("parent", {})
    parent_id = parent.get("page_id") or parent.get("block_id")
    if not parent_id:
        print("[ERROR] Parent ID not found for block; cannot append image.")
        return False

    # delete original block (best-effort)
    try:
        notion.blocks.delete(block_id=block_id)
        print(f"[INFO] Deleted original block {block_id}")
    except Exception as e:
        print("[WARN] Could not delete block; will attempt to append image anyway:", e)

    # prepare image block (external)
    block = {
        "object": "block",
        "type": "image",
        "image": {
            "type": "external",
            "external": {"url": image_url}
        }
    }
    if caption:
        block["image"]["caption"] = [{"type":"text","text":{"content": caption}}]

    try:
        notion.blocks.children.append(block_id=parent_id, children=[block])
        print(f"[OK] Appended image block to parent {parent_id} (replaced block {block_id})")
        return True
    except Exception as e:
        print("[ERROR] Failed to append image block:", e)
        return False

# -------------------------
# Main
# -------------------------
def main():
    print("=== update_tree_svg_block_replace start ===")
    print("[DEBUG] NOTION_DATABASE_ID:", pretty_preview(NOTION_DATABASE_ID))
    print("[DEBUG] NOTION_BLOCK_ID   :", pretty_preview(NOTION_BLOCK_ID))
    print("[DEBUG] COMMIT_BACK:", COMMIT_BACK)

    # 1) read db
    try:
        pages = query_all_database(NOTION_DATABASE_ID)
    except Exception as e:
        print("[ERROR] query_all_database failed:", e)
        return
    print(f"[INFO] Got {len(pages)} pages from DB")
    rows = [extract_row_properties(p) for p in pages]
    rows = deduplicate_rows(rows)
    print(f"[INFO] {len(rows)} rows after dedup")

    ensure_dir(DATA_DIR)
    with open(SPECIES_JSON, "w", encoding="utf8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Wrote {SPECIES_JSON}")

    # 2) build newick + write
    tree = build_tree(rows)
    newick = build_newick(tree)
    with open(TREE_NWK, "w", encoding="utf8") as f:
        f.write(newick)
    print(f"[INFO] Wrote Newick ({len(newick)} chars) to {TREE_NWK}")

    # 3) render svg
    try:
        render_newick_to_svg(newick, TREE_SVG)
        print(f"[INFO] Rendered SVG to {TREE_SVG}")
    except Exception as e:
        print("[ERROR] render_newick_to_svg failed:", e)
        return

    # 4) obtain public image URL
    image_url = None
    if IMAGE_URL_OVERRIDE:
        image_url = IMAGE_URL_OVERRIDE
        print("[INFO] Using IMAGE_URL_OVERRIDE:", image_url)
    elif COMMIT_BACK:
        ok = git_commit_and_push([TREE_SVG, TREE_NWK, SPECIES_JSON], message=f"Auto update tree {datetime.utcnow().isoformat()}Z")
        if ok:
            url = raw_github_url(TREE_SVG)
            if url:
                image_url = url
                print("[INFO] Using raw GitHub URL for image:", image_url)
            else:
                print("[WARN] raw_github_url could not be formed; maybe GITHUB env missing")
        else:
            print("[WARN] commit/push didn't work; no image URL available")
    else:
        print("[INFO] COMMIT_BACK=false and no IMAGE_URL_OVERRIDE -> produced files locally, but no public URL to upload to Notion")

    if not image_url:
        print("[ERROR] No public image URL available to insert into Notion. Either set IMAGE_URL env or set COMMIT_BACK=true and ensure repo is public.")
        return

    # 5) replace block with image
    caption = f"Auto-generated taxonomy SVG ({datetime.utcnow().isoformat()}Z)"
    ok = replace_block_with_image(NOTION_BLOCK_ID, image_url, caption=caption)
    if not ok:
        print("[ERROR] Failed to replace notion block with image.")
    else:
        print("[OK] Notion block replaced with image.")

    print("=== done ===")

if __name__ == "__main__":
    main()
