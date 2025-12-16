import os, json, time, sqlite3, hashlib
from pathlib import Path
from PIL import Image

# 任意：無くても動く
try:
    from safetensors import safe_open
except Exception:
    safe_open = None

LORA_ROOT = Path(r"E:\AIDirectory\EasyReforge\Model\Lora")  # 変える
DB_PATH   = LORA_ROOT / "__lora_catalog.sqlite"
THUMB_DIR = LORA_ROOT / "__thumbs__"
THUMB_SIZE = 320
THUMB_QUALITY = 70

def sha256_file(path: Path, chunk=1024*1024):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()

def read_text_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def read_safetensors_metadata(path: Path):
    if safe_open is None:
        return {}
    try:
        with safe_open(str(path), framework="pt", device="cpu") as f:
            return f.metadata() or {}
    except Exception:
        return {}

def ensure_thumb(png_path: Path, sha: str):
    THUMB_DIR.mkdir(parents=True, exist_ok=True)
    out = THUMB_DIR / f"{sha}.webp"
    if out.exists():
        return out
    try:
        img = Image.open(png_path).convert("RGB")
        img.thumbnail((THUMB_SIZE, THUMB_SIZE))
        img.save(out, "WEBP", quality=THUMB_QUALITY, method=6)
        return out
    except Exception:
        return None

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;

    CREATE TABLE IF NOT EXISTS lora (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      path TEXT UNIQUE NOT NULL,
      sha256 TEXT,
      base TEXT,
      kind TEXT,
      trigger TEXT,
      notes TEXT,
      preview_full TEXT,
      preview_thumb TEXT,
      info_json TEXT,
      meta_json TEXT,
      civitai_id TEXT,
      file_size INTEGER,
      mtime INTEGER,
      scanned_at INTEGER,
      title TEXT
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS lora_fts
    USING fts5(name, trigger, notes, tags_text, title);

    CREATE TABLE IF NOT EXISTS tag (
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      title TEXT
    );

    CREATE TABLE IF NOT EXISTS lora_tag (
      lora_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL,
      weight REAL DEFAULT 1.0,
      PRIMARY KEY(lora_id, tag_id)
    );

    CREATE INDEX IF NOT EXISTS idx_lora_sha ON lora(sha256);
    CREATE INDEX IF NOT EXISTS idx_lora_mtime ON lora(mtime);
    """)
#    conn.commit()

def upsert_lora(conn, row):
    conn.execute("""
    INSERT INTO lora(
      name,path,sha256,base,kind,trigger,notes,preview_full,preview_thumb,
      info_json,meta_json,civitai_id,file_size,mtime,scanned_at,title
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(path) DO UPDATE SET
      name=excluded.name,
      sha256=excluded.sha256,
      base=excluded.base,
      trigger=excluded.trigger,
      preview_full=excluded.preview_full,
      preview_thumb=excluded.preview_thumb,
      info_json=excluded.info_json,
      meta_json=excluded.meta_json,
      civitai_id=excluded.civitai_id,
      file_size=excluded.file_size,
      mtime=excluded.mtime,
      scanned_at=excluded.scanned_at
    """, row)
#    conn.commit()
    return conn.execute("SELECT id FROM lora WHERE path=?", (row[1],)).fetchone()[0]

def set_tags(conn, lora_id: int, tags: list[str]):
    conn.execute("DELETE FROM lora_tag WHERE lora_id=?", (lora_id,))
    for t in tags:
        t = (t or "").strip()
        if not t: 
            continue
        
        conn.execute("INSERT OR IGNORE INTO tag(name, title) VALUES(?, ?)", (t,t))        
        tag_id = conn.execute("SELECT id FROM tag WHERE name=?", (t,)).fetchone()[0]
        conn.execute("INSERT OR REPLACE INTO lora_tag(lora_id, tag_id, weight) VALUES(?,?,1.0)", (lora_id, tag_id))
#    conn.commit()

def fts_exists(conn, lora_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM lora_fts WHERE rowid=? LIMIT 1", (lora_id,)).fetchone()
    return row is not None

def sync_fts(conn, lora_id: int):
    name, trigger, notes, title = conn.execute("SELECT name, trigger, notes, title FROM lora WHERE id=?", (lora_id,)).fetchone()
    tags_text = conn.execute("""
      SELECT COALESCE(group_concat(tag.name, ' '), '')
      FROM lora_tag JOIN tag ON tag.id = lora_tag.tag_id
      WHERE lora_tag.lora_id=?
    """, (lora_id,)).fetchone()[0]
    conn.execute(
        "INSERT OR REPLACE INTO lora_fts(rowid, name, trigger, notes, tags_text, title) VALUES(?,?,?,?,?,?)",
        (lora_id, name or "", trigger or "", notes or "", tags_text or "", title or "")
    )
#    conn.commit()

def find_preview_png(stem: Path):
    p = stem.with_name(stem.name + ".preview.png")
    if p.exists():
        return p

    p = stem.with_name(stem.name + ".png")
    if p.exists():
        return p

    return None

BATCH = 100

def bump_commit(conn, pending: int, batch: int = BATCH) -> int:
    pending += 1
    if pending >= batch:
        conn.commit()
        return 0
    return pending

last_print=0.0
PRINT_INTERVAL_SEC = 0.5

def print_progress(done, total, skipped, phase="scan"):
    global last_print
    now = time.time()
    if now - last_print < PRINT_INTERVAL_SEC and done < total:
        return
    last_print = now
    pct = (done / total * 100) if total else 0
    msg = f"\r[{phase}] {done}/{total} ({pct:5.1f}%) skipped:{skipped}"
    print(msg, end="", flush=True)

def main():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    now = int(time.time())
    updated = 0
    skipped = 0
    files = sorted(LORA_ROOT.rglob("*.safetensors"))
    total = len(files)
    
    fts_ids = {r[0] for r in conn.execute("SELECT rowid FROM lora_fts")}
    
    pending = 0
    for i, st in enumerate(files, 1):
        changed = False
        stat = st.stat()
        existing = conn.execute("SELECT id, mtime, file_size, preview_thumb, sha256 FROM lora WHERE path=?", (str(st),)).fetchone()

        thumb_ok = False
        if existing and existing[3]:
            try:
                thumb_ok = Path(existing[3]).exists()
            except Exception:
                thumb_ok = False
        
        file_unchanged = (
            existing
            and existing[1] == int(stat.st_mtime)
            and existing[2] == stat.st_size
        )
        
        if existing:
            lora_id = existing[0]
            
            if lora_id not in fts_ids:
                sync_fts(conn, lora_id)
                fts_ids.add(lora_id)
                changed = True
        
        need_update = not (file_unchanged and thumb_ok)
        
        if need_update:
            changed = True
            
            stem = st.with_suffix("")
            png = find_preview_png(stem)
            info = Path(str(stem) + ".info")
            meta = Path(str(stem) + ".metadata.json")
            
            info_obj = read_text_json(info) or {}
            meta_obj = read_text_json(meta) or {}
            st_md    = read_safetensors_metadata(st)
            
            existing_sha = existing[4] if existing else None
            
            if file_unchanged:
                sha = existing_sha
            else:
                sha = sha256_file(st)
            
            name = info_obj.get("name") or st.stem
            trigger = (
                info_obj.get("triggerWords")
                or meta_obj.get("trainedWords")
                or st_md.get("ss_trigger_words")
                or st_md.get("trigger_words")
                or ""
            )
            if isinstance(trigger, list):
                trigger = ", ".join(map(str, trigger))
            
            civitai_id = str(info_obj.get("id") or info_obj.get("modelId") or "")
            preview_full = str(png) if png and png.exists() else None
            preview_thumb = str(ensure_thumb(png, sha)) if png and png.exists() else None
            
            tags = []
            for key in ["tags", "trainedTags", "categories"]:
                v = info_obj.get(key) or meta_obj.get(key)
                if isinstance(v, list):
                    tags += [str(x) for x in v]
                elif isinstance(v, str):
                    tags += [x.strip() for x in v.split(",")]
            
            row = (
                name, str(st), sha,
                "SDXL",  # Illustrious前提
                None,    # kindは後で付ける（char/style/detail）
                trigger,
                None,    # notes
                preview_full,
                preview_thumb,
                json.dumps(info_obj, ensure_ascii=False) if info_obj else None,
                json.dumps(meta_obj, ensure_ascii=False) if meta_obj else None,
                civitai_id,
                stat.st_size,
                int(stat.st_mtime),
                now,
                name
            )
            
            lora_id = upsert_lora(conn, row)
            if tags:
                set_tags(conn, lora_id, tags)
            
            sync_fts(conn, lora_id)
            updated += 1
        else:
            skipped += 1
            
        if changed:
           pending = bump_commit(conn, pending)
        
        print_progress(i, total, skipped, phase="scan")
    
    conn.commit()
    print()
    print(f"done. updated={updated}, skipped={skipped}, db={DB_PATH}")

if __name__ == "__main__":
    a = time.time()
    main()
    b = time.time()
    print(f"time={b-a:.4f}s")
    
