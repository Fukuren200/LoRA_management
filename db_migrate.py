import sqlite3
from pathlib import Path

def get_schema_version(conn) -> int:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_meta (
            key    TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    return int(row[0]) if row else 0
    
def set_schema_version(conn, v: int):
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?)"
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(v),)
    )
    
def mig_001_fill_kind_from_path(conn):
    rows = conn.execute(
        "SELECT id, path FROM lora WHERE kind IS NULL OR kind=''"
    ).fetchall()
    
    for _id, p in rows:
        parent = Path(p).parent.name
        kind = parent if parent else "Unsorted"
        conn.execute("UPDATE lora SET kind=? WHERE id=?", (kind, _id))

def mig_002_fill_fts(conn):
    conn.execute("DROP TABLE IF EXISTS lora_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE lora_fts USING fts5(
            name, trigger, notes, tags_text, title,
            tokenize = "trigram"
        )
    """)

    conn.execute("""
        INSERT INTO lora_fts(rowid, name, trigger, notes, tags_text, title)
        SELECT
            l.id AS rowid,
            COALESCE(l.name, ''),
            COALESCE(l.trigger, ''),
            COALESCE(l.notes, ''),
            COALESCE(group_concat(t.name, ' '), '') AS tags_text,
            COALESCE(l.title, '')
        FROM lora l
        LEFT JOIN lora_tag lt ON lt.lora_id = l.id
        LEFT JOIN tag t ON t.id = lt.tag_id
        GROUP BY l.id
    """)

def mig_003_add_lora_preset_table(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS lora_body_preset (
            id INTEGER PRIMARY KEY,
            lora_id INTEGER NOT NULL,
            name TEXT,
            body_prompt TEXT,
            thumb TEXT,
            FOREIGN KEY(lora_id) REFERENCES lora(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS lora_outfit_preset (
            id INTEGER PRIMARY KEY,
            lora_id INTEGER NOT NULL,
            name TEXT,
            clothes_prompt TEXT,
            thumb TEXT,
            FOREIGN KEY(lora_id) REFERENCES lora(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_lora_body_preset_lora_id ON lora_body_preset(lora_id);
        CREATE INDEX IF NOT EXISTS idx_lora_outfit_preset_lora_id ON lora_outfit_preset(lora_id);
    """)
    

MIGRATIONS = [
    (1, mig_001_fill_kind_from_path),
    (2, mig_002_fill_fts),
    (3, mig_003_add_lora_preset_table)
]

def apply_migrations(conn):
    cur = get_schema_version(conn)
    
    for ver, fn in MIGRATIONS:
        if ver <= cur:
            continue
        
        conn.execute("BEGIN")
        try:            
            fn(conn)
            
            set_schema_version(conn, ver)
            conn.commit()
            cur = ver
            print(f"[migrate] -> V{ver} OK")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"migration v{ver} failed: {e}") from e
