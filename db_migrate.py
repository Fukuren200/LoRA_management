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
        
MIGRATIONS = [
    (1, mig_001_fill_kind_from_path),
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
