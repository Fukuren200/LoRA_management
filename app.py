import sqlite3
from pathlib import Path
import streamlit as st
import random
from db_migrate import apply_migrations

LORA_ROOT = Path(r"E:\AIDirectory\EasyReforge\Model\Lora")  # 変える
DB_PATH   = LORA_ROOT / "__lora_catalog.sqlite"

PAGE_SIZE = 36

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def build_fts_query(q: str) -> str | None:
    q = (q or "").strip()
    if not q:
        return None
    
    terms = [t for t in q.split() if t]
    if not terms:
        return None
    
    def esc(term: str) -> str:
        term = term.replace('"', '""')
        return f'"{term}"'
        
    parts = [f'(name:{esc(t)} OR title:{esc(t)})' for t in terms]
    return " AND ".join(parts)


@st.cache_data(show_spinner=False)
def search_ids(q: str, selected_kinds: list[str], max_hits: int) -> list[int]:
    fts_query = build_fts_query(q)
    conn = get_db()
    
    try:
        where = []
        params = []
        
        if selected_kinds:
            where.append("COALESCE(NULLIF(lora.kind, ''), 'Unsorted') IN ({})".format(
                ",".join(["?"] * len(selected_kinds))
            ))
            params.extend(selected_kinds)
        
        join_fts = ""
        if fts_query:
            join_fts = "JOIN lora_fts ON lora_fts.rowid = lora.id"
            where.append("lora_fts MATCH ?")
            params.append(fts_query)
        
        sql = f"""
            SELECT lora.id
            FROM lora 
            {join_fts}
        """
        
        if where:
            sql += "WHERE " + " AND ".join(where)
        
        sql += " ORDER BY lora.mtime DESC LIMIT ?"
        params.append(int(max_hits))
        
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def fetch_page(ids, page: int):
    conn = get_db()
    start = page * PAGE_SIZE
    chunk = ids[start:start+PAGE_SIZE]
    if not chunk:
        return []
    qmarks = ",".join(["?"] * len(chunk))
    rows = conn.execute(f"""
        SELECT id, name, trigger, preview_thumb, path, kind, title 
        FROM lora
        WHERE id IN ({qmarks})
    """, chunk).fetchall()
    conn.close()
    # INは順序が崩れるのでids順に並べ直す
    m = {r[0]: r for r in rows}
    return [m[i] for i in chunk if i in m]

def fetch_kinds():
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT DISTINCT COALESCE(NULLIF(kind, ''), 'Unsorted') AS k
            FROM lora
            ORDER BY k COLLATE NOCASE
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def lora_tag(name: str, w: float):
    # A1111の <lora:NAME:W>
    safe = name.replace(":", "_")
    return f"<lora:{safe}:{w:.2f}>"

def recipe_generate(selected, weights):
    # selected: rows of (id,name,trigger,thumb,path,kind)
    tags = []
    triggers = []
    for r in selected:
        _id, name, trigger, *_ = r
        tags.append(lora_tag(name, weights.get(_id, 0.8)))
        if trigger:
            triggers.append(trigger)
    prompt = " ".join(tags)
    if triggers:
        prompt += "\n" + ", ".join([t for t in triggers if t.strip()])
    return prompt.strip()

def update_title(lora_id: int, title: str):
    conn = get_db()
    try:
        conn.execute("BEGIN")
        conn.execute("UPDATE lora SET title=? WHERE id=?", (title, lora_id))
        
        conn.execute("""
            UPDATE lora_fts
            SET title=?
            WHERE rowid=?
        """, (new_title, _id))
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()
    
def startup_migrate():
    if "migrated" not in st.session_state:
        conn = get_db()
        try:
            apply_migrations(conn)
        finally:
            conn.close()
        st.session_state.migrated = True

startup_migrate()

st.set_page_config(layout="wide", page_title="LoRA Library (Light)")

st.title("LoRA Library (Light)")

all_kinds = fetch_kinds()

selected_kinds = st.multiselect(
    "kind filter",
    options=all_kinds,
)

q = st.text_input("検索ワード スペース区切りでAND。2文字以下はヒットしません", value="").strip()
max_hits = st.slider("最大ヒット数（増やすと重くなる）", 200, 5000, 1500, 100)

st.session_state.setdefault("picked", {})
st.session_state.setdefault("w", {})
st.session_state.setdefault("single_pick", True)

# ヒットID取得（FTS）
ids = search_ids(q, selected_kinds, max_hits)

colA, colB = st.columns([3, 1], gap="large")

with colB:
    st.subheader("Picked")
    single_pick = st.toggle(
        "単一Pickモード (新しく選ぶと入れ替え)", 
        key="single_pick",
    )
    
    picked_area = st.container()

with colA:
    st.subheader(f"Results: {len(ids)}")
    pages = max(1, (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = st.number_input("page", min_value=0, max_value=max(0, pages-1), value=0, step=1)

    rows = fetch_page(ids, page)

    # 6列グリッド
    cols = st.columns(6, gap="small")
    for i, r in enumerate(rows):
        _id, name, trigger, thumb, path,  k, title = r
        with cols[i % 6]:
            if thumb and Path(thumb).exists():
                st.image(thumb, width="stretch")
            display = title or name
            st.caption(f"{display}\n[{k or '-'}]")
            if st.button("Pick", key=f"pick_{_id}"):
                if single_pick:
                    st.session_state.picked = {_id: r}
                    st.session_state.w = {_id: 0.8}
                else:
                    st.session_state.picked[_id] = r
                    st.session_state.w.setdefault(_id, 0.8)
                st.rerun()
                
            st.text_input("trigger", value=(trigger or ""), key=f"tr_{_id}", disabled=True)

with picked_area:    
    if st.button("Pickedをクリア"):
        st.session_state.picked = {}
        st.session_state.w = {}
        st.rerun()

    if st.button("ランダムで追加（今の検索結果から1つ）") and ids:
        rid = random.choice(ids)
        # 取得して追加（簡易）
        r = fetch_page([rid], 0)
        if r:
            if single_pick:
                st.session_state.picked = {rid: r[0]}
                st.session_state.w = {rid: 0.8}
            else:
                st.session_state.picked[rid] = r[0]
                st.session_state.w.setdefault(rid, 0.8)
            st.rerun()

    st.caption(f"選択数: {len(st.session_state.picked)}")

    # 重み調整
    for _id, row in list(st.session_state.picked.items()):
        name = row[1]
        thumb = row[3]
        title = row[6]
        display = title or name
        
        if thumb and Path(thumb).exists():
                st.image(thumb, width="stretch")
        
        new_title = st.text_input(f"Title: {display}", value=(title or name), key=f"title_{_id}")
        if st.button("Save Title", key=f"save_title_{_id}"):
            update_title(_id, new_title)
            
            row = list(row)
            row[6] = new_title
            st.session_state.picked[_id] = tuple(row)
            st.success("saved")
            # キャッシュ対策。必要な時に有効化。古いデータが画面上に表示されるときとか
#            st.cache_data.clear()
        
        st.session_state.w[_id] = st.slider(name, 0.1, 1.5, float(st.session_state.w.get(_id, 0.8)), 0.05)

    if st.session_state.picked:
        st.divider()
        st.subheader("Prompt")
        out = recipe_generate(list(st.session_state.picked.values()), st.session_state.w)
        st.code(out, language="text")
