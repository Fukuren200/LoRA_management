import sqlite3
from pathlib import Path
import streamlit as st
import random

LORA_ROOT = Path(r"E:\AIDirectory\EasyReforge\Model\Lora")  # 変える
DB_PATH   = LORA_ROOT / "__lora_catalog.sqlite"

PAGE_SIZE = 36

def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data(show_spinner=False)
def search_ids(q: str, kind: str, limit: int):
    conn = db()
    where_kind = "" if kind == "all" else "AND l.kind = ?"
    params = [q]
    if kind != "all":
        params.append(kind)
    rows = conn.execute(f"""
        SELECT l.id
        FROM lora_fts f
        JOIN lora l ON l.id = f.rowid
        WHERE lora_fts MATCH ?
        {where_kind}
        ORDER BY bm25(lora_fts)
        LIMIT ?
    """, (*params, limit)).fetchall()
    conn.close()
    return [r[0] for r in rows]

def fetch_page(ids, page: int):
    conn = db()
    start = page * PAGE_SIZE
    chunk = ids[start:start+PAGE_SIZE]
    if not chunk:
        return []
    qmarks = ",".join(["?"] * len(chunk))
    rows = conn.execute(f"""
        SELECT id, name, trigger, preview_thumb, path, kind
        FROM lora
        WHERE id IN ({qmarks})
    """, chunk).fetchall()
    conn.close()
    # INは順序が崩れるのでids順に並べ直す
    m = {r[0]: r for r in rows}
    return [m[i] for i in chunk if i in m]

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

st.set_page_config(layout="wide", page_title="LoRA Library (Light)")

st.title("LoRA Library (Light)")

q = st.text_input("検索（例：anime portrait blue_hair）", value="anime")
kind = st.selectbox("kind", ["all", "char", "style", "detail", "concept"], index=0)
max_hits = st.slider("最大ヒット数（増やすと重くなる）", 200, 5000, 1500, 100)

if "picked" not in st.session_state:
    st.session_state.picked = {}   # id -> row
if "w" not in st.session_state:
    st.session_state.w = {}        # id -> weight

# ヒットID取得（FTS）
ids = []
if q.strip():
    ids = search_ids(q.strip(), kind, max_hits)

colA, colB = st.columns([3, 1], gap="large")

with colB:
    st.subheader("Picked")
    if st.button("Pickedをクリア"):
        st.session_state.picked = {}
        st.session_state.w = {}

    if st.button("ランダムで追加（今の検索結果から1つ）") and ids:
        rid = random.choice(ids)
        # 取得して追加（簡易）
        r = fetch_page([rid], 0)
        if r:
            st.session_state.picked[rid] = r[0]
            st.session_state.w.setdefault(rid, 0.8)

    st.caption(f"選択数: {len(st.session_state.picked)}")

    # 重み調整
    for _id, row in list(st.session_state.picked.items()):
        name = row[1]
        st.session_state.w[_id] = st.slider(name, 0.1, 1.5, float(st.session_state.w.get(_id, 0.8)), 0.05)

    if st.session_state.picked:
        st.divider()
        st.subheader("A1111 Prompt")
        out = recipe_generate(list(st.session_state.picked.values()), st.session_state.w)
        st.code(out, language="text")

with colA:
    st.subheader(f"Results: {len(ids)}")
    pages = max(1, (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = st.number_input("page", min_value=0, max_value=max(0, pages-1), value=0, step=1)

    rows = fetch_page(ids, page)

    # 6列グリッド
    cols = st.columns(6, gap="small")
    for i, r in enumerate(rows):
        _id, name, trigger, thumb, path, k = r
        with cols[i % 6]:
            if thumb and Path(thumb).exists():
                st.image(thumb, width="stretch")
            st.caption(f"{name}\n[{k or '-'}]")
            if st.button("Pick", key=f"pick_{_id}"):
                st.session_state.picked[_id] = r
                st.session_state.w.setdefault(_id, 0.8)
            st.text_input("trigger", value=(trigger or ""), key=f"tr_{_id}", disabled=True)
