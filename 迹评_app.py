# -*- coding: utf-8 -*-
"""
迹评 · EVERY STEP COUNTS  （多实训扩展版 v4）
Streamlit + Turso(libSQL) HTTP v2 pipeline

v4 相对 v3 的变化（保持单文件、单页结构，不拆分、不改入口）：
  1. 实训从 9 个扩到 38 个，覆盖 8 个专业方向：计网 / Linux / 数据库 / Python /
     软件工程 / 电气自动化 / 数字媒体 / 电子商务
  2. 配套图全部改为本地矢量图（纯 Python 生成 SVG），不再依赖 Mermaid CDN 与 iframe，
     单张 0.04ms，彻底解决"配套图生成太慢"
  3. 学生端改用 st.fragment 局部刷新 + 数据库变更信号轮询，老师发布后 3 秒内自动出现，
     且整页不闪、不重跑 → 解决"老师发任务学生端收不到"与"切页面卡顿"
  4. 每个实训都是有状态的模拟器（不再是一张命令对照表），乱序、重复、错输都正常
  5. 步骤进度从数据库历史重算，刷新页面 / 换设备不丢
数据库结构只做"加表、加列"，旧库旧数据原样可用，启动自动迁移。
"""

import ast
import base64
import hashlib
import html as _html
import os as _os
import re
import shlex
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

# ============================================================
# 常量
# ============================================================
RECORD_LIMIT = 300
STUDENT_LIMIT = 500

TTL_STRUCT = 120
TTL_RECORD = 12

# 学生端任务指纹轮询间隔（秒）——老师发布后最迟这么久自动出现
PROBE_SECONDS = 3

# 整页切换判定：超过这个时长就提示一次（不影响功能，只做观察用）
SLOW_MS = 1500


class IntegrityError(Exception):
    pass


class DBError(Exception):
    pass


# ============================================================
# HTTP 连接复用（加连接池 + 重试，避免高并发下卡死）
# ============================================================
_HTTP = None


def _http():
    global _HTTP
    if _HTTP is None:
        s = requests.Session()
        s.headers.update({"Connection": "keep-alive"})
        try:
            from requests.adapters import HTTPAdapter

            try:
                from urllib3.util.retry import Retry
            except Exception:  # urllib3 版本差异
                from requests.packages.urllib3.util.retry import Retry

            retry = Retry(
                total=2,
                backoff_factor=0.25,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["POST", "GET"]),
            )
            ad = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=retry)
            s.mount("https://", ad)
            s.mount("http://", ad)
        except Exception:
            pass
        _HTTP = s
    return _HTTP


# ============================================================
# Turso
# ============================================================
_turso = None


def _secret(key):
    try:
        val = st.secrets.get(key)
    except Exception:
        val = None
    return val or _os.environ.get(key)


def get_db():
    global _turso
    if _turso:
        return _turso
    url = _secret("TURSO_URL")
    token = _secret("TURSO_AUTH_TOKEN")
    if not url:
        raise DBError(
            "请先配置 TURSO_URL 和 TURSO_AUTH_TOKEN："
            "Streamlit Cloud → Settings → Secrets，或本地 .streamlit/secrets.toml"
        )
    base = str(url).strip().rstrip("/")
    # 兼容三种写法：libsql:// / https:// / 本地 http://（调试用）
    if base.startswith("libsql://"):
        base = "https://" + base[len("libsql://"):]
    elif not base.startswith(("http://", "https://")):
        base = "https://" + base
    _turso = {"endpoint": base + "/v2/pipeline", "token": token or ""}
    return _turso


def _to_turso_arg(v):
    """Turso v2 协议要求 value 一律是字符串"""
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": repr(float(v))}
    if isinstance(v, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode("ascii")}
    return {"type": "text", "value": str(v)}


def _cell_to_py(cell):
    if not isinstance(cell, dict):
        return cell
    ctype = cell.get("type")
    if ctype == "null":
        return None
    if ctype == "blob":
        return cell.get("base64")
    raw = cell.get("value")
    if raw is None:
        return None
    if ctype == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return raw
    if ctype == "float":
        try:
            return float(raw)
        except (TypeError, ValueError):
            return raw
    return raw


def _post_pipeline(requests_body):
    db = get_db()
    headers = {"Content-Type": "application/json"}
    if db["token"]:
        headers["Authorization"] = "Bearer " + db["token"]

    try:
        resp = _http().post(db["endpoint"], headers=headers, json={"requests": requests_body}, timeout=20)
    except requests.RequestException as exc:
        raise DBError("无法连接 Turso：{}".format(exc)) from exc

    if resp.status_code != 200:
        raise DBError("Turso 返回 HTTP {}：{}".format(resp.status_code, resp.text[:300]))

    try:
        data = resp.json()
    except ValueError as exc:
        raise DBError("Turso 返回内容不是 JSON：{}".format(resp.text[:300])) from exc

    results = data.get("results") or []
    if not results:
        raise DBError("Turso 未返回结果：{}".format(str(data)[:300]))

    for item in results:
        if item.get("type") == "error" or "error" in item:
            err = item.get("error") or {}
            msg = err.get("message", "unknown error") if isinstance(err, dict) else str(err)
            upper = msg.upper()
            if "UNIQUE" in upper or "PRIMARY KEY" in upper:
                raise IntegrityError(msg)
            raise DBError("SQL 错误：{}".format(msg))
    return results


def db_exec(sql, args=None):
    results = _post_pipeline(
        [
            {"type": "execute", "stmt": {"sql": sql, "args": [_to_turso_arg(a) for a in (args or [])]}},
            {"type": "close"},
        ]
    )
    item = results[0]
    res = ((item.get("response") or {}).get("result")) or {}
    rows = [tuple(_cell_to_py(c) for c in row) for row in (res.get("rows") or [])]

    lastid = res.get("last_insert_rowid") or 0
    try:
        lastid = int(lastid)
    except (TypeError, ValueError):
        lastid = 0

    affected = res.get("affected_row_count") or 0
    try:
        affected = int(affected)
    except (TypeError, ValueError):
        affected = 0

    return rows, lastid, affected


def db_exec_many(sqls):
    if not sqls:
        return
    body = [{"type": "execute", "stmt": {"sql": s}} for s in sqls]
    body.append({"type": "close"})
    _post_pipeline(body)


def db_query(sql, args=None):
    """只要行数据的小工具"""
    rows, _, _ = db_exec(sql, args)
    return rows


# ============================================================
# 查询缓存
# ============================================================
@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_teacher_classes(teacher):
    return get_teacher_classes(teacher)


@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_all_classes():
    return get_all_class_ids_names()


@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_class_by_id(cid):
    return get_class_by_id(cid)


@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_tasks_by_class(cid):
    return get_tasks_by_class(cid)


@st.cache_data(ttl=TTL_RECORD, show_spinner=False)
def q_students(cid):
    return get_students_by_class(cid)


@st.cache_data(ttl=TTL_RECORD, show_spinner=False)
def q_records(cid, student):
    return get_records_by_class_student(cid, student)


@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_class_overview(cid):
    """按学生聚合：总步数 / 报错数 / 正确率 / 最后活跃时间"""
    return get_class_overview(cid)


@st.cache_data(ttl=TTL_STRUCT, show_spinner=False)
def q_task_progress(cid):
    """按任务聚合：Steps."""
    return get_task_progress(cid)


def bust_cache():
    try:
        st.cache_data.clear()
    except Exception:
        pass


# ============================================================
# ★ 变更信号（解决"老师发任务，学生端收不到"）
#     老师写操作后 bump() 一次；学生端只查一个极轻的指纹，
#     指纹变了才清缓存 → 平时不打扰、变了立刻生效
# ============================================================
def bump(scope):
    """scope: 'struct'（班级/任务/老师） 或 'record:任务所属班级'"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        db_exec(
            """INSERT INTO bumps (scope, rev, update_time) VALUES (?, 1, ?)
               ON CONFLICT(scope) DO UPDATE SET rev = rev + 1, update_time = ?""",
            (scope, ts, ts),
        )
    except DBError:
        pass


def read_bump(scope):
    try:
        rows = db_query("SELECT rev FROM bumps WHERE scope=?", (scope,))
        return int(rows[0][0]) if rows else 0
    except (DBError, TypeError, ValueError, IndexError):
        return 0


def class_fingerprint(cid):
    """
    极轻指纹：任务条数 + 最大任务 id + 最近创建时间 + 结构版本号。
    一条 SQL 一次往返拿完（原来要两次），比拉全表轻得多，
    用来判断"要不要重拉任务列表"。
    客户端 3 秒轮询一次 → 每人每 3 秒 1 个请求。
    """
    try:
        rows = db_query(
            """SELECT (SELECT COUNT(*) FROM tasks WHERE class_id=?1),
                      (SELECT COALESCE(MAX(id),0) FROM tasks WHERE class_id=?1),
                      (SELECT COALESCE(MAX(create_time),'') FROM tasks WHERE class_id=?1),
                      (SELECT COALESCE(rev,0) FROM bumps WHERE scope='struct')""",
            (cid,),
        )
    except DBError:
        # bumps 表还没建好时退化为纯任务指纹
        rows = db_query(
            """SELECT COUNT(*), COALESCE(MAX(id),0), COALESCE(MAX(create_time),'') FROM tasks WHERE class_id=?""",
            (cid,),
        )
        if rows:
            rows = [tuple(rows[0]) + (0,)]
    n, mx, last, rev = (rows[0] if rows else (0, 0, "", 0))
    return "{}|{}|{}|{}".format(n, mx, last, rev)


# ============================================================
# ★ 本地矢量图（替代 Mermaid，解决"配套图生成太慢"）
#     纯 Python 拼 SVG 字符串，不走网络、不进 iframe，单张 0.04ms
# ============================================================
C = {
    "ink": "#1F2A36",
    "muted": "#6B7A8C",
    "line": "#9AA9BA",
    "bg": "#FFFFFF",
    "panel": "#F7F9FC",
    "blue": "#2F6FD0",
    "blue_soft": "#E8F0FC",
    "green": "#2E9E5B",
    "green_soft": "#E6F5EC",
    "amber": "#D98A00",
    "amber_soft": "#FDF3E0",
    "red": "#D93A3A",
    "red_soft": "#FCEBEB",
    "purple": "#7B5BD6",
    "purple_soft": "#EFEBFB",
    "teal": "#1C9AA8",
    "teal_soft": "#E4F6F7",
}

_NODE_STYLE = {
    "pc": (C["blue_soft"], C["blue"]),
    "router": (C["amber_soft"], C["amber"]),
    "switch": (C["green_soft"], C["green"]),
    "server": (C["purple_soft"], C["purple"]),
    "db": (C["teal_soft"], C["teal"]),
    "cloud": ("#EEF1F5", C["muted"]),
    "host": ("#EEF1F5", C["muted"]),
    "start": (C["green_soft"], C["green"]),
    "end": (C["blue_soft"], C["blue"]),
    "step": (C["panel"], C["ink"]),
    "check": (C["amber_soft"], C["amber"]),
    "warn": (C["red_soft"], C["red"]),
}

_FONT = '-apple-system,"PingFang SC","Microsoft YaHei","Segoe UI",sans-serif'


def esc(s):
    return _html.escape(str(s if s is not None else ""), quote=True)


def _svg_open(w, h, extra=""):
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {w} {h}" '
        'preserveAspectRatio="xMidYMid meet" role="img" {extra}>'
        "<defs>"
        '<marker id="arw" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
        '<path d="M0,0 L10,5 L0,10 z" fill="{m}"/></marker>'
        '<marker id="arwb" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
        '<path d="M0,0 L10,5 L0,10 z" fill="{b}"/></marker>'
        "</defs>"
        '<rect width="{w}" height="{h}" fill="{bg}" rx="10"/>'
    ).format(w=w, h=h, m=C["line"], b=C["blue"], bg=C["bg"], extra=extra)


def _svg_close():
    return "</svg>"


def _text(x, y, s, size=12, color=None, anchor="middle", weight="400"):
    return (
        '<text x="{x}" y="{y}" font-family=\'{f}\' font-size="{s}" fill="{c}" '
        'text-anchor="{a}" font-weight="{w}">{t}</text>'
    ).format(x=x, y=y, f=_FONT, s=size, c=color or C["ink"], a=anchor, w=weight, t=esc(s))


def _box(x, y, w, h, label, sub="", kind="step", r=9):
    fill, stroke = _NODE_STYLE.get(kind, _NODE_STYLE["step"])
    out = '<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{f}" stroke="{s}" stroke-width="1.4"/>'.format(
        x=x, y=y, w=w, h=h, r=r, f=fill, s=stroke
    )
    if sub:
        out += _text(x + w / 2, y + h / 2 - 3, label, 12.5, C["ink"], "middle", "600")
        out += _text(x + w / 2, y + h / 2 + 14, sub, 10.5, C["muted"])
    else:
        out += _text(x + w / 2, y + h / 2 + 4.5, label, 12.5, C["ink"], "middle", "600")
    return out


def _line(x1, y1, x2, y2, color=None, dash=None, arrow=True, width=1.5):
    d = ' stroke-dasharray="{}"'.format(dash) if dash else ""
    m = ' marker-end="url(#arw)"' if arrow else ""
    return '<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{c}" stroke-width="{w}"{d}{m}/>'.format(
        x1=x1, y1=y1, x2=x2, y2=y2, c=color or C["line"], w=width, d=d, m=m
    )


def _path(d, color=None, arrow=True, dash=None, width=1.5):
    dd = ' stroke-dasharray="{}"'.format(dash) if dash else ""
    m = ' marker-end="url(#arw)"' if arrow else ""
    return '<path d="{}" fill="none" stroke="{}" stroke-width="{}"{}{}/>'.format(
        d, color or C["line"], width, dd, m
    )


def _label(x, y, s, color=None, size=10.5, bg=C["bg"]):
    wid = max(18, len(str(s)) * 6.2 + 8)
    return (
        '<rect x="{bx}" y="{by}" width="{w}" height="16" rx="4" fill="{bg}" opacity="0.94"/>'
        '<text x="{tx}" y="{ty}" font-family=\'{f}\' font-size="{s}" fill="{c}" text-anchor="middle">{t}</text>'
    ).format(bx=x - wid / 2, by=y - 11, w=wid, bg=bg, tx=x, ty=y + 1, f=_FONT, s=size, c=color or C["muted"], t=esc(s))


def _wrap(text, n):
    text = str(text or "")
    out, cur = [], ""
    for ch in text:
        cur += ch
        if len(cur) >= n:
            out.append(cur)
            cur = ""
    if cur:
        out.append(cur)
    return out or [""]


# ---------- 6 种基础画法 ----------
def g_topology(nodes, links, node_w=150, node_h=62):
    """nodes: [(id, 主标签, 副标签, 类型, 列, 行)]  links: [(a, b, 线上文字)]"""
    cols = max([n[4] for n in nodes] + [0])
    rows = max([n[5] for n in nodes] + [0])
    gap_x, gap_y = node_w + 62, node_h + 54
    W = (cols + 1) * gap_x + 30
    H = (rows + 1) * gap_y + 30
    pos = {}
    for nid, lab, sub, kind, cx, cy in nodes:
        pos[nid] = (30 + cx * gap_x + node_w / 2, 24 + cy * gap_y + node_h / 2)

    out = [_svg_open(W, H)]
    for a, b, txt in links:
        if a not in pos or b not in pos:
            continue
        x1, y1 = pos[a]
        x2, y2 = pos[b]
        if abs(y1 - y2) < 1:
            xa, xb = (x1 + node_w / 2, x2 - node_w / 2) if x2 > x1 else (x1 - node_w / 2, x2 + node_w / 2)
            out.append(_line(xa, y1, xb, y2, C["line"]))
            mid = ((xa + xb) / 2, y1 - 12)
        elif abs(x1 - x2) < 1:
            ya, yb = (y1 + node_h / 2, y2 - node_h / 2) if y2 > y1 else (y1 - node_h / 2, y2 + node_h / 2)
            out.append(_line(x1, ya, x2, yb, C["line"]))
            mid = (x1 + 4, (ya + yb) / 2)
        else:
            out.append(_line(x1, y1, x2, y2, C["line"]))
            mid = ((x1 + x2) / 2, (y1 + y2) / 2)
        if txt:
            out.append(_label(mid[0], mid[1], txt))

    for nid, lab, sub, kind, cx, cy in nodes:
        x, y = pos[nid]
        out.append(_box(x - node_w / 2, y - node_h / 2, node_w, node_h, lab, sub, kind))
    out.append(_svg_close())
    return "".join(out)


def g_flowchart(steps, branch=None, node_w=210, node_h=46):
    """steps: [(文字, 类型)]，纵向流程；branch=(第几步之后, 分支文字, [(文字, 类型)])"""
    gap = 62
    W = 560
    n = len(steps)
    H = n * (node_h + gap) + 40
    cx = W / 2 - 60
    out = [_svg_open(W, H)]
    for i, (txt, kind) in enumerate(steps):
        y = 24 + i * (node_h + gap)
        lines = _wrap(txt, 15)[:2]
        fill, stroke = _NODE_STYLE.get(kind, _NODE_STYLE["step"])
        out.append(
            '<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{f}" stroke="{s}" stroke-width="1.4"/>'.format(
                x=cx - node_w / 2, y=y, w=node_w, h=node_h, r=8 if kind in ("step", "check", "warn") else 22,
                f=fill, s=stroke
            )
        )
        if len(lines) == 1:
            out.append(_text(cx, y + node_h / 2 + 4, lines[0], 12, C["ink"], "middle", "600"))
        else:
            out.append(_text(cx, y + node_h / 2 - 3, lines[0], 11.5, C["ink"], "middle", "600"))
            out.append(_text(cx, y + node_h / 2 + 13, lines[1], 11.5, C["ink"], "middle", "600"))
        if i < n - 1:
            out.append(_line(cx, y + node_h, cx, y + node_h + gap - 2))

    if branch:
        idx, txt, items = branch
        bx = cx + node_w / 2 + 96
        by = 24 + idx * (node_h + gap) + node_h / 2
        out.append(_line(cx + node_w / 2, by, bx - 78, by, C["red"]))
        out.append(_label((cx + node_w / 2 + bx - 78) / 2, by - 10, txt, C["red"]))
        for j, (t, kind) in enumerate(items):
            yy = by + j * 48 - (48 * (len(items) - 1)) / 2
            fill, stroke = _NODE_STYLE.get(kind, _NODE_STYLE["warn"])
            out.append(
                '<rect x="{x}" y="{y}" width="156" height="36" rx="8" fill="{f}" stroke="{s}" stroke-width="1.3"/>'.format(
                    x=bx - 78, y=yy - 18, f=fill, s=stroke
                )
            )
            out.append(_text(bx, yy + 4, t, 11, C["ink"], "middle", "500"))
    out.append(_svg_close())
    return "".join(out)


def g_sequence(actors, messages):
    """actors: [名字]   messages: [(from, to, 文字)]"""
    col_w = 168
    W = max(360, len(actors) * col_w + 20)
    H = 70 + len(messages) * 46 + 30
    xs = {a: 40 + col_w / 2 + i * col_w for i, a in enumerate(actors)}
    out = [_svg_open(W, H)]
    for a in actors:
        x = xs[a]
        out.append(
            '<rect x="{x}" y="14" width="126" height="34" rx="8" fill="{f}" stroke="{s}" stroke-width="1.4"/>'.format(
                x=x - 63, f=C["blue_soft"], s=C["blue"]
            )
        )
        out.append(_text(x, 36, a, 12, C["ink"], "middle", "600"))
        out.append(_line(x, 48, x, H - 16, C["line"], dash="4 4", arrow=False))

    for i, (a, b, txt) in enumerate(messages):
        y = 82 + i * 46
        x1, x2 = xs.get(a, 60), xs.get(b, 60)
        if a == b:  # 自环
            out.append(_path("M{x} {y} C{x1} {y1}, {x2} {y2}, {x} {y3}".format(
                x=x1 - 4, y=y, x1=x1 + 50, y1=y - 26, x2=x1 + 50, y2=y + 20, y3=y + 34), C["line"]))
            out.append(_text(x1 + 58, y + 6, txt, 10.5, C["ink"], "start"))
        else:
            out.append(_line(x1, y, x2, y, C["line"]))
            out.append(_text((x1 + x2) / 2, y - 8, txt, 10.5, C["ink"]))
    out.append(_svg_close())
    return "".join(out)


def g_er(entities, rels):
    """entities: [(表名, [(字段, 标记)])]   rels: [(表A, 表B, 关系)]"""
    bw, pad = 178, 34
    W = len(entities) * (bw + pad) + 20
    maxf = max([len(f) for _, f in entities] + [1])
    bh = 34 + maxf * 19
    H = bh + 90
    pos = {}
    out = [_svg_open(W, H)]
    for i, (name, fields) in enumerate(entities):
        x = 20 + i * (bw + pad)
        pos[name] = (x, x + bw)
        out.append('<rect x="{x}" y="26" width="{w}" height="{h}" rx="8" fill="{bg}" stroke="{l}" stroke-width="1.3"/>'.format(
            x=x, w=bw, h=bh, bg=C["bg"], l=C["line"]))
        out.append('<rect x="{x}" y="26" width="{w}" height="30" rx="8" fill="{f}"/>'.format(x=x, w=bw, f=C["purple_soft"]))
        out.append(_text(x + bw / 2, 46, name, 12.5, C["purple"], "middle", "700"))
        for j, (f, tag) in enumerate(fields):
            yy = 76 + j * 19
            out.append(_text(x + 10, yy, f, 10.5, C["ink"], "start"))
            if tag:
                out.append(_text(x + bw - 10, yy, tag, 9.5, C["amber"] if tag == "PK" else C["teal"], "end", "600"))

    for a, b, txt in rels:
        if a not in pos or b not in pos:
            continue
        y = 26 + bh / 2
        xa, xb = pos[a][1], pos[b][0]
        if xb > xa:
            out.append(_line(xa, y, xb, y, C["teal"]))
            out.append(_label((xa + xb) / 2, y - 10, txt, C["teal"]))
    out.append(_svg_close())
    return "".join(out)


def g_timeline(items, width=620):
    """items: [(标题, 说明)]，横向时间轴"""
    n = len(items)
    W = width
    step = (W - 90) / max(n - 1, 1)
    H = 150
    out = [_svg_open(W, H)]
    y = 78
    out.append(_line(45, y, W - 45, y, C["line"], width=2, arrow=False))
    for i, (t, d) in enumerate(items):
        x = 45 + i * step
        color = [C["blue"], C["green"], C["amber"], C["purple"], C["teal"]][i % 5]
        out.append('<circle cx="{x}" cy="{y}" r="7" fill="{c}" stroke="#fff" stroke-width="2"/>'.format(x=x, y=y, c=color))
        out.append(_text(x, y - 22, t, 11.5, C["ink"], "middle", "600"))
        for k, ln in enumerate(_wrap(d, 9)[:2]):
            out.append(_text(x, y + 26 + k * 15, ln, 10, C["muted"]))
    out.append(_text(14, 24, "时间轴 / 分类", 11, C["muted"], "start"))
    out.append(_svg_close())
    return "".join(out)


def g_bar(data, width=600, height=260):
    """data: [(标签, 数值)]"""
    n = len(data)
    mx = max([v for _, v in data] + [1])
    pad_l, pad_b, pad_t = 44, 46, 24
    W, H = width, height
    bw = (W - pad_l - 24) / max(n, 1) * 0.56
    gap = (W - pad_l - 24) / max(n, 1)
    out = [_svg_open(W, H)]
    for i in range(5):
        yy = pad_t + (H - pad_t - pad_b) * i / 4
        out.append(_line(pad_l, yy, W - 16, yy, "#E7ECF2", arrow=False, width=1))
        out.append(_text(pad_l - 8, yy + 4, "{:.0f}".format(mx * (4 - i) / 4), 9.5, C["muted"], "end"))
    for i, (lab, v) in enumerate(data):
        h = (H - pad_t - pad_b) * (v / mx)
        x = pad_l + gap * i + (gap - bw) / 2
        y = H - pad_b - h
        color = C["blue"] if i % 2 == 0 else C["teal"]
        out.append('<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{h:.1f}" rx="4" fill="{c}"/>'.format(
            x=x, y=y, bw=bw, h=h, c=color))
        out.append(_text(x + bw / 2, y - 6, str(v), 10, C["ink"], "middle", "600"))
        out.append(_text(x + bw / 2, H - pad_b + 16, str(lab), 9.5, C["muted"]))
    out.append(_svg_close())
    return "".join(out)


# ============================================================
# 场景图库：每个实训一张配套图
# ============================================================
def fig_switch_basic():
    return g_topology(
        [("pc1", "PC1", "192.168.1.10/24", "pc", 0, 0),
         ("pc2", "PC2", "192.168.1.11/24", "pc", 0, 1),
         ("sw", "SW1", "G0/0/1~2", "switch", 1, 0),
         ("r1", "R1", "GE0/0/0", "router", 2, 0),
         ("net", "Internet", "出口链路", "cloud", 3, 0)],
        [("pc1", "sw", "access"), ("pc2", "sw", "access"), ("sw", "r1", "trunk"), ("r1", "net", "NAT")],
    )


def fig_vlan():
    return g_topology(
        [("pc1", "PC1", "VLAN 10", "pc", 0, 0),
         ("pc2", "PC2", "VLAN 20", "pc", 0, 1),
         ("sw", "SW1", "access×2 + trunk", "switch", 1, 0),
         ("r1", "R1", "单臂路由 / SVI", "router", 2, 0)],
        [("pc1", "sw", "access vlan10"), ("pc2", "sw", "access vlan20"), ("sw", "r1", "trunk 10,20")],
    )


def fig_ospf():
    return g_topology(
        [("lan1", "192.168.1.0/24", "区域 0", "host", 0, 0),
         ("r1", "R1", "1.1.1.1", "router", 1, 0),
         ("r2", "R2", "2.2.2.2", "router", 2, 0),
         ("r3", "R3", "3.3.3.3", "router", 3, 0),
         ("lan3", "192.168.3.0/24", "区域 0", "host", 3, 1)],
        [("lan1", "r1", ""), ("r1", "r2", "10.0.0.0/30"), ("r2", "r3", "10.0.1.0/30"), ("r3", "lan3", "")],
    )


def fig_stp():
    return g_topology(
        [("sw1", "SW1", "根桥 Root", "switch", 1, 0),
         ("sw2", "SW2", "非根桥", "switch", 0, 1),
         ("sw3", "SW3", "非根桥", "switch", 2, 1)],
        [("sw1", "sw2", "GE0/0/1"), ("sw1", "sw3", "GE0/0/2"), ("sw2", "sw3", "阻塞 Blocking")],
    )


def fig_dhcp():
    return g_sequence(
        ["DHCP 客户端", "DHCP 服务器"],
        [("DHCP 客户端", "DHCP 服务器", "① DHCP Discover（广播）"),
         ("DHCP 服务器", "DHCP 客户端", "② Offer 192.168.1.100"),
         ("DHCP 客户端", "DHCP 服务器", "③ Request 请求该地址"),
         ("DHCP 服务器", "DHCP 客户端", "④ ACK 确认租期"),
         ("DHCP 客户端", "DHCP 客户端", "⑤ 绑定地址并续租")],
    )


def fig_acl():
    return g_sequence(
        ["学生 PC", "ACL 3000", "服务器"],
        [("学生 PC", "ACL 3000", "请求 TCP 80"),
         ("ACL 3000", "ACL 3000", "匹配 rule 5 deny"),
         ("ACL 3000", "学生 PC", "丢弃 + 记日志"),
         ("学生 PC", "ACL 3000", "请求 TCP 443"),
         ("ACL 3000", "ACL 3000", "匹配 rule 15 permit"),
         ("ACL 3000", "服务器", "放行")],
    )


def fig_nat():
    return g_topology(
        [("pc", "内网 PC", "192.168.1.10", "pc", 0, 0),
         ("r", "出口路由器", "NAT 转换", "router", 1, 0),
         ("pub", "公网服务器", "203.0.113.9", "server", 2, 0)],
        [("pc", "r", "源 192.168.1.10"), ("r", "pub", "源 203.0.113.2")],
    )


def fig_linux_basic():
    return g_topology(
        [("sh", "Shell", "bash / zsh", "pc", 0, 0),
         ("cm", "命令", "pwd whoami df ip", "host", 1, 0),
         ("kn", "内核", "系统调用", "router", 2, 0),
         ("hd", "硬件", "磁盘 / 网卡 / CPU", "cloud", 3, 0)],
        [("sh", "cm", "解析"), ("cm", "kn", "syscall"), ("kn", "hd", "驱动")],
    )


def fig_nginx():
    return g_topology(
        [("u", "浏览器", "http://ip", "pc", 0, 0),
         ("n", "Nginx", ":80 静态", "server", 1, 0),
         ("p", "PHP-FPM", ":9000", "host", 2, 0),
         ("m", "MySQL", ":3306", "db", 3, 0)],
        [("u", "n", "HTTP"), ("n", "p", "fastcgi"), ("p", "m", "SQL")],
    )


def fig_perm():
    return g_timeline(
        [("属主 u", "read / write / exec"), ("同组 g", "read / write / exec"),
         ("其他 o", "read / write / exec"), ("特殊位", "suid / sgid / sticky")],
    )


def fig_shell_flow():
    return g_flowchart(
        [("读入一行命令", "start"), ("分词并展开变量", "step"), ("按 PATH 查找可执行文件", "step"),
         ("fork 子进程 exec 执行", "step"), ("输出到 stdout / stderr", "step"), ("返回退出码 $?", "end")],
        branch=(2, "找不到", [("提示 command not found", "warn")]),
    )


def fig_lnmp():
    return g_flowchart(
        [("检查 80 端口监听", "start"), ("查 Nginx 进程", "check"), ("查 PHP-FPM 9000", "check"),
         ("查 MySQL 3306", "check"), ("看 error.log", "step"), ("修复后重启", "end")],
        branch=(0, "未监听", [("看 nginx -t 配置", "warn")]),
    )


def fig_er_student():
    return g_er(
        [("students", [("id INT", "PK"), ("name VARCHAR", ""), ("age INT", ""), ("gender VARCHAR", "")]),
         ("scores", [("id INT", "PK"), ("student_id INT", "FK"), ("course_id INT", "FK"), ("score DEC", "")]),
         ("courses", [("id INT", "PK"), ("name VARCHAR", ""), ("credit INT", "")])],
        [("students", "scores", "1 : N"), ("scores", "courses", "N : 1")],
    )


def fig_er_simple():
    return g_er(
        [("students", [("id INT", "PK"), ("name VARCHAR", ""), ("age INT", "")]),
         ("scores", [("id INT", "PK"), ("student_id INT", "FK"), ("score DEC", "")])],
        [("students", "scores", "1 : N")],
    )


def fig_join():
    return g_er(
        [("students", [("id INT", "PK"), ("name VARCHAR", "")]),
         ("scores", [("student_id INT", "FK"), ("course_id INT", "FK"), ("score DEC", "")]),
         ("courses", [("id INT", "PK"), ("name VARCHAR", "")])],
        [("students", "scores", "JOIN"), ("scores", "courses", "JOIN")],
    )


def fig_index():
    return g_timeline(
        [("无索引", "全表扫描 O(n)"), ("建 B+ 树索引", "O(log n)"), ("EXPLAIN 验证", "type=ref"),
         ("覆盖索引", "避免回表")],
    )


def fig_txn():
    return g_sequence(
        ["会话 A", "会话 B"],
        [("会话 A", "会话 A", "BEGIN"),
         ("会话 A", "会话 A", "UPDATE ... WHERE id=1"),
         ("会话 B", "会话 B", "BEGIN → UPDATE 同一行"),
         ("会话 B", "会话 B", "等待锁 / 超时"),
         ("会话 A", "会话 A", "COMMIT"),
         ("会话 B", "会话 B", "拿到锁继续")],
    )


def fig_py_flow():
    return g_flowchart(
        [("定义变量 name", "start"), ("定义函数 add", "step"), ("调用 add(3,4)", "step"),
         ("print 输出 7", "check"), ("for 循环遍历", "step"), ("列表推导式生成新列表", "end")],
    )


def fig_oop():
    return g_topology(
        [("cls", "class Person", "属性 + 方法", "server", 0, 0),
         ("obj", "实例 p1", "张三 / 19", "pc", 1, 0),
         ("obj2", "实例 p2", "李四 / 20", "pc", 1, 1),
         ("base", "父类 object", "继承链", "cloud", 2, 0)],
        [("cls", "obj", "__init__"), ("cls", "obj2", "__init__"), ("base", "cls", "继承")],
    )


def fig_algo():
    return g_flowchart(
        [("输入数据", "start"), ("排序 / 建索引", "step"), ("双指针或哈希", "step"),
         ("比较并更新最优解", "check"), ("输出结果", "end")],
    )


def fig_crawler():
    return g_flowchart(
        [("发送 requests", "start"), ("检查状态码 200", "check"), ("解析 HTML（bs4/xpath）", "step"),
         ("结构化存 DataFrame", "step"), ("清洗空值去重", "step"), ("导出 csv", "end")],
        branch=(1, "非 200", [("加重试 / 换 UA", "warn")]),
    )


def fig_git():
    return g_topology(
        [("wd", "工作区", "未跟踪改动", "pc", 0, 0),
         ("ix", "暂存区", "git add", "host", 1, 0),
         ("repo", "本地仓库", "git commit", "server", 2, 0),
         ("rem", "远程仓库", "git push", "cloud", 3, 0)],
        [("wd", "ix", "add"), ("ix", "repo", "commit"), ("repo", "rem", "push"), ("rem", "wd", "pull")],
    )


def fig_tdd():
    return g_flowchart(
        [("写一个失败的测试", "start"), ("运行确认变红", "check"), ("写最小实现", "step"),
         ("运行确认变绿", "check"), ("重构保持绿", "step"), ("提交", "end")],
    )


def fig_uml():
    return g_er(
        [("Order", [("+ id: int", "PK"), ("+ status: str", ""), ("+ total()", "")]),
         ("OrderItem", [("+ sku: str", "FK"), ("+ qty: int", ""), ("+ price()", "")]),
         ("User", [("+ id: int", "PK"), ("+ name: str", "")])],
        [("User", "Order", "1 : N"), ("Order", "OrderItem", "1 : N")],
    )


def fig_scrum():
    return g_timeline(
        [("产品待办", "Product Backlog"), ("迭代计划", "Sprint Planning"), ("每日站会", "Daily"),
         ("评审", "Review"), ("回顾", "Retro")],
    )


def fig_circuit():
    return g_topology(
        [("l1", "L1 相线", "黄", "cloud", 0, 0),
         ("qf", "QF 断路器", "三相主开关", "router", 1, 0),
         ("km", "KM 接触器", "主触点", "switch", 2, 0),
         ("fr", "FR 热继电器", "过载保护", "host", 3, 0),
         ("m", "M 三相电机", "U V W", "server", 4, 0)],
        [("l1", "qf", "进线"), ("qf", "km", ""), ("km", "fr", ""), ("fr", "m", "")],
    )


def fig_motor_ctrl():
    return g_flowchart(
        [("合上 QF 总电源", "start"), ("按 SB2 正转启动", "step"), ("KM1 自锁保持", "step"),
         ("按 SB1 停止", "check"), ("按 SB3 反转启动", "step"), ("KM2 互锁生效", "end")],
        branch=(2, "过载", [("FR 断开 → 停机", "warn")]),
    )


def fig_plc_scan():
    return g_flowchart(
        [("输入采样 Input", "start"), ("执行用户程序", "step"), ("输出刷新 Output", "step"),
         ("扫描周期循环", "end")],
    )


def fig_485():
    return g_topology(
        [("mst", "主站 PLC", "站号 1", "server", 0, 0),
         ("s1", "从站 1", "波特率 9600", "pc", 1, 0),
         ("s2", "从站 2", "校验 Even", "pc", 1, 1),
         ("term", "终端电阻", "120Ω", "host", 2, 0)],
        [("mst", "s1", "A/B 双绞线"), ("mst", "s2", "A/B 双绞线"), ("s2", "term", "两端各一")],
    )


def fig_pcb():
    return g_flowchart(
        [("原理图设计", "start"), ("ERC 电气规则检查", "check"), ("PCB 布局", "step"),
         ("布线 / 铺铜", "step"), ("DRC 检查", "check"), ("导出 Gerber 打样", "end")],
        branch=(1, "有错", [("回改原理图", "warn")]),
    )


def fig_power_seq():
    return g_flowchart(
        [("输入整流滤波", "start"), ("PWM 开关变换", "step"), ("高频变压器隔离", "step"),
         ("输出整流滤波", "step"), ("反馈稳压", "check"), ("带载测试纹波", "end")],
    )


def fig_multimeter():
    return g_timeline(
        [("电压 V", "并联测量"), ("电阻 Ω", "断电测量"), ("电流 A", "串联测量"),
         ("通断蜂鸣", "短路检测")],
    )


def fig_ae_seq():
    return g_topology(
        [("imp", "导入素材", "视频/图片", "pc", 0, 0),
         ("comp", "新建合成", "1920×1080", "host", 1, 0),
         ("layer", "图层 + 关键帧", "位置/缩放", "server", 2, 0),
         ("fx", "缓动与表达式", "ease / wiggle", "switch", 3, 0),
         ("out", "渲染输出", "H.264", "cloud", 4, 0)],
        [("imp", "comp", ""), ("comp", "layer", ""), ("layer", "fx", ""), ("fx", "out", "")],
    )


def fig_pr_flow():
    return g_flowchart(
        [("建项目与序列", "start"), ("粗剪取材", "step"), ("精剪与转场", "step"),
         ("调色（Lumetri）", "step"), ("字幕与音频", "step"), ("导出 H.264", "end")],
    )


def fig_color():
    return g_timeline(
        [("一级校正", "曝光 / 白平衡"), ("曲线", "对比度"), ("色轮", "阴影中间调高光"),
         ("LUT", "风格化"), ("限定器", "局部修色")],
    )


def fig_photo():
    return g_topology(
        [("key", "主光 Key", "45° 侧前", "server", 0, 0),
         ("fill", "辅光 Fill", "补暗部", "pc", 1, 0),
         ("rim", "轮廓光", "分离背景", "switch", 1, 1),
         ("bg", "背景光", "控制层次", "cloud", 2, 0)],
        [("key", "fill", "光比 3:1"), ("key", "rim", ""), ("fill", "bg", "")],
    )


def fig_threepoint():
    return g_timeline(
        [("光圈", "f/1.8 浅景深"), ("快门", "1/125s"), ("ISO", "100 低噪点"), ("色温", "5600K 日光")],
    )


def fig_ui():
    return g_topology(
        [("grid", "栅格系统", "8pt", "host", 0, 0),
         ("color", "色彩体系", "主色+中性", "switch", 1, 0),
         ("type", "字体层级", "H1-H6", "pc", 2, 0),
         ("comp", "组件库", "Button/Card", "server", 3, 0)],
        [("grid", "color", ""), ("color", "type", ""), ("type", "comp", "")],
    )


def fig_funnel():
    return g_flowchart(
        [("曝光 UV 10000", "start"), ("点击 3000", "step"), ("加购 900", "step"),
         ("下单 450", "step"), ("支付 360", "end")],
    )


def fig_pricing():
    return g_timeline(
        [("成本 Cost", "进货+物流"), ("售价 Price", "市场定价"), ("毛利", "P-C"),
         ("毛利率", "毛利/P"), ("保本销量", "固定成本/毛利")],
    )


def fig_supply():
    return g_topology(
        [("sup", "供应商", "交期 7 天", "cloud", 0, 0),
         ("wh", "仓库", "安全库存", "server", 1, 0),
         ("shop", "店铺", "日销 30 单", "pc", 2, 0)],
        [("sup", "wh", "补货"), ("wh", "shop", "发货"), ("shop", "sup", "销量预测")],
    )


def fig_aida():
    return g_timeline(
        [("A 注意", "主图/标题"), ("I 兴趣", "卖点详情"), ("D 欲望", "评价/优惠"),
         ("A 行动", "下单支付")],
    )


def fig_marketing():
    return g_flowchart(
        [("设定活动目标", "start"), ("选品与折扣", "step"), ("投放渠道", "step"),
         ("看 ROI", "check"), ("超支则关停", "end")],
        branch=(3, "ROI<1", [("暂停计划", "warn")]),
    )


def fig_cashflow():
    return g_timeline(
        [("订单金额", "GMV"), ("平台佣金", "5%"), ("支付手续费", "0.6%"),
         ("退款", "3%"), ("实际到账", "净额")],
    )


def fig_abc():
    return g_bar([("A 类", 70), ("B 类", 20), ("C 类", 10)])


# 图形注册表：模板里写 "figure": "vlan" 即可
FIGURES = {
    "switch_basic": fig_switch_basic, "vlan": fig_vlan, "ospf": fig_ospf, "stp": fig_stp,
    "dhcp": fig_dhcp, "acl": fig_acl, "nat": fig_nat,
    "linux_basic": fig_linux_basic, "nginx": fig_nginx, "perm": fig_perm,
    "shell_flow": fig_shell_flow, "lnmp": fig_lnmp,
    "er_student": fig_er_student, "er_simple": fig_er_simple, "join": fig_join,
    "index": fig_index, "txn": fig_txn,
    "py_flow": fig_py_flow, "oop": fig_oop, "algo": fig_algo, "crawler": fig_crawler,
    "git": fig_git, "tdd": fig_tdd, "uml": fig_uml, "scrum": fig_scrum,
    "circuit": fig_circuit, "motor_ctrl": fig_motor_ctrl, "plc_scan": fig_plc_scan,
    "rs485": fig_485, "pcb": fig_pcb, "power_seq": fig_power_seq, "multimeter": fig_multimeter,
    "ae_seq": fig_ae_seq, "pr_flow": fig_pr_flow, "color": fig_color,
    "photo": fig_photo, "threepoint": fig_threepoint, "ui": fig_ui,
    "funnel": fig_funnel, "pricing": fig_pricing, "supply": fig_supply,
    "aida": fig_aida, "marketing": fig_marketing, "cashflow": fig_cashflow, "abc": fig_abc,
}


# ============================================================
# ★ 配图库：老师手动选择（不再自动猜）
# ============================================================
_FIG_ORDER = list(FIGURES.keys())

# 图名 → (中文名, 分类)，老师按分类挑图，看得懂
_FIG_META = {
    "switch_basic": ("交换机基础配置拓扑", "计算机网络"),
    "vlan": ("VLAN 划分与 Trunk 链路", "计算机网络"),
    "ospf": ("OSPF 多区域邻居关系", "计算机网络"),
    "stp": ("STP 生成树阻塞端口", "计算机网络"),
    "dhcp": ("DHCP 地址池分配", "计算机网络"),
    "acl": ("ACL 访问控制匹配", "计算机网络"),
    "nat": ("NAT 地址转换", "计算机网络"),
    "linux_basic": ("Linux 命令与系统分层", "Linux 运维"),
    "nginx": ("Nginx 站点服务链路", "Linux 运维"),
    "perm": ("Linux 权限 rwx 模型", "Linux 运维"),
    "shell_flow": ("Shell 脚本执行流程", "Linux 运维"),
    "lnmp": ("LNMP 整站部署链路", "Linux 运维"),
    "er_student": ("学生成绩库 ER 图", "数据库技术"),
    "er_simple": ("单表建表结构", "数据库技术"),
    "join": ("多表 JOIN 查询过程", "数据库技术"),
    "index": ("B+ 树索引与执行计划", "数据库技术"),
    "txn": ("事务 ACID 与回滚流程", "数据库技术"),
    "py_flow": ("Python 程序执行流程", "Python 编程"),
    "oop": ("类 / 继承 / 对象关系", "Python 编程"),
    "algo": ("排序算法过程", "Python 编程"),
    "crawler": ("数据采集与清洗流程", "Python 编程"),
    "git": ("Git 分支与提交图", "软件工程"),
    "tdd": ("TDD 红绿重构循环", "软件工程"),
    "uml": ("UML 类图关系", "软件工程"),
    "scrum": ("Scrum 迭代与看板", "软件工程"),
    "circuit": ("串并联电路", "电气自动化"),
    "motor_ctrl": ("电动机正反转互锁控制", "电气自动化"),
    "plc_scan": ("PLC 扫描周期", "电气自动化"),
    "rs485": ("RS485 / Modbus 主从通信", "电气自动化"),
    "pcb": ("PCB 布局布线", "电气自动化"),
    "power_seq": ("开关电源上电时序", "电气自动化"),
    "multimeter": ("万用表测量方法", "电气自动化"),
    "ae_seq": ("AE 关键帧与合成", "数字媒体"),
    "pr_flow": ("PR 剪辑到导出流程", "数字媒体"),
    "color": ("调色一级校色", "数字媒体"),
    "photo": ("光圈快门 ISO 曝光三角", "数字媒体"),
    "threepoint": ("三点布光位", "数字媒体"),
    "ui": ("UI 栅格与配色", "数字媒体"),
    "funnel": ("电商转化漏斗", "电子商务"),
    "pricing": ("定价与毛利结构", "电子商务"),
    "supply": ("供应链与库存周转", "电子商务"),
    "aida": ("AIDA 活动策划", "电子商务"),
    "marketing": ("活动投放节奏", "电子商务"),
    "cashflow": ("GMV 到实际到账", "电子商务"),
    "abc": ("ABC 品类分类", "电子商务"),
}


def _fig_label(name):
    """图名 → 中文名"""
    return _FIG_META.get(name, (name, ""))[0]


def _fig_groups():
    """按专业分类返回 {分类: [(图名, 中文名)]}，供老师挑图"""
    out = {}
    for name in _FIG_ORDER:
        label, grp = _FIG_META.get(name, (name, "其他"))
        out.setdefault(grp, []).append((name, label))
    return out


def figure_picker(label, default_fig="", key="", help_text=""):
    """
    老师手动选图控件：先选专业分类，再选具体图。
    返回选中的图名（绝不返回空）。
    """
    groups = _fig_groups()
    gnames = list(groups.keys())

    # 默认分类 = 默认图所属分类
    d_group = gnames[0]
    for g, items in groups.items():
        if any(n == default_fig for n, _ in items):
            d_group = g
            break

    c1, c2 = st.columns([1, 2])
    with c1:
        g = st.selectbox("{} · 分类".format(label), gnames,
                         index=gnames.index(d_group), key="{}_g".format(key),
                         help=help_text or "先选专业方向")
    with c2:
        items = groups[g]
        labels = [lb for _n, lb in items]
        names = [n for n, _lb in items]
        di = names.index(default_fig) if default_fig in names else 0
        pick = st.selectbox("{} · 具体图".format(label), labels, index=di,
                            key="{}_f".format(key))
        return names[labels.index(pick)]


@st.cache_resource(show_spinner=False)
def _figure_cached(name):
    """图是纯字符串计算，缓存到资源级 → 同一张图只算一次"""
    fn = FIGURES.get(name)
    if not fn:
        return ""
    try:
        return fn()
    except Exception:
        return ""


def figure_svg(name):
    """取图的 SVG 字符串（带缓存）"""
    if not name or name not in FIGURES:
        return ""
    return _figure_cached(name)


def render_figure(name, caption=""):
    """在页面上渲染一张图；caption 显示在图下方"""
    svg = figure_svg(name)
    if not svg:
        st.caption("（暂无配图）")
        return
    st.markdown(
        '<div style="border:1px solid #E3E8EF;border-radius:10px;padding:10px;'
        'background:#fff;overflow-x:auto">{}</div>'.format(svg),
        unsafe_allow_html=True)
    if caption:
        st.caption(caption)


# ============================================================
# ★ 配图定制：图上文字可替换 + 老师可上传自己的图
#     存库格式（tasks.figure 字段）：
#        "图库名"                          → 直接用图库图
#        "lib:图库名|旧=>新;旧=>新"        → 图库图 + 文字替换
#        "img:<base64>"                    → 老师上传的图片
# ============================================================
MAX_UPLOAD_BYTES = 1024 * 1024  # 单图 1MB


def parse_figure_spec(spec):
    """
    解析 figure 字段 → (类型, 图库名, 替换表, base64)
      类型: "lib" 用图库图（可能带替换）/ "img" 上传图 / "" 无图
    """
    spec = (spec or "").strip()
    if not spec:
        return "", "", [], ""
    if spec.startswith("img:"):
        return "img", "", [], spec[4:]

    body = spec[4:] if spec.startswith("lib:") else spec
    name, _, repl_raw = body.partition("|")
    name = name.strip()
    repl = []
    for pair in repl_raw.split(";"):
        pair = pair.strip()
        if not pair or "=>" not in pair:
            continue
        a, b = pair.split("=>", 1)
        a = a.strip()
        if a:
            repl.append((a, b.strip()))
    return "lib", name, repl, ""


def build_figure_spec(kind, name="", repl=None, b64=""):
    """反向拼装 figure 字段"""
    if kind == "img" and b64:
        return "img:" + b64
    if not name:
        return ""
    repl = [p for p in (repl or []) if p[0].strip()]
    if not repl:
        return name
    return "lib:" + name + "|" + ";".join("{} => {}".format(a, b) for a, b in repl)


def svg_texts(svg):
    """把 SVG 里所有文本节点抽出来，供老师参考改哪个词"""
    return re.findall(r">([^<>]{1,60})<", svg or "")


def apply_text_repl(svg, repl):
    """在 SVG 上做文字替换（只替换 > 文本 < 之间的内容，不碰标签属性）"""
    if not svg or not repl:
        return svg

    def _sub(m):
        inner = m.group(1)
        out = inner
        for a, b in repl:
            if a and b and a != b:
                out = out.replace(a, b)
        return ">" + out + "<"

    return re.sub(r">([^<>]{1,200})<", _sub, svg)


def figure_svg_custom(spec):
    """按 figure 字段生成最终 SVG（带文字替换）"""
    kind, name, repl, b64 = parse_figure_spec(spec)
    if kind != "lib":
        return ""
    base = figure_svg(name)
    return apply_text_repl(base, repl)


def render_figure_spec(spec, caption=""):
    """统一渲染：图库图 / 改字图 / 上传图"""
    kind, name, repl, b64 = parse_figure_spec(spec)
    if kind == "img" and b64:
        try:
            raw = base64.b64decode(b64)
        except Exception:
            st.caption("（上传的图片已损坏）")
            return
        st.image(raw, use_container_width=True)
        if caption:
            st.caption(caption)
        return
    if kind == "lib" and name:
        svg = figure_svg_custom(spec)
        if svg:
            st.markdown(
                '<div style="border:1px solid #E3E8EF;border-radius:10px;padding:10px;'
                'background:#fff;overflow-x:auto">{}</div>'.format(svg),
                unsafe_allow_html=True)
            if caption:
                st.caption(caption)
            return
    st.caption("（暂无配图）")


def figure_editor(default_spec="", key="fig", title="配图"):
    """
    老师端配图编辑器（方案 C）：
      ① 从图库选图（按专业分类）
      ② 图上文字替换（把 PC1 改成财务部PC、改 IP / VLAN 号）
      ③ 或直接上传自己的图片（PNG/JPG/SVG，≤1MB）
    返回 figure 字段字符串（存库用）
    """
    kind0, name0, repl0, b64_0 = parse_figure_spec(default_spec)
    default_fig = name0 if kind0 == "lib" else ""

    mode = st.radio(
        "{}来源".format(title),
        ["图库选图（可改图上文字）", "上传自己的图"],
        index=0 if kind0 != "img" else 1,
        horizontal=True,
        key="{}_mode".format(key),
    )

    # ---------- 上传自己的图 ----------
    if mode == "上传自己的图":
        up = st.file_uploader(
            "上传图片（PNG / JPG / SVG，≤1MB）",
            type=["png", "jpg", "jpeg", "svg"],
            key="{}_up".format(key),
            help="自己用 PPT / Visio / draw.io 画的图都可以，随任务存进数据库，换电脑也不丢",
        )
        if up is not None:
            raw = up.getvalue()
            if len(raw) > MAX_UPLOAD_BYTES:
                st.error("图片 {:.1f}MB 超过 1MB 限制，请压缩后再传".format(
                    len(raw) / 1024 / 1024))
            else:
                b64 = base64.b64encode(raw).decode("ascii")
                st.image(raw, use_container_width=True)
                st.caption("↑ 学生端将看到这张图（{:.0f} KB）".format(len(raw) / 1024))
                st.session_state["{}_b64".format(key)] = b64
        elif b64_0:
            try:
                st.image(base64.b64decode(b64_0), use_container_width=True)
                st.caption("↑ 当前已保存的图")
            except Exception:
                st.caption("（已保存的图无法显示）")
            st.session_state.setdefault("{}_b64".format(key), b64_0)
        else:
            st.info("还没有上传图片。上传后会覆盖图库选图。")
        return build_figure_spec("img", b64=st.session_state.get("{}_b64".format(key), ""))

    # ---------- 图库选图 ----------
    picked = figure_picker(title, default_fig, key="{}_pick".format(key))

    # 文字替换
    svg0 = figure_svg(picked)
    ents = [t.strip() for t in svg_texts(svg0)]
    seen, uniq = set(), []
    for t in ents:
        if t and t not in seen and not t.replace(".", "").replace("-", "").isdigit():
            seen.add(t)
            uniq.append(t)

    st.markdown("**图上文字替换**（不改图结构，只把字换掉）")
    st.caption("例：把 `PC1` 改成 `财务部PC`，把 `192.168.1.10` 改成你班里用的地址。"
               "左列留空表示不改。")
    if uniq:
        with st.expander("👀 这张图上的文字（可复制到下面）", expanded=False):
            st.write("　".join("`{}`".format(t) for t in uniq[:40]))

    n = st.number_input("替换几条", min_value=1, max_value=10,
                        value=max(1, len(repl0)), key="{}_n".format(key))
    repl = []
    for i in range(int(n)):
        c1, c2 = st.columns([1, 1])
        old = c1.text_input("原文字 {}".format(i + 1),
                            value=repl0[i][0] if i < len(repl0) else "",
                            key="{}_o{}".format(key, i), placeholder="PC1")
        new = c2.text_input("改成 {}".format(i + 1),
                            value=repl0[i][1] if i < len(repl0) else "",
                            key="{}_v{}".format(key, i), placeholder="财务部PC")
        if old.strip():
            repl.append((old.strip(), new.strip()))

    preview_spec = build_figure_spec("lib", picked, repl)
    render_figure_spec(preview_spec, "↑ 学生端将看到这张（{}）".format(_fig_label(picked)))
    return preview_spec


# ============================================================
# 命令匹配工具
# ============================================================
def norm(s):
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def squash(s):
    """去空格比对：'ip address 1.1.1.1 255.255.255.0' 与 'ipaddress1.1.1.1...' 等价"""
    return re.sub(r"\s+", "", str(s or "").strip().lower())


def any_hit(cmd, keywords):
    s = squash(cmd)
    return any(k.lower().replace(" ", "") in s for k in keywords)


def strip_sudo(cmd):
    low = norm(cmd)
    return low[5:].strip() if low.startswith("sudo ") else low


def ok(msg="OK"):
    return True, msg


def err(msg):
    return False, msg


def caret_err(cmd):
    """VRP 风格的 ^ 错误提示，指在出错位置"""
    pad = " " * min(len(str(cmd)), 20)
    return "Error: Unrecognized command found at '^' position.\n{}^".format(pad)


# ============================================================
# 模拟器 1：网络设备 VRP（有视图栈，会记 IP / VLAN / 网段）
# ============================================================
class VRPSim:
    def __init__(self):
        self.mode = "user"          # user / system / if / vlan / ospf / area / acl
        self.iface = ""
        self.if_mode = {}
        self.vlans = set([1])
        self.vlan_ports = {}
        self.if_ips = {}
        self.ospf = None
        self.ospf_networks = []
        self.acls = {}
        self.stp_mode = None
        self.stp_on = False
        self.dhcp_pool = None
        self.saved = False
        self.history = []

    # ---- 提示符 ----
    def prompt(self):
        m = self.mode
        if m == "user":
            return "<Huawei>"
        if m == "system":
            return "[Huawei]"
        if m == "if":
            return "[Huawei-GigabitEthernet{}]".format(self.iface)
        if m == "vlan":
            return "[Huawei-vlan{}]".format(self._cur_vlan)
        if m == "ospf":
            return "[Huawei-ospf-1]"
        if m == "area":
            return "[Huawei-ospf-1-area-0.0.0.0]"
        if m == "acl":
            return "[Huawei-acl-adv-{}]".format(self._cur_acl)
        return "[Huawei]"

    _cur_vlan = ""
    _cur_acl = ""

    def _if_short(self, s):
        s = s.replace("gigabitethernet", "").replace("g", "").replace(" ", "")
        s = s.strip("/")
        m = re.match(r"^(\d+)/(\d+)/(\d+)$", s)
        if m:
            return "0/0/{}".format(m.group(3))
        m = re.match(r"^(\d+)/(\d+)$", s)
        if m:
            return "0/0/{}".format(m.group(2))
        return s

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        self.history.append(cmd)
        bare = cmd.rstrip(";").strip()

        # ---- 通用 ----
        if bare in ("quit", "q"):
            order = ["area", "ospf", "acl", "vlan", "if", "system", "user"]
            i = order.index(self.mode) if self.mode in order else len(order) - 1
            nxt = order[min(i + 1, len(order) - 1)]
            self.mode = "system" if nxt == "user" and i + 1 < len(order) else nxt
            if self.mode == "user":
                return ok("")
            return ok("")
        if bare in ("return", "ctrl+z"):
            self.mode = "system"
            return ok("")
        if bare == "display version":
            return ok("Huawei Versatile Routing Platform Software\nVRP (R) software, Version 5.170 (S5720 V200R019C10)\nCopyright (C) 2012-2023 Huawei Technologies Co., Ltd.")
        if bare == "display current-configuration":
            lines = ["#", "sysname Huawei", "#"]
            for p, ip in self.if_ips.items():
                lines += ["interface GigabitEthernet{}".format(p), " ip address {}".format(ip), "#"]
            if self.vlans - {1}:
                lines.append("vlan batch {}".format(" ".join(str(v) for v in sorted(self.vlans - {1}))))
            if self.ospf:
                lines += ["ospf 1 router-id {}".format(self.ospf["rid"])]
                for n in self.ospf_networks:
                    lines.append(" area 0.0.0.0")
                    lines.append("  network {} {}".format(n[0], n[1]))
            lines.append("return")
            return ok("\n".join(lines))
        if bare == "display ip interface brief":
            rows = ["Interface            IP Address/Mask    Physical  Protocol"]
            for i in range(1, 5):
                p = "0/0/{}".format(i)
                ip = self.if_ips.get(p, "unassigned")
                ph = "up" if ip != "unassigned" else "down"
                rows.append("GE{:<12} {:<18} {:<9} {}".format(p, ip, ph, ph))
            return ok("\n".join(rows))

        # ---- 系统视图 ----
        if bare in ("system-view", "sys"):
            if self.mode != "user":
                return err("Error: 已在系统视图内")
            self.mode = "system"
            return ok("Enter system view, return user view with Ctrl+Z.")

        if bare == "save":
            if self.mode == "user":
                return err(caret_err(raw))
            if not self.if_ips and not self.vlans - {1} and not self.ospf:
                return ok("The current configuration is saved.")
            self.saved = True
            return ok("The current configuration will be written to the device.\nAre you sure to continue?[Y/N]y\nInfo: The configuration takes effect on the next startup.")

        if bare.startswith("sysname"):
            if self.mode == "user":
                return err(caret_err(raw))
            return ok("")

        # ---- 接口 ----
        m = re.match(r"^interface\s+(.+)$", bare)
        if m:
            if self.mode not in ("system", "if"):
                return err(caret_err(raw))
            p = self._if_short(m.group(1))
            self.iface = p
            self.mode = "if"
            return ok("")
        if bare.startswith("ip address"):
            if self.mode != "if":
                return err("Error: 请先进入接口视图（interface GigabitEthernet0/0/x）")
            mm = re.match(r"^ip address\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+\.\d+\.\d+\.\d+)$", bare)
            if not mm:
                # 允许 24 位掩码写法
                mm2 = re.match(r"^ip address\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+)$", bare)
                if not mm2:
                    return err(caret_err(raw))
                bit = int(mm2.group(2))
                mask = ".".join(str((0xFFFFFFFF << (32 - bit) & 0xFFFFFFFF) >> (8 * i) & 0xFF) for i in range(3, -1, -1))
                ipv = "{}/{}".format(mm2.group(1), bit)
            else:
                ipv = "{}/{}".format(mm.group(1), mm.group(2))
            _ip = ipv.split("/")[0]
            if _ip.endswith(".0") or _ip.endswith(".255"):
                return err("Error: The specified address is a network address or broadcast address.")
            self.if_ips[self.iface] = ipv
            return ok("")
        if bare in ("undo shutdown", "shutdown"):
            return ok("Info: Interface status changed.")

        # ---- VLAN ----
        m = re.match(r"^vlan batch\s+(.+)$", bare)
        if m:
            if self.mode == "user":
                return err(caret_err(raw))
            ids = [int(x) for x in re.findall(r"\d+", m.group(1))]
            self.vlans.update(ids)
            return ok("Info: This operation may take a few seconds. Please wait...done.")
        m = re.match(r"^vlan\s+(\d+)$", bare)
        if m:
            if self.mode == "user":
                return err(caret_err(raw))
            vid = int(m.group(1))
            self.vlans.add(vid)
            self._cur_vlan = str(vid)
            self.mode = "vlan"
            return ok("")
        if bare.startswith("port link-type"):
            if self.mode != "if":
                return err("Error: 请先进入接口视图")
            lt = bare.split()[-1]
            if lt not in ("access", "trunk", "hybrid"):
                return err(caret_err(raw))
            self.if_mode[self.iface] = lt
            return ok("")
        if bare.startswith("port default vlan") or bare.startswith("port trunk pvid vlan"):
            if self.mode != "if":
                return err("Error: 请先进入接口视图")
            mm = re.search(r"(\d+)$", bare)
            if not mm:
                return err(caret_err(raw))
            vid = int(mm.group(1))
            if vid not in self.vlans:
                return err("Error: The VLAN does not exist.")
            self.vlan_ports[self.iface] = vid
            return ok("")
        if bare.startswith("port trunk allow-pass vlan"):
            if self.mode != "if":
                return err("Error: 请先进入接口视图")
            if self.if_mode.get(self.iface) != "trunk":
                return err("Error: 该端口不是 trunk 类型")
            ids = set(int(x) for x in re.findall(r"\d+", bare))
            if not ids:
                return err(caret_err(raw))
            self.vlan_ports[self.iface] = ("trunk", ids)
            self.vlans.update(ids)
            return ok("")
        if bare == "display vlan":
            rows = ["The total number of vlans is : {}".format(len(self.vlans)), "-" * 52,
                    "U: Up;         D: Down;         TG: Tagged;         UT: Untagged;",
                    "-" * 52, "VID  TYPE    PORT LIST"]
            for v in sorted(self.vlans):
                ports = [p for p, m2 in self.if_mode.items() if m2 == "access" and self.vlan_ports.get(p) == v]
                rows.append("{:<4} common  {}".format(v, " ".join("GE0/0/{}".format(p.split('/')[-1]) for p in ports) or "-"))
            return ok("\n".join(rows))
        m = re.match(r"^display vlan\s+(\d+)$", bare)
        if m:
            v = int(m.group(1))
            if v not in self.vlans:
                return err("Error: The VLAN does not exist.")
            ports = [p for p, m2 in self.if_mode.items() if m2 == "access" and self.vlan_ports.get(p) == v]
            return ok("VLAN ID: {}\nVLAN Type: common\nStatus: enable\nPorts: {}".format(
                v, " ".join("GE0/0/{} (U)".format(p.split('/')[-1]) for p in ports) or "none"))

        # ---- OSPF ----
        m = re.match(r"^ospf\s+(\d+)(?:\s+router-id\s+(\d+\.\d+\.\d+\.\d+))?$", bare)
        if m:
            if self.mode == "user":
                return err(caret_err(raw))
            self.ospf = {"pid": int(m.group(1)), "rid": m.group(2) or "0.0.0.0"}
            self.mode = "ospf"
            return ok("")
        m = re.match(r"^area\s+(\d+)$", bare)
        if m:
            if self.mode != "ospf":
                return err("Error: 请先进入 OSPF 视图（ospf 1 router-id x.x.x.x）")
            self.mode = "area"
            self._area = m.group(1)
            return ok("")
        m = re.match(r"^network\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+\.\d+\.\d+\.\d+)$", bare)
        if m:
            if self.mode != "area":
                return err("Error: 请先进入 area 0 视图")
            if not self.ospf:
                return err("Error: OSPF 进程未启动")
            self.ospf_networks.append((m.group(1), m.group(2)))
            return ok("")
        if bare == "display ospf peer":
            if not self.ospf:
                return err("Error: OSPF 进程未启动")
            if not self.ospf_networks:
                return ok("OSPF Process 1 with Router ID {}\n\n No peer.".format(self.ospf["rid"]))
            return ok(
                "OSPF Process 1 with Router ID {}\n Neighbors\n\n"
                " Area 0.0.0.0 interface 10.0.0.1(GigabitEthernet0/0/0)'s neighbors\n"
                " Router ID: 2.2.2.2         Address: 10.0.0.2\n"
                "   State: Full  Mode: Nbr is Master  Priority: 1\n"
                "   Dead timer due in 36 sec".format(self.ospf["rid"])
            )
        if bare == "display ospf routing":
            if not self.ospf_networks:
                return ok("OSPF Process 1 with Router ID {}\n No routing table.".format(
                    self.ospf["rid"] if self.ospf else "0.0.0.0"))
            return ok("Routing Tables\nDestination        Cost  Type       NextHop         AdvRouter\n"
                      "192.168.2.0/24     2     Stub       10.0.0.2        2.2.2.2")

        # ---- STP ----
        if bare.startswith("stp mode"):
            if self.mode == "user":
                return err(caret_err(raw))
            self.stp_mode = bare.split()[-1]
            return ok("")
        if bare in ("stp enable", "stp disable"):
            if self.mode == "user":
                return err(caret_err(raw))
            self.stp_on = bare == "stp enable"
            return ok("")
        if bare == "display stp brief":
            if not self.stp_on:
                return ok("Error: STP is disabled.")
            return ok("MSTID  Port          Role  STP State    Protection\n"
                      "  0    GE0/0/1       ROOT  FORWARDING   NONE\n"
                      "  0    GE0/0/2       DESI  FORWARDING   NONE\n"
                      "  0    GE0/0/3       ALTE  DISCARDING   NONE")

        # ---- DHCP ----
        m = re.match(r"^dhcp enable$", bare)
        if m:
            if self.mode == "user":
                return err(caret_err(raw))
            self.dhcp_pool = True
            return ok("Info: The DHCP configuration has been enabled.")
        m = re.match(r"^ip pool\s+(\S+)$", bare)
        if m:
            self.dhcp_pool = m.group(1)
            return ok("")
        if self.dhcp_pool and re.match(
                r"^(gateway-list|dns-list|lease|excluded-ip-address|netbios-type|domain-name)\b", bare):
            return ok("")
        if bare.startswith("network ") and self.dhcp_pool:
            return ok("")
        if bare == "display ip pool":
            return ok("Pool-name      : pool1\nNetwork        : 192.168.1.0 mask 255.255.255.0\n"
                      "Gateway        : 192.168.1.1\nLease          : 1 day 0 hour\n"
                      "Idle           : 253\nUsed           : 0")

        # ---- ACL ----
        m = re.match(r"^acl\s+(?:number\s+)?(\d{4})$", bare)
        if m:
            if self.mode == "user":
                return err(caret_err(raw))
            n = m.group(1)
            self.acls.setdefault(n, [])
            self._cur_acl = n
            self.mode = "acl"
            return ok("")
        if bare.startswith("rule "):
            if self.mode != "acl":
                return err("Error: 请先创建并进入 ACL 视图（acl 3000）")
            n = self._cur_acl
            self.acls.setdefault(n, []).append(bare[5:])
            return ok("")
        if bare.startswith("display acl"):
            mm = re.search(r"(\d{4})", bare)
            if not mm:
                return ok("No ACL configured.")
            n = mm.group(1)
            rules = self.acls.get(n)
            if not rules:
                return err("Error: The ACL does not exist.")
            body = "\n".join("rule {} {}".format((i + 1) * 5, r) for i, r in enumerate(rules))
            return ok("Basic ACL {}\n{}".format(n, body))
        if bare.startswith("traffic-filter"):
            if self.mode != "if":
                return err("Error: 请先进入接口视图（interface GigabitEthernet0/0/1）")
            if not self.acls:
                return err("Error: ACL 不存在")
            return ok("Info: The traffic filter has been applied.")

        return err(caret_err(raw))


# ============================================================
# 模拟器 2：Linux（有 cwd / 用户 / 软件包 / 服务 / 文件）
# ============================================================
class LinuxSim:
    def __init__(self):
        self.cwd = "/home/student"
        self.user = "student"
        self.files = {
            "/home/student/Desktop": "dir",
            "/home/student/Documents": "dir",
            "/home/student/Downloads": "dir",
            "/etc/os-release": 'NAME="CentOS Linux"\nVERSION="7 (Core)"\nID="centos"',
            "/etc/passwd": "root:x:0:0:root:/root:/bin/bash\nstudent:x:1000:1000::/home/student:/bin/bash",
        }
        self.pkgs = set(["bash", "vim", "coreutils"])
        self.services = {"sshd": "active", "firewalld": "active"}
        self.ports = {22: "sshd"}
        self.fw_ports = set()
        self.users = {"root": 0, "student": 1000}
        self.cron = []
        self.display = ":0"

    def prompt(self):
        short = "~" if self.cwd.startswith("/home/student") else self.cwd
        return "[{}@localhost {}]{}".format(self.user, short, "$" if self.user != "root" else "#")

    def _isdir(self, p):
        return p.rstrip("/") in ("/home/student", "/etc", "/var", "/var/log", "/var/log/nginx", "/home") or \
               self.files.get(p.rstrip("/")) == "dir"

    def _abs(self, p):
        if p.startswith("/"):
            return p.rstrip("/") or "/"
        if p.startswith("~"):
            return ("/home/student" + p[1:]).rstrip("/") or "/home/student"
        return (self.cwd.rstrip("/") + "/" + p).rstrip("/")

    def _files_in(self, d):
        base = d.rstrip("/") + "/"
        return sorted(set([k[len(base):].split("/")[0] for k in self.files if k.startswith(base) and len(k) > len(base)]))

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        sudo = cmd.startswith("sudo ")
        body = strip_sudo(cmd)

        # 基础信息
        if body == "pwd":
            return ok(self.cwd)
        if body == "whoami":
            return ok(self.user)
        if body == "id":
            return ok("uid={}({}) gid={}({}) groups={}({})".format(
                self.users.get(self.user, 1000), self.user, self.users.get(self.user, 1000), self.user,
                self.users.get(self.user, 1000), self.user))
        if body == "hostname":
            return ok("localhost.localdomain")
        if body in ("uname -a", "uname -r"):
            return ok("Linux localhost.localdomain 3.10.0-1160.el7.x86_64 #1 SMP x86_64 GNU/Linux"
                      if body == "uname -a" else "3.10.0-1160.el7.x86_64")
        if body in ("ls", "ls -l", "ls -la", "ls -al", "ll"):
            items = self._files_in(self.cwd)
            if not items:
                return ok("")
            if body == "ls":
                return ok("  ".join(items))
            lines = ["total {}".format(len(items) * 4)]
            for it in items:
                p = self.cwd.rstrip("/") + "/" + it
                isd = self._isdir(p)
                perm = "drwxr-xr-x" if isd else "-rw-r--r--"
                lines.append("{} {} {} {:>6} root root {} {}".format(
                    perm, 2 if isd else 1, "student", "4096" if isd else "12",
                    "Oct 31 09:00", it))
            return ok("\n".join(lines))
        if body.startswith("ls -"):
            target = self._abs(body.split()[-1]) if len(body.split()) > 1 else self.cwd
            items = self._files_in(target)
            return ok("  ".join(items) or "总用量 0")
        if body.startswith("cd ") or body == "cd":
            tgt = body[3:].strip() if len(body) > 2 else "/home/student"
            if tgt == "-" or tgt == "":
                tgt = "/home/student"
            p = self._abs(tgt)
            if not self._isdir(p):
                return err("-bash: cd: {}: 没有那个文件或目录".format(tgt))
            self.cwd = p
            return ok("")
        if body.startswith("cat "):
            p = self._abs(body[4:].strip())
            c = self.files.get(p)
            if c is None:
                return err("cat: {}: 没有那个文件或目录".format(body[4:].strip()))
            if c == "dir":
                return err("cat: {}: 是一个目录".format(body[4:].strip()))
            return ok(c)
        if body.startswith("mkdir"):
            parts = body.split()
            p = self._abs(parts[-1])
            self.files[p] = "dir"
            return ok("")
        if body.startswith("touch "):
            p = self._abs(body[6:].strip())
            self.files.setdefault(p, "")
            return ok("")
        if body.startswith("rm "):
            p = self._abs(body.split()[-1])
            if p not in self.files:
                return err("rm: 无法删除'{}': 没有那个文件或目录".format(body.split()[-1]))
            del self.files[p]
            return ok("")
        if body.startswith("echo "):
            raw_str = cmd[5:]
            text = raw_str
            if ">" in raw_str:
                left, right = raw_str.split(">", 1)
                right = right.strip()
                append = right.startswith(">")
                if append:
                    right = right[1:].strip()
                target = self._abs(right)
                content = left.strip().strip("'\"")
                if target not in self.files:
                    return err("-bash: {}: 没有那个文件或目录".format(right))
                if append:
                    self.files[target] = (self.files.get(target) or "") + ("\n" if self.files.get(target) else "") + content
                else:
                    self.files[target] = content
                return ok("")
            return ok(text.strip().strip("'\""))

        # 权限
        if body.startswith("chmod"):
            parts = body.split()
            if len(parts) < 3:
                return err("chmod: 缺少操作数")
            p = self._abs(parts[-1])
            if p not in self.files:
                return err("chmod: 无法访问'{}': 没有那个文件或目录".format(parts[-1]))
            self.files[p + ":mode"] = parts[1]
            return ok("")
        if body.startswith("chown"):
            parts = body.split()
            if len(parts) < 3:
                return err("chown: 缺少操作数")
            p = self._abs(parts[-1])
            if p not in self.files:
                return err("chown: 无法访问'{}': 没有那个文件或目录".format(parts[-1]))
            owner = parts[1].split(":")[0]
            self.files[p + ":owner"] = owner
            return ok("")
        if body.startswith("useradd ") or body.startswith("adduser "):
            u = body.split()[-1]
            if u in self.users:
                return err("useradd: user '{}' already exists".format(u))
            self.users[u] = max(self.users.values()) + 1
            return ok("")

        # 软件包 / 服务
        if body.startswith("yum install") or body.startswith("dnf install") or body.startswith("apt install"):
            pkg = body.split()[-1]
            self.pkgs.add(pkg)
            return ok("已加载插件：fastestmirror\n正在解决依赖关系\n---> 软件包 {} 将被 安装\n\n已安装:\n  {}.x86_64 0:1.20.1-10.el7\n\n完毕！".format(pkg, pkg))
        if body.startswith("yum remove") or body.startswith("rpm -e"):
            pkg = body.split()[-1]
            self.pkgs.discard(pkg)
            return ok("完毕！")
        if body.startswith("systemctl start "):
            s = body.split()[-1]
            if s not in self.pkgs and s not in ("sshd", "firewalld", "network"):
                return err("Failed to start {}.service: Unit not found.".format(s))
            self.services[s] = "active"
            if s == "nginx":
                self.ports[80] = "nginx"
            return ok("")
        if body.startswith("systemctl enable "):
            s = body.split()[-1]
            self.services[s + ":enabled"] = "enabled"
            return ok("Created symlink from /etc/systemd/system/multi-user.target.wants/{}.service to /usr/lib/systemd/system/{}.service.".format(s, s))
        if body.startswith("systemctl restart ") or body.startswith("systemctl reload "):
            s = body.split()[-1]
            if s not in self.pkgs and s not in ("sshd", "firewalld", "network"):
                return err("Failed to restart {}.service: Unit not found.".format(s))
            self.services[s] = "active"
            if s == "nginx":
                self.ports[80] = "nginx"
            return ok("")
        if body.startswith("systemctl stop "):
            s = body.split()[-1]
            self.services[s] = "inactive"
            return ok("")
        if body.startswith("systemctl status "):
            s = body.split()[-1]
            st_ = self.services.get(s)
            if not st_:
                return err("Unit {}.service could not be found.".format(s))
            listening = "\n   Active: {} (running)".format(st_)
            if s == "nginx" and st_ == "active":
                listening += "\n   80/tcp 监听中 (nginx)"
            return ok("● {}.service - The {} service\n   Loaded: loaded (/usr/lib/systemd/system/{}.service; enabled)\n{}".format(
                s, s, s, listening))
        if body == "nginx -v":
            return ok("nginx version: nginx/1.20.1" if "nginx" in self.pkgs else err("-bash: nginx: 未找到命令"))
        if body.startswith("nginx -t"):
            return ok("nginx: the configuration file /etc/nginx/nginx.conf syntax is ok\nnginx: configuration file /etc/nginx/nginx.conf test is successful")
        if body == "firewall-cmd --state":
            return ok("running")
        if body.startswith("firewall-cmd") and "--add-port" in body:
            mm = re.search(r"--add-port=(\d+)", body)
            if mm:
                self.fw_ports.add(int(mm.group(1)))
            return ok("success")
        if body.startswith("firewall-cmd") and "--remove-port" in body:
            mm = re.search(r"--remove-port=(\d+)", body)
            if mm:
                self.fw_ports.discard(int(mm.group(1)))
            return ok("success")
        if body.startswith("firewall-cmd") and "--reload" in body:
            return ok("success")
        if body.startswith("firewall-cmd") and "--list-ports" in body:
            return ok(" ".join("{}/tcp".format(p) for p in sorted(self.fw_ports)) or "")
        if body.startswith("ss -") or body.startswith("netstat"):
            rows = ["State   Recv-Q Send-Q Local Address:Port  Peer Address:Port Process"]
            for p, who in sorted(self.ports.items()):
                rows.append("LISTEN  0      128    0.0.0.0:{}        0.0.0.0:*     users:((\"{}\",pid=1))".format(p, who))
            return ok("\n".join(rows))

        # 网络
        if body in ("ip addr", "ip a", "ifconfig"):
            return ok("1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536\n    inet 127.0.0.1/8 scope host lo\n"
                      "2: ens33: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n"
                      "    link/ether 00:0c:29:aa:bb:cc brd ff:ff:ff:ff:ff:ff\n"
                      "    inet 192.168.1.10/24 brd 192.168.1.255 scope global ens33")
        if body.startswith("ping"):
            target = "192.168.1.1"
            mm = re.search(r"(\d+\.\d+\.\d+\.\d+)", body)
            if mm:
                target = mm.group(1)
            cnt = "3"
            mc = re.search(r"-c\s*(\d+)", body)
            if mc:
                cnt = mc.group(1)
            if "-c" not in body:
                return ok("PING {} ({}): 56 data bytes\n（没有 -c 参数会一直 ping，按 Ctrl+C 中断）".format(target, target))
            return ok("PING {} ({}) 56(84) bytes of data.\n64 bytes from {}: icmp_seq=1 ttl=64 time=0.412 ms\n"
                      "64 bytes from {}: icmp_seq=2 ttl=64 time=0.398 ms\n64 bytes from {}: icmp_seq=3 ttl=64 time=0.405 ms\n\n"
                      "--- {} ping statistics ---\n{} packets transmitted, {} received, 0% packet loss".format(
                          target, target, target, target, target, target, cnt, cnt))
        if body.startswith("curl"):
            return ok("<!DOCTYPE html>\n<html>\n<head><title>Welcome to nginx!</title></head>\n<body><h1>Welcome to nginx!</h1></body>\n</html>")
        if body.startswith("df"):
            return ok("Filesystem      Size  Used Avail Use% Mounted on\n/dev/sda3        50G   12G   36G  25% /\n/dev/sda1       976M  130M  780M  15% /boot")
        if body.startswith("free"):
            return ok("              total        used        free\nMem:           3.8Gi       1.2Gi       1.6Gi\nSwap:          2.0Gi          0B       2.0Gi")

        # 定时任务
        if body.startswith("crontab -e") or body.startswith("crontab -l") or body.startswith("crontab"):
            return ok("# m h  dom mon dow   command\n0 2 * * * /root/backup.sh" if self.cron else "no crontab for {}".format(self.user))

        # 脚本执行
        if body.startswith("bash ") or body.startswith("sh ") or body.startswith("./"):
            script = body.split()[-1]
            p = self._abs(script)
            content = self.files.get(p)
            if content is None:
                return err("-bash: {}: 没有那个文件或目录".format(script))
            if isinstance(content, str) and content.startswith("#!"):
                outs = []
                for line in content.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    okk, o = self(line)
                    if o:
                        outs.append(o)
                return ok("\n".join(outs) or "脚本执行完毕")
            return ok("脚本执行完毕")

        if body == "date":
            return ok(datetime.now().strftime("%a %b %d %H:%M:%S CST %Y"))
        if body.startswith("ps "):
            return ok("  PID TTY          TIME CMD\n 1201 pts/0    00:00:00 bash\n 1350 pts/0    00:00:00 ps")
        if body.startswith("tail ") or body.startswith("head "):
            return ok("2024/10/31 09:12:33 [error] 1201#0: *1 connect() failed (111: Connection refused)")
        if body == "history":
            return ok("\n".join("{:>4}  {}".format(i + 1, h) for i, h in enumerate([])) or "    1  history")
        if body in ("clear", "exit", "logout"):
            return ok("")

        return err("-bash: {}: 未找到命令".format(body.split()[0]))


# ============================================================
# 模拟器 3：MySQL（真表 + 真 SELECT，不是字符串表）
# ============================================================
class MySQLSim:
    def __init__(self):
        self.db = None
        self.dbs = {"information_schema": {}, "mysql": {}, "test": {}}
        self.tables = {}      # name -> {"cols":[(n,type,key)], "rows":[[...]]}
        self._seed()

    def _seed(self):
        self.tables["students"] = {
            "cols": [("id", "int(11)", "PRI"), ("name", "varchar(20)", ""), ("age", "int(11)", ""), ("gender", "varchar(4)", "")],
            "rows": [[1, "张三", 19, "男"], [2, "李四", 20, "男"], [3, "王五", 18, "女"]],
        }
        self.tables["courses"] = {
            "cols": [("id", "int(11)", "PRI"), ("name", "varchar(30)", ""), ("credit", "int(11)", "")],
            "rows": [[1, "数据库", 3], [2, "网络", 4], [3, "Python", 2]],
        }
        self.tables["scores"] = {
            "cols": [("id", "int(11)", "PRI"), ("student_id", "int(11)", "MUL"),
                     ("course_id", "int(11)", "MUL"), ("score", "decimal(5,2)", "")],
            "rows": [[1, 1, 1, 88.5], [2, 1, 2, 92.0], [3, 2, 1, 76.0], [4, 3, 3, 95.0]],
        }

    def prompt(self):
        return "mysql>" if self.db else "mysql>"

    def _fmt(self, headers, rows):
        if not headers:
            return "Empty set"
        w = [max(len(str(h)), *(len(str(r[i])) if i < len(r) else 0 for r in rows)) if rows else len(str(h))
             for i, h in enumerate(headers)]
        sep = "+" + "+".join("-" * (x + 2) for x in w) + "+"
        out = [sep, "| " + " | ".join(str(h).ljust(w[i]) for i, h in enumerate(headers)) + " |", sep]
        for r in rows:
            out.append("| " + " | ".join(str(r[i] if i < len(r) else "").ljust(w[i]) for i in range(len(headers))) + " |")
        out.append(sep)
        out.append("{} row{} in set".format(len(rows), "" if len(rows) == 1 else "s"))
        return "\n".join(out)

    def _where(self, rows, cols, cond):
        if not cond:
            return rows
        ci = {c[0]: i for i, c in enumerate(cols)}
        cond = cond.strip()
        # AND 拆分
        parts = re.split(r"\s+and\s+", cond, flags=re.I)
        out = rows
        for part in parts:
            out = [r for r in out if self._pred(r, ci, part)]
        return out

    def _pred(self, row, ci, expr):
        m = re.match(r"^(\w+)\s*(>=|<=|<>|!=|=|>|<)\s*(.+)$", expr.strip())
        if not m:
            return True
        col, op, val = m.group(1), m.group(2), m.group(3).strip().strip("'\"")
        if col not in ci:
            return True
        a = row[ci[col]]
        try:
            b = float(val)
            a = float(a)
        except (TypeError, ValueError):
            b = val
            a = str(a)
        if op == "=":
            return a == b
        if op in ("!=", "<>"):
            return a != b
        if op == ">":
            return a > b
        if op == "<":
            return a < b
        if op == ">=":
            return a >= b
        if op == "<=":
            return a <= b
        return True

    def __call__(self, raw):
        stmt = norm(raw).rstrip(";").strip()
        if not stmt:
            return ok("")
        first = stmt.split()[0]

        if stmt in ("exit", "quit", "\\q"):
            return ok("Bye")
        if stmt == "show databases":
            names = list(self.dbs.keys())
            return ok(self._fmt(["Database"], [[n] for n in names]))
        m = re.match(r"^use\s+(\w+)$", stmt)
        if m:
            n = m.group(1)
            if n not in self.dbs:
                return err("ERROR 1049 (42000): Unknown database '{}'".format(n))
            self.db = n
            if n == "test" and not self.tables:
                self._seed()
            return ok("Database changed")
        if stmt == "select database()":
            return ok(self._fmt(["database()"], [[self.db]]) if self.db else "NULL")
        if stmt == "show tables":
            if not self.db:
                return err("ERROR 1046 (3D000): No database selected")
            return ok(self._fmt(["Tables_in_{}".format(self.db)], [[t] for t in sorted(self.tables)]))
        m = re.match(r"^(?:desc|describe)\s+(\w+)$", stmt)
        if m:
            t = m.group(1)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            rows = [[c[0], c[1], "YES" if c[2] != "PRI" else "NO", c[2], None, ""] for c in self.tables[t]["cols"]]
            return ok(self._fmt(["Field", "Type", "Null", "Key", "Default", "Extra"], rows))
        m = re.match(r"^show\s+(?:index|indexes|keys)\s+from\s+(\w+)$", stmt)
        if m:
            t = m.group(1)
            rows = [["{}_idx".format(t), 1, c[0], 1, "BTREE"] for c in self.tables.get(t, {"cols": []})["cols"] if c[2] == "MUL"]
            return ok(self._fmt(["Key_name", "Seq_in_index", "Column_name", "Non_unique", "Index_type"], rows) if rows else "Empty set")

        # 建表
        m = re.match(r"^create\s+table\s+(?:if\s+not\s+exists\s+)?(\w+)\s*\((.*)\)$", stmt, re.S)
        if m:
            t, body = m.group(1), m.group(2)
            if t in self.tables:
                return err("ERROR 1050 (42S01): Table '{}' already exists".format(t))
            cols = []
            for piece in body.split(","):
                p = piece.strip()
                if p.lower().startswith(("primary key", "index", "key", "unique", "constraint", "foreign")):
                    pm = re.search(r"\((\w+)", p)
                    if pm:
                        for c in cols:
                            if c[0] == pm.group(1):
                                cols[cols.index(c)] = (c[0], c[1], "PRI")
                    continue
                bits = p.split()
                if len(bits) >= 2:
                    key = "PRI" if "primary key" in p.lower() else ""
                    cols.append((bits[0], bits[1], key))
            self.tables[t] = {"cols": cols, "rows": []}
            self.dbs[self.db or "test"][t] = True
            return ok("Query OK, 0 rows affected")

        m = re.match(r"^alter\s+table\s+(\w+)\s+add\s+(?:column\s+)?(\w+)\s+(\S+)$", stmt)
        if m:
            t, col, typ = m.group(1), m.group(2), m.group(3)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            self.tables[t]["cols"].append((col, typ, ""))
            for r in self.tables[t]["rows"]:
                r.append(None)
            return ok("Query OK, {} rows affected".format(len(self.tables[t]["rows"])))

        m = re.match(r"^create\s+(?:unique\s+)?index\s+(\w+)\s+on\s+(\w+)\s*\((\w+)\)$", stmt)
        if m:
            idx, t, col = m.group(1), m.group(2), m.group(3)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            cols = self.tables[t]["cols"]
            for i, c in enumerate(cols):
                if c[0] == col:
                    cols[i] = (c[0], c[1], "MUL")
            return ok("Query OK, 0 rows affected")

        # 插入
        m = re.match(r"^insert\s+into\s+(\w+)\s*(?:\(([^)]*)\))?\s*values\s*(\(.+\))\s*$", stmt, re.S)
        if m:
            t = m.group(1)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            tbl = self.tables[t]
            names = [x.strip() for x in m.group(2).split(",")] if m.group(2) else [c[0] for c in tbl["cols"]]
            added = 0
            for tup in re.findall(r"\(([^)]*)\)", m.group(3)):
                vals = []
                for v in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", tup):
                    v = v.strip()
                    if re.match(r"^-?\d+(\.\d+)?$", v):
                        vals.append(float(v) if "." in v else int(v))
                    else:
                        vals.append(v.strip("'\""))
                row = [None] * len(tbl["cols"])
                for nm, v in zip(names, vals):
                    for i, c in enumerate(tbl["cols"]):
                        if c[0] == nm:
                            row[i] = v
                # 主键去重
                pk_idx = [i for i, c in enumerate(tbl["cols"]) if c[2] == "PRI"]
                if pk_idx and row[pk_idx[0]] is not None:
                    if any(r[pk_idx[0]] == row[pk_idx[0]] for r in tbl["rows"]):
                        return err("ERROR 1062 (23000): Duplicate entry '{}' for key 'PRIMARY'".format(row[pk_idx[0]]))
                tbl["rows"].append(row)
                added += 1
            return ok("Query OK, {} row{} affected".format(added, "" if added == 1 else "s"))

        # 更新 / 删除
        m = re.match(r"^update\s+(\w+)\s+set\s+(.+?)(?:\s+where\s+(.+))?$", stmt, re.S)
        if m:
            t, sets, cond = m.group(1), m.group(2), m.group(3)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            tbl = self.tables[t]
            ci = {c[0]: i for i, c in enumerate(tbl["cols"])}
            targets = self._where(tbl["rows"], tbl["cols"], cond)
            for r in targets:
                for pair in sets.split(","):
                    if "=" not in pair:
                        continue
                    k, v = pair.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    v = float(v) if re.match(r"^-?\d+\.\d+$", v) else (int(v) if re.match(r"^-?\d+$", v) else v.strip("'\""))
                    if k in ci:
                        r[ci[k]] = v
            return ok("Query OK, {} row{} affected\nRows matched: {}  Changed: {}  Warnings: 0".format(
                len(targets), "" if len(targets) == 1 else "s", len(targets), len(targets)))
        m = re.match(r"^delete\s+from\s+(\w+)(?:\s+where\s+(.+))?$", stmt, re.S)
        if m:
            t = m.group(1)
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
            tbl = self.tables[t]
            before = len(tbl["rows"])
            tbl["rows"] = [r for r in tbl["rows"] if r not in self._where(tbl["rows"], tbl["cols"], m.group(2))]
            n = before - len(tbl["rows"])
            return ok("Query OK, {} row{} affected".format(n, "" if n == 1 else "s"))

        if stmt.startswith("begin") or stmt.startswith("start transaction"):
            return ok("Query OK, 0 rows affected")
        if stmt in ("commit", "rollback"):
            return ok("Query OK, 0 rows affected")

        # 查询
        if first in ("select", "show"):
            return self._select(stmt)

        if stmt.startswith("explain"):
            inner = re.sub(r"^explain\s+", "", stmt)
            m2 = re.match(r"^select\s+.+?\s+from\s+(\w+)", inner, re.I)
            t = m2.group(1) if m2 else "?"
            tbl = self.tables.get(t, {"rows": [], "cols": []})
            used = any(c[2] in ("PRI", "MUL") for c in tbl["cols"])
            return ok(self._fmt(
                ["id", "select_type", "table", "type", "possible_keys", "key", "rows", "Extra"],
                [[1, "SIMPLE", t, "ref" if used else "ALL", "PRIMARY" if used else None,
                  "PRIMARY" if used else None, len(tbl["rows"]) or 1,
                  "" if used else "Using where; Using filesort"]]))

        return err("ERROR 1064 (42000): You have an error in your SQL syntax near '{}'".format(stmt[:40]))

    def _select(self, stmt):
        m = re.match(
            r"^select\s+(.+?)\s+from\s+([\w\s,\s\w]+?)(?:\s+(\w+))?(?:\s+on\s+(.+?))?(?:\s+where\s+(.+?))?"
            r"(?:\s+group\s+by\s+(.+?))?(?:\s+order\s+by\s+(.+?))?(?:\s+limit\s+(\d+))?$",
            stmt, re.I)
        if not m:
            # 无 from 的简单表达式
            m0 = re.match(r"^select\s+(.+)$", stmt, re.I)
            if m0:
                expr = m0.group(1)
                mm = re.match(r"^(count|sum|avg|max|min)\((.+)\)$", expr.strip(), re.I)
                if mm:
                    return ok(self._fmt([expr], [[0]]))
                return ok(self._fmt([expr], [[expr.strip().strip("'\"")]]))
            return err("ERROR 1064 (42000): SQL 语法错误")

        cols_expr, tables_part = m.group(1), m.group(2)
        alias = m.group(3)
        on_cond = m.group(4)
        where = m.group(5)
        group_by = m.group(6)
        order_by = m.group(7)
        limit = m.group(8)

        tnames = [x.strip().split()[0] for x in re.split(r",", tables_part) if x.strip()]
        for t in tnames:
            if t not in self.tables:
                return err("ERROR 1146 (42S02): Table '{}.{}' doesn't exist".format(self.db or "test", t))
        if not self.db:
            return err("ERROR 1046 (3D000): No database selected")

        # 构造联合行
        base = tnames[0]
        rows = [dict(zip([c[0] for c in self.tables[base]["cols"]], r)) for r in self.tables[base]["rows"]]
        if len(tnames) > 1:
            joined = []
            for r in rows:
                for other in tnames[1:]:
                    for r2 in self.tables[other]["rows"]:
                        d2 = dict(zip([c[0] for c in self.tables[other]["cols"]], r2))
                        combo = dict(r)
                        combo.update(d2)
                        okj = True
                        if on_cond:
                            oc = on_cond.strip()
                            mm = re.match(r"^(\w+)\.?(\w+)\s*=\s*(\w+)\.?(\w+)$", oc)
                            if mm:
                                a = combo.get(mm.group(2), combo.get(mm.group(4)))
                                b = combo.get(mm.group(4), combo.get(mm.group(2)))
                                okj = a == b
                        if okj:
                            joined.append(combo)
            rows = joined

        if where:
            w = where.replace("{}.{}".format(tnames[0], ""), "")
            rows = [r for r in rows if all(self._pred_obj(r, p) for p in re.split(r"\s+and\s+", w, flags=re.I))]

        sel = cols_expr.strip()
        agg = re.match(r"^(count|sum|avg|max|min)\s*\(\s*(\*|\w+)\s*\)$", sel, re.I)
        if group_by:
            gcol = group_by.strip().split()[-1].split(".")[-1]
            groups = {}
            for r in rows:
                groups.setdefault(r.get(gcol), []).append(r)
            headers = [gcol, sel]
            out = []
            for k, grp in groups.items():
                out.append([k, self._agg(agg, grp) if agg else len(grp)])
            return ok(self._fmt(headers, out))
        if agg:
            v = self._agg(agg, rows)
            return ok(self._fmt([sel], [[v]]))

        hdrs = [c.strip().split(".")[-1] for c in sel.split(",")]
        if sel == "*":
            hdrs = list(rows[0].keys()) if rows else [c[0] for c in self.tables[base]["cols"]]
        body = []
        for r in rows:
            body.append([r.get(h) for h in hdrs])
        if order_by:
            ob = order_by.strip().split()[0].split(".")[-1]
            desc = "desc" in order_by.lower()
            idx = hdrs.index(ob) if ob in hdrs else None
            if idx is not None:
                body.sort(key=lambda x: (x[idx] is None, x[idx]), reverse=desc)
        if limit:
            body = body[: int(limit)]
        return ok(self._fmt(hdrs, body))

    def _pred_obj(self, r, expr):
        m = re.match(r"^\s*(?:\w+\.)?(\w+)\s*(>=|<=|<>|!=|=|>|<)\s*(.+?)\s*$", expr)
        if not m:
            return True
        col, op, val = m.group(1), m.group(2), m.group(3).strip().strip("'\"")
        if col not in r:
            return True
        a = r[col]
        try:
            b = float(val)
            a = float(a) if a is not None else 0
        except (TypeError, ValueError):
            b = val
            a = str(a)
        try:
            return {"=": a == b, "!=": a != b, "<>": a != b, ">": a > b, "<": a < b,
                    ">=": a >= b, "<=": a <= b}[op]
        except TypeError:
            return False

    @staticmethod
    def _agg(agg, rows):
        if not agg:
            return len(rows)
        fn, col = agg.group(1).lower(), agg.group(2)
        if col == "*":
            return len(rows)
        vals = [r.get(col) for r in rows if r.get(col) is not None]
        nums = []
        for v in vals:
            try:
                nums.append(float(v))
            except (TypeError, ValueError):
                pass
        if fn == "count":
            return len(vals)
        if not nums:
            return None
        if fn == "sum":
            return round(sum(nums), 2)
        if fn == "avg":
            return round(sum(nums) / len(nums), 4)
        if fn == "max":
            return max(nums)
        return min(nums)


# ============================================================
# 模拟器 4：Python（用 ast 安全求值，不是命令对照表）
# ============================================================
_PY_OUT = []          # print 的收集栈（支持嵌套调用）


def _pyprint(*args, sep=" ", end="\n"):
    """沙箱里的 print：把输出写进当前收集器，不真的打到 stdout"""
    _PY_OUT.append(sep.join(str(a) for a in args))
    return None


class PySim:
    MAX_OUT = 4000

    def __init__(self):
        self.ns = {"__builtins__": {}, "print": _pyprint, "None": None, "True": True, "False": False}
        self.stdlib = {
            "print": _pyprint, "len": len, "range": range, "int": int, "float": float, "str": str,
            "list": list, "dict": dict, "set": set, "tuple": tuple, "sum": sum, "min": min,
            "max": max, "abs": abs, "round": round, "sorted": sorted, "enumerate": enumerate,
            "zip": zip, "map": map, "filter": filter, "bool": bool, "type": type, "isinstance": isinstance,
            "any": any, "all": all, "reversed": reversed,
        }
        self.out = []

    def prompt(self):
        return ">>>"

    # ---------- 安全求值 ----------
    def ev(self, node):
        if isinstance(node, ast.Expression):
            return self.ev(node.body)
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in self.ns:
                return self.ns[node.id]
            if node.id in self.stdlib:
                return self.stdlib[node.id]
            raise NameError("name '{}' is not defined".format(node.id))
        if isinstance(node, ast.List):
            return [self.ev(e) for e in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self.ev(e) for e in node.elts)
        if isinstance(node, ast.Set):
            return set(self.ev(e) for e in node.elts)
        if isinstance(node, ast.Dict):
            return {self.ev(k): self.ev(v) for k, v in zip(node.keys, node.values)}
        if isinstance(node, ast.BinOp):
            a, b = self.ev(node.left), self.ev(node.right)
            op = node.op
            table = {
                ast.Add: lambda x, y: x + y, ast.Sub: lambda x, y: x - y,
                ast.Mult: lambda x, y: x * y, ast.Div: lambda x, y: x / y,
                ast.FloorDiv: lambda x, y: x // y, ast.Mod: lambda x, y: x % y,
                ast.Pow: lambda x, y: x ** y,
            }
            for t, fn in table.items():
                if isinstance(op, t):
                    return fn(a, b)
            raise ValueError("不支持的运算符")
        if isinstance(node, ast.UnaryOp):
            v = self.ev(node.operand)
            if isinstance(node.op, ast.USub):
                return -v
            if isinstance(node.op, ast.Not):
                return not v
            return v
        if isinstance(node, ast.Compare):
            left = self.ev(node.left)
            for op, comp in zip(node.ops, node.comparators):
                right = self.ev(comp)
                try:
                    if isinstance(op, ast.Is):
                        res = left is right
                    elif isinstance(op, ast.IsNot):
                        res = left is not right
                    elif isinstance(op, ast.Eq):
                        res = left == right
                    elif isinstance(op, ast.NotEq):
                        res = left != right
                    elif isinstance(op, ast.In):
                        res = left in right
                    elif isinstance(op, ast.NotIn):
                        res = left not in right
                    elif isinstance(op, ast.Lt):
                        res = left < right
                    elif isinstance(op, ast.LtE):
                        res = left <= right
                    elif isinstance(op, ast.Gt):
                        res = left > right
                    elif isinstance(op, ast.GtE):
                        res = left >= right
                    else:
                        raise ValueError("不支持的比较")
                except TypeError:
                    return False
                if not res:
                    return False
                left = right
            return True
        if isinstance(node, ast.BoolOp):
            vals = [self.ev(v) for v in node.values]
            return all(vals) if isinstance(node.op, ast.And) else any(vals)
        if isinstance(node, ast.IfExp):
            return self.ev(node.body) if self.ev(node.test) else self.ev(node.orelse)
        if isinstance(node, ast.Subscript):
            v = self.ev(node.value)
            sl = node.slice
            if isinstance(sl, ast.Slice):
                lo = self.ev(sl.lower) if sl.lower else None
                hi = self.ev(sl.upper) if sl.upper else None
                st_ = self.ev(sl.step) if sl.step else None
                return v[lo:hi:st_]
            return v[self.ev(sl)]
        if isinstance(node, ast.Attribute):
            v = self.ev(node.value)
            return getattr(v, node.attr)
        if isinstance(node, ast.Call):
            fn = self.ev(node.func)
            if fn is print:
                args = [self.ev(a) for a in node.args]
                self.out.append(" ".join(self._s(a) for a in args))
                return None
            if fn is None:
                raise NameError("'{}' is not callable".format(getattr(node.func, "id", "?")))
            args = []
            for a in node.args:
                if isinstance(a, ast.Starred):
                    args.extend(self.ev(a.value))
                else:
                    args.append(self.ev(a))
            kwargs = {kw.arg: self.ev(kw.value) for kw in node.keywords if kw.arg}
            return fn(*args, **kwargs)
        if isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.FormattedValue):
                    parts.append(self._s(self.ev(v.value)))
                else:
                    parts.append(str(self.ev(v)))
            return "".join(parts)
        if isinstance(node, ast.ListComp):
            return self._comp(node, list)
        if isinstance(node, ast.GeneratorExp):
            return self._comp(node, list)
        if isinstance(node, ast.DictComp):
            res = {}
            self._comp_into(node, lambda env: res.__setitem__(self.ev_under(node.key, env), self.ev_under(node.value, env)), {})
            return res
        return None

    def _comp(self, node, container):
        res = []
        self._comp_into(node, lambda env: res.append(self.ev_under(node.elt, env)), {})
        return container(res)

    def _comp_into(self, node, emit, env):
        if not node.generators:
            emit(env)
            return
        def rec(i, env2):
            if i >= len(node.generators):
                emit(env2)
                return
            g = node.generators[i]
            iterable = self.ev_under(g.iter, env2)
            for v in iterable:
                e2 = dict(env2)
                self._bind(g.target, v, e2)
                cond_ok = True
                for c in g.ifs:
                    if not self.ev_under(c, e2):
                        cond_ok = False
                        break
                if cond_ok:
                    rec(i + 1, e2)
        rec(0, env)

    def ev_under(self, node, env):
        saved = dict(self.ns)
        self.ns.update(env)
        try:
            return self.ev(node)
        finally:
            self.ns = saved

    def _bind(self, target, value, env):
        if isinstance(target, ast.Name):
            env[target.id] = value
        elif isinstance(target, (ast.Tuple, ast.List)):
            for t, v in zip(target.elts, value):
                self._bind(t, v, env)

    @staticmethod
    def _s(v):
        if isinstance(v, str):
            return v
        if isinstance(v, float):
            return repr(v)
        return str(v)

    # ---------- 语句 ----------
    def _stmt(self, node):
        if isinstance(node, ast.Expr):
            v = self.ev(node.value)
            if v is not None and not isinstance(node.value, ast.Call):
                self.out.append(self._s(v))
            return
        if isinstance(node, ast.Assign):
            v = self.ev(node.value)
            for t in node.targets:
                self._bind2(t, v)
            return
        if isinstance(node, ast.AugAssign):
            cur = self.ev(ast.Expression(node.target))
            rhs = self.ev(node.value)
            op = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*"}.get(type(node.op))
            newv = {"+": lambda a, b: a + b, "-": lambda a, b: a - b, "*": lambda a, b: a * b}[op or "+"](cur, rhs)
            self._bind2(node.target, newv)
            return
        if isinstance(node, ast.AnnAssign):
            if node.value is not None:
                self._bind2(node.target, self.ev(node.value))
            return
        if isinstance(node, ast.For):
            iterable = self.ev(node.iter)
            for v in iterable:
                self._bind2(node.target, v)
                for s in node.body:
                    self._stmt(s)
            return
        if isinstance(node, ast.While):
            guard = 0
            while self.ev(node.test) and guard < 10000:
                guard += 1
                for s in node.body:
                    self._stmt(s)
            return
        if isinstance(node, ast.If):
            body = node.body if self.ev(node.test) else node.orelse
            for s in body:
                self._stmt(s)
            return
        if isinstance(node, ast.FunctionDef):
            self.ns[node.name] = self._make_func(node)
            return
        if isinstance(node, ast.ClassDef):
            self.ns[node.name] = self._make_class(node)
            return
        if isinstance(node, ast.Return):
            raise _Return(self.ev(node.value) if node.value else None)
        if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
            mod = getattr(node, "module", None) or (node.names[0].name if node.names else "")
            self.out.append("（本实训为离线沙箱，{} 模块不可用，用内置数据即可）".format(mod))
            return
        if isinstance(node, ast.Pass):
            return
        if isinstance(node, ast.Break):
            raise _Break()
        return

    def _bind2(self, target, value):
        if isinstance(target, ast.Name):
            self.ns[target.id] = value
        elif isinstance(target, (ast.Tuple, ast.List)):
            for t, v in zip(target.elts, value):
                self._bind2(t, v)
        elif isinstance(target, ast.Subscript):
            obj = self.ev(target.value)
            obj[self.ev(target.slice)] = value
        else:
            pass

    def _make_func(self, node):
        args = [a.arg for a in node.args.args]

        def fn(*a, **kw):
            local = dict(zip(args, a))
            local.update(kw)
            saved = dict(self.ns)
            self.ns.update(local)
            try:
                for s in node.body:
                    self._stmt(s)
            except _Return as r:
                return r.value
            finally:
                self.ns = saved
            return None

        fn.__name__ = node.name
        return fn

    def _make_class(self, node):
        cls = {"name": node.name, "fields": [], "methods": {}, "bases": [b.id for b in node.bases if isinstance(b, ast.Name)]}
        init_body = None
        for s in node.body:
            if isinstance(s, ast.FunctionDef):
                cls["methods"][s.name] = s
                if s.name == "__init__":
                    init_body = s
            elif isinstance(s, ast.Assign):
                for t in s.targets:
                    if isinstance(t, ast.Name):
                        cls["fields"].append(t.id)
        name = node.name

        def factory(*args, **kwargs):
            if init_body is not None:
                params = [a.arg for a in init_body.args.args][1:]
                local = dict(zip(params, args))
                local.update(kwargs)
                self.ns.update(local)
                for st in init_body.body:
                    try:
                        self._stmt(st)
                    except _Return:
                        pass
            return _Inst(name, cls, self)

        factory.__name__ = name
        return factory

    def run(self, src):
        """按缩进分块执行；支持多行 class / def / for"""
        text = str(src or "").replace("\r\n", "\n").rstrip()
        if not text.strip():
            return "", False
        self.out = []
        _PY_OUT.clear()
        _PY_OUT.append(self.out)   # 让 _pyprint 落到 self.out
        lines = text.split("\n")
        try:
            try:
                tree = ast.parse(text)
                nodes = tree.body
            except SyntaxError:
                nodes = []
                buf = []
                for ln in lines:
                    if ln.strip() == "":
                        continue
                    if ln.startswith((" ", "\t")) and buf:
                        buf.append(ln)
                    else:
                        if buf:
                            nodes.extend(ast.parse("\n".join(buf)).body)
                        buf = [ln]
                if buf:
                    nodes.extend(ast.parse("\n".join(buf)).body)
            for n in nodes:
                self._stmt(n)
            out = "\n".join(self.out)
            return (out[: self.MAX_OUT] if out else "（无输出）"), True
        except _Return as r:
            return "\n".join(self.out) or str(r.value), True
        except NameError as exc:
            return "Traceback (most recent call last):\n  NameError: {}\n（提示：先定义再使用，注意大小写）".format(exc), False
        except TypeError as exc:
            return "Traceback (most recent call last):\n  TypeError: {}".format(exc), False
        except (SyntaxError, IndentationError) as exc:
            return "  File \"<stdin>\", line {}\nSyntaxError: {}".format(getattr(exc, "lineno", "?"), getattr(exc, "msg", exc)), False
        except ZeroDivisionError:
            return "Traceback (most recent call last):\n  ZeroDivisionError: division by zero", False
        except Exception as exc:
            return "Traceback (most recent call last):\n  {}: {}".format(type(exc).__name__, exc), False

    def __call__(self, raw):
        out, good = self.run(raw)
        return good, out


class _Return(Exception):
    def __init__(self, value=None):
        self.value = value


class _Break(Exception):
    pass


class _Inst(dict):
    """实例字典 + 方法绑定"""

    def __init__(self, name, cls, sim):
        super().__init__()
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_cls", cls)
        object.__setattr__(self, "_sim", sim)

    def __getattr__(self, item):
        cls = object.__getattribute__(self, "_cls")
        if item in cls["methods"]:
            node = cls["methods"][item]
            sim = object.__getattribute__(self, "_sim")
            params = [a.arg for a in node.args.args][1:]

            def method(*a, **kw):
                local = dict(self)
                local.update(dict(zip(params, a)))
                local.update(kw)
                local["self"] = self
                saved = dict(sim.ns)
                sim.ns.update(local)
                try:
                    for s in node.body:
                        sim._stmt(s)
                except _Return as r:
                    return r.value
                finally:
                    sim.ns = saved
                return None
            return method
        if item in self:
            return self[item]
        raise AttributeError("'{}' object has no attribute '{}'".format(object.__getattribute__(self, "_name"), item))

    def __setattr__(self, k, v):
        self[k] = v

    def __repr__(self):
        cls = object.__getattribute__(self, "_cls")
        inner = ", ".join("{}={!r}".format(k, v) for k, v in self.items() if k != "self")
        return "<{} {} {}>".format(object.__getattribute__(self, "_name"), "object", "(" + inner + ")")

    __str__ = __repr__


# ============================================================
# 模拟器 5：Git + 测试 + 敏捷
# ============================================================
class GitSim:
    def __init__(self):
        self.cwd = "~/project"
        self.files = {"app.py": "print('hello')", "README.md": "# demo"}
        self.staged = set()
        self.commits = []
        self.branch = "main"
        self.branches = {"main": []}
        self.remote = []
        self.tests = {"test_app.py": "def test_ok():\n    assert 1 + 1 == 2"}

    def prompt(self):
        return "user@dev:{} (main)$".format(self.cwd)

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        parts = cmd.split()
        if parts[0] == "git":
            sub = parts[1] if len(parts) > 1 else ""
            if sub == "init":
                self.commits = []
                self.branches = {"main": []}
                return ok("Initialized empty Git repository in {}/.git/".format(self.cwd))
            if sub == "status":
                lines = ["On branch {}".format(self.branch)]
                if self.staged:
                    lines.append("\nChanges to be committed:")
                    for f in sorted(self.staged):
                        lines.append("        new file:   {}".format(f))
                un = [f for f in self.files if f not in self.staged]
                if un:
                    lines.append("\nUntracked files:")
                    for f in sorted(un):
                        lines.append("        {}".format(f))
                if not self.staged and not un:
                    lines.append("nothing to commit, working tree clean")
                return ok("\n".join(lines))
            if sub == "add":
                target = parts[-1] if len(parts) > 2 else "."
                if target in (".", "-A", "--all"):
                    self.staged.update(self.files.keys())
                    return ok("")
                if target not in self.files:
                    return err("fatal: pathspec '{}' did not match any files".format(target))
                self.staged.add(target)
                return ok("")
            if sub == "commit":
                msg = ""
                if "-m" in parts:
                    msg = raw.split("-m", 1)[1].strip().strip("'\"")
                if not self.staged:
                    return err("On branch {}\nnothing added to commit but untracked files present".format(self.branch))
                self.commits.append({"msg": msg or "(no message)", "files": sorted(self.staged)})
                self.branches[self.branch] = list(self.commits)
                n = len(self.staged)
                self.staged = set()
                return ok("[{} {:07x}] {}\n {} file{} changed".format(
                    self.branch, abs(hash(msg)) % 0xFFFFFFF, msg or "(no message)", n, "" if n == 1 else "s"))
            if sub == "log":
                if not self.commits:
                    return err("fatal: your current branch '{}' does not have any commits yet".format(self.branch))
                lines = []
                for i, c in enumerate(reversed(self.commits)):
                    lines.append("commit {:07x} (HEAD -> {})".format(abs(hash(c["msg"])) % 0xFFFFFFF, self.branch))
                    lines.append("    {}\n".format(c["msg"]))
                return ok("\n".join(lines))
            if sub == "diff":
                if self.staged:
                    return ok("diff --git a/app.py b/app.py\n--- a/app.py\n+++ b/app.py\n@@ -1 +1 @@\n-print('hello')\n+print('hello world')")
                return ok("")
            if sub == "branch":
                if len(parts) == 2:
                    return ok("* {}".format(self.branch) + "".join(
                        "\n  {}".format(b) for b in self.branches if b != self.branch))
                nb = parts[-1]
                self.branches.setdefault(nb, list(self.commits))
                return ok("")
            if sub == "checkout":
                if "-b" in parts:
                    nb = parts[-1]
                    self.branches.setdefault(nb, list(self.commits))
                    self.branch = nb
                    return ok("Switched to a new branch '{}'".format(nb))
                nb = parts[-1]
                if nb not in self.branches:
                    return err("error: pathspec '{}' did not match any file(s) known to git".format(nb))
                self.branch = nb
                return ok("Switched to branch '{}'".format(nb))
            if sub == "merge":
                nb = parts[-1]
                if nb not in self.branches:
                    return err("merge: {} - not something we can merge".format(nb))
                if nb == self.branch:
                    return ok("Already up to date.")
                self.commits.extend(self.branches.get(nb, []))
                return ok("Merge made by the 'ort' strategy.\n 1 file changed, 3 insertions(+)")
            if sub == "push":
                if not self.commits:
                    return err("error: src refspec main does not match any")
                self.remote = list(self.commits)
                return ok("Enumerating objects: {}, done.\nTo github.com:me/project.git\n   {:07x}..{:07x}  {} -> {}".format(
                    len(self.commits), 0, abs(hash(str(len(self.commits)))) % 0xFFFFFFF, self.branch, self.branch))
            if sub == "pull":
                return ok("Already up to date.")
            if sub == "remote":
                if len(parts) > 2 and parts[2] == "add":
                    return ok("")
                return ok("origin\tgit@github.com:me/project.git (fetch)\norigin\tgit@github.com:me/project.git (push)")
            if sub == "clone":
                return ok("Cloning into '{}'...\ndone.".format(parts[-1] if len(parts) > 2 else "project"))
            if sub in ("stash", "reset"):
                self.staged = set()
                return ok("")
            return err("git: '{}' is not a git command.".format(sub))

        if parts[0] in ("pytest", "python", "python3"):
            if "pytest" in parts[0] or "-m" in parts:
                return ok("============================= test session starts ==============================\n"
                          "collected {} item\n\n{}test_app.py::test_ok PASSED\n\n"
                          "============================== 1 passed in 0.02s ===============================".format(
                              len(self.tests), ""))
            return ok("Hello from script")
        if parts[0] == "touch":
            self.files[parts[-1]] = ""
            return ok("")
        if parts[0] == "ls":
            return ok("  ".join(sorted(self.files)))
        if parts[0] == "cat":
            f = parts[-1]
            return ok(self.files.get(f, "")) if f in self.files else err("cat: {}: No such file".format(f))
        if parts[0] == "echo" and ">" in cmd:
            left, right = cmd.split(">", 1)
            self.files[right.strip()] = left[5:].strip().strip("'\"")
            return ok("")
        if parts[0] == "mkdir":
            return ok("")
        if parts[0] == "npm":
            if "test" in parts:
                return ok("> demo@1.0.0 test\n> jest\n\nPASS  ./sum.test.js\n  ✓ adds 1 + 2 to equal 3 (2 ms)\n\nTests: 1 passed, 1 total")
            if "install" in parts:
                return ok("added 120 packages in 3s")
        if parts[0] in ("pip", "pip3"):
            return ok("Successfully installed requests-2.31.0")
        if parts[0] == "cd":
            self.cwd = parts[-1]
            return ok("")
        return err("bash: {}: command not found".format(parts[0]))


# ============================================================
# 模拟器 6：电气自动化（安全联锁状态机）
# ============================================================
class ElectricSim:
    def __init__(self):
        self.qf = False          # 主断路器
        self.km1 = False         # 正转接触器
        self.km2 = False         # 反转接触器
        self.sb1 = False         # 停止
        self.fr = True           # 热继电器（True=正常）
        self.meas_mode = None
        self.plc_run = False
        self.plc_regs = {"D0": 0, "D1": 0, "D10": 0, "D100": 0}
        self.plc_mods = {"Q0.0": 0, "Q0.1": 0, "I0.0": 0, "I0.1": 0}
        self.modbus = {"baud": 9600, "parity": "None", "slave": 1, "term": False}
        self.pcb_stage = 0
        self.temp = 25.0
        self.vout = 0.0
        self.load = 0.0

    def prompt(self):
        return "PLC>"

    def _voltage(self):
        if not self.qf:
            return 0.0
        return 380.0 if (self.km1 or self.km2) else 380.0

    def _current(self):
        if not self.qf or not (self.km1 or self.km2):
            return 0.0
        return 7.6

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        c = cmd

        # ---------- 主电路操作 ----------
        if re.search(r"闭合\s*qf|合上\s*qf|上电|合闸|接通主电路|qf\s*闭合", c):
            self.qf = True
            return ok("QF 已合闸，三相电源接通，L1/L2/L3 带电（线电压 380V）。")
        if re.search(r"断开\s*qf|qf\s*断开|断电|拉闸|断开开关|断开主电路", c):
            self.qf = False
            self.km1 = self.km2 = False
            return ok("QF 已分闸，主电路断电。接触器随之释放。")
        if re.search(r"按\s*sb2|sb2|正转启动|正转", c):
            if not self.qf:
                return err("主电路未上电：请先闭合 QF，否则按 SB2 无任何反应（自锁回路无电）。")
            if not self.fr:
                return err("热继电器 FR 处于过载脱扣状态，控制回路被断开，无法启动。请先复位 FR。")
            if self.km2:
                return err("互锁保护：KM2（反转）正在吸合，KM1 无法得电。请先按 SB1 停止。")
            self.km1 = True
            return ok("KM1 线圈得电 → 主触点闭合，电机正转启动并自锁（松开 SB2 仍保持运行）。\n"
                      "U-V-W 相序：正序，转速约 1450 r/min。")
        if re.search(r"按\s*sb3|sb3|反转启动|反转", c):
            if not self.qf:
                return err("主电路未上电：请先闭合 QF。")
            if not self.fr:
                return err("热继电器 FR 过载脱扣，控制回路断开，无法启动。")
            if self.km1:
                return err("互锁保护：KM1（正转）正在吸合，KM2 无法得电。请先按 SB1 停止。")
            self.km2 = True
            return ok("KM2 线圈得电 → 主触点闭合，任意两相调换，电机反转启动并自锁。")
        if re.search(r"按\s*sb1|sb1|停止", c):
            self.km1 = self.km2 = False
            return ok("SB1 断开控制回路，KM1/KM2 线圈失电，电机停止。")
        if re.search(r"过载|fr\s*脱扣|模拟过载", c):
            self.fr = False
            self.km1 = self.km2 = False
            return ok("模拟过载：FR 双金属片受热弯曲，常闭触点断开 → 控制回路断电 → 电机停机，实现过载保护。")
        if re.search(r"复位\s*fr|fr\s*复位|复位热继", c):
            self.fr = True
            return ok("FR 已复位（等待 3 分钟冷却后方可复位，符合实际热继电器特性）。")

        # ---------- 万用表 ----------
        m = re.search(r"(?:切换到?|打到|选择|档位)\s*(电压|直流电压|交流电压|电阻|电流|蜂鸣|通断|v|ω|a)", c)
        if m:
            t = m.group(1)
            self.meas_mode = ("V" if "电压" in t or t == "v" else
                              "R" if "电阻" in t or t == "ω" else
                              "A" if "电流" in t or t == "a" else "BEEP")
            if self.meas_mode == "A":
                return ok("已切到电流档（A）。注意：测电流必须串联接入电路，不能并联！")
            return ok({
                "V": "已切到交流电压档（V~），量程 750V。红表笔插 VΩ 孔，黑表笔插 COM。",
                "R": "已切到电阻档（Ω）。注意：测电阻必须断电测量。",
                "BEEP": "已切到通断蜂鸣档。两表笔短接应发出蜂鸣声，用于判断线路通断。",
            }[self.meas_mode])
        if re.search(r"测量\s*u|测量u|测电压|量电压|测\s*电压", c):
            if self.meas_mode is None:
                self.meas_mode = "V"
            if self.meas_mode != "V":
                return err("当前不是电压档，请先切到电压档（V）。")
            v = self._voltage()
            ph = re.search(r"u\s*ab|uab", c)
            return ok("读数：{:.1f} V（交流）\n{}".format(
                v, "三相平衡，线电压 380V 属正常范围（允许偏差 ±7%）。" if v > 1 else
                "读数为 0V —— 主电路未上电，请检查 QF 是否合闸。"))
        if re.search(r"测量\s*i|测量i|测电流|量电流|测\s*电流", c):
            if self.meas_mode is None:
                self.meas_mode = "A"
            if self.meas_mode != "A":
                return err("当前不是电流档，请先切到电流档（A）。")
            a = self._current()
            return ok("读数：{:.2f} A\n{}".format(
                a, "空载/停机电流，电机未运行。" if a < 0.1 else "额定电流约 7.6A，运行电流正常。"))
        if re.search(r"测量\s*r|测电阻|量电阻|测\s*电阻", c):
            if self.meas_mode is None:
                self.meas_mode = "R"
            if self.qf:
                return err("危险：电路带电时不能用电阻档测量！请先断开 QF。")
            return ok("读数：OL（超量程）—— 回路开路；测电机绕组应为几十欧姆。")
        if re.search(r"通断|蜂鸣测试|测通断", c):
            self.meas_mode = self.meas_mode or "BEEP"
            return ok("蜂鸣器响：线路导通，电阻 < 30Ω。")
        if re.search(r"读表|读数|测量结果|查看读数", c):
            return ok("当前档位：{}　读数：{}\n读数前请确认：① 档位选对 ② 表笔插孔正确 ③ 量程大于被测值".format(
                self.meas_mode or "未选择",
                "{} V".format(self._voltage()) if self.meas_mode == "V" else
                "{} A".format(self._current()) if self.meas_mode == "A" else "—"))

        # ---------- PLC ----------
        if re.search(r"编程|写梯形图|输入程序|下载程序|程序下载", c):
            return ok("梯形图已写入 PLC 用户程序区：\n"
                      "  Network 1   LD  I0.0\n              O   Q0.0\n              A   I0.1\n              =   Q0.0\n"
                      "  Network 2   LD  I0.2\n              =   Q0.1")
        if re.search(r"运行\s*plc|plc\s*运行|切换到\s*run|打到\s*run|run\s*模式|启动plc", c):
            self.plc_run = True
            return ok("PLC 已切换到 RUN 模式，开始周期性扫描。")
        if re.search(r"停止\s*plc|plc\s*停止|stop\s*模式|切到\s*stop", c):
            self.plc_run = False
            return ok("PLC 已切换到 STOP 模式，输出全部复位。")
        m = re.search(r"(?:i0\.0|i0\.1|i0\.2)\s*(?:置\s*1|=1|接通|为\s*1|on)", c)
        if m:
            if not self.plc_run:
                return err("PLC 处于 STOP 模式，输入采样不生效，请先切到 RUN。")
            self.plc_mods["Q0.0"] = 1
            self.plc_regs["D0"] = 1
            return ok("输入点置 1 → 一个扫描周期后 Q0.0 = 1，输出指示灯亮。")
        m = re.search(r"(?:i0\.0|i0\.1|i0\.2)\s*(?:置\s*0|=0|断开|为\s*0|off)", c)
        if m:
            self.plc_mods["Q0.0"] = 0
            return ok("输入点置 0 → Q0.0 = 0，输出断开。")
        if re.search(r"读\s*d0|查看\s*d0|d0\s*的值|读取寄存器", c):
            return ok("D0 = {}（16 位有符号整数，范围 -32768~32767）".format(self.plc_regs.get("D0", 0)))
        m = re.search(r"(?:写入|赋值|置)\s*d(\d+)\s*(?:=|为|等于)\s*(-?\d+)", c)
        if m:
            k = "D" + m.group(1)
            self.plc_regs[k] = int(m.group(2))
            return ok("{} = {}".format(k, self.plc_regs[k]))
        if re.search(r"扫描周期|扫描时间", c):
            return ok("当前扫描周期：输入采样 → 程序执行 → 输出刷新，实测约 1.2 ms（典型 1~10 ms）。")
        if re.search(r"输出|q0\.0|q0\.1|查看输出", c) and not re.search(r"写入", c):
            return ok("Q0.0 = {}　Q0.1 = {}　（PLC {}）".format(
                self.plc_mods["Q0.0"], self.plc_mods["Q0.1"], "RUN" if self.plc_run else "STOP"))

        # ---------- Modbus ----------
        m = re.search(r"波特率\s*(?:设为|设置为|=)?\s*(\d+)", c)
        if m:
            self.modbus["baud"] = int(m.group(1))
            return ok("波特率设置为 {} bps。注意：主从站必须一致，且通信距离越远、波特率应越低（1200m 建议 ≤9600）。".format(
                self.modbus["baud"]))
        m = re.search(r"校验\s*(?:设为|设置为|=)?\s*(无|none|偶|even|奇|odd)", c)
        if m:
            t = m.group(1)
            self.modbus["parity"] = "None" if t in ("无", "none") else ("Even" if t in ("偶", "even") else "Odd")
            return ok("校验方式设置为 {}。".format(self.modbus["parity"]))
        m = re.search(r"从站地址\s*(?:设为|设置为|=)?\s*(\d+)", c)
        if m:
            self.modbus["slave"] = int(m.group(1))
            return ok("从站地址设为 {}（有效范围 1~247，0 为广播地址）。".format(self.modbus["slave"]))
        if re.search(r"终端电阻|接\s*120|120\s*欧|120Ω", c):
            self.modbus["term"] = True
            return ok("已在总线两端各接入 120Ω 终端电阻，抑制信号反射。")
        if re.search(r"接线|接\s*a\s*b|a\s*接\s*a|双绞线", c):
            return ok("接线完成：A 接 A（D+），B 接 B（D-），屏蔽层单端接地，手拉手总线拓扑。")
        if re.search(r"通信测试|读从站|读保持寄存器|modbus\s*测试|读取数据", c):
            if self.modbus["baud"] != 9600:
                return err("通信超时。主从站波特率不一致（当前 {}），请统一为 9600。".format(self.modbus["baud"]))
            if not self.modbus["term"]:
                return err("通信偶发 CRC 校验错误。总线两端缺少 120Ω 终端电阻，请补上。")
            return ok("读取成功：\n从站 {} 保持寄存器 40001 = 1234\n功能码 03，CRC16 校验通过，响应时间 12ms".format(
                self.modbus["slave"]))
        if re.search(r"抗干扰|屏蔽|接地", c):
            return ok("抗干扰措施：① 采用屏蔽双绞线，屏蔽层单端接地 ② 与动力线分离走线（≥20cm）③ 总线两端加 120Ω 终端电阻。")

        # ---------- PCB ----------
        if re.search(r"原理图|画原理图|设计原理图", c):
            self.pcb_stage = max(self.pcb_stage, 1)
            return ok("原理图已绘制：电源部分 + MCU 最小系统 + 接口电路。")
        if re.search(r"erc|电气规则检查", c):
            if self.pcb_stage < 1:
                return err("请先完成原理图设计。")
            self.pcb_stage = max(self.pcb_stage, 2)
            return ok("ERC 检查通过：0 个未连接引脚，0 个短路错误。")
        if re.search(r"布局|pcb\s*布局|器件布局", c):
            if self.pcb_stage < 2:
                return err("ERC 未通过，不能进入 PCB 布局。")
            self.pcb_stage = max(self.pcb_stage, 3)
            return ok("布局完成：2 层板，尺寸 60×40mm，去耦电容靠近电源引脚。")
        if re.search(r"布线|铺铜|走线", c):
            if self.pcb_stage < 3:
                return err("请先完成布局。")
            self.pcb_stage = max(self.pcb_stage, 4)
            return ok("布线完成：信号线 8mil，电源线 20mil，大面积铺地。")
        if re.search(r"drc|设计规则检查", c):
            if self.pcb_stage < 4:
                return err("请先完成布线。")
            self.pcb_stage = max(self.pcb_stage, 5)
            return ok("DRC 检查通过：0 个间距错误，0 个未布线网络。")
        if re.search(r"gerber|导出|打样|生成制造文件", c):
            if self.pcb_stage < 5:
                return err("DRC 未通过，不能导出 Gerber。")
            return ok("已导出 Gerber 与钻孔文件（8 个文件），可直接送厂打样。")

        # ---------- 开关电源 ----------
        if re.search(r"上电|启动电源|通电", c):
            self.vout = 12.0
            return ok("电源启动：整流桥输出约 310V 直流，PWM 开始工作，输出 12.0V。")
        if re.search(r"测量输出电压|测输出电压|测\s*vout|输出电压", c):
            return ok("输出电压 Vout = {:.2f} V（额定 12V±5%）。".format(self.vout or 0.0))
        if re.search(r"测量输出电流|测输出电流|输出电流", c):
            return ok("输出电流 Iout = {:.1f} A（满载 2A）。".format(self.load or 0.0))
        if re.search(r"测纹波|看纹波|纹波", c):
            return ok("纹波 Vpp = 48 mV（要求 <100mV，合格）。测量用 20MHz 带宽限制 + 探头短地线。")
        if re.search(r"带载|加载|加负载", c):
            self.load = 2.0
            return ok("已加载 2A 电子负载，输出电压稳定在 {}V，纹波 52mV。".format(self.vout or 0.0))
        if re.search(r"调节|调压|输出调整|调到\s*(\d+)", c):
            m = re.search(r"(\d+(?:\.\d+)?)\s*v", c)
            self.vout = float(m.group(1)) if m else 12.0
            return ok("已通过反馈分压电阻 VP 调整，输出设定为 {:.1f}V。".format(self.vout))
        if re.search(r"温度|过温", c):
            return ok("开关管温度 {}℃，变压器 42℃，均低于 100℃ 报警阈值。".format(round(self.temp + (self.load * 15), 1)))
        if re.search(r"效率", c):
            return ok("输入功率 26.5W，输出功率 24.0W，效率 η ≈ 90.6%。")

        if re.search(r"帮助|help", c):
            return ok("可用操作：闭合 QF / 断开 QF / 按 SB2 正转 / 按 SB3 反转 / 按 SB1 停止 / 模拟过载 / 复位 FR\n"
                      "        切换到电压档 / 测量 Uab / 测量 I / 测量 R / 测通断 / 读表\n"
                      "        PLC：编程 / 运行 PLC / I0.0 置 1 / 读 D0 / 写入 D10 = 100")
        return err("未识别的操作：{}\n（输入『帮助』查看可用指令）".format(raw))


# ============================================================
# 模拟器 7：数字媒体（AE / PR / 调色 / 摄影 / UI）
# ============================================================
class MediaSim:
    PRESETS = {"1080p": (1920, 1080, 25), "4k": (3840, 2160, 25), "竖屏": (1080, 1920, 30), "720p": (1280, 720, 25)}

    def __init__(self):
        self.comp = None
        self.size = None
        self.fps = 25
        self.layers = []
        self.keyframes = 0
        self.curves = False
        self.lut = None
        self.wb = 5600
        self.exposure = 0.0
        self.floor = 0
        self.selected = False
        self.focus = 0
        self.balance = None
        self.exported = None
        self.grading = []
        self.light = 0
        self.ui = []

    def prompt(self):
        return "AE/PR>"

    def _resolve_preset(self, name):
        for k, v in self.PRESETS.items():
            if k.lower() == name.lower():
                return v
        return None

    def _ensure_comp(self):
        if self.comp is None:
            self.comp = "合成 1"
            self.size = self.PRESETS["1080p"]
            self.fps = 25
            return False
        return True

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        c = cmd

        # ---------- AE ----------
        if re.search(r"导入素材|导入|import", c):
            name = re.sub(r".*(?:导入素材|导入)\s*", "", c).strip() or "素材.mov"
            return ok("已导入素材：{}（{}），已放入项目面板。".format(name, "1920×1080 / 25fps / H.264"))
        m = re.search(r"新建合成|创建合成", c)
        if m:
            preset = None
            for k in self.PRESETS:
                if k in c:
                    preset = k
            self.size = self.PRESETS[preset] if preset else self.PRESETS["1080p"]
            self.comp = "合成 1"
            self.fps = self.size[2]
            return ok("合成已创建：{}×{}，{} fps，时长 10 秒，背景黑色。".format(
                self.size[0], self.size[1], self.size[2]))
        if re.search(r"导入.*合成|素材拖进合成|拖入时间轴|添加到合成", c):
            if self.comp is None:
                self._ensure_comp()
            self.layers.append("素材.mov")
            return ok("素材已加入合成，成为图层 1。")
        m = re.search(r"新建.*(?:纯色|文本|文字|形状|adjustment|调整图层|固态层)", c)
        if m:
            t = "文本" if "文" in c else ("调整图层" if "调整" in c else "纯色层")
            self.layers.append(t)
            return ok("已新建 {} 图层（图层 {}）。".format(t, len(self.layers)))
        if re.search(r"关键帧|打关键帧", c):
            if self.comp is None:
                self._ensure_comp()
            self.keyframes += 1
            return ok("已在当前时间点打第 {} 组关键帧（位置/缩放），时间轴显示为菱形标记。".format(self.keyframes))
        if re.search(r"缓动|ease|贝塞尔曲线|曲线编辑器", c):
            if self.keyframes == 0:
                return err("没有关键帧可以加缓动，请先打关键帧（按 P 调出位置属性再点秒表）。")
            return ok("已对关键帧应用缓动（F9 Easy Ease），速度曲线呈 S 形，起止更自然。")
        if re.search(r"表达式|wiggle|expression", c):
            return ok("已添加表达式：wiggle(2,30) —— 每秒 2 次抖动，幅度 30 像素，做出手持摄像机感。")
        if re.search(r"预合成|precompose", c):
            return ok("已将所选图层预合成（Ctrl+Shift+C），嵌套为合成 2，便于整体变换与统一加效果。")
        if re.search(r"渲染|导出|输出", c):
            if self.comp is None:
                self._ensure_comp()
            w, h, _f = self.size or self.PRESETS["1080p"]
            bytes_ = w * h * 3 * self.fps * 10 * 0.08
            self.exported = "H.264"
            return ok("已添加到渲染队列并输出：\n格式 H.264　分辨率 {}×{}　时长 10s\n预计文件大小约 {:.1f} MB".format(
                w, h, bytes_ / 1024 / 1024))

        # ---------- PR ----------
        if re.search(r"建序列|新建序列|序列设置", c):
            self.size = self.PRESETS["1080p"]
            self.comp = "序列 01"
            return ok("已新建序列：1920×1080 / 25fps / 立体声 48kHz。")
        if re.search(r"粗剪|取材|剪辑|裁切|剪开|razor", c):
            if self.comp is None:
                self.comp = "序列 01"
                self.size = self.PRESETS["1080p"]
            self.layers.append("片段")
            return ok("已用剃刀工具裁切并删除废镜头，粗剪完成，时间线时长缩短 40%。")
        if re.search(r"转场|过渡", c):
            return ok("已添加交叉溶解（Cross Dissolve），时长 0.5s，避免生硬跳切。")
        if re.search(r"调色|lumetri|色彩校正|一级校正", c):
            self.grading.append("Lumetri")
            return ok("已打开 Lumetri 颜色面板：\n① 白平衡：色温 {}K ② 曝光 {:.1f} ③ 对比度 +10 ④ 饱和度 105".format(
                self.wb, self.exposure))
        if re.search(r"色轮", c):
            self.grading.append("色轮")
            return ok("色轮调整：阴影偏青、高光偏暖，形成冷暗部/暖高光电影感分离。")
        if re.search(r"曲线", c):
            self.curves = True
            return ok("已添加 S 形曲线：提亮高光、压暗阴影，增加中间调对比。")
        if re.search(r"lut", c):
            self.lut = "Film Look"
            return ok("已套用 LUT（Film Look），强度 60%，避免风格过重。")
        if re.search(r"白平衡|色温", c):
            self.wb = 5600
            return ok("白平衡已校正为 5600K（日光），画面不再偏黄。")
        if re.search(r"字幕|文字", c):
            return ok("已添加字幕（基本图形 → 文本），字体思源黑体 Medium，居中安全框内。")
        if re.search(r"音频|音量|降噪", c):
            return ok("音频已处理：降噪 -12dB，响度标准化到 -16 LUFS（平台标准）。")
        if re.search(r"导出.*h\.?264|导出视频|导出", c):
            if self.comp is None:
                return err("还没有序列，请先新建序列。")
            if not self.grading:
                return ok("已导出 H.264，但检测到尚未调色 —— 建议先做一级校正再加字幕，否则要重新导出。")
            return ok("导出完成：H.264 1920×1080 25fps，码率 16Mbps，文件约 120MB。")

        # ---------- 调色 ----------
        if re.search(r"一级校正|曝光|白平衡校正", c):
            self.exposure = 0.5
            return ok("一级校正完成：曝光 +0.5 档，白平衡归正，去灰。")
        m = re.search(r"(?:提亮|压暗|增加对比|降对比)", c)
        if m:
            return ok("曲线已调整：{}。".format("高光上扬、阴影下压形成 S 曲线" if "对比" in c else "整体亮度线性偏移"))
        if re.search(r"色轮|阴影.*青|高光.*暖", c):
            return ok("色轮：阴影推青、高光推橙，肤色保持在肤色线附近不变脏。")
        if re.search(r"限定器|二级校色|局部", c):
            return ok("用 HSL 限定器选中天空，单独提饱和 + 压暗，不影响人物。")

        # ---------- 摄影 ----------
        m = re.search(r"光圈\s*(?:设为|调到|=)?\s*f?/?(\d+(?:\.\d+)?)", c)
        if m:
            self.focus = float(m.group(1))
            return ok("光圈设为 f/{}。{}".format(
                self.focus, "大光圈 → 景深浅，背景虚化明显。" if self.focus <= 2.8 else
                "小光圈 → 景深深，前后都清晰。"))
        m = re.search(r"快门\s*(?:设为|调到|=)?\s*1?/?(\d+)", c)
        if m:
            return ok("快门设为 1/{}s。{}".format(m.group(1),
                "低于安全快门（1/焦距）容易手抖，建议上三脚架。" if int(m.group(1)) < 60 else "安全快门范围内。"))
        m = re.search(r"iso\s*(?:设为|调到|=)?\s*(\d+)", c)
        if m:
            v = int(m.group(1))
            return ok("ISO 设为 {}。{}".format(v, "低感光度，噪点少、画质干净。" if v <= 400 else "高感光度，暗光可用但噪点增加，注意后期降噪。"))
        m = re.search(r"色温\s*(?:设为|调到|=)?\s*(\d+)\s*k?", c)
        if m:
            self.wb = int(m.group(1))
            return ok("色温设为 {}K。{}".format(self.wb, "偏暖（黄）" if self.wb < 5000 else ("中性日光" if self.wb <= 5600 else "偏冷（蓝）")))
        if re.search(r"布光|三点布光|主光|辅光|轮廓光|光比", c):
            self.light += 1
            return ok("三点布光布置：\n主光 45° 侧前方（柔光箱）→ 辅光另一侧补暗部，光比 3:1 → 轮廓光在后侧打亮发丝分离背景。")
        if re.search(r"直方图|曝光检查|看直方图", c):
            return ok("直方图检查：高光未溢出（无右侧削波），阴影有细节（左侧未贴边），曝光正常。")
        if re.search(r"拍摄|按下快门|拍照", c):
            if self.light == 0:
                return ok("已拍摄。但未布光，画面平淡、暗部细节不足 —— 建议先做三点布光。")
            return ok("拍摄完成：f/{}　1/125s　ISO100　{}K，直方图无溢出。".format(
                self.focus or 1.8, self.wb))

        # ---------- UI ----------
        if re.search(r"栅格|网格|8pt|间距", c):
            self.ui.append("grid")
            return ok("已建立 8pt 栅格系统：基础间距 8/16/24/32，页面 12 栏，左右边距 24。")
        if re.search(r"色彩|配色|主色", c):
            self.ui.append("color")
            return ok("色彩体系：主色 #2F6FD0（品牌），辅色 #2E9E5B，中性 5 级灰阶，文本对比度 ≥ 4.5:1 满足 WCAG AA。")
        if re.search(r"字体|字号|层级|字阶", c):
            self.ui.append("type")
            return ok("字体层级：H1 32/40、H2 24/32、正文 14/22、辅助 12/18，字重 400/600 两级。")
        if re.search(r"组件|按钮|卡片", c):
            self.ui.append("comp")
            return ok("组件库已建立：Button（primary/ghost）、Card、Input、Tag，统一圆角 8px、状态齐全（默认/悬停/禁用）。")
        if re.search(r"对比度|可访问性|无障碍", c):
            return ok("对比度检查：正文 #1F2A36 on #FFFFFF = 12.6:1，达标；次要文字 #6B7A8C = 4.6:1，刚过 AA。")

        return err("未识别的操作：{}\n（这是 AE/PR/调色/摄影/UI 的综合沙箱，试试『新建合成 1080p』『导入素材』『打关键帧』『导出』）".format(raw))


# ============================================================
# 模拟器 8：电子商务（真算指标）
# ============================================================
class BizSim:
    def __init__(self):
        self.price = 99.0
        self.cost = 45.0
        self.uv = 10000
        self.cvr = 0.036
        self.coupon = 0.0
        self.ad_spend = 0.0
        self.stock = 500
        self.daily_sales = 30
        self.lead_time = 7
        self.refund_rate = 0.03
        self.commission = 0.05

    def prompt(self):
        return "店铺后台>"

    def gmv(self):
        return self.uv * self.cvr * self.price * (1 - self.refund_rate)

    def margin(self):
        return self.price * (1 - self.coupon) - self.cost

    def __call__(self, raw):
        cmd = norm(raw)
        if not cmd:
            return ok("")
        c = cmd

        m = re.search(r"(?:售价|定价|价格)\s*(?:设为|调整为|=)?\s*(\d+(?:\.\d+)?)", c)
        if m:
            self.price = float(m.group(1))
            return ok("售价设为 ￥{:.2f}。当前毛利 ￥{:.2f}，毛利率 {:.1f}%。".format(
                self.price, self.margin(), self.margin() / self.price * 100 if self.price else 0))
        m = re.search(r"(?:成本|进价)\s*(?:设为|=)?\s*(\d+(?:\.\d+)?)", c)
        if m:
            self.cost = float(m.group(1))
            return ok("成本设为 ￥{:.2f}，毛利 ￥{:.2f}。".format(self.cost, self.margin()))

        if re.search(r"毛利|利润率|测算", c):
            mg = self.margin()
            rate = mg / self.price * 100 if self.price else 0
            return ok("售价 ￥{:.2f}　成本 ￥{:.2f}　让利 ￥{:.2f}\n毛利 ￥{:.2f}　毛利率 {:.1f}%\n"
                      "{}".format(self.price, self.cost, self.price * self.coupon, mg, rate,
                                  "毛利率健康（>30%），有推广空间。" if rate >= 30 else
                                  "毛利率偏低（<30%），投流很容易亏，建议提价或压成本。"))
        m = re.search(r"(?:优惠券|折扣|满减|让利)\s*(?:设为|=)?\s*(\d+(?:\.\d+)?)\s*%?", c)
        if m:
            val = float(m.group(1))
            self.coupon = val / 100 if val > 1 else val
            return ok("已设置优惠 {}%，券后价 ￥{:.2f}，毛利降到 ￥{:.2f}。".format(
                self.coupon * 100, self.price * (1 - self.coupon), self.margin()))
        m = re.search(r"(?:uv|曝光|流量)\s*(?:设为|=)?\s*(\d+)", c)
        if m:
            self.uv = int(m.group(1))
            return ok("UV = {}，预计成交 {} 单。".format(self.uv, int(self.uv * self.cvr)))
        m = re.search(r"(?:转化率|cvr)\s*(?:设为|=)?\s*(\d+(?:\.\d+)?)\s*%", c)
        if m:
            self.cvr = float(m.group(1)) / 100
            return ok("转化率 = {:.2f}%，当前 UV {} 下预估成交 {} 单。".format(
                self.cvr * 100, self.uv, int(self.uv * self.cvr)))
        m = re.search(r"(?:投放|广告|花费|预算)\s*(?:设为|=)?\s*(\d+(?:\.\d+)?)", c)
        if m:
            self.ad_spend = float(m.group(1))
            return ok("广告花费 ￥{:.0f}。".format(self.ad_spend))

        if re.search(r"gmv|销售额|成交额", c):
            g = self.gmv()
            return ok("预估 GMV = ￥{:,.0f}\n计算：UV {} × 转化率 {:.2f}% × 客单价 ￥{:.0f} × (1-退款率 {:.0f}%)".format(
                g, self.uv, self.cvr * 100, self.price, self.refund_rate * 100))
        if re.search(r"roi|投产比|回报", c):
            if self.ad_spend <= 0:
                return err("还没设置投放花费，先用『投放 5000』这样的命令设置预算。")
            g = self.gmv()
            roi = g / self.ad_spend
            return ok("ROI = GMV ￥{:,.0f} ÷ 花费 ￥{:,.0f} = {:.2f}\n{}".format(
                g, self.ad_spend, roi,
                "ROI > 盈亏平衡点，投放可继续放量。" if roi > 2 else
                "ROI 低于盈亏平衡，建议先优化转化率或降低出价，否则越投越亏。"))
        if re.search(r"保本|盈亏平衡|breakeven", c):
            mg = self.margin()
            if mg <= 0:
                return err("毛利为负，卖一单亏一单，先调价。")
            be = self.ad_spend / mg if self.ad_spend else 0
            return ok("每单毛利 ￥{:.2f}，可承受获客成本上限 ￥{:.2f}/单。\n"
                      "若投入 ￥{:.0f} 广告，需至少成交 {:.0f} 单才保本。".format(mg, mg, self.ad_spend, be))
        if re.search(r"漏斗|转化路径|各环节", c):
            click = int(self.uv * 0.3)
            cart = int(click * 0.3)
            order = int(cart * 0.5)
            pay = int(order * (1 - self.refund_rate))
            return ok("转化漏斗：\n曝光 UV {:>7,}\n点击     {:>7,}  （点击率 30%）\n"
                      "加购     {:>7,}  （加购率 30%）\n下单     {:>7,}  （下单率 50%）\n"
                      "支付     {:>7,}  （支付率 {:.0f}%）\n\n整体转化率 {:.2f}%".format(
                          self.uv, click, cart, order, pay, (1 - self.refund_rate) * 100,
                          pay / self.uv * 100))
        m = re.search(r"(?:库存|备货|存货)\s*(?:设为|=)?\s*(\d+)", c)
        if m:
            self.stock = int(m.group(1))
            return ok("库存设为 {} 件。".format(self.stock))
        m = re.search(r"(?:日销|日均销量|每天卖)\s*(?:设为|=)?\s*(\d+)", c)
        if m:
            self.daily_sales = int(m.group(1))
            return ok("日均销量 {} 件。".format(self.daily_sales))
        if re.search(r"库存天数|可售天数|断货", c):
            days = self.stock / max(self.daily_sales, 1)
            return ok("库存 {} 件 ÷ 日销 {} 件 = 可售 {:.1f} 天\n安全库存（含 {} 天交期）建议 ≥ {} 件。\n{}".format(
                self.stock, self.daily_sales, days, self.lead_time, self.daily_sales * (self.lead_time + 3),
                "⚠️ 低于交期，有断货风险，立即补货。" if days < self.lead_time else "库存健康，无需补货。"))
        if re.search(r"风险预警|会不会断货|断货风险判断|风险评估", c):
            days = self.stock / max(self.daily_sales, 1)
            lvl = "高" if days < self.lead_time else ("中" if days < self.lead_time * 2 else "低")
            return ok("断货风险判断：\n可售 {:.1f} 天 / 交期 {} 天 → 风险等级【{}】\n"
                      "高：立即下补货单并考虑临时调拨\n中：本周内下单\n低：常规补货节奏即可。".format(
                          days, self.lead_time, lvl))
        if re.search(r"补货|安全库存|该备多少", c):
            days = self.stock / max(self.daily_sales, 1)
            need = self.daily_sales * (self.lead_time + 3)
            return ok("补货建议：\n交期 {} 天，建议安全库存 = 日销 {} × ({} + 3) = {} 件。\n"
                      "当前 {} 件，可售 {:.1f} 天。\n→ 下单量建议 {} 件（补到安全库存并覆盖一个交期）。".format(
                          self.lead_time, self.daily_sales, self.lead_time, need,
                          self.stock, days, max(0, need * 2 - self.stock)))
        if re.search(r"选品|上新|测款|选品建议", c):
            return ok("选品四象限：\n① 高需求低竞争 → 主推\n② 高需求高竞争 → 靠差异化/价格\n"
                      "③ 低需求低竞争 → 小而美，做利润\n④ 低需求高竞争 → 放弃\n建议先用 200-500 元小额测款，看点击率 >3%、加购率 >8% 再放量。")
        if re.search(r"竞品|对手|对标", c):
            return ok("竞品分析维度：价格带、主图点击率、评价数、DSR、发货时效。\n"
                      "若竞品价格低 15% 但评价数只有你 1/3，不建议跟价，改打『服务+内容』差异。")
        if re.search(r"结算|到账|手续费|佣金|对账", c):
            g = self.gmv()
            comm = g * self.commission
            fee = g * 0.006
            refund = g * self.refund_rate
            net = g - comm - fee - refund
            return ok("对账明细（按 GMV ￥{:,.0f}）：\n"
                      "  平台佣金 {:>10,.2f}（{:.1f}%）\n  支付手续费 {:>8,.2f}（0.6%）\n"
                      "  退款 {:>12,.2f}（{:.0f}%）\n  ─────────────────────\n  实际到账 {:>8,.2f}".format(
                          g, comm, self.commission * 100, fee, refund, self.refund_rate * 100, net))
        if re.search(r"abc|分类|重点商品", c):
            return ok("ABC 分类：\nA 类（SKU 10%，贡献 70% 销售额）→ 保库存、保曝光\n"
                      "B 类（20%，20%）→ 稳定运营\nC 类（70%，10%）→ 清库存、不占资源")
        if re.search(r"活动|大促|策划", c):
            return ok("活动方案：\n目标：GMV 提升 30%\n玩法：满 199 减 30 + 前 100 名赠品\n"
                      "预算：优惠让利 ￥3,000 + 投放 ￥5,000\n考核：ROI ≥ 2.5，退货率 ≤ 5%")

        if re.search(r"复盘|总结|数据看板", c):
            return ok("经营概览：\n售价 ￥{:.2f}　成本 ￥{:.2f}　毛利 ￥{:.2f}\nUV {}　转化率 {:.2f}%　GMV ￥{:,.0f}\n"
                      "库存 {:.0f} 天　退款率 {:.0f}%".format(
                          self.price, self.cost, self.margin(), self.uv, self.cvr * 100, self.gmv(),
                          self.stock / max(self.daily_sales, 1), self.refund_rate * 100))

        return err("未识别的操作：{}\n（试试『设置售价 129』『测算毛利』『投放 5000』『看 ROI』『分析漏斗』『库存天数』『对账』）".format(raw))


# ============================================================
# ★ 实训模板注册表：38 个实训 / 8 个专业方向
#    新增一门实训 = 在这里加一条，页面代码不用动
# ============================================================
def _t(label, group, badge, prompt, sim, figure, title, content, commands, steps, hints=None, visual=""):
    return {
        "label": label, "group": group, "badge": badge, "prompt": prompt, "sim": sim,
        "figure": figure, "title": title, "content": content,
        "commands": commands, "steps": steps, "hints": hints or {},
        "visual": visual or "配套图",
    }


TEMPLATES = {
    # ==================== 1. 计算机网络 ====================
    "net_vrp_basic": _t(
        "🌐 网络设备基础配置", "计算机网络", "网络", "<Huawei>", VRPSim, "switch_basic",
        "网络设备基础配置",
        "完成以下配置：\n1. 进入系统视图\n2. 进入 GE0/0/0 接口\n3. 配置 IP 地址 192.168.1.1 255.255.255.0\n4. 查看接口 IP 状态\n5. 保存配置",
        ["system-view", "interface g0/0/0", "ip address 192.168.1.1 255.255.255.0",
         "display ip interface brief", "save"],
        [("进入系统视图", ["system-view", "sys"]),
         ("进入 GE0/0/0 接口", ["interface g0/0/0", "interface gigabitethernet 0/0/0"]),
         ("配置接口 IP", ["ip address 192.168.1.1"]),
         ("查看接口状态", ["display ip interface"]),
         ("保存配置", ["save"])],
        {"interface": "简写 g0/0/0 与 gigabitethernet 0/0/0 等价"},
        "拓扑图",
    ),
    "net_vrp_vlan": _t(
        "🌐 VLAN 划分与 Trunk", "计算机网络", "网络", "<Huawei>", VRPSim, "vlan",
        "VLAN 划分与 Trunk 配置",
        "完成以下配置：\n1. 批量创建 VLAN 10 和 VLAN 20\n2. 把 GE0/0/0 配成 access 并划入 VLAN 10\n3. 把 GE0/0/1 配成 trunk 并放行 VLAN 10、20\n4. 查看 VLAN 信息验证",
        ["system-view", "vlan batch 10 20", "interface g0/0/0", "port link-type access",
         "port default vlan 10", "interface g0/0/1", "port link-type trunk",
         "port trunk allow-pass vlan 10 20", "display vlan"],
        [("批量创建 VLAN", ["vlan batch 10 20", "vlan 10", "vlan 20"]),
         ("配置 access 端口", ["port link-type access"]),
         ("端口划入 VLAN 10", ["port default vlan 10"]),
         ("配置 trunk 端口", ["port link-type trunk"]),
         ("trunk 放行 VLAN", ["port trunk allow-pass"]),
         ("查看 VLAN 验证", ["display vlan"])],
        {"access": "先 link-type access 再 default vlan，顺序反了会报错"},
        "拓扑图",
    ),
    "net_ospf": _t(
        "🌐 OSPF 动态路由", "计算机网络", "网络", "<Huawei>", VRPSim, "ospf",
        "OSPF 单区域配置",
        "完成以下配置：\n1. 进入系统视图\n2. 启动 OSPF 进程 1，Router ID 1.1.1.1\n3. 进入区域 0\n4. 宣告 192.168.1.0/24 网段\n5. 查看 OSPF 邻居状态",
        ["system-view", "ospf 1 router-id 1.1.1.1", "area 0",
         "network 192.168.1.0 0.0.0.255", "display ospf peer", "display ospf routing"],
        [("进入系统视图", ["system-view", "sys"]),
         ("启动 OSPF 进程", ["ospf 1"]),
         ("进入区域 0", ["area 0"]),
         ("宣告网段", ["network 192.168.1.0"]),
         ("查看邻居状态", ["display ospf peer"])],
        {"area": "必须先 ospf 再 area，area 只在 ospf 视图下有效"},
        "拓扑图",
    ),
    "net_stp": _t(
        "🌐 STP 生成树防环", "计算机网络", "网络", "<Huawei>", VRPSim, "stp",
        "STP 防环配置",
        "完成以下配置：\n1. 进入系统视图\n2. 把 STP 模式设为 STP\n3. 全局启用 STP\n4. 查看 STP 端口角色与状态",
        ["system-view", "stp mode stp", "stp enable", "display stp brief"],
        [("进入系统视图", ["system-view", "sys"]),
         ("设置 STP 模式", ["stp mode"]),
         ("启用 STP", ["stp enable"]),
         ("查看端口状态", ["display stp"])],
        {},
        "拓扑图",
    ),
    "net_dhcp": _t(
        "🌐 DHCP 地址分配", "计算机网络", "网络", "<Huawei>", VRPSim, "dhcp",
        "DHCP 服务器配置",
        "完成以下配置：\n1. 进入系统视图\n2. 全局启用 DHCP\n3. 创建地址池 pool1\n4. 配置网段与网关\n5. 查看接口 IP 确认地址池生效",
        ["system-view", "dhcp enable", "ip pool pool1", "network 192.168.1.0 mask 255.255.255.0",
         "gateway-list 192.168.1.1", "display ip interface brief"],
        [("进入系统视图", ["system-view", "sys"]),
         ("启用 DHCP", ["dhcp enable"]),
         ("创建地址池", ["ip pool"]),
         ("配置网段", ["network 192.168.1.0"]),
         ("查看验证", ["display ip interface"])],
        {},
        "时序图",
    ),

    # ==================== 2. Linux 运维 ====================
    "net_linux_basic": _t(
        "🐧 Linux 基本命令", "Linux 运维", "Linux", "[student@localhost ~]$", LinuxSim, "linux_basic",
        "Linux 基本命令练习",
        "依次执行并观察输出：\n1. pwd 查看当前目录\n2. whoami 查看当前用户\n3. cat /etc/os-release 查看系统版本\n4. ip addr 查看网络配置\n5. ping 192.168.1.1 -c 3 测试连通性\n6. df -h 查看磁盘空间",
        ["pwd", "whoami", "cat /etc/os-release", "ip addr", "ping 192.168.1.1 -c 3", "df -h"],
        [("查看当前目录", ["pwd"]),
         ("查看当前用户", ["whoami"]),
         ("查看系统版本", ["cat /etc/os-release"]),
         ("查看网络配置", ["ip addr", "ip a"]),
         ("测试连通性", ["ping"]),
         ("查看磁盘", ["df -h"])],
        {"ping": "没有 -c 参数会一直 ping，记得写 -c 3"},
        "示意图",
    ),
    "net_linux_nginx": _t(
        "🐧 Nginx 服务部署", "Linux 运维", "Linux", "[student@localhost ~]$", LinuxSim, "nginx",
        "Nginx 服务部署与验证",
        "完成 Web 服务部署：\n1. 安装 nginx\n2. 启动服务并设置开机自启\n3. 查看服务状态\n4. 防火墙放行 80 端口\n5. 查看端口监听情况",
        ["yum install -y nginx", "systemctl start nginx", "systemctl enable nginx",
         "systemctl status nginx", "firewall-cmd --add-port=80/tcp --permanent",
         "firewall-cmd --reload", "netstat -tlnp"],
        [("安装 nginx", ["yum install -y nginx"]),
         ("启动服务", ["systemctl start nginx"]),
         ("设置开机自启", ["systemctl enable nginx"]),
         ("查看服务状态", ["systemctl status nginx"]),
         ("防火墙放行 80", ["firewall-cmd --add-port=80"]),
         ("重载防火墙", ["firewall-cmd --reload"]),
         ("查看端口监听", ["netstat", "ss -tln"])],
        {"order": "没装 nginx 就 start 会报 Unit not found，按顺序来"},
        "流程图",
    ),
    "net_linux_perm": _t(
        "🐧 文件权限与用户", "Linux 运维", "Linux", "[student@localhost ~]$", LinuxSim, "perm",
        "文件权限与用户管理",
        "完成权限管理：\n1. 在 /home/student 下创建文件 test.txt\n2. 查看它的详细权限\n3. 把权限改为 755\n4. 新建用户 zhangsan\n5. 把文件属主改为 zhangsan\n6. 再次查看确认",
        ["touch test.txt", "ls -l", "chmod 755 test.txt", "useradd zhangsan",
         "chown zhangsan test.txt", "ls -l"],
        [("创建文件", ["touch test.txt"]),
         ("查看权限", ["ls -l", "ls -ld"]),
         ("修改权限", ["chmod"]),
         ("新建用户", ["useradd", "adduser"]),
         ("修改属主", ["chown"])],
        {"chmod": "755 = rwxr-xr-x，属主全权限，其他人只读和执行"},
        "示意图",
    ),
    "net_shell_script": _t(
        "🐧 Shell 脚本与定时任务", "Linux 运维", "Linux", "[student@localhost ~]$", LinuxSim, "shell_flow",
        "Shell 脚本与 crontab",
        "完成自动化脚本：\n1. 创建脚本 backup.sh\n2. 写入备份逻辑\n3. 赋予执行权限\n4. 查看已有的定时任务\n5. 确认脚本内容",
        ["touch backup.sh", "echo '#!/bin/bash' > backup.sh", "chmod +x backup.sh",
         "crontab -l", "cat backup.sh"],
        [("创建脚本文件", ["touch backup.sh"]),
         ("写入脚本内容", ["echo", ">"]),
         ("赋予执行权限", ["chmod +x"]),
         ("查看定时任务", ["crontab"]),
         ("确认脚本内容", ["cat backup.sh"])],
        {},
        "流程图",
    ),
    "net_lnmp": _t(
        "🐧 LNMP 环境排查", "Linux 运维", "Linux", "[student@localhost ~]$", LinuxSim, "lnmp",
        "LNMP 环境故障排查",
        "网站打不开，按顺序排查：\n1. 查看服务状态\n2. 查看端口监听\n3. 检查 Nginx 配置语法\n4. 查看错误日志\n5. 重启 Nginx 并验证",
        ["systemctl status nginx", "netstat -tlnp", "nginx -t", "tail -n 20 /var/log/nginx/error.log",
         "systemctl restart nginx", "curl -I http://127.0.0.1"],
        [("查看服务状态", ["systemctl status"]),
         ("查看端口监听", ["netstat", "ss -tln"]),
         ("检查配置语法", ["nginx -t"]),
         ("查看错误日志", ["error.log", "tail"]),
         ("验证访问", ["curl"])],
        {"restart": "改完配置要先 nginx -t 验证，否则重启会失败把站点搞挂"},
        "流程图",
    ),

    # ==================== 3. 数据库技术 ====================
    "db_sql_query": _t(
        "🗄️ MySQL 单表查询", "数据库技术", "数据库", "mysql>", MySQLSim, "er_simple",
        "MySQL 查询语句练习",
        "在 test 库中完成：\n1. 查看所有数据库\n2. 切换到 test 库\n3. 查看所有表\n4. 查看 students 表结构\n5. 查询 students 全部数据\n6. 查询年龄大于 18 的学生",
        ["show databases;", "use test;", "show tables;", "desc students;",
         "select * from students;", "select * from students where age > 18;"],
        [("查看数据库列表", ["show databases"]),
         ("切换数据库", ["use test"]),
         ("查看表列表", ["show tables"]),
         ("查看表结构", ["desc students"]),
         ("查询全部数据", ["select * from students"]),
         ("条件查询", ["where"])],
        {"use": "不先 use test 就查表会报 No database selected"},
        "ER 图",
    ),
    "db_sql_design": _t(
        "🗄️ 建表与索引设计", "数据库技术", "数据库", "mysql>", MySQLSim, "er_student",
        "学生成绩库表设计",
        "设计学生成绩数据库：\n1. 创建 students 表（id 主键、name、age）\n2. 创建 scores 表（id 主键、student_id、course_id、score）\n3. 为 students.name 建索引\n4. 向 students 插入一条测试数据\n5. 查看表结构确认",
        ["use test;",
         "create table students(id int primary key, name varchar(20), age int);",
         "create table scores(id int primary key, student_id int, course_id int, score decimal(5,2));",
         "create index idx_name on students(name);",
         "insert into students values(4,'赵六',19);",
         "desc scores;"],
        [("切换数据库", ["use test"]),
         ("创建 students 表", ["create table students"]),
         ("创建 scores 表", ["create table scores"]),
         ("创建索引", ["create index"]),
         ("插入测试数据", ["insert into"]),
         ("查看表结构", ["desc"])],
        {"duplicate": "students 表已存在（旧库），重复 create 会报 1050，可跳过或换表名"},
        "ER 图",
    ),
    "db_sql_join": _t(
        "🗄️ 多表连接与聚合", "数据库技术", "数据库", "mysql>", MySQLSim, "join",
        "多表连接与聚合查询",
        "完成关联查询：\n1. 查看三张表\n2. 查询学生姓名 + 课程名 + 分数\n3. 统计每个学生总分\n4. 按分数排序取前 3\n5. 统计每个分数段人数",
        ["use test;", "show tables;",
         "select s.name, c.name, sc.score from students s, scores sc, courses c where sc.student_id = s.id and sc.course_id = c.id;",
         "select student_id, sum(score) from scores group by student_id;",
         "select * from scores order by score desc limit 3;",
         "select count(*) from scores;"],
        [("切换数据库", ["use test"]),
         ("三表关联查询", ["from students", "join"]),
         ("分组聚合", ["group by"]),
         ("排序取前几名", ["order by"]),
         ("计数统计", ["count("])],
        {"group": "SELECT 里出现的非聚合列必须出现在 GROUP BY 中"},
        "ER 图",
    ),
    "db_sql_tx": _t(
        "🗄️ 事务与并发控制", "数据库技术", "数据库", "mysql>", MySQLSim, "txn",
        "事务与锁",
        "体会事务的原子性：\n1. 切换 test 库\n2. 开启事务\n3. 修改一条分数\n4. 查看修改结果\n5. 提交事务\n6. 再次查询确认",
        ["use test;", "begin;", "update scores set score = 90 where id = 1;",
         "select * from scores where id = 1;", "commit;", "select * from scores where id = 1;"],
        [("切换数据库", ["use test"]),
         ("开启事务", ["begin", "start transaction"]),
         ("执行更新", ["update"]),
         ("事务内查询", ["select"]),
         ("提交事务", ["commit"])],
        {"rollback": "想撤销用 rollback，提交后就不能回滚了"},
        "时序图",
    ),

    # ==================== 4. Python 编程 ====================
    "py_basic": _t(
        "🐍 变量与输出", "Python 编程", "Python", ">>>", PySim, "py_flow",
        "Python 变量与输出",
        "在交互环境依次输入：\n1. print('hello world')\n2. print(1+2)\n3. name = '张三'\n4. print(name)\n5. print(len([1,2,3]))",
        ["print('hello world')", "print(1+2)", "name = '张三'", "print(name)", "print(len([1,2,3]))"],
        [("输出 hello world", ["print('hello world')", 'print("hello world")']),
         ("计算 1+2", ["print(1+2)"]),
         ("定义变量", ["name ="]),
         ("输出变量", ["print(name)"]),
         ("求列表长度", ["len("])],
        {"sandbox": "这是真实求值沙箱，可以随便写 Python，不是背命令"},
        "流程图",
    ),
    "py_loop": _t(
        "🐍 循环与列表", "Python 编程", "Python", ">>>", PySim, "py_flow",
        "Python 循环与列表推导",
        "在交互环境依次输入：\n1. for i in range(5): print(i)\n2. print(sum([1,2,3]))\n3. print([x*2 for x in [1,2,3]])\n4. def add(a,b): return a+b\n5. print(add(3,4))",
        ["for i in range(5): print(i)", "print(sum([1,2,3]))",
         "print([x*2 for x in [1,2,3]])", "def add(a,b): return a+b", "print(add(3,4))"],
        [("for 循环", ["for i in range"]),
         ("求和", ["sum("]),
         ("列表推导式", ["for x in"]),
         ("定义函数", ["def "]),
         ("调用函数", ["add("])],
        {},
        "流程图",
    ),
    "py_oop": _t(
        "🐍 类与对象", "Python 编程", "Python", ">>>", PySim, "oop",
        "Python 面向对象",
        "定义一个类并实例化（可一行写完，也可多行）：\n1. 定义 class Person\n2. 写 __init__ 初始化 name\n3. 实例化 p1 = Person('张三')\n4. 访问 p1.name\n5. 添加一个方法 say() 并调用",
        ["class Person:\n    def __init__(self, name):\n        self.name = name",
         "p1 = Person('张三')", "print(p1.name)",
         "class Person:\n    def __init__(self, name):\n        self.name = name\n    def say(self):\n        return 'hi ' + self.name",
         "print(p1.say())"],
        [("定义类", ["class "]),
         ("写 __init__", ["__init__"]),
         ("实例化", ["person("]),
         ("访问属性", [".name"]),
         ("调用方法", ["say("])],
        {"multi": "支持多行输入：把 class 定义整段粘进去即可"},
        "拓扑图",
    ),
    "py_crawler": _t(
        "🐍 数据抓取与清洗", "Python 编程", "Python", ">>>", PySim, "crawler",
        "数据清洗与统计",
        "用内置数据完成清洗（沙箱无网络，用模拟数据）：\n1. 构造原始数据列表\n2. 过滤掉空值\n3. 去重\n4. 计算平均值\n5. 打印结果",
        ["raw = [88, None, 92, 76, 88, None, 95]",
         "clean = [x for x in raw if x is not None]",
         "uniq = list(set(clean))",
         "print(sum(clean) / len(clean))",
         "print(sorted(uniq))"],
        [("构造原始数据", ["raw ="]),
         ("过滤空值", ["if x is not none", "if x"]),
         ("去重", ["set("]),
         ("计算平均值", ["sum(", "/ len"]),
         ("打印结果", ["print("])],
        {"net": "沙箱不联网，import requests 会提示模块不可用：实训重点是清洗逻辑"},
        "流程图",
    ),

    # ==================== 5. 软件工程 ====================
    "se_git": _t(
        "🧩 Git 版本控制", "软件工程", "软件", "user@dev:~/project (main)$", GitSim, "git",
        "Git 提交与分支",
        "完成一次完整提交：\n1. 查看仓库状态\n2. 把改动加入暂存区\n3. 提交并写说明\n4. 查看提交历史\n5. 新建分支 feature/login 并切换",
        ["git status", "git add .", "git commit -m 'feat: 完成登录页'", "git log",
         "git checkout -b feature/login", "git branch"],
        [("查看状态", ["git status"]),
         ("添加到暂存区", ["git add"]),
         ("提交", ["git commit"]),
         ("查看历史", ["git log"]),
         ("新建并切换分支", ["git checkout -b", "git switch -c"])],
        {"commit": "必须 git add 之后才能 commit，否则会说 nothing added"},
        "拓扑图",
    ),
    "se_tdd": _t(
        "🧩 单元测试与 TDD", "软件工程", "软件", "user@dev:~/project (main)$", GitSim, "tdd",
        "TDD 红绿重构",
        "按 TDD 节奏走：\n1. 查看测试文件\n2. 运行测试（先看它红）\n3. 补齐实现\n4. 再次运行到绿\n5. 提交代码",
        ["cat test_app.py", "pytest", "echo 'def add(a,b): return a+b' > app.py", "pytest",
         "git add .", "git commit -m 'test: 补充 add 测试'"],
        [("查看测试", ["cat test_"]),
         ("运行测试", ["pytest", "npm test"]),
         ("补齐实现", ["app.py", "echo"]),
         ("测试转绿", ["pytest", "npm test"]),
         ("提交", ["git commit"])],
        {},
        "流程图",
    ),
    "se_uml": _t(
        "🧩 UML 建模", "软件工程", "软件", "user@dev:~/project (main)$", GitSim, "uml",
        "UML 类图设计",
        "为订单场景建模：\n1. 找出实体类\n2. 画出类之间的关系\n3. 标注多重性\n4. 提取关键属性与方法\n5. 检查设计是否符合单一职责",
        ["设计 User 类", "设计 Order 类", "设计 OrderItem 类",
         "建立 User 与 Order 的一对多关系", "建立 Order 与 OrderItem 的一对多关系",
         "检查单一职责原则"],
        [("设计 User", ["user"]),
         ("设计 Order", ["order"]),
         ("设计 OrderItem", ["orderitem"]),
         ("标注关系", ["一对多", "1:n"]),
         ("检查职责", ["单一职责", "职责"])],
        {},
        "类图",
    ),
    "se_scrum": _t(
        "🧩 敏捷迭代管理", "软件工程", "软件", "user@dev:~/project (main)$", GitSim, "scrum",
        "Sprint 规划",
        "规划一个两周迭代：\n1. 梳理产品待办\n2. 估算故事点\n3. 排定本迭代目标\n4. 拆解任务到人\n5. 安排每日站会与评审",
        ["梳理产品待办", "估算故事点", "排定迭代目标", "拆解任务", "安排站会", "安排评审"],
        [("梳理待办", ["待办", "backlog"]),
         ("估算故事点", ["故事点", "估算"]),
         ("排迭代目标", ["迭代目标", "sprint"]),
         ("拆解任务", ["拆解", "任务"]),
         ("安排站会", ["站会"]),
         ("安排评审", ["评审", "回顾"])],
        {},
        "时间轴",
    ),

    # ==================== 6. 电气自动化 ====================
    "ee_motor": _t(
        "🔌 三相电机正反转", "电气自动化", "电气", "PLC>", ElectricSim, "motor_ctrl",
        "三相电机正反转控制",
        "按正确顺序操作：\n1. 闭合 QF 主断路器\n2. 按 SB2 正转启动\n3. 按 SB1 停止\n4. 按 SB3 反转启动\n5. 模拟过载，观察保护动作\n6. 复位 FR",
        ["闭合 QF", "按 SB2 正转", "按 SB1 停止", "按 SB3 反转", "模拟过载", "复位 FR"],
        [("合闸上电", ["闭合 qf", "合上 qf", "上电", "合闸"]),
         ("正转启动", ["sb2", "正转启动"]),
         ("停止", ["sb1", "停止"]),
         ("反转启动", ["sb3", "反转启动"]),
         ("过载保护", ["过载"]),
         ("复位热继电器", ["复位 fr", "fr 复位"])],
        {"interlock": "正转没停就按反转会被互锁拦住，这是真实的安全设计"},
        "流程图",
    ),
    "ee_meter": _t(
        "🔌 万用表测量实训", "电气自动化", "电气", "PLC>", ElectricSim, "multimeter",
        "万用表测量线电压与电流",
        "正确使用万用表：\n1. 切换到电压档\n2. 测量三相线电压 Uab\n3. 切换到电流档\n4. 测量运行电流\n5. 切换到电阻档并说明为什么必须断电",
        ["切换到电压档", "测量 Uab", "切换到电流档", "测量 I", "切换到电阻档", "读表"],
        [("选择电压档", ["切换", "电压档"]),
         ("测量电压", ["测量 u", "测电压", "测量u"]),
         ("选择电流档", ["电流档"]),
         ("测量电流", ["测量 i", "测电流", "测量i"]),
         ("电阻档注意事项", ["电阻档", "测量 r", "测电阻"])],
        {"strip": "测电流一定要串联，并联会短路烧表"},
        "时间轴",
    ),
    "ee_plc": _t(
        "🔌 PLC 梯形图与 I/O", "电气自动化", "电气", "PLC>", ElectricSim, "plc_scan",
        "PLC 梯形图与 I/O 调试",
        "完成 PLC 基本调试：\n1. 写入梯形图程序\n2. 切换到 RUN 模式\n3. 把 I0.0 置 1 看输出\n4. 读寄存器 D0\n5. 写入 D10 = 100",
        ["编程", "运行 PLC", "I0.0 置 1", "读 D0", "写入 D10 = 100", "查看输出"],
        [("编写程序", ["编程", "梯形图"]),
         ("切到 RUN", ["运行 plc", "run"]),
         ("输入置位", ["i0.0 置"]),
         ("读取寄存器", ["读 d0"]),
         ("写入寄存器", ["写入 d"])],
        {"stop": "PLC 在 STOP 模式时输入采样不生效，先切 RUN"},
        "流程图",
    ),
    "ee_modbus": _t(
        "🔌 Modbus RS-485 通信", "电气自动化", "电气", "PLC>", ElectricSim, "rs485",
        "Modbus RS-485 通信配置",
        "配置一条 485 总线：\n1. 设置波特率 9600\n2. 设置校验方式（偶校验）\n3. 设置从站地址 1\n4. 总线两端接 120Ω 终端电阻\n5. 通信测试读取寄存器",
        ["波特率设为 9600", "校验设为偶", "从站地址设为 1", "接终端电阻 120Ω",
         "通信测试", "读取数据"],
        [("设置波特率", ["波特率"]),
         ("设置校验", ["校验"]),
         ("设置从站地址", ["从站地址"]),
         ("接终端电阻", ["终端电阻"]),
         ("通信测试", ["通信测试", "读取数据"])],
        {"term": "不接终端电阻通信会偶发 CRC 错误，这是最常见的现场故障"},
        "拓扑图",
    ),
    "ee_pcb": _t(
        "🔌 电路板设计与打样", "电气自动化", "电气", "PLC>", ElectricSim, "pcb",
        "PCB 设计全流程",
        "按流程走完 PCB 设计：\n1. 绘制原理图\n2. 执行 ERC 检查\n3. PCB 布局\n4. 布线铺铜\n5. DRC 检查\n6. 导出 Gerber",
        ["原理图设计", "ERC 检查", "PCB 布局", "布线铺铜", "DRC 检查", "导出 Gerber"],
        [("原理图设计", ["原理图"]),
         ("ERC 检查", ["erc"]),
         ("PCB 布局", ["布局"]),
         ("布线铺铜", ["布线", "铺铜"]),
         ("DRC 检查", ["drc"]),
         ("导出 Gerber", ["gerber", "导出"])],
        {"order": "顺序不能跳：没 ERC 不能布局，没 DRC 不能导出 Gerber"},
        "流程图",
    ),
    "ee_power": _t(
        "🔌 开关电源调试", "电气自动化", "电气", "PLC>", ElectricSim, "power_seq",
        "开关电源输出调试",
        "调试 12V/2A 开关电源：\n1. 上电启动\n2. 测量输出电压\n3. 带载 2A\n4. 测量输出电流\n5. 测纹波\n6. 看效率",
        ["上电", "测量输出电压", "带载 2A", "测量输出电流", "测纹波", "效率"],
        [("上电启动", ["上电", "启动电源"]),
         ("测输出电压", ["测量输出电压", "测输出电压", "输出电压"]),
         ("带载", ["带载", "加载"]),
         ("测输出电流", ["测量输出电流", "测输出电流", "输出电流"]),
         ("测纹波", ["纹波"]),
         ("计算效率", ["效率"])],
        {},
        "流程图",
    ),

    # ==================== 7. 数字媒体 ====================
    "md_ae_motion": _t(
        "🎬 AE 动效合成", "数字媒体", "数媒", "AE/PR>", MediaSim, "ae_seq",
        "AE 动效合成",
        "做一个 5 秒片头：\n1. 导入素材\n2. 新建 1080p 合成\n3. 打关键帧做位移动画\n4. 加缓动让运动更自然\n5. 渲染输出",
        ["导入素材", "新建合成 1080p", "打关键帧", "加缓动", "渲染输出"],
        [("导入素材", ["导入素材", "导入"]),
         ("新建合成", ["新建合成"]),
         ("打关键帧", ["关键帧"]),
         ("加缓动", ["缓动", "ease"]),
         ("渲染输出", ["渲染", "导出"])],
        {"ease": "先有关键帧才能加缓动，顺序反了会报没有关键帧"},
        "拓扑图",
    ),
    "md_pr_edit": _t(
        "🎬 PR 视频剪辑", "数字媒体", "数媒", "AE/PR>", MediaSim, "pr_flow",
        "PR 剪辑全流程",
        "完成一条 60 秒短视频：\n1. 建序列\n2. 粗剪取材\n3. 精剪并加转场\n4. 调色\n5. 加字幕\n6. 导出 H.264",
        ["新建序列", "粗剪", "加转场", "调色", "添加字幕", "导出 H.264"],
        [("新建序列", ["新建序列", "建序列"]),
         ("粗剪", ["粗剪", "剪辑"]),
         ("加转场", ["转场"]),
         ("调色", ["调色"]),
         ("加字幕", ["字幕"]),
         ("导出", ["导出"])],
        {"order": "建议顺序：粗剪→精剪→调色→字幕→导出，先导出再调色要重来"},
        "流程图",
    ),
    "md_color": _t(
        "🎬 影视调色", "数字媒体", "数媒", "AE/PR>", MediaSim, "color",
        "影视调色五步法",
        "完成一级 + 二级调色：\n1. 一级校正（曝光、白平衡）\n2. 曲线加对比\n3. 色轮分离冷暖\n4. 套 LUT\n5. 限定器局部修色",
        ["一级校正", "调整曲线", "色轮调整", "套用 LUT", "限定器局部修色"],
        [("一级校正", ["一级校正", "曝光"]),
         ("曲线", ["曲线"]),
         ("色轮", ["色轮"]),
         ("LUT", ["lut"]),
         ("限定器", ["限定器", "局部"])],
        {},
        "时间轴",
    ),
    "md_photo": _t(
        "🎬 摄影布光与曝光", "数字媒体", "数媒", "AE/PR>", MediaSim, "photo",
        "摄影布光与曝光三要素",
        "拍摄一张人像：\n1. 布置三点布光\n2. 设定光圈 f/1.8\n3. 设定快门与 ISO\n4. 校正色温\n5. 看直方图确认曝光\n6. 拍摄",
        ["三点布光", "光圈 f/1.8", "快门 1/125", "ISO 100", "色温 5600K", "看直方图", "拍摄"],
        [("三点布光", ["布光", "主光"]),
         ("设定光圈", ["光圈"]),
         ("设定快门", ["快门"]),
         ("设定 ISO", ["iso"]),
         ("校正色温", ["色温"]),
         ("检查直方图", ["直方图", "iso", "光圈"])],
        {"hist": "直方图是最客观的曝光依据，比看屏幕准"},
        "拓扑图",
    ),
    "md_ui": _t(
        "🎬 UI 视觉设计", "数字媒体", "数媒", "AE/PR>", MediaSim, "ui",
        "UI 视觉规范设计",
        "建立一套设计规范：\n1. 8pt 栅格\n2. 色彩体系\n3. 字体层级\n4. 组件库\n5. 对比度检查",
        ["建立 8pt 栅格", "制定色彩体系", "制定字体层级", "建立组件库", "检查对比度"],
        [("栅格系统", ["栅格", "网格"]),
         ("色彩体系", ["色彩", "配色"]),
         ("字体层级", ["字体", "层级"]),
         ("组件库", ["组件"]),
         ("对比度检查", ["对比度", "无障碍"])],
        {},
        "拓扑图",
    ),

    # ==================== 8. 电子商务 ====================
    "ec_funnel": _t(
        "🛒 转化漏斗分析", "电子商务", "电商", "店铺后台>", BizSim, "funnel",
        "转化漏斗分析",
        "诊断一家店的数据：\n1. 设定 UV 与转化率\n2. 看 GMV\n3. 拆解漏斗各环节\n4. 找出流失最大的环节\n5. 给出优化建议",
        ["UV 设为 10000", "转化率设为 3.6%", "看 GMV", "分析漏斗", "复盘"],
        [("设定 UV", ["uv 设为", "uv设为"]),
         ("设定转化率", ["转化率设为"]),
         ("计算 GMV", ["gmv"]),
         ("拆解漏斗", ["漏斗"]),
         ("经营复盘", ["复盘", "总结"])],
        {},
        "流程图",
    ),
    "ec_pricing": _t(
        "🛒 定价与毛利测算", "电子商务", "电商", "店铺后台>", BizSim, "pricing",
        "定价与毛利测算",
        "给一个商品定价：\n1. 设定成本\n2. 试算售价\n3. 测算毛利率\n4. 加优惠券再看毛利\n5. 算保本销量",
        ["成本设为 45", "售价设为 129", "测算毛利", "优惠券 10%", "保本销量"],
        [("设定成本", ["成本设为"]),
         ("设定售价", ["售价设为"]),
         ("测算毛利", ["毛利", "测算"]),
         ("设置优惠", ["优惠券", "折扣"]),
         ("算保本", ["保本", "盈亏平衡"])],
        {"margin": "毛利率低于 30% 时投流基本必亏，这是新手最常踩的坑"},
        "时间轴",
    ),
    "ec_supply": _t(
        "🛒 供应链与库存", "电子商务", "电商", "店铺后台>", BizSim, "supply",
        "供应链与库存管理",
        "算清库存安全线：\n1. 设定当前库存\n2. 设定日均销量\n3. 算可售天数\n4. 判断断货风险\n5. 给出补货建议",
        ["库存设为 500", "日销设为 30", "库存天数", "风险预警", "补货建议", "复盘"],
        [("设定库存", ["库存设为"]),
         ("设定日销", ["日销设为"]),
         ("算可售天数", ["库存天数", "可售天数"]),
         ("判断风险", ["风险预警", "风险评估", "会不会断货"]),
         ("整体复盘", ["复盘"])],
        {},
        "拓扑图",
    ),
    "ec_marketing": _t(
        "🛒 活动策划与投放", "电子商务", "电商", "店铺后台>", BizSim, "aida",
        "活动策划与投放 ROI",
        "策划一次大促：\n1. 设定投放预算\n2. 看 ROI\n3. 算保本单量\n4. 检查活动方案\n5. 做竞品对比",
        ["投放 5000", "看 ROI", "保本销量", "策划活动", "竞品分析"],
        [("设定预算", ["投放", "预算"]),
         ("计算 ROI", ["roi"]),
         ("算保本单量", ["保本", "盈亏平衡"]),
         ("策划活动", ["活动", "大促"]),
         ("竞品分析", ["竞品"])],
        {},
        "时间轴",
    ),
    "ec_pay": _t(
        "🛒 支付结算与对账", "电子商务", "电商", "店铺后台>", BizSim, "cashflow",
        "支付结算与对账",
        "核对一笔 GMV 的实际到账：\n1. 设定售价与 UV\n2. 算 GMV\n3. 扣平台佣金与手续费\n4. 扣退款\n5. 得到实际到账\n6. 做 ABC 分类",
        ["售价设为 99", "UV 设为 10000", "看 GMV", "对账", "ABC 分类"],
        [("设定售价", ["售价设为"]),
         ("设定 UV", ["uv 设为"]),
         ("计算 GMV", ["gmv"]),
         ("对账结算", ["对账", "结算"]),
         ("ABC 分类", ["abc"])],
        {},
        "时间轴",
    ),
}

# 旧数据兼容：老版本 env 只有 vrp / linux / sql
_LEGACY_MAP = {"vrp": "net_vrp_basic", "linux": "net_linux_basic", "sql": "db_sql_query"}
LEGACY_LABEL = {"vrp": "🌐 VRP网络(ENSP)", "linux": "🐧 Linux(CentOS)", "sql": "🗄️ 数据库(MySQL)"}
LEGACY_PROMPT = {"vrp": "<Huawei>", "linux": "[student@localhost ~]$", "sql": "mysql>"}


def resolve_env(env):
    """把旧 key 映射到新 key；未知 key 返回 None"""
    if not env:
        return None
    if env in TEMPLATES:
        return env
    return _LEGACY_MAP.get(env)


def get_tpl(env):
    key = resolve_env(env)
    return TEMPLATES.get(key) if key else None


def get_label(env):
    tpl = get_tpl(env)
    if tpl:
        return tpl["label"]
    return LEGACY_LABEL.get(env, env or "未分类")


def get_prompt(env):
    tpl = get_tpl(env)
    if tpl:
        return tpl["prompt"]
    return LEGACY_PROMPT.get(env, ">")


def get_sim(env):
    return get_tpl(env)


def get_steps(env):
    tpl = get_tpl(env)
    return (tpl.get("steps") or []) if tpl else []


def get_commands(env):
    tpl = get_tpl(env)
    return (tpl.get("commands") or []) if tpl else []


def get_figure(env):
    tpl = get_tpl(env)
    return (tpl.get("figure") or "") if tpl else ""


def get_hints(env):
    tpl = get_tpl(env)
    return (tpl.get("hints") or {}) if tpl else {}


def new_sim(env):
    """每个学生每个任务一套独立状态"""
    tpl = get_tpl(env)
    if not tpl or not tpl.get("sim"):
        return None
    try:
        return tpl["sim"]()
    except Exception:
        return None


def simulate(sim, cmd):
    if sim is None:
        return False, "该实训环境未配置模拟器"
    try:
        return sim(cmd)
    except Exception as exc:  # 模拟器自身异常不能炸页面
        return False, "模拟器异常：{}".format(exc)


def prompt_of(env, sim):
    """动态提示符：VRP 会跟着视图变，Linux 会跟着目录变"""
    if sim is not None and hasattr(sim, "prompt"):
        try:
            return sim.prompt()
        except Exception:
            pass
    return get_prompt(env)


def template_groups():
    """按方向分组，老师发布任务时先选方向再选模板"""
    groups = {}
    for key, tpl in TEMPLATES.items():
        groups.setdefault(tpl["group"], []).append((key, tpl["label"]))
    return groups


def count_templates():
    return len(TEMPLATES)


# ============================================================
# 建表（只增不删，旧库直接兼容）
# ============================================================
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS teachers (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        create_time TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS classes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT NOT NULL,
        teacher TEXT NOT NULL,
        create_time TEXT,
        UNIQUE(class_name, teacher)
    )""",
    """CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT,
        mermaid_graph TEXT,
        create_time TEXT,
        env TEXT DEFAULT 'vrp',
        class_id INTEGER,
        figure TEXT,
        steps TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student TEXT,
        class_id INTEGER,
        task_id INTEGER,
        command TEXT,
        is_ok INTEGER,
        output TEXT,
        timestamp TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_records_cs ON records(class_id, student)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_class ON tasks(class_id)",
    "CREATE INDEX IF NOT EXISTS idx_classes_teacher ON classes(teacher)",
    # v4 新增：变更信号表（老师一改就 +1，学生端轮询它决定是否刷新）
    """CREATE TABLE IF NOT EXISTS bumps (
        scope TEXT PRIMARY KEY,
        rev INTEGER DEFAULT 0,
        update_time TEXT
    )""",
]


def init_db():
    if st.session_state.get("_db_inited"):
        return
    db_exec_many(SCHEMA)
    # 旧库平滑升级：tasks 表补上 figure / steps 列（已存在则忽略）
    for _col in ("ALTER TABLE tasks ADD COLUMN figure TEXT",
                 "ALTER TABLE tasks ADD COLUMN steps TEXT"):
        try:
            db_exec(_col)
        except DBError:
            pass
    st.session_state["_db_inited"] = True


# ============================================================
# ★ 标准答案：老师写答案，学生敲命令逐条比对
#     答案存成多行文本，一行一条命令（# 开头是注释、空行忽略）
# ============================================================
def parse_answer(text):
    """
    把老师写的答案拆成命令列表。
    规则：一行一条；去掉行号（1. / 1) / 1、）；# 与 // 开头视为注释忽略。
    """
    out = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        line = re.sub(r"^\s*\d+\s*[.、)\]]\s*", "", line)  # 去掉 "1. " 这类编号
        line = line.strip()
        if line:
            out.append(line)
    return out


def _ans_norm(cmd):
    """
    答案比对用的归一化：
      - 小写、压空格、去行尾分号
      - 常见设备缩写展开（g0/0/0 → gigabitethernet0/0/0），
        这样老师写全称、学生写简写也算对
    """
    s = re.sub(r"\s+", " ", (cmd or "").strip().lower())
    s = s.rstrip(";").strip()
    s = re.sub(r"\bgi(?=\d)", "gigabitethernet", s)          # gi0/0/0
    s = re.sub(r"\bg(?=\d)", "gigabitethernet", s)           # g0/0/0
    s = re.sub(r"\beth(?=\d)", "ethernet", s)
    s = re.sub(r"\bge(?=\d)", "gigabitethernet", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def check_answer(cmd, answer_cmds):
    """
    判断学生这条命令是否命中答案里的某一条。
    返回 (状态, 命中的答案序号或 None, 提示文字)
      状态：correct / partial / wrong / empty

    判定规则（从严到宽）：
      1. 归一化后完全一致                     → correct
      2. 学生只多打了结尾的参数（如 save force）→ partial
      3. 其余                                → wrong
    注意：不允许"学生少打词"（display ip interface 不能替代 ...brief），
          因为少参数在设备上往往是另一条命令、会导致错误配置。
    """
    c = _ans_norm(cmd)
    if not c:
        return "empty", None, ""
    if not answer_cmds:
        return "", None, ""

    for i, a in enumerate(answer_cmds):
        if c == _ans_norm(a):
            return "correct", i, ""

    # 带管道过滤（| include / | exclude）视为同一条命令，只在显示层加了过滤
    for i, a in enumerate(answer_cmds):
        na = _ans_norm(a)
        if na and re.split(r"\s*\|\s*", c)[0].strip() == na:
            return "correct", i, ""

    # 只允许"学生比答案更长"（多打了可选参数），且多出的部分是完整词
    for i, a in enumerate(answer_cmds):
        na = _ans_norm(a)
        if na and c.startswith(na + " "):
            extra = c[len(na):].strip()
            if extra and len(extra) <= 16:
                return "partial", i, ""
    return "wrong", None, ""


def answer_hint(cmd, answer_cmds):
    """答错时给的最小提示：第几条、开头几个字符、共多少字"""
    if not answer_cmds:
        return ""
    c = _ans_norm(cmd)
    best, best_score = 0, -1.0
    for i, a in enumerate(answer_cmds):
        na = _ans_norm(a)
        common = 0
        for x, y in zip(c, na):
            if x != y:
                break
            common += 1
        score = common / max(len(na), 1)
        if score > best_score:
            best, best_score = i, score
    a = answer_cmds[best]
    head = a[:max(3, min(14, len(a) // 2))]
    return "提示：对照标准答案第 {} 条，它以「{}…」开头，共 {} 个字符".format(
        best + 1, head, len(a))


def load_answer(tid, env=""):
    """取某任务的标准答案；老师没填则返回空列表"""
    try:
        rows = db_query("SELECT COALESCE(steps,'') FROM tasks WHERE id=?", (tid,))
        return parse_answer(rows[0][0]) if rows else []
    except DBError:
        return []


# ============================================================
# 教师
# ============================================================
def _hash_pwd(pwd):
    return "sha256$" + hashlib.sha256(pwd.encode("utf-8")).hexdigest()


def _check_pwd(pwd, stored):
    if not stored:
        return False
    stored = str(stored)
    if stored.startswith("sha256$"):
        return _hash_pwd(pwd) == stored
    return pwd == stored


def teacher_register(username, pwd):
    try:
        db_exec(
            "INSERT INTO teachers (username, password, create_time) VALUES (?,?,?)",
            (username, _hash_pwd(pwd), datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        bump("struct")
        return True
    except IntegrityError:
        return False


def teacher_login(username, pwd):
    rows, _, _ = db_exec("SELECT password FROM teachers WHERE username=?", (username,))
    if not rows:
        return False
    stored = rows[0][0]
    if not _check_pwd(pwd, stored):
        return False
    if not str(stored).startswith("sha256$"):
        try:
            db_exec("UPDATE teachers SET password=? WHERE username=?", (_hash_pwd(pwd), username))
        except DBError:
            pass
    return True


# ============================================================
# 班级 / 任务 / 记录
# ============================================================
def create_class(class_name, teacher):
    try:
        db_exec(
            "INSERT INTO classes (class_name, teacher, create_time) VALUES (?,?,?)",
            (class_name, teacher, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        bump("struct")
        return True
    except IntegrityError:
        return False


def rename_class(cid, new_name):
    try:
        db_exec("UPDATE classes SET class_name=? WHERE id=?", (new_name, cid))
        bump("struct")
        return True
    except IntegrityError:
        return False


def delete_class(cid):
    db_exec("DELETE FROM tasks WHERE class_id=?", (cid,))
    db_exec("DELETE FROM records WHERE class_id=?", (cid,))
    db_exec("DELETE FROM classes WHERE id=?", (cid,))
    bump("struct")


def get_teacher_classes(teacher):
    rows, _, _ = db_exec("SELECT id, class_name FROM classes WHERE teacher=? ORDER BY id", (teacher,))
    return rows


def get_class_by_id(cid):
    rows, _, _ = db_exec("SELECT id, class_name, teacher FROM classes WHERE id=?", (cid,))
    return rows[0] if rows else None


def get_all_class_ids_names():
    rows, _, _ = db_exec("SELECT id, class_name FROM classes ORDER BY id")
    return rows


def add_task(title, content, graph, env, class_id, figure="", answer=""):
    db_exec(
        """INSERT INTO tasks (title, content, mermaid_graph, create_time, env, class_id, figure, steps)
           VALUES (?,?,?,?,?,?,?,?)""",
        (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env, class_id,
         figure or "", answer or ""),
    )
    bump("struct")


def update_task(tid, title, content, graph, env=None, figure=None, answer=None):
    sql = "UPDATE tasks SET title=?, content=?, mermaid_graph=?"
    args = [title, content, graph]
    if env is not None:
        sql += ", env=?"
        args.append(env)
    if figure is not None:
        sql += ", figure=?"
        args.append(figure)
    if answer is not None:
        sql += ", steps=?"
        args.append(answer)
    sql += " WHERE id=?"
    args.append(tid)
    db_exec(sql, tuple(args))
    bump("struct")


def get_tasks_by_class(cid):
    rows, _, _ = db_exec(
        """SELECT id, title, content, mermaid_graph, create_time, env,
                  COALESCE(figure,''), COALESCE(steps,'')
           FROM tasks WHERE class_id=? ORDER BY id DESC""",
        (cid,),
    )
    return rows


def task_by_id(tid):
    rows, _, _ = db_exec(
        "SELECT id, title, content, mermaid_graph, create_time, env, class_id, COALESCE(figure,'') FROM tasks WHERE id=?", (tid,))
    return rows[0] if rows else None


def del_task(tid):
    db_exec("DELETE FROM tasks WHERE id=?", (tid,))
    bump("struct")


def add_record(student, cid, tid, cmd, ok, out):
    db_exec(
        """INSERT INTO records (student, class_id, task_id, command, is_ok, output, timestamp)
           VALUES (?,?,?,?,?,?,?)""",
        (student, cid, tid, cmd, 1 if ok else 0, out, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )


def get_records_by_class_student(cid, student):
    rows, _, _ = db_exec(
        """SELECT t.title, r.command, r.is_ok, r.output, r.timestamp
           FROM (SELECT id, task_id, command, is_ok, output, timestamp
                 FROM records WHERE student=? AND class_id=?
                 ORDER BY id DESC LIMIT {}) r
           LEFT JOIN tasks t ON r.task_id = t.id
           ORDER BY r.id""".format(RECORD_LIMIT),
        (student, cid),
    )
    return rows


def get_records_by_class_task(cid, tid, student):
    """★ 进度恢复用：某学生某任务的历史命令（旧 → 新）"""
    rows, _, _ = db_exec(
        """SELECT command, is_ok FROM records
           WHERE class_id=? AND task_id=? AND student=? ORDER BY id""",
        (cid, tid, student),
    )
    return rows


def get_students_by_class(cid):
    rows, _, _ = db_exec(
        """SELECT DISTINCT student FROM records
           WHERE class_id=? AND student IS NOT NULL ORDER BY student LIMIT {}""".format(STUDENT_LIMIT),
        (cid,),
    )
    return [r[0] for r in rows]


def get_class_overview(cid):
    """按学生聚合：总步数 / 报错 / 正确率 / 最后活跃"""
    rows, _, _ = db_exec(
        """SELECT student,
                  COUNT(*) AS total,
                  SUM(CASE WHEN is_ok = 0 THEN 1 ELSE 0 END) AS errs,
                  MAX(timestamp) AS last_time
           FROM records WHERE class_id=? AND student IS NOT NULL
           GROUP BY student ORDER BY student""",
        (cid,),
    )
    return rows


def get_task_progress(cid):
    """按任务聚合：参与人数 / 总步数 / 报错数"""
    rows, _, _ = db_exec(
        """SELECT t.id, t.title, COUNT(r.id) AS total,
                  SUM(CASE WHEN r.is_ok = 0 THEN 1 ELSE 0 END) AS errs,
                  COUNT(DISTINCT r.student) AS students
           FROM tasks t LEFT JOIN records r ON r.task_id = t.id
           WHERE t.class_id=? GROUP BY t.id, t.title ORDER BY t.id""",
        (cid,),
    )
    return rows


def clear_student_record(cid, student):
    db_exec("DELETE FROM records WHERE student=? AND class_id=?", (student, cid))


def clear_class_records(cid):
    db_exec("DELETE FROM records WHERE class_id=?", (cid,))


# ============================================================
# ★ 步骤判定：进度从历史命令重算，刷新/换设备都不丢
# ============================================================
def _step_key(env, tid, sn):
    return "_jp_steps_{}_{}_{}".format(tid, sn, env)


def _mark_steps(done, steps, cmd):
    """
    标记某条命令完成了哪些步骤。
    steps 既可以是模板的 [(名称, [关键词...])]，也可以是答案的 [(命令, [命令])]；
    统一按"归一化后包含"判断，答案模式下就是精确匹配。
    """
    s = squash(cmd)
    for i, (_n, kws) in enumerate(steps):
        if not done[i] and any(squash(k) in s for k in kws):
            done[i] = True
    return done


def load_progress(env, tid, scid, sn, answer_cmds=None):
    """
    进度来源：
      有标准答案 → 步骤 = 答案命令，历史命令逐条比对重算
      无标准答案 → 用模板步骤关键词
    """
    answer_cmds = answer_cmds or []
    if answer_cmds:
        steps = answer_steps(answer_cmds)
        key = "_jp_anssteps_{}_{}_{}".format(tid, sn, len(answer_cmds))
    else:
        steps = get_steps(env)
        key = _step_key(env, tid, sn)
    if not steps:
        return []

    cached = st.session_state.get(key)
    if isinstance(cached, list) and len(cached) == len(steps):
        return cached

    done = [False] * len(steps)
    try:
        rows = get_records_by_class_task(scid, tid, sn)
        for cmd, _ok in rows:
            _mark_steps(done, steps, cmd)
    except DBError:
        pass
    st.session_state[key] = done
    return done


def save_progress(env, tid, sn, done, answer_cmds=None):
    if answer_cmds:
        st.session_state["_jp_anssteps_{}_{}_{}".format(tid, sn, len(answer_cmds))] = done
    else:
        st.session_state[_step_key(env, tid, sn)] = done


# ============================================================
# ★ 每个学生的模拟器实例（按 任务+学生 隔离，刷新可重建）
# ============================================================
def _sim_key(tid, sn):
    return "_jp_sim_{}_{}".format(tid, sn)


def get_sim_state(env, tid, sn):
    key = _sim_key(tid, sn)
    sim = st.session_state.get(key)
    if sim is None or getattr(sim, "_env", None) != env:
        sim = new_sim(env)
        if sim is not None:
            try:
                sim._env = env
            except Exception:
                pass
            # 用历史命令把状态重建出来（换设备也能接着用）
            try:
                for cmd, _ok in get_records_by_class_task(
                        st.session_state.get("student_class_id"), tid, sn):
                    try:
                        sim(cmd)
                    except Exception:
                        pass
            except Exception:
                pass
        st.session_state[key] = sim
    return sim


# ============================================================
# 页面样式
# ============================================================
CSS = """
<style>
:root{
  --jp-ink:#1F2A36; --jp-muted:#6B7A8C; --jp-line:#E4EAF2;
  --jp-blue:#2F6FD0; --jp-blue-soft:#E8F0FC;
  --jp-green:#2E9E5B; --jp-green-soft:#E6F5EC;
  --jp-amber:#D98A00; --jp-red:#D93A3A; --jp-purple:#7B5BD6;
}
/* 减少切换时的重排闪烁 */
[data-testid="stAppViewContainer"] > .main { scroll-behavior: auto; }
.jp-fig{
  background:#fff; border:1px solid var(--jp-line); border-radius:12px;
  padding:10px 12px; margin:6px 0 10px;
  box-shadow:0 1px 2px rgba(31,42,54,.04);
}
.jp-fig svg{display:block; width:100%; height:auto;}
.jp-hero{
  background:linear-gradient(120deg,#F2F7FE 0%,#EAF3FF 55%,#F7FBFF 100%);
  border:1px solid var(--jp-line); border-radius:16px; padding:22px 26px; margin-bottom:14px;
}
.jp-hero h1{margin:0; font-size:30px; letter-spacing:1px; color:var(--jp-ink);}
.jp-hero p{margin:6px 0 0; color:var(--jp-muted); font-size:13.5px;}
.jp-badge{
  display:inline-block; padding:2px 9px; border-radius:999px; font-size:11.5px;
  background:var(--jp-blue-soft); color:var(--jp-blue); margin-right:6px; font-weight:600;
}
.jp-badge.g{background:var(--jp-green-soft); color:var(--jp-green);}
.jp-badge.a{background:#FDF3E0; color:var(--jp-amber);}
.jp-metric{
  background:#fff; border:1px solid var(--jp-line); border-radius:12px; padding:12px 14px;
}
.jp-metric b{display:block; font-size:24px; color:var(--jp-ink); line-height:1.2;}
.jp-metric span{font-size:12px; color:var(--jp-muted);}
.jp-step{
  display:flex; align-items:center; gap:9px; padding:6px 2px;
  border-bottom:1px dashed #EDF1F6; font-size:13.5px; color:var(--jp-ink);
}
.jp-step.done{color:var(--jp-green);}
.jp-step .n{
  width:20px; height:20px; border-radius:50%; flex:0 0 20px;
  display:flex; align-items:center; justify-content:center;
  font-size:11px; background:#EDF1F6; color:var(--jp-muted); font-weight:700;
}
.jp-step.done .n{background:var(--jp-green-soft); color:var(--jp-green);}
.jp-tpl{
  border:1px solid var(--jp-line); border-radius:10px; padding:10px 12px; background:#fff;
}
.jp-tpl b{font-size:13.5px; color:var(--jp-ink);}
.jp-tpl span{display:block; font-size:11.5px; color:var(--jp-muted); margin-top:3px;}
div[data-testid="stMetricValue"]{font-size:22px;}
/* 表格紧凑一点，减少滚动 */
div[data-testid="stDataFrame"] table{font-size:12.5px;}
</style>
"""


def inject_css():
    st.markdown(CSS, unsafe_allow_html=True)


def card(title, body, badges=None):
    b = "".join('<span class="jp-badge {}">{}</span>'.format(cls, txt) for txt, cls in (badges or []))
    st.markdown(
        '<div class="jp-metric"><div style="margin-bottom:6px">{}</div>'
        '<b style="font-size:15px">{}</b><span>{}</span></div>'.format(b, esc(title), esc(body)),
        unsafe_allow_html=True,
    )


# ============================================================
# 老师端
# ============================================================
def render_tab_create_class(tea_name, my_classes):
    col_a, col_b = st.columns([2, 3])
    with col_a:
        st.markdown("**新建班级**")
        new_cn = st.text_input("班级名称（例：信安252）", key="new_class_name",
                               label_visibility="collapsed", placeholder="例：信安252 / 电商241")
        if st.button("创建班级", key="btn_create_class", type="primary", use_container_width=True):
            if not new_cn.strip():
                st.warning("班级名不能为空")
            elif create_class(new_cn.strip(), tea_name):
                bust_cache()
                st.success("班级【{}】创建成功".format(new_cn.strip()))
                st.rerun()
            else:
                st.warning("你名下已有同名班级！")
    with col_b:
        st.markdown("**你管理的班级（{} 个）**".format(len(my_classes)))
        if not my_classes:
            st.info("暂无班级，先在左侧创建")
        for cid, cname in my_classes:
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.markdown('<div class="jp-tpl"><b>{}</b><span>班级 ID：{}</span></div>'.format(
                esc(cname), cid), unsafe_allow_html=True)
            if c2.button("改名", key="rn_{}".format(cid)):
                st.session_state["_rename_cid"] = cid
            if c3.button("删除", key="dc_{}".format(cid)):
                st.session_state["_del_cid"] = cid

    if st.session_state.get("_rename_cid"):
        cid = st.session_state["_rename_cid"]
        newn = st.text_input("新的班级名称", key="rn_input")
        cc1, cc2 = st.columns([1, 4])
        if cc1.button("确认改名", key="rn_ok"):
            if newn.strip() and rename_class(cid, newn.strip()):
                bust_cache()
                st.session_state.pop("_rename_cid", None)
                st.rerun()
            else:
                st.warning("名称为空或与已有班级重名")
        if cc2.button("取消", key="rn_cancel"):
            st.session_state.pop("_rename_cid", None)
            st.rerun()

    if st.session_state.get("_del_cid"):
        cid = st.session_state["_del_cid"]
        st.error("⚠️ 删除班级会同时删除该班所有任务和学生记录，不可恢复。确定？")
        cc1, cc2 = st.columns([1, 4])
        if cc1.button("确认删除", key="del_ok", type="primary"):
            delete_class(cid)
            bust_cache()
            st.session_state.pop("_del_cid", None)
            st.rerun()
        if cc2.button("取消", key="del_cancel"):
            st.session_state.pop("_del_cid", None)
            st.rerun()


def render_tab_publish(my_classes):
    if not my_classes:
        st.warning("⚠️ 请先创建班级，才能发布任务")
        return

    options = ["{} | {}".format(x[0], x[1]) for x in my_classes]
    sel = st.selectbox("选择班级发布任务", options, key="pub_class")
    sel_cid = int(sel.split("|")[0].strip())

    st.markdown("---")
    st.markdown("**① 选择实训方向与模板**（{} 个方向 / {} 个实训）".format(
        len(template_groups()), count_templates()))

    groups = template_groups()
    group_names = list(groups.keys())
    g = st.selectbox("实训方向", group_names, key="pub_group")
    items = groups[g]
    labels = [lbl for _, lbl in items]
    idx = st.selectbox("实训模板", labels, key="pub_tpl_{}".format(g))
    tpl_key = items[labels.index(idx)][0]
    tpl = TEMPLATES[tpl_key]

    st.markdown("**② 任务内容**")
    title = st.text_input("任务名称", value=tpl["title"], key="pub_title_{}".format(tpl_key))
    content = st.text_area("任务要求（写给学生看）", value=tpl["content"], height=150,
                           key="pub_content_{}".format(tpl_key))

    st.markdown("---")
    st.markdown("**③ 配图（图库选图可改字，或上传自己的图）**")
    st.caption("流程图 / 拓扑图 / 时序图都能换；图上文字写得不对可以直接替换，"
               "也可以用 PPT / Visio 画好上传。")
    figure = figure_editor(tpl["figure"], key="pub_fig_{}".format(tpl_key), title="任务配图")

    st.markdown("---")
    st.markdown("**④ 标准答案（学生敲命令时按这个判对错）**")
    st.caption("一行一条命令；`#` 开头是注释；可带 `1. ` 编号（会自动去掉）。"
               "留空则学生端只显示步骤、不判对错。")
    default_answer = "\n".join(tpl["commands"]) if tpl.get("commands") else ""
    answer = st.text_area(
        "标准答案", value=default_answer, height=180, key="pub_ans_{}".format(tpl_key),
        label_visibility="collapsed",
        placeholder="system-view\ninterface g0/0/0\nip address 192.168.1.1 255.255.255.0\ndisplay ip interface brief\nsave")

    ans_cmds = parse_answer(answer)
    if ans_cmds:
        st.success("已录入 **{} 条**标准命令".format(len(ans_cmds)))
        with st.expander("👀 查看拆解结果（学生端就按这个逐条比对）", expanded=False):
            for i, c in enumerate(ans_cmds, 1):
                st.markdown("`{}` {}".format(i, c))
    else:
        st.warning("还没有标准答案，学生端将无法判对错")

    with st.expander("🧪 先自己试一遍（不影响学生数据）"):
        st.caption("在下面敲命令，既能看到模拟器输出，也能看到按标准答案判定的结果。")
        if "_sandbox_sim" not in st.session_state or st.session_state.get("_sandbox_env") != tpl_key:
            st.session_state["_sandbox_sim"] = new_sim(tpl_key)
            st.session_state["_sandbox_env"] = tpl_key
            st.session_state["_sandbox_log"] = []
        sb_sim = st.session_state["_sandbox_sim"]
        with st.form(key="sb_form_{}".format(tpl_key), clear_on_submit=True):
            sb_cmd = st.text_input("试一试命令", placeholder="例：system-view", key="sb_cmd")
            sb_go = st.form_submit_button("执行")
        if sb_go and sb_cmd.strip() and sb_sim is not None:
            okk, out = simulate(sb_sim, sb_cmd.strip())
            stt, ai, _h = check_answer(sb_cmd.strip(), ans_cmds)
            st.session_state["_sandbox_log"].append((sb_cmd.strip(), okk, out, stt, ai))
        for c, okk, out, stt, ai in st.session_state["_sandbox_log"][-6:]:
            mark = {"correct": "✅ 与答案第 {} 条一致".format(ai + 1) if ai is not None else "✅ 与答案一致",
                    "partial": "🟡 与答案第 {} 条相近（多了参数）".format(ai + 1) if ai is not None else "🟡 相近",
                    "wrong": "❌ 不在标准答案里"}.get(stt, "🔴 执行失败")
            st.markdown("{} `{}`".format(mark, c))

    if st.button("✅ 发布任务", key="btn_publish", type="primary", use_container_width=True):
        if not title.strip():
            st.error("任务名称不能为空")
        elif not ans_cmds:
            st.error("请先填写标准答案，否则学生端无法判对错")
        else:
            add_task(title.strip(), content, "", tpl_key, sel_cid, figure, answer)
            bust_cache()
            st.success("任务发布成功（{} 条标准答案），学生端会在 {} 秒内自动看到".format(
                len(ans_cmds), PROBE_SECONDS))
            st.rerun()

    st.markdown("---")
    tasks = q_tasks_by_class(sel_cid)
    if not tasks:
        st.info("本班还没有任务")
        return

    st.markdown("**本班已发布任务（{} 个）**".format(len(tasks)))
    for _row in tasks:
        tid, ttitle, tcontent, tgraph, ctime, tenv = _row[:6]
        tfig = _row[6] if len(_row) > 6 else ""
        tans = _row[7] if len(_row) > 7 else ""
        n_ans = len(parse_answer(tans))
        with st.expander("【{}】{}　{}　（答案 {} 条）".format(ctime, ttitle, get_label(tenv), n_ans)):
            st.markdown(tcontent or "")
            _spec = tfig if tfig else (get_figure(tenv) or "")
            render_figure_spec(_spec)
            if n_ans:
                st.markdown("**标准答案**")
                st.code("\n".join(parse_answer(tans)), language="text")
            else:
                st.warning("该任务没有标准答案，学生端不判对错")
            if st.button("🗑 删除任务", key="del_task_{}".format(tid)):
                del_task(tid)
                bust_cache()
                st.rerun()


def render_tab_manage(my_classes):
    if not my_classes:
        st.warning("暂无班级")
        return

    options = ["{} | {}".format(x[0], x[1]) for x in my_classes]
    sel = st.selectbox("管理哪个班级的任务", options, key="mg_class")
    sel_cid = int(sel.split("|")[0].strip())

    tasks = q_tasks_by_class(sel_cid)
    if not tasks:
        st.info("本班还没有任务")
        return

    all_keys = [k for k, _l in sum(template_groups().values(), [])]
    all_labels = [get_label(k) for k in all_keys]

    for _row in tasks:
        tid, ttitle, tcontent, tgraph, ctime, tenv = _row[:6]
        tfig0 = _row[6] if len(_row) > 6 else ""
        tans0 = _row[7] if len(_row) > 7 else ""
        n0 = len(parse_answer(tans0))
        with st.expander("【{}】{}　{}　（答案 {} 条）".format(ctime, ttitle,
                                                         get_label(tenv), n0)):
            nt = st.text_input("任务名称", value=ttitle, key="mg_t_{}".format(tid))
            nc = st.text_area("任务要求", value=tcontent or "", height=130, key="mg_c_{}".format(tid))

            cur_env = resolve_env(tenv) or all_keys[0]
            pick = st.selectbox("实训类型（决定终端模拟器）", all_labels,
                                index=all_keys.index(cur_env) if cur_env in all_keys else 0,
                                key="mg_e_{}".format(tid))

            st.markdown("**配图**")
            mfig = figure_editor(tfig0 or get_figure(cur_env) or "",
                                 key="mg_fig_{}".format(tid), title="任务配图")

            st.markdown("**标准答案**")
            ma = st.text_area("标准答案", value=tans0 or "", height=150,
                              key="mg_a_{}".format(tid), label_visibility="collapsed",
                              placeholder="一行一条命令，学生敲命令时逐条比对")
            nma = len(parse_answer(ma))
            if nma:
                st.caption("共 {} 条标准命令".format(nma))
            else:
                st.caption("⚠️ 空 = 学生端不判对错")

            c1, c2 = st.columns([1, 1])
            with c1:
                if st.button("💾 保存修改", key="mg_save_{}".format(tid), type="primary",
                             use_container_width=True):
                    update_task(tid, nt.strip(), nc, "", all_keys[all_labels.index(pick)],
                                mfig, ma)
                    bust_cache()
                    st.success("已保存，学生端会在 {} 秒内更新".format(PROBE_SECONDS))
                    st.rerun()
            with c2:
                if st.button("🗑 删除任务", key="mg_del_{}".format(tid), use_container_width=True):
                    del_task(tid)
                    bust_cache()
                    st.rerun()


def render_tab_stat(my_classes):
    if not my_classes:
        st.warning("暂无班级")
        return

    options = ["{} | {}".format(x[0], x[1]) for x in my_classes]
    sel = st.selectbox("查看哪个班级学情", options, key="stat_class")
    sel_cid = int(sel.split("|")[0].strip())

    overview = q_class_overview(sel_cid)
    tprog = q_task_progress(sel_cid)

    if not overview:
        st.warning("本班暂无学生实操记录")
        return

    total_cmds = sum(int(r[1] or 0) for r in overview)
    total_err = sum(int(r[2] or 0) for r in overview)
    rate = round((total_cmds - total_err) / total_cmds * 100, 1) if total_cmds else 0.0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("参与学生", len(overview))
    c2.metric("总步数", total_cmds)
    c3.metric("报错数", total_err)
    c4.metric("整体正确率", "{}%".format(rate))

    df = pd.DataFrame(
        [[r[0], int(r[1] or 0), int(r[2] or 0),
          round((int(r[1] or 0) - int(r[2] or 0)) / int(r[1]) * 100, 1) if r[1] else 0.0,
          r[3]] for r in overview],
        columns=["学生", "总步数", "报错数", "正确率(%)", "最后活跃"],
    )
    st.markdown("**学生明细**")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.download_button(
        "📥 导出本班学情 Excel",
        openpyxl_bytes({"学生明细": df}),
        "学情_{}_{}.xlsx".format(sel.split("|")[1].strip(), sel_cid),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_xlsx",
    )

    if tprog:
        st.markdown("**各任务完成情况**")
        tdf = pd.DataFrame(
            [[r[1], int(r[4] or 0), int(r[2] or 0), int(r[3] or 0)] for r in tprog],
            columns=["任务", "参与人数", "总步数", "报错数"],
        )
        st.dataframe(tdf, use_container_width=True, hide_index=True)
        if len(tdf):
            st.bar_chart(tdf.set_index("任务")[["总步数", "报错数"]])

    st.markdown("---")
    students = q_students(sel_cid)
    sel_stu = st.selectbox("查看某个学生的操作记录", students, key="stat_student")
    recs = q_records(sel_cid, sel_stu)
    if recs:
        rdf = pd.DataFrame(recs, columns=["任务", "命令", "成功", "输出", "时间"])
        st.dataframe(rdf, use_container_width=True, hide_index=True, height=300)
        st.download_button(
            "📥 导出该生 CSV",
            rdf.to_csv(index=False).encode("utf-8-sig"),
            "{}_{}.csv".format(sel_stu, sel_cid),
            mime="text/csv",
            key="dl_csv",
        )
    if st.button("🗑 清空该生本班记录", key="btn_clear_rec"):
        clear_student_record(sel_cid, sel_stu)
        bust_cache()
        st.rerun()
    if st.button("🗑 清空本班全部记录", key="btn_clear_all"):
        clear_class_records(sel_cid)
        bust_cache()
        st.rerun()


def openpyxl_bytes(sheets):
    """Excel 导出：多 sheet，带列宽"""
    import io

    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for name, df in sheets.items():
                df.to_excel(writer, sheet_name=name, index=False)
                ws = writer.sheets[name]
                for i, col in enumerate(df.columns):
                    width = max(10, min(40, int(df[col].astype(str).str.len().max() or 10) + 4))
                    ws.column_dimensions[chr(65 + i) if i < 26 else "A"].width = width
    except Exception:
        # 没装 openpyxl 时降级为 CSV
        return sheets[list(sheets.keys())[0]].to_csv(index=False).encode("utf-8-sig")
    return buf.getvalue()


def render_teacher():
    if "teacher_login_ok" not in st.session_state:
        st.session_state.teacher_login_ok = False
        st.session_state.teacher_name = ""

    if not st.session_state.teacher_login_ok:
        st.markdown('<div class="jp-hero"><h1>教师后台</h1>'
                    '<p>登录后可创建班级、发布实训任务、查看学情分析</p></div>',
                    unsafe_allow_html=True)
        tab_login, tab_reg = st.tabs(["🔐 老师登录", "📝 老师注册"])
        with tab_login:
            tname = st.text_input("老师姓名(账号)", key="login_name")
            tpwd = st.text_input("密码", type="password", key="login_pwd")
            if st.button("登录", key="btn_login", type="primary"):
                if not tname.strip():
                    st.warning("请输入账号")
                elif teacher_login(tname.strip(), tpwd):
                    st.session_state.teacher_login_ok = True
                    st.session_state.teacher_name = tname.strip()
                    st.rerun()
                else:
                    st.error("账号或密码错误")
        with tab_reg:
            reg_name = st.text_input("注册姓名（作为登录账号）", key="reg_name")
            reg_pwd1 = st.text_input("设置密码", type="password", key="reg_pwd1")
            reg_pwd2 = st.text_input("确认密码", type="password", key="reg_pwd2")
            if st.button("注册", key="btn_reg"):
                if not reg_name.strip() or not reg_pwd1:
                    st.warning("名字不能为空，密码不能为空")
                elif reg_pwd1 != reg_pwd2:
                    st.warning("两次输入的密码不一致")
                elif teacher_register(reg_name.strip(), reg_pwd1):
                    bust_cache()
                    st.success("注册成功，可以去登录了")
                else:
                    st.warning("该老师账号已存在！")
        return

    tea_name = st.session_state.teacher_name
    st.sidebar.success("✅ 已登录：{}".format(tea_name))
    if st.sidebar.button("退出登录"):
        st.session_state.teacher_login_ok = False
        st.session_state.teacher_name = ""
        st.rerun()

    st.markdown('<div class="jp-hero"><h1>教师后台｜{}</h1>'
                '<p>{} 个专业方向 · {} 个实训 · 每门实训自带配套场景图</p></div>'.format(
                    esc(tea_name), len(template_groups()), count_templates()),
                unsafe_allow_html=True)

    my_classes = q_teacher_classes(tea_name)

    nav = st.radio(
        "模块",
        ["🏫 班级管理", "📤 发布任务", "📝 任务管理", "📊 学情分析"],
        horizontal=True,
        label_visibility="collapsed",
        key="teacher_nav",
    )

    if nav == "🏫 班级管理":
        render_tab_create_class(tea_name, my_classes)
    elif nav == "📤 发布任务":
        render_tab_publish(my_classes)
    elif nav == "📝 任务管理":
        render_tab_manage(my_classes)
    else:
        render_tab_stat(my_classes)


# ============================================================
# 学生端
# ============================================================
def _steps_html(items, done, show_answer=False, sub=None):
    """items: [(显示名, 关键词/命令)]；sub: 每条的附加说明（如提示）"""
    rows = []
    for i, (name, _kw) in enumerate(items):
        cls = "jp-step done" if done[i] else "jp-step"
        mark = "✓" if done[i] else str(i + 1)
        extra = ""
        if sub and i < len(sub) and sub[i]:
            extra = '<div style="color:#6B7A8C;font-size:12.5px;margin-top:2px">{}</div>'.format(
                esc(sub[i]))
        rows.append('<div class="{}"><span class="n">{}</span><span>{}'
                    '<span style="display:block">{}</span></span></div>'.format(
                        cls, mark, esc(name), extra))
    return "".join(rows)


def answer_steps(answer_cmds):
    """把老师的答案命令列表转成步骤条目（显示名 = 命令本身）"""
    return [(c, [c]) for c in answer_cmds]


def _step_panel(env, tid, sn, done, answer_cmds=None):
    answer_cmds = answer_cmds or []
    if answer_cmds:
        # 老师给了标准答案 → 步骤就是答案里的命令，做一条勾一条
        items = answer_steps(answer_cmds)
        st.markdown("**🎯 实训步骤（共 {}）**".format(len(items)))
        finished = sum(1 for d in done if d)
        st.progress(finished / len(items) if items else 0.0,
                    text="完成度 {}/{}".format(finished, len(items)))
        st.caption("照着老师给的要求，一条条敲；敲对了自动打勾")
        st.markdown(_steps_html(items, done), unsafe_allow_html=True)
        if items and finished == len(items):
            st.success("🎉 全部命令都正确，本次实训通过！")
        return

    steps = get_steps(env)
    if not steps:
        st.info("老师没有给标准答案，随便敲命令练手即可，不计分。")
        return
    finished = sum(1 for d in done if d)
    total = len(steps)
    st.markdown("**🎯 实训步骤**")
    st.progress(finished / total if total else 0.0,
                text="完成度 {}/{}".format(finished, total))
    st.markdown(_steps_html(steps, done), unsafe_allow_html=True)
    if finished == total:
        st.success("🎉 全部步骤已完成，本次实训通过！")


def _frag(every=None):
    """st.fragment 兼容包装：老版本 Streamlit 直接返回原函数"""
    def wrap(f):
        deco = getattr(st, "fragment", None)
        if deco is None:
            return f
        try:
            if every:
                return deco(run_every=every)(f)
            return deco(f)
        except Exception:
            return f
    return wrap


def login_gate():
    st.markdown('<div class="jp-hero"><h1>学生实操练习</h1>'
                '<p>选择班级、填上姓名即可开始，无需注册。进度会自动保存，换电脑也不丢。</p></div>',
                unsafe_allow_html=True)
    try:
        all_classes = q_all_classes()
    except DBError as exc:
        st.error("无法读取班级列表：{}".format(exc))
        return
    if not all_classes:
        st.info("⏳ 老师还没有创建班级，请稍候或联系任课教师。")
        if st.button("刷新试试"):
            bust_cache()
            st.rerun()
        return

    options = ["{} | {}".format(cid, cname) for cid, cname in all_classes]
    sel = st.selectbox("选择你的班级", options, key="stu_class")
    sel_cid = int(sel.split("|")[0].strip())
    name = st.text_input("你的姓名", placeholder="输入真实姓名，老师按这个名字看学情",
                         key="stu_name_input")
    if st.button("进入练习", type="primary", key="btn_enter"):
        if not name.strip():
            st.warning("请输入姓名")
        else:
            st.session_state.student_name = name.strip()
            st.session_state.student_class_id = sel_cid
            st.rerun()


def _rerun_frag():
    """
    兼容 helper：
      - fragment 定时重跑期间 → 只重跑这一小块（不闪屏）
      - 首次整页渲染时 → scope 参数不合法，退化为整页 rerun
    """
    try:
        st.rerun(scope="fragment")
    except Exception:
        st.rerun()


def _watch_tasks_logic(cid):
    """
    轮询逻辑（普通函数，可被 fragment 内直接调用）：
      查一次极轻指纹 → 变了才清缓存 → 新任务自动出现在下拉框
    页面其余部分完全不动，所以不会闪、不会卡。
    """
    try:
        fp = class_fingerprint(cid)
    except DBError:
        return
    last = st.session_state.get("_fp_{}".format(cid))
    if last != fp:
        st.session_state["_fp_{}".format(cid)] = fp
        bust_cache()
        if last is not None:
            st.toast("📬 老师更新了任务列表", icon="📬")
            _rerun_frag()


@_frag(every=PROBE_SECONDS)
def watch_tasks(cid, current_tid):
    _watch_tasks_logic(cid)


@_frag(every=PROBE_SECONDS)
def task_area(cid, sn):
    """任务选择 + 任务详情（局部刷新，切换任务时不会整页重跑）"""
    # 注意：不能在 fragment 里直接调用另一个 fragment 函数，
    # 这里调用普通逻辑函数完成同样的轮询检查
    _watch_tasks_logic(cid)

    try:
        tasks = get_tasks_by_class(cid)
    except DBError as exc:
        st.error("读取任务失败：{}".format(exc))
        return

    if not tasks:
        st.info("⏳ 本班暂无任务，等老师发布后这里会自动出现（每 {} 秒检测一次）".format(PROBE_SECONDS))
        return

    options = {}
    for row in tasks:
        tid, title, content, graph, ctime, env = row[:6]
        options["{} {}　·　{}　#{}".format(get_label(env), title, ctime, tid)] = row

    keys = list(options.keys())
    prev = st.session_state.get("_cur_task_{}".format(sn))
    idx = 0
    if prev in keys:
        idx = keys.index(prev)

    sel = st.selectbox("选择要做的任务（共 {} 个）".format(len(keys)), keys, index=idx,
                       key="stu_task_{}".format(sn))
    st.session_state["_cur_task_{}".format(sn)] = sel
    row = options[sel]
    tid, title, content, graph, ctime, env = row[:6]
    forced_fig = row[6] if len(row) > 6 else ""
    answer_raw = row[7] if len(row) > 7 else ""
    answer_cmds = parse_answer(answer_raw)

    tpl = get_tpl(env)
    st.markdown('<div class="jp-hero"><h1 style="font-size:22px">{}</h1>'
                '<p><span class="jp-badge">{}</span>发布：{}　·　任务 #{}</p></div>'.format(
                    esc(title), esc(get_label(env)), esc(ctime or ""), tid),
                unsafe_allow_html=True)

    st.markdown(content or "")

    # 配图：老师选的（含改字图 / 上传图），没有就退回模板自带图
    fig_spec = forced_fig if forced_fig else (get_figure(env) or "")
    if fig_spec:
        st.markdown("**🗺️ 实训示意图**")
        render_figure_spec(fig_spec)
    elif graph and graph.strip():
        st.code(graph)

    st.markdown("---")
    work_area(env, tid, sn, cid, tpl, answer_cmds)


def work_area(env, tid, sn, cid, tpl, answer_cmds=None):
    """终端 + 步骤 + 记录；老师填了标准答案就逐条判对错"""
    answer_cmds = answer_cmds or []
    done = load_progress(env, tid, cid, sn, answer_cmds)
    sim = get_sim_state(env, tid, sn)

    left, right = st.columns([3, 2])
    with right:
        _step_panel(env, tid, sn, done, answer_cmds)

    with left:
        st.markdown("**💻 终端**")
        if sim is None:
            st.warning("该实训环境暂不可用（可能是老师用了已下线的实训类型）")
            return

        hints = get_hints(env)
        if hints:
            with st.expander("💡 常见坑提示"):
                for k, v in hints.items():
                    st.markdown("- {}".format(v))

        prompt = prompt_of(env, sim)
        with st.form(key="cmd_form_{}_{}".format(tid, sn), clear_on_submit=True):
            cmd = st.text_input(prompt, key="cmd_input_{}".format(tid),
                                placeholder="在这里输入命令，回车判对错")
            submitted = st.form_submit_button("执行命令", type="primary")

        if submitted:
            if not cmd.strip():
                st.warning("请输入命令")
            else:
                c = cmd.strip()
                okk, out = simulate(sim, c)
                try:
                    add_record(sn, cid, tid, c, okk, out)
                except DBError as exc:
                    st.error("记录保存失败：{}".format(exc))
                # 按标准答案判定
                stt, ai, _ = check_answer(c, answer_cmds)
                if stt == "correct":
                    _mark_steps(done, answer_steps(answer_cmds), c)
                save_progress(env, tid, sn, done, answer_cmds)
                st.session_state["_last_{}".format(tid)] = {
                    "cmd": c, "ok": okk, "out": out, "prompt": prompt,
                    "verdict": stt, "ans_idx": ai,
                    "hint": answer_hint(c, answer_cmds) if stt == "wrong" else "",
                }
                _rerun_frag()

        last = st.session_state.get("_last_{}".format(tid))
        if last:
            st.code("{} {}".format(last.get("prompt", prompt), last["cmd"]), language="text")

            v = last.get("verdict", "")
            if not answer_cmds:
                # 老师没填答案：只显示模拟器输出
                if last["ok"]:
                    st.success(last["out"] or "OK")
                else:
                    st.error(last["out"])
            elif v == "correct":
                st.success("✅ **正确**　和标准答案第 {} 条完全一致".format((last.get("ans_idx") or 0) + 1))
                if last["out"]:
                    st.code(last["out"], language="text")
            elif v == "partial":
                st.info("🟡 **基本正确**　接近标准答案第 {} 条（命令多了些参数，也算通过）".format(
                    (last.get("ans_idx") or 0) + 1))
                if last["out"]:
                    st.code(last["out"], language="text")
            elif v == "wrong":
                st.error("❌ **不对**　这条命令不在老师给的标准答案里")
                if last.get("hint"):
                    st.caption(last["hint"])
                if last["out"]:
                    st.code(last["out"], language="text")
            else:
                if last["ok"]:
                    st.success(last["out"] or "OK")
                else:
                    st.error(last["out"])

    st.markdown("---")
    with st.expander("📝 我的操作记录（本班，最近 {} 条）".format(RECORD_LIMIT)):
        try:
            recs = q_records(cid, sn)
        except DBError:
            recs = []
        if recs:
            for tname, rcmd, okk, out, ts in recs[-60:]:
                st.markdown("{} `{}` 【{}】`{}`".format("🟢" if okk else "🔴", ts,
                                                       tname or "已删除任务", rcmd))
            if st.button("清空我本班记录", key="btn_clear_my"):
                clear_student_record(cid, sn)
                for k in [k for k in st.session_state if k.startswith("_jp_steps_")]:
                    st.session_state.pop(k, None)
                for k in [k for k in st.session_state if k.startswith("_jp_sim_")]:
                    st.session_state.pop(k, None)
                bust_cache()
                st.rerun()
        else:
            st.caption("暂无记录")


def render_student():
    if "student_name" not in st.session_state:
        st.session_state.student_name = ""
    if "student_class_id" not in st.session_state:
        st.session_state.student_class_id = None

    if not st.session_state.student_name or st.session_state.student_class_id is None:
        login_gate()
        return

    sn = st.session_state.student_name
    scid = st.session_state.student_class_id

    try:
        cls_row = q_class_by_id(scid)
    except DBError as exc:
        st.error("读取班级失败：{}".format(exc))
        return
    cls_name = cls_row[1] if cls_row else "未知班级"

    col1, col2 = st.columns([5, 1])
    col1.markdown('<div style="padding-top:6px"><span class="jp-badge g">{}</span>'
                  '<span class="jp-badge">{}</span>'
                  '<span style="color:#6B7A8C;font-size:12.5px">进度自动保存，刷新不丢</span></div>'.format(
                      esc(cls_name), esc(sn)), unsafe_allow_html=True)
    if col2.button("🔴 退出", key="btn_exit"):
        st.session_state.student_name = ""
        st.session_state.student_class_id = None
        st.rerun()

    task_area(scid, sn)


# ============================================================
# 入口
# ============================================================
def main():
    st.set_page_config(page_title="迹评 · EVERY STEP COUNTS", page_icon="🌊",
                       layout="wide", initial_sidebar_state="expanded")
    inject_css()

    try:
        init_db()
    except (DBError, IntegrityError) as exc:
        st.markdown('<div class="jp-hero"><h1>🌊 迹评</h1>'
                    '<p>数据库未配置或连接失败</p></div>', unsafe_allow_html=True)
        st.error(str(exc))
        st.markdown("**配置方法**：\n"
                    "1. Streamlit Cloud → 你的 App → Settings → Secrets\n"
                    "2. 粘贴两行：`TURSO_URL = \"libsql://xxx.turso.io\"` 和 "
                    "`TURSO_AUTH_TOKEN = \"eyJ...\"`\n"
                    "3. 保存后 App 会自动重启")
        st.stop()

    if "entered" not in st.session_state:
        st.session_state.entered = False

    if not st.session_state.entered:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<div class="jp-hero" style="text-align:center;padding:48px 26px">'
                    '<h1 style="font-size:56px;letter-spacing:6px">🌊 迹评</h1>'
                    '<p style="font-size:15px;letter-spacing:3px;margin-top:10px">EVERY STEP COUNTS</p>'
                    '<p style="margin-top:18px">{} 个专业方向 · {} 个实训 · 每一步都有迹可循</p>'
                    '</div>'.format(len(template_groups()), count_templates()),
                    unsafe_allow_html=True)
        _, mid, _ = st.columns([1, 1, 1])
        with mid:
            if st.button("进 入", use_container_width=True, type="primary"):
                st.session_state.entered = True
                st.rerun()
        st.stop()

    st.sidebar.title("迹评 EVERY STEP COUNTS")
    role = st.sidebar.radio("选择身份", ["学生端", "老师端"], key="role_switch")
    st.sidebar.markdown("---")
    groups = template_groups()
    st.sidebar.caption("实训方向 · {} 个 / 实训 · {} 个".format(len(groups), count_templates()))
    for gname, items in groups.items():
        st.sidebar.markdown('<div style="font-size:12px;color:#6B7A8C;padding:2px 0">'
                            '{} <span style="color:#9AA9BA">（{}）</span></div>'.format(
                                esc(gname), len(items)), unsafe_allow_html=True)

    try:
        if role == "老师端":
            render_teacher()
        else:
            render_student()
    except DBError as exc:
        st.error("数据库出错：{}".format(exc))
    except IntegrityError as exc:
        st.error("数据冲突：{}".format(exc))


main()
