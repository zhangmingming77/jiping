# -*- coding: utf-8 -*-
import streamlit as st
import streamlit.components.v1 as components
import sqlite3
from datetime import datetime
import pandas as pd
import html as _html
import os as _os

_MERMAID_JS_CACHE = ""

def _load_mermaid_js():
    global _MERMAID_JS_CACHE
    if _MERMAID_JS_CACHE: return _MERMAID_JS_CACHE
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "mermaid.min.js")
    try:
        with open(p, "r", encoding="utf-8") as f: _MERMAID_JS_CACHE = f.read()
    except: _MERMAID_JS_CACHE = ""
    return _MERMAID_JS_CACHE

def render_mermaid(code, height=420):
    if not code or not code.strip(): return
    escaped = _html.escape(code); js = _load_mermaid_js()
    components.html(f"""<div style="background:#fff;padding:14px;border-radius:10px;">
<pre class="mermaid" style="font-size:14px;">{escaped}</pre></div>
<script>{js}</script>
<script>try{{mermaid.initialize({{startOnLoad:true,theme:'default',securityLevel:'loose'}});}}catch(e){{}}</script>""", height=height)

def _ensure_column(c, table, col, ddl):
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})")]
    if col not in cols: c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

def init_db():
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, content TEXT, mermaid_graph TEXT, create_time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT, student TEXT, task_id INTEGER, command TEXT, is_ok INTEGER, output TEXT, timestamp TEXT, FOREIGN KEY(task_id) REFERENCES tasks(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS online (student TEXT PRIMARY KEY, last_seen TEXT)''')
    _ensure_column(c, "tasks", "env", "TEXT DEFAULT 'vrp'")
    conn.commit(); conn.close()

def touch_online(student):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO online (student, last_seen) VALUES (?,?)", (student, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit(); conn.close()

def get_online_students():
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("SELECT student, last_seen FROM online"); rows = c.fetchall(); conn.close()
    online = []
    for name, ts in rows:
        try:
            diff = (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
            if diff < 300: online.append((name, ts))
        except: pass
    return online

def add_task(title, content, graph, env="vrp"):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("INSERT INTO tasks (title, content, mermaid_graph, create_time, env) VALUES (?,?,?,?,?)", (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env))
    conn.commit(); conn.close()

def get_all_tasks():
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("SELECT id, title, content, mermaid_graph, create_time, env FROM tasks ORDER BY id DESC")
    rows = c.fetchall(); conn.close(); return rows

def del_task(tid):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("DELETE FROM tasks WHERE id=?", (tid,)); conn.commit(); conn.close()

def add_record(student, tid, cmd, ok, out):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("INSERT INTO records (student, task_id, command, is_ok, output, timestamp) VALUES (?,?,?,?,?,?)", (student, tid, 1 if ok else 0, out, datetime.now().strftime("%H:%M:%S")))
    conn.commit(); conn.close()

def get_records(student=None):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    if student:
        c.execute("SELECT t.title, r.command, r.is_ok, r.output, r.timestamp FROM records r LEFT JOIN tasks t ON r.task_id=t.id WHERE r.student=? ORDER BY r.id", (student,))
    else:
        c.execute("SELECT r.student, t.title, r.command, r.is_ok, r.output, r.timestamp FROM records r LEFT JOIN tasks t ON r.task_id=t.id ORDER BY r.id")
    rows = c.fetchall(); conn.close(); return rows

def get_students():
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("SELECT DISTINCT student FROM records"); rows = c.fetchall(); conn.close()
    return [r[0] for r in rows]

def clear_student_record(s):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("DELETE FROM records WHERE student=?", (s,)); conn.commit(); conn.close()

ENV_LABEL = {"vrp": "🌐 VRP网络", "linux": "🐧 Linux", "sql": "🗄️ 数据库"}
ENV_PROMPT = {"vrp": "<Huawei>", "linux": "[student@localhost ~]$", "sql": "mysql>"}
ENV_ERROR = {"vrp": "Error:Unrecognized command.", "linux": "-bash: command not found", "sql": "ERROR 1064: SQL syntax error"}

def vrp_sim(cmd):
    m = {"system-view": "Enter system view", "sys": "Enter system view", "q": "Return to user view",
        "display ip interface brief": "GE0/0/0  192.168.1.1/24  up  up", "display vlan": "VLAN1  GE0/0/1\nVLAN10 GE0/0/0",
        "vlan 10": "VLAN 10 created", "interface g0/0/0": "GigabitEthernet0/0/0",
        "ip address 192.168.1.1 255.255.255.0": "IP configured", "ospf 1 router-id 1.1.1.1": "OSPF started",
        "area 0": "Enter Area 0", "network 192.168.1.0 0.0.0.255": "Network added",
        "display ospf peer": "Router ID: 2.2.2.2  State: Full", "vrrp vrid 1 virtual-ip 192.168.1.254": "VRRP group created",
        "save": "Save OK"}
    k = cmd.strip().lower()
    return (True, m[k]) if k in m else (False, ENV_ERROR["vrp"])

def linux_sim(cmd):
    cl = cmd.strip().lower()
    s = cl[5:].strip() if cl.startswith("sudo ") else cl
    m = {"pwd": "/home/student", "whoami": "student", "ls": "Desktop Documents Downloads",
        "cat /etc/os-release": 'CentOS Linux 7 (Core)', "ip addr": "ens33: 192.168.1.10/24",
        "ping 192.168.1.1 -c 3": "3 received, 0% loss", "firewall-cmd --state": "running",
        "systemctl status network": "active", "netstat -tlnp": "0.0.0.0:22 sshd"}
    return (True, m[s]) if s in m else (False, ENV_ERROR["linux"])

def sql_sim(cmd):
    k = cmd.strip().lower().rstrip(";").strip()
    m = {"show databases": "test", "use test": "Database changed", "show tables": "students",
        "select * from students": "1 张三 19\n2 李四 20", "desc students": "id int\nname varchar(20)", "exit": "Bye"}
    return (True, m[k]) if k in m else (False, ENV_ERROR["sql"])

def simulate(env, cmd):
    if env == "linux": return linux_sim(cmd)
    if env == "sql": return sql_sim(cmd)
    return vrp_sim(cmd)

DEFAULT_CONTENT = {
    "vrp": "1. system-view\n2. interface g0/0/0\n3. ip address 192.168.1.1 255.255.255.0\n4. ospf 1 router-id 1.1.1.1",
    "linux": "1. cat /etc/os-release\n2. ip addr\n3. ping 192.168.1.1 -c 3\n4. firewall-cmd --state",
    "sql": "1. show databases;\n2. use test;\n3. show tables;\n4. select * from students;"}
DEFAULT_MERMAID = "graph LR\n    R1[R1] --- GE0/0/0\n    GE0/0/0 --- LINK{{链路}}\n    LINK --- GE0/0/1\n    GE0/0/1 --- R2[R2]\n    style R1 fill:#87CEFA"

st.set_page_config(page_title="迹评", page_icon="🌊", layout="wide")
init_db()

if "entered" not in st.session_state: st.session_state.entered = False
if not st.session_state.entered:
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align:center;font-size:60px;'>🌊 迹评</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align:center;color:#888;'>EVERY STEP COUNTS</h3>", unsafe_allow_html=True)
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, c2, _ = st.columns([1,1,1])
    with c2:
        if st.button("进 入", use_container_width=True, type="primary"):
            st.session_state.entered = True; st.rerun()
    st.stop()

st.sidebar.title("迹评 EVERY STEP COUNTS")
role = st.sidebar.radio("选择身份", ["学生端", "老师端"])
if role == "老师端":
    pwd = st.sidebar.text_input("老师密码", type="password")
    if pwd != "123456": st.sidebar.warning("密码错误"); st.stop()

if role == "学生端":
    st.title("学生实操练习")
    if "student_name" not in st.session_state or not st.session_state.student_name:
        st.subheader("请输入姓名")
        ni = st.text_input("姓名", placeholder="输入你的名字")
        if st.button("进入", type="primary"):
            if ni.strip(): st.session_state.student_name = ni.strip(); st.rerun()
            else: st.warning("请输入姓名")
        st.stop()
    sn = st.session_state.student_name
    touch_online(sn)
    st.caption(f"当前学生：{sn}")
    if st.button("切换学生"): del st.session_state.student_name; st.rerun()
    tasks = get_all_tasks()
    if not tasks: st.warning("暂无任务")
    else:
        opts = {f"[{ENV_LABEL.get(r[5],r[5])}] {r[1]}": r for r in tasks}
        sel = st.selectbox("选择任务", list(opts.keys()))
        row = opts[sel]; tid, title, content, graph, ctime, env = row
        st.subheader(f"📋 {title}  `{ENV_LABEL.get(env,env)}`")
        st.markdown(content)
        if env=="vrp" and graph and graph.strip(): render_mermaid(graph)
        st.markdown("---"); st.subheader(f"💻 终端")
        cmd = st.text_input(ENV_PROMPT.get(env,">"), key="tc")
        if cmd.strip():
            ok, out = simulate(env, cmd); add_record(sn, tid, cmd, ok, out)
            st.code(f"{ENV_PROMPT.get(env,'>')} {cmd}", language="bash")
            st.success(out if out else "OK") if ok else st.error(out)
        st.markdown("---"); st.subheader("📝 我的记录")
        recs = get_records(sn)
        if recs:
            for tn, c2, ok, out, ts in recs:
                st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】`{c2}`"); st.code(out)
            if st.button("清空我的记录"): clear_student_record(sn); st.rerun()
        else: st.info("暂无记录")
else:
    st.title("老师管理后台")
    t1, t2 = st.tabs(["📤 发布任务", "📊 学情查看"])
    with t1:
        env = st.selectbox("环境", ["vrp","linux","sql"], format_func=lambda x: ENV_LABEL[x])
        tt = st.text_input("任务名称", key=f"t_{env}")
        tc = st.text_area("任务要求", value=DEFAULT_CONTENT[env], height=150, key=f"c_{env}")
        mg = ""
        if env=="vrp": mg = st.text_area("Mermaid拓扑", value=DEFAULT_MERMAID, height=150, key=f"m_{env}")
        if st.button("✅ 发布"):
            if tt.strip(): add_task(tt.strip(), tc, mg.strip(), env); st.success("成功"); st.rerun()
            else: st.error("名称不能为空")
        st.markdown("---")
        for tid, title, content, graph, ctime, tenv in get_all_tasks():
            with st.expander(f"【{ctime}】{ENV_LABEL.get(tenv,tenv)} {title}"):
                st.markdown(content)
                if tenv=="vrp" and graph: render_mermaid(graph)
                if st.button("🗑 删除", key=f"d{tid}"): del_task(tid); st.rerun()
    with t2:
        on = get_online_students()
        if on: st.success(f"🟢 在线 {len(on)} 人：{', '.join(n for n,_ in on)}")
        else: st.info("⚪ 暂无学生在线")
        st.subheader("学生记录")
        students = get_students()
        if not students: st.warning("暂无学生")
        else:
            stu = st.selectbox("选择学生", students)
            recs = get_records(stu)
            total = len(recs); err = sum(1 for r in recs if r[2]==0)
            rate = round((total-err)/total*100,1) if total>0 else 0
            a,b,c = st.columns(3)
            a.metric("总步数", total); b.metric("报错", err); c.metric("正确率", f"{rate}%")
            st.markdown("---")
            for tn, cmd, ok, out, ts in recs:
                st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】`{cmd}`"); st.code(out)
            df = pd.DataFrame(recs, columns=["任务","命令","成功","输出","时间"])
            st.download_button("📥导出CSV", df.to_csv(index=False,encoding="utf-8-sig"), f"{stu}.csv")
            if st.button("🗑清空"): clear_student_record(stu); st.rerun()
