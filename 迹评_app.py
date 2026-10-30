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
    if _MERMAID_JS_CACHE:
        return _MERMAID_JS_CACHE
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "mermaid.min.js")
    try:
        with open(p, "r", encoding="utf-8") as f:
            _MERMAID_JS_CACHE = f.read()
    except Exception:
        _MERMAID_JS_CACHE = ""
    return _MERMAID_JS_CACHE

def render_mermaid(code: str, height: int = 420):
    if not code or not code.strip():
        return
    escaped = _html.escape(code)
    js = _load_mermaid_js()
    components.html(
        f"""<div style="background:#ffffff;padding:14px;border-radius:10px;">
        <pre class="mermaid" style="font-size:14px;">{escaped}</pre></div>
        <script>{js}</script>
        <script>try{{mermaid.initialize({{startOnLoad:true,theme:'default',securityLevel:'loose'}});}}catch(e){{}}</script>""",
        height=height)

def _ensure_column(c, table, col, ddl):
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

def init_db():
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, content TEXT, mermaid_graph TEXT, create_time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT, student TEXT, task_id INTEGER, command TEXT, is_ok INTEGER, output TEXT, timestamp TEXT, FOREIGN KEY(task_id) REFERENCES tasks(id))''')
    _ensure_column(c, "tasks", "env", "TEXT DEFAULT 'vrp'")
    conn.commit(); conn.close()

def add_task(title, content, graph, env="vrp"):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("INSERT INTO tasks (title, content, mermaid_graph, create_time, env) VALUES (?,?,?,?,?)",
              (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env))
    conn.commit(); conn.close()

def get_all_tasks():
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("SELECT id, title, content, mermaid_graph, create_time, env FROM tasks ORDER BY id DESC")
    rows = c.fetchall(); conn.close(); return rows

def del_task(task_id):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("DELETE FROM tasks WHERE id=?", (task_id,)); conn.commit(); conn.close()

def add_record(student, task_id, cmd, is_ok, out):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("INSERT INTO records (student, task_id, command, is_ok, output, timestamp) VALUES (?,?,?,?,?,?)",
              (student, task_id, 1 if is_ok else 0, out, datetime.now().strftime("%H:%M:%S")))
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

def clear_student_record(student):
    conn = sqlite3.connect("jiping.db"); c = conn.cursor()
    c.execute("DELETE FROM records WHERE student=?", (student,)); conn.commit(); conn.close()

ENV_LABEL = {"vrp": "🌐 VRP网络(ENSP)", "linux": "🐧 Linux(CentOS)", "sql": "🗄️ 数据库(MySQL)"}
ENV_PROMPT = {"vrp": "<Huawei>", "linux": "[student@localhost ~]$", "sql": "mysql>"}
ENV_ERROR = {"vrp": "Error:Unrecognized command found at '^' position.", "linux": "-bash: 未找到该命令，请检查拼写", "sql": "ERROR 1064 (42000): 你的SQL语法有错误，请检查手册"}

def vrp_simulator(cmd):
    cmd_low = cmd.strip().lower()
    m = {"sys": "Enter system view, return user view with Ctrl+Z.", "system-view": "Enter system view, return user view with Ctrl+Z.", "q": "Return to user view", "quit": "Return to user view",
        "display ip interface brief": "*down: administratively down\n^down: standby\nInterface    IP/Mask      Physical  Protocol\nGE0/0/0      192.168.1.1/24  up     up\nGE0/0/1      unassigned     down   down",
        "display vlan": "VLAN ID Type  Status  Ports\n1       static active GE0/0/1\n10      static active GE0/0/0",
        "vlan 10": "Info: VLAN 10 has been created.", "interface g0/0/0": "GigabitEthernet0/0/0",
        "interface gigabitethernet 0/0/0": "GigabitEthernet0/0/0",
        "ip address 192.168.1.1 255.255.255.0": "Info: The IP address has been configured successfully.",
        "ospf 1 router-id 1.1.1.1": "Info: OSPF process 1 started.", "area 0": "Enter OSPF area 0 view",
        "network 192.168.1.0 0.0.0.255": "Info: Network added to OSPF area 0.",
        "display ospf peer": "OSPF Process 1 with Router ID 1.1.1.1\n Area 0.0.0.0\n Router ID: 2.2.2.2  State: Full",
        "vrrp vrid 1 virtual-ip 192.168.1.254": "Info: VRRP group 1 created.",
        "save": "Info: Save configuration successfully."}
    return (True, m[cmd_low]) if cmd_low in m else (False, ENV_ERROR["vrp"])

def linux_simulator(cmd):
    raw = cmd.strip(); cl = raw.lower().rstrip("/").strip()
    s = cl[5:].strip() if cl.startswith("sudo ") else cl
    m = {"pwd": "/home/student", "whoami": "student", "hostname": "localhost.localdomain",
        "ls": "Desktop Documents Downloads Music Pictures Public Templates Videos",
        "cat /etc/os-release": 'NAME="CentOS Linux"\nVERSION="7 (Core)"\nPRETTY_NAME="CentOS Linux 7 (Core)"',
        "ip addr": "ens33: inet 192.168.1.10/24\nlo: inet 127.0.0.1/8",
        "ip a": "ens33: inet 192.168.1.10/24\nlo: inet 127.0.0.1/8",
        "ping 192.168.1.1 -c 3": "PING 192.168.1.1\n3 packets transmitted, 3 received, 0% packet loss",
        "systemctl status network": "Active: active (exited)", "firewall-cmd --state": "running",
        "netstat -tlnp": "tcp  0.0.0.0:22  LISTEN  sshd", "yum install -y vim": "Complete!"}
    return (True, m[s]) if s in m else (False, ENV_ERROR["linux"])

def sql_simulator(cmd):
    key = cmd.strip().lower().rstrip(";").strip()
    m = {"show databases": "+--------------------+\n| Database |\n+--------------------+\n| test |\n+--------------------+",
        "use test": "Database changed", "show tables": "+----------------+\n| Tables_in_test |\n+----------------+\n| students |\n+----------------+",
        "select * from students": "+----+--------+------+\n| id | name | age |\n+----+--------+------+\n| 1 | 张三 | 19 |\n| 2 | 李四 | 20 |\n+----+--------+------+",
        "desc students": "+-------+-------------+\n| Field | Type |\n+-------+-------------+\n| id | int |\n| name | varchar(20) |\n+-------+-------------+",
        "exit": "Bye"}
    return (True, m[key]) if key in m else (False, ENV_ERROR["sql"])

def simulate(env, cmd):
    if env == "linux": return linux_simulator(cmd)
    if env == "sql": return sql_simulator(cmd)
    return vrp_simulator(cmd)

DEFAULT_CONTENT = {
    "vrp": "1. 进入系统视图\n2. 配置IP\n3. 启动OSPF\n4. 宣告网段",
    "linux": "1. cat /etc/os-release\n2. ip addr\n3. ping 192.168.1.1 -c 3\n4. firewall-cmd --state",
    "sql": "1. show databases;\n2. use test;\n3. show tables;\n4. select * from students;"}
DEFAULT_MERMAID = "graph LR\n    R1[R1] --- GE0/0/0\n    GE0/0/0 --- LINK{{链路}}\n    LINK --- GE0/0/1\n    GE0/0/1 --- R2[R2]\n    style R1 fill:#87CEFA\n    style R2 fill:#87CEFA"

st.set_page_config(page_title="迹评", page_icon="🌊", layout="wide")
init_db()

if "entered" not in st.session_state: st.session_state.entered = False
if not st.session_state.entered:
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align:center;font-size:60px;'>🌊 迹评</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align:center;color:#888;'>EVERY STEP COUNTS</h3>", unsafe_allow_html=True)
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1,c2,c3 = st.columns([1,1,1])
    with c2:
        if st.button("进 入", use_container_width=True, type="primary"):
            st.session_state.entered = True; st.rerun()
    st.stop()

TEACHER_PASSWORD = "123456"
st.sidebar.title("迹评 EVERY STEP COUNTS")
role = st.sidebar.radio("选择身份", ["学生端", "老师端"])
if role == "老师端":
    pwd = st.sidebar.text_input("老师密码", type="password")
    if pwd != TEACHER_PASSWORD:
        st.sidebar.warning("请输入正确密码"); st.stop()

if role == "学生端":
    st.title("学生实操练习")
    if "student_name" not in st.session_state or not st.session_state.student_name:
        st.subheader("请输入你的姓名开始")
        ni = st.text_input("姓名", placeholder="输入你的名字")
        if st.button("进入", type="primary"):
            if ni.strip():
                st.session_state.student_name = ni.strip(); st.rerun()
            else: st.warning("请输入姓名")
        st.stop()
    student_name = st.session_state.student_name
    st.caption(f"当前学生：{student_name}")
    if st.button("切换学生"):
        del st.session_state.student_name; st.rerun()
    task_list = get_all_tasks()
    if not task_list:
        st.warning("⚠️ 当前暂无任务，请等待老师发布任务！")
    else:
        task_options = {f"[{ENV_LABEL.get(r[5],r[5])}] {r[1]}": r for r in task_list}
        selected_name = st.selectbox("选择任务", list(task_options.keys()))
        row = task_options[selected_name]
        tid, title, content, graph_code, ctime, env = row
        st.subheader(f"📋 {title}  `{ENV_LABEL.get(env,env)}`")
        st.markdown(content)
        if env == "vrp" and graph_code and graph_code.strip():
            st.markdown("---"); st.subheader("🗺️ 拓扑图"); render_mermaid(graph_code)
        st.markdown("---"); st.subheader(f"💻 终端（{ENV_LABEL.get(env,env)}）")
        cmd = st.text_input(ENV_PROMPT.get(env,">"), key="term_cmd")
        if cmd.strip():
            ok, out = simulate(env, cmd); add_record(student_name, tid, cmd, ok, out)
            st.code(f"{ENV_PROMPT.get(env,'>')} {cmd}", language="bash")
            st.success(out if out else "执行成功") if ok else st.error(out)
        st.markdown("---"); st.subheader("📝 你的操作记录")
        records = get_records(student_name)
        if records:
            for tn, c2, ok, out, ts in records:
                st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】 `{c2}`"); st.code(out)
            if st.button("清空我的记录"):
                clear_student_record(student_name); st.rerun()
        else: st.info("还没有操作记录")
else:
    st.title("老师管理后台")
    tab1, tab2 = st.tabs(["📤 发布新任务", "📊 学情查看"])
    with tab1:
        st.subheader("新建实操任务")
        env = st.selectbox("实操环境", ["vrp","linux","sql"], format_func=lambda x: ENV_LABEL[x])
        task_title = st.text_input("任务名称", value={"vrp":"OSPF配置","linux":"CentOS网络检查","sql":"MySQL查询练习"}[env], key=f"t_{env}")
        task_content = st.text_area("任务要求", value=DEFAULT_CONTENT[env], height=160, key=f"c_{env}")
        mermaid_graph = ""
        if env == "vrp":
            mermaid_graph = st.text_area("Mermaid拓扑代码", value=DEFAULT_MERMAID, height=180, key=f"m_{env}")
        if st.button("✅ 发布任务"):
            if task_title.strip():
                add_task(task_title.strip(), task_content, mermaid_graph.strip(), env)
                st.success("发布成功"); st.rerun()
            else: st.error("任务名称不能为空")
        st.markdown("---"); st.subheader("已发布任务")
        allt = get_all_tasks()
        if allt:
            for tid, title, content, graph, ctime, tenv in allt:
                with st.expander(f"【{ctime}】{ENV_LABEL.get(tenv,tenv)} {title}"):
                    st.markdown(content)
                    if tenv=="vrp" and graph: render_mermaid(graph)
                    if st.button("🗑 删除", key=f"d{tid}"): del_task(tid); st.rerun()
    with tab2:
        st.subheader("学生操作记录")
        students = get_students()
        if not students: st.warning("暂无学生记录")
        else:
            stu = st.selectbox("选择学生", students)
            records = get_records(stu)
            total = len(records); err = sum(1 for r in records if r[2]==0)
            rate = round((total-err)/total*100,1) if total>0 else 0
            c1,c2,c3 = st.columns(3)
            c1.metric("总步数", total); c2.metric("报错", err); c3.metric("正确率", f"{rate}%")
            st.markdown("---")
            for tn, cmd, ok, out, ts in records:
                st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】`{cmd}`"); st.code(out)
            df = pd.DataFrame(records, columns=["任务","命令","成功","输出","时间"])
            st.download_button("📥导出CSV", df.to_csv(index=False,encoding="utf-8-sig"), f"{stu}_记录.csv")
            if st.button("🗑清空该学生"): clear_student_record(stu); st.rerun()
