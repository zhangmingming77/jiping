# -*- coding: utf-8 -*-
import streamlit as st
import streamlit.components.v1 as components
import requests
from datetime import datetime
import pandas as pd
import html as _html
import os as _os

_MERMAID_JS_CACHE = ""

# ========== Turso 云端数据库（HTTP 接口，无需本地数据库文件） ==========
class IntegrityError(Exception):
    """唯一约束冲突（模拟 sqlite3.IntegrityError）"""

_turso = None

def get_db():
    """读取 Streamlit Secrets，构造 Turso HTTP 连接配置（全局复用）"""
    global _turso
    if _turso:
        return _turso
    url = st.secrets.get("TURSO_URL") or _os.environ.get("TURSO_URL")
    token = st.secrets.get("TURSO_AUTH_TOKEN") or _os.environ.get("TURSO_AUTH_TOKEN")
    if not url:
        st.error("请先在 Streamlit Cloud 的 Settings → Secrets 里配置 TURSO_URL 和 TURSO_AUTH_TOKEN")
        st.stop()
    endpoint = url.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
    _turso = {"endpoint": endpoint, "token": token}
    return _turso

def db_exec(sql, args=None):
    """执行一条 SQL。返回 (rows, lastid, rowcount)。"""
    db = get_db()
    body = {"requests": [
        {"type": "execute", "stmt": {"sql": sql, "args": args or []}},
        {"type": "close"}
    ]}
    r = requests.post(
        db["endpoint"],
        headers={
            "Authorization": f"Bearer {db['token']}",
            "Content-Type": "application/json"
        },
        json=body, timeout=30
    )
    if r.status_code != 200:
        raise RuntimeError(f"Turso 返回 HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    item = data["results"][0]
    if "error" in item:
        msg = item["error"].get("message", "unknown error")
        if "UNIQUE" in msg or "PRIMARY KEY" in msg:
            raise IntegrityError(msg)
        raise RuntimeError(msg)
    res = item.get("response", {}).get("result", {}) or {}
    rows = [tuple(row) for row in res.get("rows", [])]
    lastid = res.get("last_insert_rowid", 0) or 0
    affected = res.get("affected_row_count", 0) or 0
    return rows, lastid, affected

def _load_mermaid_js():
    global _MERMAID_JS_CACHE
    if _MERMAID_JS_CACHE:
        return _MERMAID_JS_CACHE
    p = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "mermaid.min.js")
    try:
        with open(p, "r", encoding="utf-8") as f:
            _MERMAID_JS_CACHE = f.read()
    except:
        _MERMAID_JS_CACHE = ""
    return _MERMAID_JS_CACHE

def render_mermaid(code, height=420):
    if not code or not code.strip():
        return
    escaped = _html.escape(code)
    js = _load_mermaid_js()
    components.html(f"""<div style="background:#fff;padding:14px;border-radius:10px;">
<pre class="mermaid" style="font-size:14px;">{escaped}</pre></div>
<script>{js}</script>
<script>try{{mermaid.initialize({{startOnLoad:true,theme:'default',securityLevel:'loose'}});}}catch(e){{}}</script>""", height=height)

def init_db():
    db_exec('''CREATE TABLE IF NOT EXISTS teachers (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        create_time TEXT
    )''')
    db_exec('''CREATE TABLE IF NOT EXISTS classes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT NOT NULL,
        teacher TEXT NOT NULL,
        create_time TEXT,
        UNIQUE(class_name, teacher)
    )''')
    db_exec('''CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT,
        mermaid_graph TEXT,
        create_time TEXT,
        env TEXT DEFAULT 'vrp',
        class_id INTEGER
    )''')
    db_exec('''CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student TEXT,
        class_id INTEGER,
        task_id INTEGER,
        command TEXT,
        is_ok INTEGER,
        output TEXT,
        timestamp TEXT
    )''')

# ========== 教师 ==========
def teacher_register(username, pwd):
    try:
        db_exec("INSERT INTO teachers (username, password, create_time) VALUES (?,?,?)",
                (username, pwd, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return True
    except IntegrityError:
        return False

def teacher_login(username, pwd):
    rows, _, _ = db_exec("SELECT 1 FROM teachers WHERE username=? AND password=?", (username, pwd))
    return len(rows) > 0

# ========== 班级 ==========
def create_class(class_name, teacher):
    try:
        db_exec("INSERT INTO classes (class_name, teacher, create_time) VALUES (?,?,?)",
                (class_name, teacher, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return True
    except IntegrityError:
        return False

def get_teacher_classes(teacher):
    rows, _, _ = db_exec("SELECT id, class_name FROM classes WHERE teacher=?", (teacher,))
    return rows

def get_class_by_id(cid):
    rows, _, _ = db_exec("SELECT id, class_name, teacher FROM classes WHERE id=?", (cid,))
    return rows[0] if rows else None

def get_all_class_ids_names():
    rows, _, _ = db_exec("SELECT id, class_name FROM classes")
    return rows

# ========== 任务 ==========
def add_task(title, content, graph, env, class_id):
    db_exec('''INSERT INTO tasks (title, content, mermaid_graph, create_time, env, class_id)
               VALUES (?,?,?,?,?,?)''',
            (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env, class_id))

def get_tasks_by_class(cid):
    rows, _, _ = db_exec("SELECT id, title, content, mermaid_graph, create_time, env FROM tasks WHERE class_id=? ORDER BY id DESC", (cid,))
    return rows

def del_task(tid):
    db_exec("DELETE FROM tasks WHERE id=?", (tid,))

# ========== 记录 ==========
def add_record(student, cid, tid, cmd, ok, out):
    db_exec('''INSERT INTO records (student, class_id, task_id, command, is_ok, output, timestamp)
               VALUES (?,?,?,?,?,?,?)''',
            (student, cid, tid, cmd, 1 if ok else 0, out, datetime.now().strftime("%H:%M:%S")))

def get_records_by_class_student(cid, student):
    rows, _, _ = db_exec('''SELECT t.title, r.command, r.is_ok, r.output, r.timestamp
                 FROM records r LEFT JOIN tasks t ON r.task_id=t.id
                 WHERE r.student=? AND r.class_id=? ORDER BY r.id''', (student, cid))
    return rows

def get_students_by_class(cid):
    rows, _, _ = db_exec("SELECT DISTINCT student FROM records WHERE class_id=?", (cid,))
    return [r[0] for r in rows]

def clear_student_record(cid, s):
    db_exec("DELETE FROM records WHERE student=? AND class_id=?", (s, cid))

# ========== 模拟器 ==========
ENV_LABEL = {"vrp": "🌐 VRP网络(ENSP)", "linux": "🐧 Linux(CentOS)", "sql": "🗄️ 数据库(MySQL)"}
ENV_PROMPT = {"vrp": "<Huawei>", "linux": "[student@localhost ~]$", "sql": "mysql>"}
ENV_ERROR = {"vrp": "Error:Unrecognized command found at '^' position.", "linux": "-bash: 未找到该命令", "sql": "ERROR 1064: SQL语法错误"}

def vrp_sim(cmd):
    m = {"system-view": "Enter system view, return user view with Ctrl+Z.", "sys": "Enter system view", "q": "Return to user view", "quit": "Return to user view",
        "display ip interface brief": "Interface    IP/Mask      Physical  Protocol\nGE0/0/0      192.168.1.1/24  up     up\nGE0/0/1      unassigned     down   down",
        "display vlan": "VLAN ID Type  Status  Ports\n1       static active GE0/0/1\n10      static active GE0/0/0",
        "vlan 10": "Info: VLAN 10 has been created.", "interface g0/0/0": "GigabitEthernet0/0/0",
        "interface gigabitethernet 0/0/0": "GigabitEthernet0/0/0",
        "ip address 192.168.1.1 255.255.255.0": "Info: IP configured.", "ospf 1 router-id 1.1.1.1": "Info: OSPF started.",
        "area 0": "Enter Area 0 view", "network 192.168.1.0 0.0.0.255": "Info: Network added.",
        "display ospf peer": "OSPF Process 1  Router ID: 2.2.2.2  State: Full",
        "vrrp vrid 1 virtual-ip 192.168.1.254": "Info: VRRP group 1 created.", "save": "Info: Save OK."}
    k = cmd.strip().lower()
    return (True, m[k]) if k in m else (False, ENV_ERROR["vrp"])

def linux_sim(cmd):
    cl = cmd.strip().lower()
    s = cl[5:].strip() if cl.startswith("sudo ") else cl
    m = {"pwd": "/home/student", "whoami": "student", "hostname": "localhost.localdomain",
        "ls": "Desktop Documents Downloads Music Pictures",
        "cat /etc/os-release": 'NAME="CentOS Linux"\nVERSION="7 (Core)"',
        "ip addr": "ens33: inet 192.168.1.10/24\nlo: inet 127.0.0.1/8",
        "ip a": "ens33: inet 192.168.1.10/24",
        "ping 192.168.1.1 -c 3": "3 received, 0% packet loss",
        "systemctl status network": "Active: active", "firewall-cmd --state": "running",
        "netstat -tlnp": "tcp 0.0.0.0:22 LISTEN sshd", "yum install -y vim": "Complete!"}
    return (True, m[s]) if s in m else (False, ENV_ERROR["linux"])

def sql_sim(cmd):
    k = cmd.strip().lower().rstrip(";").strip()
    m = {"show databases": "information_schema  mysql  test", "use test": "Database changed",
        "show tables": "students  courses", "select * from students": "1 张三 19\n2 李四 20",
        "desc students": "id int\nname varchar(20)", "exit": "Bye"}
    return (True, m[k]) if k in m else (False, ENV_ERROR["sql"])

def simulate(env, cmd):
    if env == "linux": return linux_sim(cmd)
    if env == "sql": return sql_sim(cmd)
    return vrp_sim(cmd)

DEFAULT_CONTENT = {
    "vrp": "1. system-view\n2. interface g0/0/0\n3. ip address 192.168.1.1 255.255.255.0\n4. ospf 1 router-id 1.1.1.1",
    "linux": "1. cat /etc/os-release\n2. ip addr\n3. ping 192.168.1.1 -c 3\n4. firewall-cmd --state",
    "sql": "1. show databases;\n2. use test;\n3. show tables;\n4. select * from students;"}
DEFAULT_MERMAID = "graph LR\n    R1[R1] --- GE0/0/0\n    GE0/0/0 --- LINK{{链路}}\n    LINK --- GE0/0/1\n    GE0/0/1 --- R2[R2]\n    style R1 fill:#87CEFA\n    style R2 fill:#87CEFA"

# ==================== 页面入口 ====================
st.set_page_config(page_title="迹评", page_icon="🌊", layout="wide")
try:
    init_db()
except Exception as e:
    st.error(f"数据库连接失败：{e}")
    st.stop()

if "entered" not in st.session_state:
    st.session_state.entered = False
if not st.session_state.entered:
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align:center;font-size:60px;'>🌊 迹评</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align:center;color:#888;'>EVERY STEP COUNTS</h3>", unsafe_allow_html=True)
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, c2, _ = st.columns([1,1,1])
    with c2:
        if st.button("进 入", use_container_width=True, type="primary"):
            st.session_state.entered = True
            st.rerun()
    st.stop()

st.sidebar.title("迹评 EVERY STEP COUNTS")
role = st.sidebar.radio("选择身份", ["学生端", "老师端"])

# -------------------------- 老师端 --------------------------
if role == "老师端":
    if "teacher_login_ok" not in st.session_state:
        st.session_state.teacher_login_ok = False
        st.session_state.teacher_name = ""
    if not st.session_state.teacher_login_ok:
        tab_login, tab_reg = st.tabs(["🔐 老师登录", "📝 老师注册"])
        with tab_login:
            tname = st.text_input("老师姓名(账号)")
            tpwd = st.text_input("密码", type="password")
            if st.button("登录"):
                if teacher_login(tname, tpwd):
                    st.session_state.teacher_login_ok = True
                    st.session_state.teacher_name = tname
                    st.rerun()
                else:
                    st.error("账号或密码错误")
        with tab_reg:
            reg_name = st.text_input("注册姓名（作为登录账号）")
            reg_pwd1 = st.text_input("设置密码", type="password")
            reg_pwd2 = st.text_input("确认密码", type="password")
            if st.button("注册"):
                if reg_name.strip() and reg_pwd1 and reg_pwd1 == reg_pwd2:
                    if teacher_register(reg_name.strip(), reg_pwd1):
                        st.success("注册成功，可以去登录了")
                    else:
                        st.warning("该老师账号已存在！")
                else:
                    st.warning("名字不能为空，两次密码一致")
        st.stop()
    tea_name = st.session_state.teacher_name
    st.sidebar.success(f"✅ 已登录：{tea_name}")
    if st.sidebar.button("退出登录"):
        st.session_state.teacher_login_ok = False
        st.session_state.teacher_name = ""
        st.rerun()

    my_classes = get_teacher_classes(tea_name)
    st.title("教师后台｜" + tea_name)
    tab_create_class, tab_publish, tab_stat = st.tabs(["🏫 创建班级", "📤 发布任务", "📊 本班学情"])

    with tab_create_class:
        new_cn = st.text_input("班级名称（例：信安252）")
        if st.button("创建班级"):
            if new_cn.strip():
                if create_class(new_cn.strip(), tea_name):
                    st.success(f"班级【{new_cn}】创建成功")
                    st.rerun()
                else:
                    st.warning("你名下已有同名班级！")
            else:
                st.warning("班级名不能为空")
        st.markdown("### 你管理的班级列表")
        if my_classes:
            for cid, cname in my_classes:
                st.write(f"- {cname} (id:{cid})")
        else:
            st.info("暂无班级，请先创建班级")

    with tab_publish:
        if not my_classes:
            st.warning("⚠️ 请先创建班级，才能发布任务")
            st.stop()
        cid_list = [str(x[0]) + " | " + x[1] for x in my_classes]
        sel_c_str = st.selectbox("选择班级发布任务", cid_list)
        sel_cid = int(sel_c_str.split("|")[0].strip())
        env = st.selectbox("环境", ["vrp","linux","sql"], format_func=lambda x: ENV_LABEL[x])
        tt = st.text_input("任务名称", value={"vrp":"OSPF配置","linux":"CentOS网络检查","sql":"MySQL查询"}[env])
        tc = st.text_area("任务要求", value=DEFAULT_CONTENT[env], height=150)
        mg = ""
        if env=="vrp":
            mg = st.text_area("Mermaid拓扑", value=DEFAULT_MERMAID, height=150)
        if st.button("✅ 发布任务"):
            if tt.strip():
                add_task(tt.strip(), tc, mg.strip(), env, sel_cid)
                st.success("任务发布成功")
                st.rerun()
            else:
                st.error("任务名称不能为空")
        st.markdown("---")
        task_list = get_tasks_by_class(sel_cid)
        for tid, title, content, graph, ctime, tenv in task_list:
            with st.expander(f"【{ctime}】{ENV_LABEL.get(tenv,tenv)} {title}"):
                st.markdown(content)
                if tenv=="vrp" and graph:
                    render_mermaid(graph)
                if st.button("🗑 删除任务", key=f"d{tid}"):
                    del_task(tid)
                    st.rerun()

    with tab_stat:
        if not my_classes:
            st.warning("暂无班级")
            st.stop()
        cid_list = [str(x[0]) + " | " + x[1] for x in my_classes]
        sel_c_str = st.selectbox("查看哪个班级学情", cid_list, key="statclass")
        sel_cid = int(sel_c_str.split("|")[0].strip())
        st.subheader("本班学生记录")
        stu_list = get_students_by_class(sel_cid)
        if not stu_list:
            st.warning("本班暂无学生实操记录")
        else:
            sel_stu = st.selectbox("选择学生查看", stu_list)
            recs = get_records_by_class_student(sel_cid, sel_stu)
            total = len(recs)
            err = sum(1 for r in recs if r[2]==0)
            rate = round((total-err)/total*100,1) if total>0 else 0
            a,b,c = st.columns(3)
            a.metric("总步数", total)
            b.metric("报错", err)
            c.metric("正确率", f"{rate}%")
            st.markdown("---")
            for tn, cmd, ok, out, ts in recs:
                st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】`{cmd}`")
                st.code(out)
            df = pd.DataFrame(recs, columns=["任务","命令","成功","输出","时间"])
            st.download_button("📥导出CSV", df.to_csv(index=False,encoding="utf-8-sig"), f"{sel_stu}_{sel_cid}.csv")
            if st.button("🗑清空该生本班记录"):
                clear_student_record(sel_cid, sel_stu)
                st.rerun()

# -------------------------- 学生端 --------------------------
else:
    st.title("学生实操练习")
    if "student_name" not in st.session_state:
        st.session_state.student_name = ""
    if "student_class_id" not in st.session_state:
        st.session_state.student_class_id = None

    if not st.session_state.student_name or st.session_state.student_class_id is None:
        st.subheader("请选择班级 + 输入姓名进入练习")
        all_classes = get_all_class_ids_names()
        if not all_classes:
            st.warning("⚠️ 老师还没有创建任何班级，请等待老师创建班级！")
            st.stop()
        opt_cls = [f"{cid} | {cname}" for cid,cname in all_classes]
        sel_cls_str = st.selectbox("选择你的班级", opt_cls)
        sel_cid = int(sel_cls_str.split("|")[0].strip())
        ni = st.text_input("你的姓名", placeholder="输入名字")
        if st.button("进入练习", type="primary"):
            if ni.strip():
                st.session_state.student_name = ni.strip()
                st.session_state.student_class_id = sel_cid
                st.rerun()
            else:
                st.warning("请输入姓名")
        st.stop()
    sn = st.session_state.student_name
    scid = st.session_state.student_class_id
    c1, c2 = st.columns([3,1])
    c1.caption(f"当前学生：{sn}｜班级：{get_class_by_id(scid)[1]}")
    if c2.button("🔴 退出"):
        del st.session_state.student_name
        del st.session_state.student_class_id
        st.rerun()
    tasks = get_tasks_by_class(scid)
    if not tasks:
        st.warning("⚠️ 本班暂无任务，请等待老师发布！")
        st.stop()
    opts = {f"[{ENV_LABEL.get(r[5],r[5])}] {r[1]}": r for r in tasks}
    sel = st.selectbox("选择任务", list(opts.keys()))
    row = opts[sel]
    tid, title, content, graph, ctime, env = row
    st.subheader(f"📋 {title}  `{ENV_LABEL.get(env,env)}`")
    st.markdown(content)
    if env=="vrp" and graph and graph.strip():
        st.markdown("---")
        st.subheader("🗺️ 拓扑图")
        render_mermaid(graph)
    st.markdown("---")
    st.subheader("💻 终端")
    cmd = st.text_input(ENV_PROMPT.get(env,">"), key="tc")
    if cmd.strip():
        ok, out = simulate(env, cmd)
        add_record(sn, scid, tid, cmd, ok, out)
        st.code(f"{ENV_PROMPT.get(env,'>')} {cmd}", language="bash")
        if ok:
            st.success(out if out else "OK")
        else:
            st.error(out)
    st.markdown("---")
    st.subheader("📝 我的记录（本班）")
    recs = get_records_by_class_student(scid, sn)
    if recs:
        for tn, c2cmd, ok, out, ts in recs:
            st.markdown(f"{'🟢' if ok else '🔴'} `{ts}` 【{tn}】`{c2cmd}`")
            st.code(out)
        if st.button("清空我本班记录"):
            clear_student_record(scid, sn)
            st.rerun()
    else:
        st.info("暂无记录")
