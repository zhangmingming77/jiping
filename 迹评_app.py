# -*- coding: utf-8 -*-
"""
迹评 · EVERY STEP COUNTS
Streamlit + Turso(libSQL) HTTP v2 pipeline 版本

修复要点见文件末尾注释。
"""

import hashlib
import html as _html
import os as _os
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

_MERMAID_JS_CACHE = ""
_turso = None


# ============================================================
# 异常
# ============================================================
class IntegrityError(Exception):
    """唯一约束冲突（模拟 sqlite3.IntegrityError）"""


class DBError(Exception):
    """数据库/网络层错误"""


# ============================================================
# Turso 连接与执行
# ============================================================
def _secret(key):
    """安全读取 secrets，本地没有 secrets.toml 时不会崩"""
    val = None
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
    base = str(url).strip()
    base = base.replace("libsql://", "https://").replace("http://", "https://")
    base = base.rstrip("/")
    _turso = {"endpoint": base + "/v2/pipeline", "token": token or ""}
    return _turso


def _to_turso_arg(v):
    """
    ★ 核心修复点 ★
    Turso v2 pipeline 协议要求 value 一律是「字符串」。
    旧代码把 int/float 原样塞进去，服务端反序列化时要求 borrowed string，
    于是报：invalid type: integer 1, expected a borrowed string
    """
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        # 用 repr 保留精度，且必须是字符串
        return {"type": "float", "value": repr(float(v))}
    if isinstance(v, (bytes, bytearray)):
        import base64
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode("ascii")}
    return {"type": "text", "value": str(v)}


def _cell_to_py(cell):
    """把 Turso 返回的单元格转成 Python 值（修复 null 被当成 dict 的 bug）"""
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


def db_exec(sql, args=None):
    """执行单条 SQL，返回 (rows, last_insert_rowid, affected_row_count)"""
    db = get_db()
    body = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": [_to_turso_arg(a) for a in (args or [])],
                },
            },
            {"type": "close"},
        ]
    }
    headers = {"Content-Type": "application/json"}
    if db["token"]:
        headers["Authorization"] = "Bearer " + db["token"]

    try:
        resp = requests.post(db["endpoint"], headers=headers, json=body, timeout=30)
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

    item = results[0]
    if item.get("type") == "error" or "error" in item:
        err = item.get("error") or {}
        msg = err.get("message", "unknown error") if isinstance(err, dict) else str(err)
        upper = msg.upper()
        if "UNIQUE" in upper or "PRIMARY KEY" in upper:
            raise IntegrityError(msg)
        raise DBError("SQL 错误：{}".format(msg))

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


# ============================================================
# Mermaid 渲染
# ============================================================
def _load_mermaid_js():
    global _MERMAID_JS_CACHE
    if _MERMAID_JS_CACHE:
        return _MERMAID_JS_CACHE
    path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static", "mermaid.min.js")
    try:
        with open(path, "r", encoding="utf-8") as fp:
            _MERMAID_JS_CACHE = fp.read()
    except Exception:
        _MERMAID_JS_CACHE = ""
    return _MERMAID_JS_CACHE


def render_mermaid(code, height=420):
    if not code or not code.strip():
        return
    escaped = _html.escape(code)
    js = _load_mermaid_js()
    if js:
        loader = "<script>{}</script>".format(js)
    else:
        # 本地没有 mermaid.min.js 时走 CDN，避免白屏
        loader = (
            '<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.0/dist/mermaid.min.js"></script>'
        )
    components.html(
        """
        <div style="background:#fff;padding:14px;border-radius:10px;">
          <pre class="mermaid" style="font-size:14px;">{code}</pre>
        </div>
        {loader}
        <script>
          try {{
            mermaid.initialize({{ startOnLoad: true, theme: 'default', securityLevel: 'loose' }});
          }} catch (e) {{}}
        </script>
        """.format(code=escaped, loader=loader),
        height=height,
    )


# ============================================================
# 建表
# ============================================================
def init_db():
    if st.session_state.get("_db_inited"):
        return
    db_exec(
        """CREATE TABLE IF NOT EXISTS teachers (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            create_time TEXT
        )"""
    )
    db_exec(
        """CREATE TABLE IF NOT EXISTS classes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT NOT NULL,
            teacher TEXT NOT NULL,
            create_time TEXT,
            UNIQUE(class_name, teacher)
        )"""
    )
    db_exec(
        """CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            mermaid_graph TEXT,
            create_time TEXT,
            env TEXT DEFAULT 'vrp',
            class_id INTEGER
        )"""
    )
    db_exec(
        """CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student TEXT,
            class_id INTEGER,
            task_id INTEGER,
            command TEXT,
            is_ok INTEGER,
            output TEXT,
            timestamp TEXT
        )"""
    )
    st.session_state["_db_inited"] = True


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
    # 兼容旧版本存的明文密码
    return pwd == stored


def teacher_register(username, pwd):
    try:
        db_exec(
            "INSERT INTO teachers (username, password, create_time) VALUES (?,?,?)",
            (username, _hash_pwd(pwd), datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
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
    # 老账号明文密码自动升级为哈希
    if not str(stored).startswith("sha256$"):
        try:
            db_exec("UPDATE teachers SET password=? WHERE username=?", (_hash_pwd(pwd), username))
        except DBError:
            pass
    return True


# ============================================================
# 班级
# ============================================================
def create_class(class_name, teacher):
    try:
        db_exec(
            "INSERT INTO classes (class_name, teacher, create_time) VALUES (?,?,?)",
            (class_name, teacher, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        return True
    except IntegrityError:
        return False


def get_teacher_classes(teacher):
    rows, _, _ = db_exec("SELECT id, class_name FROM classes WHERE teacher=? ORDER BY id", (teacher,))
    return rows


def get_class_by_id(cid):
    rows, _, _ = db_exec("SELECT id, class_name, teacher FROM classes WHERE id=?", (cid,))
    return rows[0] if rows else None


def get_all_class_ids_names():
    rows, _, _ = db_exec("SELECT id, class_name FROM classes ORDER BY id")
    return rows


# ============================================================
# 任务
# ============================================================
def add_task(title, content, graph, env, class_id):
    db_exec(
        """INSERT INTO tasks (title, content, mermaid_graph, create_time, env, class_id)
           VALUES (?,?,?,?,?,?)""",
        (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env, class_id),
    )


def get_tasks_by_class(cid):
    rows, _, _ = db_exec(
        """SELECT id, title, content, mermaid_graph, create_time, env
           FROM tasks WHERE class_id=? ORDER BY id DESC""",
        (cid,),
    )
    return rows


def del_task(tid):
    db_exec("DELETE FROM tasks WHERE id=?", (tid,))


# ============================================================
# 记录
# ============================================================
def add_record(student, cid, tid, cmd, ok, out):
    db_exec(
        """INSERT INTO records (student, class_id, task_id, command, is_ok, output, timestamp)
           VALUES (?,?,?,?,?,?,?)""",
        (student, cid, tid, cmd, 1 if ok else 0, out, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )


def get_records_by_class_student(cid, student):
    rows, _, _ = db_exec(
        """SELECT t.title, r.command, r.is_ok, r.output, r.timestamp
           FROM records r LEFT JOIN tasks t ON r.task_id = t.id
           WHERE r.student=? AND r.class_id=? ORDER BY r.id""",
        (student, cid),
    )
    return rows


def get_students_by_class(cid):
    rows, _, _ = db_exec(
        "SELECT DISTINCT student FROM records WHERE class_id=? AND student IS NOT NULL ORDER BY student",
        (cid,),
    )
    return [r[0] for r in rows]


def clear_student_record(cid, student):
    db_exec("DELETE FROM records WHERE student=? AND class_id=?", (student, cid))


# ============================================================
# 模拟器
# ============================================================
ENV_LABEL = {"vrp": "🌐 VRP网络(ENSP)", "linux": "🐧 Linux(CentOS)", "sql": "🗄️ 数据库(MySQL)"}
ENV_PROMPT = {"vrp": "<Huawei>", "linux": "[student@localhost ~]$", "sql": "mysql>"}
ENV_ERROR = {
    "vrp": "Error:Unrecognized command found at '^' position.",
    "linux": "-bash: 未找到该命令",
    "sql": "ERROR 1064: SQL语法错误",
}


def vrp_sim(cmd):
    table = {
        "system-view": "Enter system view, return user view with Ctrl+Z.",
        "sys": "Enter system view",
        "q": "Return to user view",
        "quit": "Return to user view",
        "display ip interface brief": (
            "Interface    IP/Mask          Physical  Protocol\n"
            "GE0/0/0      192.168.1.1/24   up        up\n"
            "GE0/0/1      unassigned       down      down"
        ),
        "display vlan": (
            "VLAN ID  Type    Status  Ports\n"
            "1        static  active  GE0/0/1\n"
            "10       static  active  GE0/0/0"
        ),
        "vlan 10": "Info: VLAN 10 has been created.",
        "interface g0/0/0": "GigabitEthernet0/0/0",
        "interface gigabitethernet 0/0/0": "GigabitEthernet0/0/0",
        "ip address 192.168.1.1 255.255.255.0": "Info: IP configured.",
        "ospf 1 router-id 1.1.1.1": "Info: OSPF started.",
        "area 0": "Enter Area 0 view",
        "network 192.168.1.0 0.0.0.255": "Info: Network added.",
        "display ospf peer": "OSPF Process 1  Router ID: 2.2.2.2  State: Full",
        "vrrp vrid 1 virtual-ip 192.168.1.254": "Info: VRRP group 1 created.",
        "save": "Info: Save OK.",
    }
    key = cmd.strip().lower()
    if key in table:
        return True, table[key]
    return False, ENV_ERROR["vrp"]


def linux_sim(cmd):
    low = cmd.strip().lower()
    body = low[5:].strip() if low.startswith("sudo ") else low
    table = {
        "pwd": "/home/student",
        "whoami": "student",
        "hostname": "localhost.localdomain",
        "ls": "Desktop  Documents  Downloads  Music  Pictures",
        "cat /etc/os-release": 'NAME="CentOS Linux"\nVERSION="7 (Core)"',
        "ip addr": "ens33: inet 192.168.1.10/24\nlo: inet 127.0.0.1/8",
        "ip a": "ens33: inet 192.168.1.10/24",
        "ping 192.168.1.1 -c 3": "3 received, 0% packet loss",
        "systemctl status network": "Active: active (running)",
        "firewall-cmd --state": "running",
        "netstat -tlnp": "tcp  0.0.0.0:22  LISTEN  sshd",
        "yum install -y vim": "Complete!",
    }
    if body in table:
        return True, table[body]
    return False, ENV_ERROR["linux"]


def sql_sim(cmd):
    key = cmd.strip().lower().rstrip(";").strip()
    table = {
        "show databases": "information_schema\nmysql\ntest",
        "use test": "Database changed",
        "show tables": "students\ncourses",
        "select * from students": "1  张三  19\n2  李四  20",
        "desc students": "id    int\nname  varchar(20)",
        "exit": "Bye",
    }
    if key in table:
        return True, table[key]
    return False, ENV_ERROR["sql"]


def simulate(env, cmd):
    if env == "linux":
        return linux_sim(cmd)
    if env == "sql":
        return sql_sim(cmd)
    return vrp_sim(cmd)


DEFAULT_CONTENT = {
    "vrp": "1. system-view\n2. interface g0/0/0\n3. ip address 192.168.1.1 255.255.255.0\n4. ospf 1 router-id 1.1.1.1",
    "linux": "1. cat /etc/os-release\n2. ip addr\n3. ping 192.168.1.1 -c 3\n4. firewall-cmd --state",
    "sql": "1. show databases;\n2. use test;\n3. show tables;\n4. select * from students;",
}

DEFAULT_MERMAID = (
    "graph LR\n"
    "    R1[R1] --- GE0/0/0\n"
    "    GE0/0/0 --- LINK{{链路}}\n"
    "    LINK --- GE0/0/1\n"
    "    GE0/0/1 --- R2[R2]\n"
    "    style R1 fill:#87CEFA\n"
    "    style R2 fill:#87CEFA"
)


# ============================================================
# 页面：老师端
# ============================================================
def render_tab_create_class(tea_name):
    new_cn = st.text_input("班级名称（例：信安252）", key="new_class_name")
    if st.button("创建班级", key="btn_create_class"):
        if not new_cn.strip():
            st.warning("班级名不能为空")
        elif create_class(new_cn.strip(), tea_name):
            st.success("班级【{}】创建成功".format(new_cn.strip()))
            st.rerun()
        else:
            st.warning("你名下已有同名班级！")

    st.markdown("### 你管理的班级列表")
    classes = get_teacher_classes(tea_name)
    if classes:
        for cid, cname in classes:
            st.write("- {} (id:{})".format(cname, cid))
    else:
        st.info("暂无班级，请先创建班级")


def render_tab_publish(my_classes):
    if not my_classes:
        st.warning("⚠️ 请先创建班级，才能发布任务")
        return

    options = ["{} | {}".format(x[0], x[1]) for x in my_classes]
    sel = st.selectbox("选择班级发布任务", options, key="pub_class")
    sel_cid = int(sel.split("|")[0].strip())

    env = st.selectbox("环境", ["vrp", "linux", "sql"], format_func=lambda x: ENV_LABEL[x], key="pub_env")
    default_title = {"vrp": "OSPF配置", "linux": "CentOS网络检查", "sql": "MySQL查询"}[env]
    title = st.text_input("任务名称", value=default_title, key="pub_title_{}".format(env))
    content = st.text_area("任务要求", value=DEFAULT_CONTENT[env], height=150, key="pub_content_{}".format(env))
    graph = ""
    if env == "vrp":
        graph = st.text_area("Mermaid拓扑", value=DEFAULT_MERMAID, height=150, key="pub_graph")

    if st.button("✅ 发布任务", key="btn_publish"):
        if not title.strip():
            st.error("任务名称不能为空")
        else:
            add_task(title.strip(), content, graph.strip(), env, sel_cid)
            st.success("任务发布成功")
            st.rerun()

    st.markdown("---")
    tasks = get_tasks_by_class(sel_cid)
    if not tasks:
        st.info("本班还没有任务")
        return

    for tid, ttitle, tcontent, tgraph, ctime, tenv in tasks:
        with st.expander("【{}】{} {}".format(ctime, ENV_LABEL.get(tenv, tenv), ttitle)):
            st.markdown(tcontent or "")
            if tenv == "vrp" and tgraph:
                render_mermaid(tgraph)
            if st.button("🗑 删除任务", key="del_task_{}".format(tid)):
                del_task(tid)
                st.rerun()


def render_tab_stat(my_classes):
    if not my_classes:
        st.warning("暂无班级")
        return

    options = ["{} | {}".format(x[0], x[1]) for x in my_classes]
    sel = st.selectbox("查看哪个班级学情", options, key="stat_class")
    sel_cid = int(sel.split("|")[0].strip())

    st.subheader("本班学生记录")
    students = get_students_by_class(sel_cid)
    if not students:
        st.warning("本班暂无学生实操记录")
        return

    sel_stu = st.selectbox("选择学生查看", students, key="stat_student")
    recs = get_records_by_class_student(sel_cid, sel_stu)

    total = len(recs)
    err = sum(1 for r in recs if r[2] == 0)
    rate = round((total - err) / total * 100, 1) if total else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("总步数", total)
    col2.metric("报错", err)
    col3.metric("正确率", "{}%".format(rate))

    st.markdown("---")
    for tname, cmd, ok, out, ts in recs:
        st.markdown(
            "{} `{}` 【{}】`{}`".format(
                "🟢" if ok else "🔴", ts, tname or "已删除任务", cmd
            )
        )
        st.code(out or "")

    df = pd.DataFrame(recs, columns=["任务", "命令", "成功", "输出", "时间"])
    st.download_button(
        "📥导出CSV",
        df.to_csv(index=False).encode("utf-8-sig"),
        "{}_{}.csv".format(sel_stu, sel_cid),
        mime="text/csv",
        key="dl_csv",
    )

    if st.button("🗑清空该生本班记录", key="btn_clear_rec"):
        clear_student_record(sel_cid, sel_stu)
        st.rerun()


def render_teacher():
    if "teacher_login_ok" not in st.session_state:
        st.session_state.teacher_login_ok = False
        st.session_state.teacher_name = ""

    if not st.session_state.teacher_login_ok:
        tab_login, tab_reg = st.tabs(["🔐 老师登录", "📝 老师注册"])
        with tab_login:
            tname = st.text_input("老师姓名(账号)", key="login_name")
            tpwd = st.text_input("密码", type="password", key="login_pwd")
            if st.button("登录", key="btn_login"):
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

    my_classes = get_teacher_classes(tea_name)
    st.title("教师后台｜" + tea_name)

    tab1, tab2, tab3 = st.tabs(["🏫 创建班级", "📤 发布任务", "📊 本班学情"])
    with tab1:
        render_tab_create_class(tea_name)
    with tab2:
        render_tab_publish(my_classes)
    with tab3:
        render_tab_stat(my_classes)


# ============================================================
# 页面：学生端
# ============================================================
def render_student():
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
            return

        options = ["{} | {}".format(cid, cname) for cid, cname in all_classes]
        sel = st.selectbox("选择你的班级", options, key="stu_class")
        sel_cid = int(sel.split("|")[0].strip())
        name = st.text_input("你的姓名", placeholder="输入名字", key="stu_name_input")

        if st.button("进入练习", type="primary", key="btn_enter"):
            if not name.strip():
                st.warning("请输入姓名")
            else:
                st.session_state.student_name = name.strip()
                st.session_state.student_class_id = sel_cid
                st.rerun()
        return

    sn = st.session_state.student_name
    scid = st.session_state.student_class_id

    cls_row = get_class_by_id(scid)
    cls_name = cls_row[1] if cls_row else "未知班级"

    col1, col2 = st.columns([3, 1])
    col1.caption("当前学生：{}｜班级：{}".format(sn, cls_name))
    if col2.button("🔴 退出", key="btn_exit"):
        st.session_state.student_name = ""
        st.session_state.student_class_id = None
        st.session_state.pop("_last_cmd_result", None)
        st.rerun()

    tasks = get_tasks_by_class(scid)
    if not tasks:
        st.warning("⚠️ 本班暂无任务，请等待老师发布！")
        return

    options = {}
    for row in tasks:
        tid, title, content, graph, ctime, env = row
        label = "[{}] {} (#{})".format(ENV_LABEL.get(env, env), title, tid)
        options[label] = row

    sel = st.selectbox("选择任务", list(options.keys()), key="stu_task")
    tid, title, content, graph, ctime, env = options[sel]

    st.subheader("📋 {}　`{}`".format(title, ENV_LABEL.get(env, env)))
    st.markdown(content or "")

    if env == "vrp" and graph and graph.strip():
        st.markdown("---")
        st.subheader("🗺️ 拓扑图")
        render_mermaid(graph)

    st.markdown("---")
    st.subheader("💻 终端")

    # ★ 用 form 包起来：只有点「执行」才记录，避免每敲一个字就写一条记录
    with st.form(key="cmd_form_{}".format(tid), clear_on_submit=True):
        cmd = st.text_input(ENV_PROMPT.get(env, ">"))
        submitted = st.form_submit_button("执行命令", type="primary")

    if submitted:
        if not cmd.strip():
            st.warning("请输入命令")
        else:
            ok, out = simulate(env, cmd)
            try:
                add_record(sn, scid, tid, cmd.strip(), ok, out)
            except DBError as exc:
                st.error("记录保存失败：{}".format(exc))
            st.session_state["_last_cmd_result"] = {
                "cmd": cmd.strip(),
                "ok": ok,
                "out": out,
                "env": env,
                "tid": tid,
            }

    last = st.session_state.get("_last_cmd_result")
    if last and last.get("tid") == tid:
        prompt = ENV_PROMPT.get(last["env"], ">")
        st.code("{} {}".format(prompt, last["cmd"]), language="bash")
        if last["ok"]:
            st.success(last["out"] or "OK")
        else:
            st.error(last["out"])

    st.markdown("---")
    st.subheader("📝 我的记录（本班）")
    recs = get_records_by_class_student(scid, sn)
    if recs:
        for tname, rcmd, ok, out, ts in recs:
            st.markdown(
                "{} `{}` 【{}】`{}`".format(
                    "🟢" if ok else "🔴", ts, tname or "已删除任务", rcmd
                )
            )
            st.code(out or "")
        if st.button("清空我本班记录", key="btn_clear_my"):
            clear_student_record(scid, sn)
            st.session_state.pop("_last_cmd_result", None)
            st.rerun()
    else:
        st.info("暂无记录")


# ============================================================
# 入口
# ============================================================
def main():
    st.set_page_config(page_title="迹评", page_icon="🌊", layout="wide")

    try:
        init_db()
    except (DBError, IntegrityError) as exc:
        st.error("数据库初始化失败：{}".format(exc))
        st.stop()

    if "entered" not in st.session_state:
        st.session_state.entered = False

    if not st.session_state.entered:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown(
            "<h1 style='text-align:center;font-size:60px;'>🌊 迹评</h1>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<h3 style='text-align:center;color:#888;'>EVERY STEP COUNTS</h3>",
            unsafe_allow_html=True,
        )
        st.markdown("<br><br>", unsafe_allow_html=True)
        _, mid, _ = st.columns([1, 1, 1])
        with mid:
            if st.button("进 入", use_container_width=True, type="primary"):
                st.session_state.entered = True
                st.rerun()
        st.stop()

    st.sidebar.title("迹评 EVERY STEP COUNTS")
    role = st.sidebar.radio("选择身份", ["学生端", "老师端"])

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
