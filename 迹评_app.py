# -*- coding: utf-8 -*-
"""
迹评 - 多环境实训模拟器（ENSP/VRP + Linux + MySQL）

功能：
1. 老师发布任务，可选环境：VRP网络 / Linux / 数据库SQL，VRP可附带Mermaid拓扑图
2. 学生选中任务，终端按环境切换提示符与回显
3. 操作记录入库，老师端查看时间线、正确率、导出CSV

运行：
pip install streamlit pandas
streamlit run 迹评_app.py
"""
import streamlit as st
import streamlit.components.v1 as components
import sqlite3
from datetime import datetime
import pandas as pd
import html as _html
import os as _os


# ==================== 拓扑图渲染（离线版，本地内联 mermaid.js） ====================
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
    """用本地 mermaid.js 渲染拓扑图（不依赖外网，不会被拦截）"""
    if not code or not code.strip():
        return
    escaped = _html.escape(code)
    js = _load_mermaid_js()
    components.html(
        f"""
        <div style="background:#ffffff;padding:14px;border-radius:10px;">
          <pre class="mermaid" style="font-size:14px;">{escaped}</pre>
        </div>
        <script>{js}</script>
        <script>
          try {{
            mermaid.initialize({{ startOnLoad: true, theme: 'default', securityLevel: 'loose' }});
          }} catch (e) {{ document.body.innerText = 'mermaid 加载失败'; }}
        </script>
        """,
        height=height,
    )


# ==================== 数据库 ====================
def _ensure_column(c, table, col, ddl):
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def init_db():
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tasks
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  title TEXT NOT NULL,
                  content TEXT,
                  mermaid_graph TEXT,
                  create_time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  student TEXT,
                  task_id INTEGER,
                  command TEXT,
                  is_ok INTEGER,
                  output TEXT,
                  timestamp TEXT,
                  FOREIGN KEY(task_id) REFERENCES tasks(id))''')
    # 兼容旧库：给 tasks 补 env 字段（vrp/linux/sql）
    _ensure_column(c, "tasks", "env", "TEXT DEFAULT 'vrp'")
    conn.commit()
    conn.close()


def add_task(title, content, graph, env="vrp"):
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("INSERT INTO tasks (title, content, mermaid_graph, create_time, env) VALUES (?,?,?,?,?)",
              (title, content, graph, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env))
    conn.commit()
    conn.close()


def get_all_tasks():
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("SELECT id, title, content, mermaid_graph, create_time, env FROM tasks ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return rows


def del_task(task_id):
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("DELETE FROM tasks WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


def add_record(student, task_id, cmd, is_ok, out):
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("""INSERT INTO records
        (student, task_id, command, is_ok, output, timestamp)
        VALUES (?,?,?,?,?,?)""",
              (student, task_id, cmd, 1 if is_ok else 0, out, datetime.now().strftime("%H:%M:%S")))
    conn.commit()
    conn.close()


def get_records(student=None):
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    if student:
        c.execute("""SELECT t.title, r.command, r.is_ok, r.output, r.timestamp
                     FROM records r LEFT JOIN tasks t ON r.task_id = t.id
                     WHERE r.student=? ORDER BY r.id""", (student,))
    else:
        c.execute("""SELECT r.student, t.title, r.command, r.is_ok, r.output, r.timestamp
                     FROM records r LEFT JOIN tasks t ON r.task_id = t.id
                     ORDER BY r.id""")
    rows = c.fetchall()
    conn.close()
    return rows


def get_students():
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT student FROM records")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]


def clear_student_record(student):
    conn = sqlite3.connect("jiping.db")
    c = conn.cursor()
    c.execute("DELETE FROM records WHERE student=?", (student,))
    conn.commit()
    conn.close()


# ==================== 环境元信息 ====================
ENV_LABEL = {
    "vrp": "🌐 VRP网络(ENSP)",
    "linux": "🐧 Linux(CentOS)",
    "sql": "🗄️ 数据库(MySQL)",
}
ENV_PROMPT = {
    "vrp": "<Huawei>",
    "linux": "[student@localhost ~]$",
    "sql": "mysql>",
}
ENV_ERROR = {
    "vrp": "Error:Unrecognized command found at '^' position.",
    "linux": "-bash: 未找到该命令，请检查拼写",
    "sql": "ERROR 1064 (42000): 你的SQL语法有错误，请检查手册",
}


# ==================== VRP 模拟器 ====================
def vrp_simulator(cmd: str):
    cmd_low = cmd.strip().lower()
    cmd_map = {
        "sys": "Enter system view, return user view with Ctrl+Z.",
        "system-view": "Enter system view, return user view with Ctrl+Z.",
        "q": "Return to user view",
        "quit": "Return to user view",
        "display ip interface brief": """*down: administratively down
^down: standby
(l): loopback
(s): spoofing
Interface                         IP Address/Mask      Physical   Protocol
GigabitEthernet0/0/0              192.168.1.1/24      up         up
GigabitEthernet0/0/1              unassigned          down       down""",
        "display vlan": """VLAN ID  Type  Status  Ports
1        static active  GE0/0/1
10       static active  GE0/0/0""",
        "vlan 10": "Info: VLAN 10 has been created.",
        "interface g0/0/0": "GigabitEthernet0/0/0",
        "interface gigabitethernet 0/0/0": "GigabitEthernet0/0/0",
        "ip address 192.168.1.1 255.255.255.0": "Info: The IP address has been configured successfully.",
        "ospf 1 router-id 1.1.1.1": "Info: OSPF process 1 started.",
        "area 0": "Enter OSPF area 0 view",
        "network 192.168.1.0 0.0.0.255": "Info: Network added to OSPF area 0.",
        "display ospf peer": """OSPF Process 1 with Router ID 1.1.1.1
 Neighbors
 Area 0.0.0.0
 interface 192.168.1.1(GigabitEthernet0/0/0)'s neighbors
 Router ID: 2.2.2.2          State: Full""",
        "vrrp vrid 1 virtual-ip 192.168.1.254": "Info: VRRP group 1 created.",
        "display current-configuration": "Now configuring the running-config ...",
        "save": "The current configuration will be written to flash:/vrpcfg.zip. Continue?[Y/N]Y\nInfo: Save configuration successfully.",
    }
    if cmd_low in cmd_map:
        return True, cmd_map[cmd_low]
    return False, ENV_ERROR["vrp"]


# ==================== Linux 模拟器 ====================
def linux_simulator(cmd: str):
    raw = cmd.strip()
    cmd_low = raw.lower().rstrip("/").strip()
    # 去掉常见 sudo 前缀再匹配
    stripped = cmd_low[5:].strip() if cmd_low.startswith("sudo ") else cmd_low
    cmd_map = {
        "pwd": "/home/student",
        "whoami": "student",
        "hostname": "localhost.localdomain",
        "ls": "Desktop  Documents  Downloads  Music  Pictures  Public  Templates  Videos",
        "ls -l": "total 0\ndrwxr-xr-x 2 student student 4096 Oct 28 10:00 Documents\ndrwxr-xr-x 2 student student 4096 Oct 28 10:00 Downloads",
        "cd documents": "",
        "cd ..": "",
        "cd": "",
        "clear": "",
        "uname -r": "3.10.0-1160.el7.x86_64",
        "cat /etc/os-release": """NAME="CentOS Linux"
VERSION="7 (Core)"
ID="centos"
PRETTY_NAME="CentOS Linux 7 (Core)""",
        "ip addr": """ens33: <BROADCAST,MULTICAST,UP,LOWER_UP>
    inet 192.168.1.10/24 brd 192.168.1.255 scope global ens33
lo:
    inet 127.0.0.1/8 scope host lo""",
        "ip a": "ens33: inet 192.168.1.10/24\nlo: inet 127.0.0.1/8",
        "ifconfig": "ens33: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500\n        inet 192.168.1.10  netmask 255.255.255.0",
        "ping 192.168.1.1 -c 3": """PING 192.168.1.1 (192.168.1.1) 56(84) bytes of data.
64 bytes from 192.168.1.1: icmp_seq=1 ttl=64 time=0.42 ms
64 bytes from 192.168.1.1: icmp_seq=2 ttl=64 time=0.38 ms
64 bytes from 192.168.1.1: icmp_seq=3 ttl=64 time=0.41 ms

--- 192.168.1.1 ping statistics ---
3 packets transmitted, 3 received, 0% packet loss""",
        "systemctl status network": "● network.service - LSB: Bring up/down networking\n   Active: active (exited) since Tue 2026-10-28 10:00:00 CST",
        "systemctl restart network": "==== AUTHENTICATING FOR org.freedesktop.systemd1.manage-units ====\n==== AUTHENTICATION COMPLETE ====",
        "systemctl stop firewalld": "",
        "firewall-cmd --state": "running",
        "netstat -tlnp": "Proto Recv-Q Send-Q Local Address    State   PID/Program\ntcp        0      0 0.0.0.0:22       LISTEN  1234/sshd\ntcp        0      0 127.0.0.1:631    LISTEN  567/cupsd",
        "yum install -y vim": "Complete!",
        "mkdir test": "",
        "touch a.txt": "",
        "reboot": "The system is going down for reboot NOW!",
    }
    if stripped in cmd_map:
        return True, cmd_map[stripped] if cmd_map[stripped] else f"[{ENV_PROMPT['linux']}]"
    return False, ENV_ERROR["linux"]


# ==================== MySQL 模拟器 ====================
def sql_simulator(cmd: str):
    raw = cmd.strip()
    # 去首尾空白和结尾分号，统一小写
    key = raw.lower().rstrip(";").strip()
    cmd_map = {
        "show databases": """+--------------------+
| Database           |
+--------------------+
| information_schema |
| mysql              |
| performance_schema |
| sys                |
| test               |
+--------------------+""",
        "use test": "Database changed",
        "show tables": """+----------------+
| Tables_in_test |
+----------------+
| students       |
| courses        |
+----------------+""",
        "desc students": """+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null| Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| id    | int(11)     | NO   | PRI | NULL    |       |
| name  | varchar(20) | YES  |     | NULL    |       |
| age   | int(11)     | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+""",
        "select * from students": """+----+-----------+------+
| id | name      | age  |
+----+-----------+------+
|  1 | 张三      |   19 |
|  2 | 李四      |   20 |
+----+-----------+------+
2 rows in set (0.00 sec)""",
        "select count(*) from students": """+----------+
| count(*) |
+----------+
|        2 |
+----------+""",
        "create table student (id int, name varchar(20))": "Query OK, 0 rows affected (0.05 sec)",
        "insert into students values (1,'zhangsan',19)": "Query OK, 1 row affected (0.03 sec)",
        "update students set name='王五' where id=1": "Query OK, 1 row affected (0.04 sec)\nRows matched: 1  Changed: 1  Warnings: 0",
        "delete from students where id=2": "Query OK, 1 row affected (0.04 sec)",
        "drop table students": "Query OK, 0 rows affected (0.12 sec)",
        "exit": "Bye",
    }
    if key in cmd_map:
        return True, cmd_map[key]
    return False, ENV_ERROR["sql"]


def simulate(env: str, cmd: str):
    if env == "linux":
        return linux_simulator(cmd)
    if env == "sql":
        return sql_simulator(cmd)
    return vrp_simulator(cmd)


# ==================== 默认任务模板 ====================
DEFAULT_CONTENT = {
    "vrp": """1. 进入系统视图
2. 在G0/0/0配置IP 192.168.1.1/24
3. 启动OSPF进程1，router-id 1.1.1.1
4. 宣告网段到Area 0""",
    "linux": """1. 查看系统版本（cat /etc/os-release）
2. 查看网卡IP（ip addr）
3. 测试网关连通性（ping 192.168.1.1 -c 3）
4. 查看防火墙状态（firewall-cmd --state）""",
    "sql": """1. 查看所有数据库（show databases;）
2. 进入 test 库（use test;）
3. 查看表（show tables;）
4. 查询 students 表数据（select * from students;）""",
}
DEFAULT_MERMAID = """graph LR
    R1[R1] --- GE0/0/0
    GE0/0/0 --- LINK{{链路}}
    LINK --- GE0/0/1
    GE0/0/1 --- R2[R2]
    style R1 fill:#87CEFA
    style R2 fill:#87CEFA
"""


# ==================== 页面配置 ====================
st.set_page_config(page_title="迹评", page_icon="🌊", layout="wide")
init_db()

st.sidebar.title("迹评 EVERY STEP COUNTS")
st.sidebar.caption("作者：张昊 · 信安252")
role = st.sidebar.radio("选择身份", ["学生端", "老师端"])


# ==================== 学生端 ====================
if role == "学生端":
    st.title("学生实操练习")
    student_name = st.text_input("你的名字", value="张昊")
    task_list = get_all_tasks()

    if not task_list:
        st.warning("⚠️ 当前暂无任务，请等待老师发布任务！")
    else:
        task_options = {f"[{ENV_LABEL.get(r[5], r[5])}] {r[1]}": r for r in task_list}
        selected_name = st.selectbox("选择任务", list(task_options.keys()))
        row = task_options[selected_name]
        tid, title, content, graph_code, ctime, env = row

        st.subheader(f"📋 {title}  `{ENV_LABEL.get(env, env)}`")
        st.markdown(content)

        if env == "vrp" and graph_code and graph_code.strip():
            st.markdown("---")
            st.subheader("🗺️ 拓扑图")
            render_mermaid(graph_code)

        st.markdown("---")
        st.subheader(f"💻 终端（{ENV_LABEL.get(env, env)}）")
        cmd = st.text_input(ENV_PROMPT.get(env, ">"), key="term_cmd")
        if cmd.strip():
            ok, out = simulate(env, cmd)
            add_record(student_name, tid, cmd, ok, out)
            st.code(f"{ENV_PROMPT.get(env,'>')} {cmd}", language="bash")
            if ok:
                st.success(out if out else "执行成功")
            else:
                st.error(out)

        st.markdown("---")
        st.subheader("📝 你的操作记录")
        records = get_records(student_name)
        if records:
            for taskname, command, isok, out, ts in records:
                icon = "🟢" if isok else "🔴"
                st.markdown(f"{icon} `{ts}` 【{taskname}】 `{command}`")
                st.code(out)
            if st.button("清空我的记录"):
                clear_student_record(student_name)
                st.rerun()
        else:
            st.info("还没有操作记录，开始操作吧")

# ==================== 老师端 ====================
else:
    st.title("老师管理后台 · 任务发布 & 学情查看")
    tab1, tab2 = st.tabs(["📤 发布新任务", "📊 学情查看"])

    with tab1:
        st.subheader("新建实操任务")
        env = st.selectbox(
            "实操环境",
            ["vrp", "linux", "sql"],
            format_func=lambda x: ENV_LABEL[x],
        )
        task_title = st.text_input("任务名称", value={
            "vrp": "OSPF基础配置（R1-R2直连）",
            "linux": "CentOS7 网络与连通性检查",
            "sql": "MySQL 学生表查询练习",
        }[env], key=f"title_{env}")

        task_content = st.text_area("任务要求（Markdown）", value=DEFAULT_CONTENT[env],
                                    height=160, key=f"content_{env}")

        mermaid_graph = ""
        if env == "vrp":
            st.info("VRP任务可附带Mermaid拓扑图；Linux/数据库任务不需要拓扑图")
            mermaid_graph = st.text_area("Mermaid拓扑代码", value=DEFAULT_MERMAID, height=180, key=f"mmd_{env}")

        if st.button("✅ 发布任务"):
            if task_title.strip():
                add_task(task_title.strip(), task_content, mermaid_graph.strip(), env)
                st.success("任务发布成功！学生端刷新即可看到")
                st.rerun()
            else:
                st.error("任务名称不能为空")

        st.markdown("---")
        st.subheader("已发布任务列表")
        allt = get_all_tasks()
        if allt:
            for tid, title, content, graph, ctime, tenv in allt:
                with st.expander(f"【{ctime}】{ENV_LABEL.get(tenv,tenv)} {title}"):
                    st.markdown(content)
                    if tenv == "vrp" and graph:
                        render_mermaid(graph)
                    if st.button(f"🗑 删除任务", key=f"dt{tid}"):
                        del_task(tid)
                        st.rerun()
        else:
            st.info("暂无任务")

    with tab2:
        st.subheader("学生操作记录")
        students = get_students()
        if not students:
            st.warning("暂无学生记录")
        else:
            stu_sel = st.selectbox("选择学生", students)
            records = get_records(stu_sel)
            total = len(records)
            # 修复：is_ok 在 r[2]，不是 r[3]
            err_cnt = sum(1 for r in records if r[2] == 0)
            ok_cnt = total - err_cnt
            rate = round(ok_cnt / total * 100, 1) if total > 0 else 0
            c1, c2, c3 = st.columns(3)
            c1.metric("总步数", total)
            c2.metric("报错次数", err_cnt)
            c3.metric("正确率", f"{rate}%")

            st.markdown("---")
            for taskname, command, isok, out, ts in records:
                icon = "🟢" if isok else "🔴"
                st.markdown(f"{icon} `{ts}` 【{taskname}】`{command}`")
                st.code(out)

            df = pd.DataFrame(records, columns=["任务", "命令", "是否成功", "输出", "时间"])
            csv = df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button("📥导出CSV", csv, f"{stu_sel}_操作记录.csv")
            if st.button("🗑清空该学生记录"):
                clear_student_record(stu_sel)
                st.rerun()

            st.markdown("---")
            st.subheader("💡教学建议")
            if total == 0:
                st.info("暂无数据")
            elif err_cnt > 3:
                st.warning(f"报错{err_cnt}次，需要针对性辅导")
            elif err_cnt > 0:
                st.info(f"存在{err_cnt}次命令识别错误，建议复盘命令语法")
            else:
                st.success("全部命令输入正确，掌握良好")
