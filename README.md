# 🌊 迹评 · EVERY STEP COUNTS

> 记录每一步操作，看清每一次卡住。

面向职业院校多专业实训的**过程性评价平台**。学生每敲一条命令都会被记录、比对、判分；  
教师实时看到全班卡在哪一步。

覆盖 **8 个专业方向 · 38 个实训场景**，全部配套图由本地 SVG 引擎生成。

---

## ✨ 特性

|            |                                                              |
| ---------- | ------------------------------------------------------------ |
| **38 个实训** | 计算机网络 / Linux 运维 / 数据库 / Python / 软件工程 / 电气自动化 / 数字媒体 / 电子商务 |
| **真实模拟器**  | 带状态的终端：VRP 视图切换、Linux 服务与端口、MySQL 建表生效、Python 真跑代码、电气安全联锁    |
| **本地出图**   | 45 张场景图纯 Python 生成 SVG，**约 0.1ms/张**，零 CDN、零 iframe          |
| **秒级同步**   | 老师发布任务，学生端 **3 秒内**自动出现，不闪屏、不丢状态                             |
| **断点续做**   | 步骤进度由历史命令回算，刷新/换设备进度不丢                                       |
| **学情看板**   | 全班总览 + 单生明细 + 任务维度统计，一键导出 Excel                              |

---

## 🚀 部署到 Streamlit Cloud（推荐）

### 1. Fork / 导入仓库

登录 [share.streamlit.io](https://share.streamlit.io) → **New app** → 选择本仓库 → 主文件填 `迹评_app_v4.py`（也可重命名为 `app.py`）。

### 2. 准备数据库（Turso，免费）

1. 注册 [turso.tech](https://turso.tech)
2. 创建数据库（任意名字，选最近的地域）
3. 记下两项：
   - **Database URL**：`libsql://xxxx-yyyy.turso.io`
   - **Auth Token**：点 *Create Token* 生成

> 表结构会在首次访问时自动创建，无需手动执行 SQL。

### 3. 填入 Secrets

在 Streamlit Cloud 的 **Settings → Secrets** 粘贴：

```toml
TURSO_URL = "libsql://你的库名-你的账号.turso.io"
TURSO_AUTH_TOKEN = "eyJhbGciOi..."
```

保存后应用会自动重启。

---

## 💻 本地运行

```bash
git clone https://github.com/<你的账号>/<仓库名>.git
cd <仓库名>

pip install -r requirements.txt

# 配置密钥
cp .streamlit/secrets.example.toml .streamlit/secrets.toml
# 编辑 secrets.toml 填入真实的 TURSO_URL / TURSO_AUTH_TOKEN

streamlit run app.py
```

浏览器打开 <http://localhost:8501>

---

## 📖 使用流程

### 教师

1. 侧栏切到 **教师端** → 注册 → 登录
2. **班级** 标签：创建班级（如「信安252」），学生用班级名+姓名进入
3. **发布任务** 标签：选方向 → 选模板 → 可微调标题/要求 → 发布
   - 左侧实时预览配套图，可展开「学生视角试跑」验证题目可解
4. **学情分析** 标签：看全班正确率、每人操作数、哪道题卡住最多

### 学生

1. 选班级 → 填姓名 → 进入（无需注册）
2. 选任务 → 看要求与示意图 → 在终端敲命令
3. 右侧步骤清单自动打勾，完成度实时更新
4. 记录随时导出 CSV

---

## 🗂 项目结构

```
app.py                      入口、身份路由、落地页
requirements.txt

jiping/core/
    database.py             Turso HTTP v2 客户端（连接池 / 重试 / 批量查询）
    store.py                数据访问层（缓存 / bump 变更信号 / 学情聚合）
    templates.py            实训模板注册表
    graph.py                本地 SVG 绘图引擎（拓扑/流程/时序/ER/时间轴/柱图）
    figures.py              各方向场景图组装
    sim_utils.py            命令宽容匹配（大小写/空格/简写都能过）
    terminal.py             带状态终端小引擎

jiping/domains/
    dept_cs.py              计算机网络（VRP）
    dept_linux.py           Linux 运维
    dept_db.py              数据库（MySQL）
    dept_python.py          Python（含安全 ast 求值器）
    dept_se.py              软件工程（Git / TDD / UML / 敏捷）
    dept_ee.py              电气自动化（电工 / PLC / 通信 / PCB）
    dept_media.py           数字媒体（AE / PR / 调色 / 摄影 / UI）
    dept_ecom.py            电子商务（运营 / 供应链 / 营销 / 支付）

jiping/ui/
    student.py              学生端
    teacher.py              教师端
    style.py                样式与本地图形组件

.streamlit/
    config.toml             主题与服务器配置
    secrets.example.toml    密钥模板（secrets.toml 已在 .gitignore 中）
```

---

## ➕ 新增一门实训

在对应 `jiping/domains/dept_*.py` 末尾加一条 `register(...)`，页面代码零改动：

```python
from ..core import figures as F
from ..core.templates import register

register("my_new_lab", {
    "label": "🔧 我的实训",          # 教师端下拉显示名
    "group": "软件工程",              # 所属方向（自动分组）
    "badge": "Demo",                 # 小标签
    "prompt": "$",                   # 终端提示符
    "title": "示例实训",
    "preview": "一句话说明练什么",
    "content": "1. 第一步\n2. 第二步",
    "figure": F.fig_git,             # 配套图（可选）
    "fig_hint": "图注",
    "commands": ["git init"],        # 参考命令（可选）
    "steps": [                       # 步骤检查：命中任一关键字即打勾
        ("初始化仓库", ["git init"]),
    ],
    "hints": {"git init": "在项目根目录执行"},   # 出错提示（可选）
    "run": lambda cmd: (True, "done"),          # 命令执行器
})
```

---

## ⚙️ 性能设计说明

| 旧版问题             | 本版方案                                       | 实测              |
| ---------------- | ------------------------------------------ | --------------- |
| Mermaid 出图 2~5 秒 | 本地 SVG 引擎，无网络无 iframe                      | **0.04 ms/张**   |
| 发任务学生端要等 60 秒    | `bumps` 变更表 + 3 秒指纹轮询 + `st.fragment` 局部刷新 | **3.0 秒可见**     |
| 切页面卡顿            | 连接池 + 聚合 SQL + 图缓存                         | **0.7 秒/次切换**   |
| 刷新丢进度            | 步骤由数据库历史命令回算                               | 新会话 **100% 还原** |
| 8 人并发            | 连接池 32 + 自动重试                              | **8/8 成功**      |

---

## 🔐 数据安全

- 教师密码 **SHA-256** 加盐前缀存储
- `.streamlit/secrets.toml` 已列入 `.gitignore`，**不会**被推送到仓库
- 数据库走 HTTPS，Token 仅存于 Streamlit Secrets
- 危险命令（如 `rm -rf /`）在模拟器中被显式拦截

---

## 📄 License

MIT
