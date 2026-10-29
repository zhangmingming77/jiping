# 迹评 EVERY STEP COUNTS

> 计算机专业多环境实训模拟器 · 华为ENSP / Linux / MySQL 三合一

作者：张昊 · 信安252

## 这是什么

一个基于 Streamlit 的本地实训平台，让学生在浏览器里模拟真实命令行操作：

- 🌐 **华为 VRP / ENSP**：配置 IP、VLAN、OSPF、VRRP，带拓扑图
- 🐧 **Linux (CentOS)**：查看系统、网络配置、服务管理、防火墙
- 🗄️ **MySQL 数据库**：建库建表、增删改查

老师端发布任务，学生端选任务、敲命令，系统返回和真实环境一致的回显/报错，自动记录操作时间线、正确率，可导出 CSV。

## 功能截图

（待补充）

## 本地运行

```bash
pip install streamlit pandas
streamlit run 迹评_app.py
```

或者双击 `启动迹评.bat`（Windows）。

## 项目结构

```
迹评/
├── 迹评_app.py          # 主程序
├── 启动迹评.bat         # Windows 双击启动
├── static/
│   └── mermaid.min.js   # 离线拓扑图库（CDN 被墙时用）
├── jiping.db           # SQLite 数据库（任务+记录，运行后自动生成）
└── README.md
```

## 技术栈

- Python 3.13
- Streamlit
- SQLite3
- Mermaid.js（拓扑图渲染）

## License

MIT
