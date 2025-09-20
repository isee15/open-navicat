# CatAIDBViewer — Copilot 指南

目标

- 实现一个基于 PyQt6 的简化版数据库客户端（类似 Navicat），支持 MySQL、PostgreSQL、SQLite。
- 主要功能：连接管理、SQL 编辑与执行、结果表格展示、表/视图/索引结构查看、基本导出（CSV）、SQL 历史与书签。
- 面向开发者与数据库管理员的轻量桌面工具，优先关注可用性与稳定性，而非覆盖所有高级功能。

范围（首个版本）

- 支持建立和管理多个数据库连接（包含连接测试、加密存储可选）。
- SQL 编辑器：语法高亮、行号、基础自动补全（表名/列名），执行当前 SQL 或选中 SQL，支持多语句执行。
- 结果展示：分页加载、排序、复制/粘贴、导出 CSV。
- 表结构查看：列信息、主外键、索引、创建语句查看。
- 支持事务控制（开始/提交/回滚）和简单错误展示。

技术与依赖

- GUI：PyQt6
- 数据库访问：SQLAlchemy（统一抽象） + 各数据库驱动（mysql-connector-python / PyMySQL、psycopg2-binary、sqlite3）
- 语法高亮：QScintilla 或使用 QPlainTextEdit + 简单高亮器（优先 QScintilla 若许可/体积可接受）
- 打包：PyInstaller（生成 Windows 可执行文件）
- 测试：pytest（核心逻辑和数据库抽象层）

项目结构（建议）

- src/
  - app.py                 # 应用入口
  - main_window.py         # 主窗口与 UI 组合
  - ui/                    # Qt Designer .ui 或自定义 widget
  - db/                    # 数据库抽象层（连接管理，执行器，元数据探查）
  - editor/                # SQL 编辑器相关（高亮器、补全）
  - models/                # 结果数据模型（QAbstractTableModel 实现）
  - utils/                 # 工具函数（CSV 导出，加密、配置）
  - tests/                 # 单元测试

编码规范

- 语言：Python 3.10+
- 风格：遵循 PEP8，类型注解（尽可能）、docstring（函数/类用途）、简单明了的变量命名。
- GUI 代码要与业务逻辑分离：UI 层仅负责展示与事件转发，数据库访问与处理放在 db/ 模块。
- 错误处理：所有数据库操作均需捕获异常并向 UI 传递友好错误信息。

配置与凭据

- 链接配置信息使用本地配置文件（例如 ~/.catdbviewer/config.json 或 Windows 对应目录），敏感信息建议使用可选加密（如基于系统密钥环或对称加密）。

测试与 CI

- 单元测试覆盖数据库抽象层、SQL 执行逻辑与导出功能。
- CI（GitHub Actions）：在 Windows 与 Ubuntu 上运行 pytest，构建可选的 PyInstaller 可执行文件作为构建工件。

贡献指南

- 提交前运行格式化（black）与静态类型检查（mypy 可选）。
- 提交清晰的 PR 描述与对应 issue（若有）。

许可

- 推荐使用 MIT 或 Apache-2.0 许可（在仓库根目录添加 LICENSE 文件）。

后续改进方向（非必须）

- 支持更多导出格式（Excel、JSON）、结果编辑（可写的网格）、表/索引设计器、可视化查询构建器。

--

本文件用于指导项目的目标与基本规范，后续可根据实际实现调整完善。