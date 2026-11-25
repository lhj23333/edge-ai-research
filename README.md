# edge-ai-research

端侧与边缘推理 AI 加速器的确定性情报雷达，按已启用信源、发布日期和筛选规则生成行业动态与每日简报。LangGraph 提供按需深度研究，优先使用已归档原文，支持事件核验、定向研究和续问。

## 页面概览

### 前台工作台

- `/` 今日简报，`/updates` 行业动态与手动采集，`/events/{id}` 事件来源与深度核验。
- `/research` 探索研究与续问，`/map` 厂商、产品和专题关系，`/compare` 产品对比。
- `/evidence` 原文与证据，`/personal` 个人批注、成果与历史简报。

### 后台运营台

- `/admin/sources` 信源配置、预览与启停，`/admin/funnel` 筛选诊断、人工评分与事件归属修正。
- `/admin/eval` SelectBench 开发集与留出集，`/settings` 模型、路由、额度和运行设置。

## 目录结构

```text
.
|-- industry/                行业配置
|   |-- sources.json         内置信源
|   |-- selection.json       筛选与聚簇规则
|   |-- gold.jsonl           SelectBench 标注
|   `-- prompts/             采集与深研提示词
|-- researcher/              应用源码
|   |-- ingest/               信源采集、时效闸门、筛选漏斗与聚簇
|   |-- app.py                FastAPI 入口与页面
|   |-- cli.py                启动与维护命令
|   |-- config.py             本地配置与模型路由
|   |-- providers.py          模型请求与费用额度
|   |-- worker.py             后台队列与调度
|   |-- research_engine.py    LangGraph 深研流程
|   |-- research_review.py    证据核验
|   |-- knowledge.py          原文提取、索引与检索
|   |-- migrations/           SQLite 迁移 001–010
|   |-- static/               前端脚本与样式
|   `-- templates/            Jinja2 页面
|-- tests/                   自动化测试、验收脚本与夹具
|-- pyproject.toml           项目配置与命令入口
`-- requirements.lock        运行与测试依赖
```

## 快速开始

在 Linux 和 Python 3.10+ 环境中，从仓库根目录执行。

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock
.venv/bin/researcher prepare-models
.venv/bin/researcher index --rebuild
.venv/bin/researcher run
```

本地模型默认使用 `cuda:0`。使用 CPU 时，先启动应用，在 `/settings` 将设备设为 `cpu`，再准备模型并重建索引。

打开 `http://127.0.0.1:8765`，在 `/settings` 保存并验证模型配置，在 `/admin/sources` 预览并启用信源，再到 `/updates` 触发采集。内置信源默认停用。

## 配置与关键机制

- **数据与模型**　`RESEARCHER_DATA_DIR` 默认 `data/`，`RESEARCHER_MODELS_DIR` 默认数据目录下的 `models/`。`RESEARCHER_INDUSTRY_DIR` 可指定行业配置目录。文档索引使用 FTS5、BGE-M3 和 sqlite-vec，检索使用 bge-reranker-v2-m3 重排。
- **运行档位**　极省、均衡和深度分别最多检索 4、10、20 个网页，最多审阅 1、2、3 轮。均衡是默认档位。极省和均衡使用规则模板生成简报，深度档位可用模型润色，用户可在高级设置中调整。
- **六个模型槽位**　`prefilter` 预筛、`score` 评分、`cluster` 摘要与聚簇、`deep_plan` 深研规划、`deep_write` 深研撰写、`deep_review` 深研审阅。档位优先使用已验证的对应层级模型。高级设置可单独覆盖每个槽位。在 `/admin/eval` 可用开发集或留出集，对比预筛与评分的准确率、Token 和估算费用。
- **费用红线**　高级设置可填写每日与每月总费用红线，按配置时区的自然日和自然月统计。可在模型配置中填写 Token 与搜索单价。发送前按预估费用预留，收到回执后更新账本。触及红线或审阅上限时，深研保存已有草稿并标记原因。定时专题研究每日共享 8、16 或 24 次调用配额，重试占用配额，任务超时后释放执行资格。
- **时效与复用**　缺少有效发布日期的资料暂缓公开，空正文、跳转链接、过短或重复内容跳过评分。内容 SHA-256 用于复用向量与摘要。聚簇和深研先用本地检索取得相关事件。专题整理与事件核验在底层证据版本未变化时复用已有结果。
- **深研与批注**　定时研究使用本地归档，符合条件的手动深研可按需联网。个人批注仅在本轮勾选后进入模型上下文，使用批注的结果独立保存，公开证据和专题判断使用公开资料。

## 后台服务、备份与测试

### 后台服务

```bash
.venv/bin/researcher install-services
systemctl --user enable --now researcher-web researcher-worker
systemctl --user status researcher-web researcher-worker
```

Web 与 worker 独立运行。升级代码或模板后执行 `systemctl --user restart researcher-web researcher-worker`，使两个进程使用同一版本。`/health` 显示 Web 构建版本、数据库迁移版本和 worker 心跳，任务错误可在任务详情与 `data/worker.log` 查看。

### 备份与恢复

```bash
mkdir -p backups
.venv/bin/researcher backup backups/research.zip
systemctl --user stop researcher-web researcher-worker
.venv/bin/researcher restore backups/research.zip --replace
.venv/bin/researcher check
.venv/bin/researcher index --rebuild
systemctl --user start researcher-web researcher-worker
```

备份目标须为新文件。备份默认省略 API 密钥，`--include-secrets` 可将其一并备份。恢复后检查模型连接与自动调度。

### 测试

```bash
mkdir -p .work
TMPDIR="$PWD/.work" HF_HUB_OFFLINE=1 RESEARCHER_MODELS_DIR="$PWD/data/models" \
  .venv/bin/python -m pytest -p no:cacheprovider -ra
.venv/bin/python tests/browser_check.py
```

测试使用 `.work/` 中的独立数据目录。pytest 需要 OpenSSL 和已缓存的 BGE-M3。使用自定义模型目录时，将 `RESEARCHER_MODELS_DIR` 和 `RESEARCHER_TEST_MODELS_DIR` 指向该目录。浏览器验收默认使用 `/usr/bin/google-chrome`，可用 `RESEARCHER_CHROME` 指定可执行文件。

第三方组件见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
