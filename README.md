# Researcher Web

端侧与边缘推理独立 AI 加速器的本机行业研究站。首页汇报行业变化、重要性、竞争影响、当前判断及待核实问题。网页供阅读、判断和写个人批注；后台 worker 负责已确认范围内的采集与整理。

## 使用

项目需要 Linux 和 Python 3.10 及以上版本。当前机器已经安装独立虚拟环境，访问地址为 `http://127.0.0.1:8765`。

```bash
cd /home/felix/researcher-web
.venv/bin/researcher run
```

其他机器可在项目目录运行：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock
.venv/bin/researcher run
```

使用顺序：

1. 在“模型与运行设置”保存服务商、模型和密钥，完成连接与联网测试；测试会实际调用 API。
2. 在“关注与更新设置”生成约 6 家厂商和 3 个专题的候选名单。逐项打开资料核对，选择或手动补充，确认资料来源范围、模型、额度和本地执行时间。
3. 在“行业动态”手动运行一次检查。首页显示本期覆盖时间、最近成功更新时间、事件、判断修订和后续问题。没有有依据的重大变化时会明确说明；采集失败时保留最近成功结果。
4. 从事件进入来源、厂商、产品和专题。个人批注保存在个人区域，只有在“定向研究”中主动勾选，才会发送给 AI；批注始终不充当事实证据。
5. 需要每日定时运行时，在“关注与更新设置”主动开启。默认显示“手动维护中 · 自动更新待配置”。

首页汇总当地当天的多次检查，保留当天已整理的重要事件；后续检查没有新增变化时也能继续阅读。旧资料与日期未明确的资料单独列为背景补充，认知修订会说明依据来自背景资料或已有证据整理。

## 研究记录

- 行业动态按事件聚合，同一公告的不同来源关联到同一事件；新版本或独立商业进展另行记录。事件保留发生日期、发布日期、摘要、重要性、竞争影响以及相关厂商和专题。
- 证据分为“公开披露”“厂商宣称”“分析判断”“待核实”。具体说法关联原始链接、检索内容版本、摘录、获取范围、发布日期和核验状态。服务商提取内容与原网页独立核验是不同状态。人工补充的事件标为待核实。
- 认知地图包含应用场景、厂商、产品、技术路线和软件生态。每条关系保留解释及来源；每个专题分别维护行业竞争、部署决策各一项综合判断，保留支持与反对证据、适用边界、未解决问题及修订前后内容。可在专题页面用已有证据重新整理判断；此操作会说明本次没有采集新资料。
- 定向研究从核实说法、解释事件、检查部署条件、检查专题或调查证据缺口进入，自动填写问题范围和完成标准。每次限定一个主问题、最多三个子问题，记录对象、用途与条件。模型返回带资料编号的要点，网站生成引用链接；没有资料依据的条目进入待核实清单，完成标准未满足时标记部分回答。部署决策缺少应用场景时会要求补充。模型只接收本次明确选择的个人批注。
- 产品对比一次最多三款。性能、模型覆盖、功耗成本、软件迁移和测试条件分别展示；测试条件缺失或不一致时页面不提供排名。
- 旧主题、简报、讨论和成果按原有链接保留为历史资料。旧内容不会自动升级为已核验说法。旧个人观察迁入个人批注，后续研究不会自动读取。

每次采集的检索规划、覆盖检查、资料、事件、专题修订与档案关联分别保存。AI 选择带编号的原文片段，服务器直接保存对应文字；发生日期必须能在所引资料中核对。产品、日期与来源相同的发布消息自动聚合，并保留不同名称的入口；不同版本和独立里程碑继续分别记录。搜索无结果、部分查询失败和全部采集失败分开显示。中断或失败后可继续处理已保存资料，仍使用原任务剩余额度。

单次任务与每日请求额度包含有限重试。当前结构化行业采集使用可返回检索内容的 DeepSeek、智谱或 Moonshot 配置；其他服务商的现有连接与联网测试保留，引用内容不足时不能生成可核对的行业事件。

## 服务商与额度

| 配置 | 当前接口 |
| --- | --- |
| DeepSeek | Search API、Chat Completions；已在本机完成真实连接与联网验证 |
| 智谱 | Web Search API、Chat Completions；需要账户搜索权限 |
| Moonshot | Web Search Pro、Chat Completions；需要对应地域的 API 地址 |
| 通义千问 | DashScope 联网生成与引用；需核实返回的资料内容范围 |
| 火山方舟 | Responses 联网工具与引用；需核实返回的资料内容范围 |
| OpenAI 兼容 | Chat Completions；不提供当前行业采集所需的独立搜索接口 |

其他服务商尚未在本机配置真实凭据，功能状态以各自的实际测试结果为准。更改地址、模型、思考强度或密钥会清除该配置的验证状态。默认每日最多 60 次 API 请求、每个任务最多 8 次、每次生成最多 4096 输出 token。每次搜索、生成及有限重试都计入额度；服务商费用以账单为准。页面的 token 费用估算只覆盖服务商返回用量的请求，不包含搜索工具额外费用。

DeepSeek 的结构化名单、事件提取、判断修订和定向研究结果使用单次请求的非思考模式；开放式回答使用模型设置中的思考强度。所有结构化结果仍须经过资料摘录、编号和来源链接校验。

## 后台服务

```bash
.venv/bin/researcher install-services
systemctl --user enable --now researcher-web researcher-worker
systemctl --user status researcher-web researcher-worker
```

后台 worker 与浏览器独立运行。用户服务仍需处于运行状态；电脑关闭、休眠或服务停止期间不会采集。恢复后，到期的每日检查合并为一次任务，保留此前成功结果。当前项目不修改系统登录策略。更新代码后运行：

```bash
systemctl --user restart researcher-web researcher-worker
```

`GET /health` 显示数据库与 worker 心跳。`data/worker.log` 保留错误类型和任务编号，不记录密钥或请求正文。数据默认保存在项目 `data/`，可用 `RESEARCHER_DATA_DIR` 指定；目录权限为 `700`，数据库、配置和密钥文件权限为 `600`。该目录已加入 Git 忽略规则。

## 备份、恢复和归档

在线备份创建 SQLite 一致快照，默认不包含密钥；文件名必须尚不存在：

```bash
.venv/bin/researcher backup research-backup.zip
```

如确需包含密钥，可添加 `--include-secrets` 并妥善保管。恢复前停止两个服务，确认已有备份：

```bash
systemctl --user stop researcher-web researcher-worker
.venv/bin/researcher restore research-backup.zip --replace
.venv/bin/researcher check
systemctl --user start researcher-web researcher-worker
```

恢复支持旧版和当前版备份，验证数据库完整性与关联，停止未完成任务并关闭自动更新。恢复后检查模型密钥并重新测试连接，再决定是否启用更新。厂商和专题可在各自页面归档；归档保留事件、证据和修订历史，并停止跟踪该对象。

## 验证

独立数据库与真实浏览器检查使用项目 `.work/`，不会修改个人研究数据：

```bash
mkdir -p .work
TMPDIR="$PWD/.work" PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider --basetemp=.work/pytest
TMPDIR="$PWD/.work" PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/browser_check.py
```

浏览器检查需要本机 Chrome，或通过 `RESEARCHER_CHROME` 指定 Chromium。`tests/browser_check.py --live` 检查当前服务的主要页面。真实 AI 验证会产生 API 调用与资料记录，运行后在任务页面查看用量、来源和错误；各服务商应分别测试。

以下检查使用当前已确认的关注范围与模型，调用计入网站额度：

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_check.py --stage research
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_check.py --stage question
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_check.py --stage revise
```

`--stage schedule --simulate-pause` 验证后台暂停、恢复后的到期检查，只安排一次任务，入队后恢复原更新设置。`--stage quota` 检查每日额度阻止新请求并恢复额度；`--stage cancel` 检查取消和 SSE 状态。

主要代码：`app.py` 与 `station_web.py` 处理网页；`station.py`、`pipeline.py` 保存资料、事件和判断；`providers.py`、`worker.py` 处理服务商与后台任务；`scheduler.py` 处理定时检查；`db.py`、`migrations/` 管理数据库版本；`cli.py` 提供启动、备份与恢复。HTMX 2.0.8 已随项目保存，许可证见 `THIRD_PARTY_NOTICES.md`。
