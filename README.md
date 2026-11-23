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
.venv/bin/researcher prepare-models
.venv/bin/researcher index
.venv/bin/researcher run
```

使用顺序：

1. 在“模型与运行设置”保存服务商、模型和密钥，完成连接、联网和研究工具调用测试；测试会实际调用 API。准备本地检索模型并建立原文索引。
2. 在“关注与更新设置”生成约 6 家厂商和 3 个专题的候选名单。逐项打开资料核对，选择或手动补充，确认资料来源范围、模型、额度和本地执行时间。
3. 在“行业动态”手动运行一次检查。首页显示本期覆盖时间、最近成功更新时间、事件、判断修订和后续问题。没有有依据的重大变化时会明确说明；采集失败时保留最近成功结果。
4. 在“探索研究”直接提出目标，并在同一会话继续追问；也可以使用“定向研究”表单明确对象和条件。从事件进入原文、厂商、产品和专题。个人批注只有本次主动勾选后才会发送给 AI，始终不充当事实证据。
5. 需要每日定时运行时，在“关注与更新设置”主动开启。默认显示“手动维护中 · 自动更新待配置”。

首页汇总当地当天的多次检查，保留当天已整理的重要事件；后续检查没有新增变化时也能继续阅读。旧资料与日期未明确的资料单独列为背景补充，认知修订会说明依据来自背景资料或已有证据整理。

## 动态研究与知识积累

探索研究、行业采集、专题整理与后续研究共用 LangGraph 研究流程。模型根据目标和已有证据选择本机知识检索、网络搜索、原文读取、专题检查等工具；每轮根据结果调整问题。完成标准满足、连续两轮没有新资料或达到任务限制时停止，保留停止原因。

文档完整保存并按约512 token、64 token重叠建立片段。SQLite FTS5、sqlite-vec 与 BGE-M3 联合检索，bge-reranker-v2-m3 重新排序。新资料、已取得但待整理的资料分别记录处理状态。模型运行设备在设置中明确指定，默认 `cuda:0`；可选择 `cpu`。模型和依赖缺失会直接报告错误。

```bash
.venv/bin/researcher prepare-models
.venv/bin/researcher index --rebuild
```

模型保存在 `data/models/`；可用 `RESEARCHER_MODELS_DIR` 指定已有模型目录。原文、片段、会话、工具结果、证据核验与知识修订保存在主数据库；`data/search.sqlite3` 是可重建的检索索引。网页正文使用 Trafilatura，文本型 PDF 使用 pypdf，并保存原始响应、版本和页码。无法提取的资料会记录错误。

返回结果逐项关联原文片段。模型分批检查原文支持程度、数值、日期、条件与来源属性，核验结果明确记录为模型判断。原说法部分成立时，仅保存核验明确确认的表述，并保留原草稿及核验说明。通过检查的说法、专题判断、档案字段和关系分别保存；待核实内容和反对证据保留。来源变化或用户指出错误后，相关知识进入重新核验状态。人工核验与模型核验分别记录。

后续问题保存触发依据、重要性和完成标准。已开启自动更新时，后台在确认范围和预算内继续研究，并整理待处理资料；手动任务优先。探索研究页面可以启动或暂停后续问题，也可以反馈内容无关、需要纠正或已经过时。证据仍有效的相同已解决问题可以直接复用研究结果。

每次工具及模型请求记录独立操作标识和返回内容。模型请求发送前保存完整输入，包括时间、证据、剩余额度和结果格式；恢复时使用相同输入，复用已完成操作。任务内容或研究配置改变时要求创建新任务。结果未知或返回内容未通过校验时不会自动重发；任务页面的“继续处理”操作明确授权再次请求，并保留此前的原始返回内容。

相同范围、条件和服务商配置的成功查询默认在6小时内复用，可在研究设置中调整。并发请求共享持久化操作记录。研究规划会看到相关历史查询、来源和仍缺少的依据；近似查询可以选择读取已有结果，再对缺少的条件继续调查。选择“检查最新资料”会主动检查来源；同次任务内已经完成的相同操作仍会复用。页面保留实际检查时间，复用不会延后此前的检查时间。

来源保留地址别名、原文版本和获取范围。完整原文优先于搜索摘要，条件式 HTTP 请求支持 ETag、Last-Modified 和304响应。正文未变化时复用原文片段和索引；不同网站的相同正文合并进入模型上下文，各自出处继续保留。

知识核验同时检查已有事实、适用条件和历史纠错原因，区分新增、沿用、补充证据、修订和冲突。分类、来源说明或展示专题不同会保留为附加信息。已有知识需要重新核验时，仅增加来源不会恢复有效状态。行业检查没有新发现时可以完成；未覆盖对象仍会列出。今日简报保留当天的重要事件，后续检查显示实际新增内容。

历史主题入口的新研究也使用共用研究流程。整理既有资料前停止服务并完成备份，然后运行 `.venv/bin/researcher consolidate-history`。此操作导入旧来源、合并确认相同的知识并核对证据版本，保留旧编号及修订记录；无法确认的证据标为需要重新核验。重复运行不会重复合并已整理的记录。

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

其他服务商尚未在本机配置真实凭据，功能状态以各自的实际测试结果为准。更改地址、模型、思考强度或密钥会清除该配置的验证状态。默认每日最多60次云端请求、自动任务最多36次；今日简报16次、交互研究24次、专题整理12次、候选名单8次、自动后续研究12次。历史任务和连接测试使用通用单任务额度，默认8次、每次生成最多4096输出token。搜索、生成及有限重试均计入额度；本机原文读取与检索分别记录。费用以服务商账单为准，页面估算覆盖服务商返回的token用量。

DeepSeek 的规划与证据核验默认使用高思考强度，分别允许最多8192与32768输出token（包含思考），结构化发现整理默认关闭思考、最多8192输出token；各阶段可以分别配置，通用生成上限用于历史任务及连接测试。核验分批调用原生工具提交结果，保留各批次状态。所有结果继续进行格式、证据编号、摘录和事件日期检查。每次研究默认最多8轮、30分钟，自动后续研究每日最多2项。

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

原文快照、知识与研究检查点包含在数据库一致性备份中，恢复使用流式读取，限制为8GB。模型权重和检索索引需要单独准备；恢复后运行 `researcher index --rebuild`。

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

`tests/research_cases.json` 提供20项基于既有公开资料的检索检查。下面的真实研究验证使用 `RESEARCHER_DATA_DIR` 指定的数据目录；research、scan、revise、resume阶段会实际调用当前模型并保存研究记录：

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_research_check.py --stage retrieval
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_research_check.py --stage research
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_research_check.py --stage resume
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_research_check.py --stage reuse
```

`reuse` 使用最近一次完成且证据仍有效的真实研究，检查再次提问时新增 API 请求为0；`resume` 主动检查最新资料，验证已完成请求在中断后继续使用保存的返回结果。

调研去重检查使用当前真实历史数据库的隔离副本，结束时清理副本。基础检查覆盖历史整理、并发操作、跨任务复用、取消和真实核验结果重复提交。`--http` 读取公开原文并验证 HTTP 缓存和本机模型；`--live` 实际调用当前服务商，检查重复搜索的 API 用量和完整研究后的重复提问。

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_dedup_check.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/live_dedup_check.py --http --live
```

主要代码：`research_engine.py` 运行研究流程；`research_models.py` 定义研究规则和结构化结果；`knowledge.py` 处理原文、检索与领域知识；`research.py` 和 `research_web.py` 管理会话、后续研究及网页入口。原有资料和判断由 `station.py`、`pipeline.py` 维护，`providers.py`、`worker.py` 和 `scheduler.py` 负责服务商、后台执行与调度。第三方说明见 `THIRD_PARTY_NOTICES.md`。
