# 第三方组件

## HTMX 2.0.8

- 项目：https://github.com/bigskysoftware/htmx
- 本地文件：`researcher/static/htmx.min.js`
- 许可证：Zero-Clause BSD（0BSD）
- 原文：https://github.com/bigskysoftware/htmx/blob/v2.0.8/LICENSE

Permission to use, copy, modify, and/or distribute this software for
any purpose with or without fee is hereby granted.
THE SOFTWARE IS PROVIDED “AS IS” AND THE AUTHOR DISCLAIMS ALL
WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE
FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY
DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

## Inter 5.2.8

- 项目：https://github.com/rsms/inter
- 本地文件：`researcher/static/fonts/inter-latin-400.woff2`、`inter-latin-500.woff2`、`inter-latin-600.woff2`
- 许可证：SIL Open Font License 1.1
- 原文：https://github.com/rsms/inter/blob/master/LICENSE.txt
- 来源：https://cdn.jsdelivr.net/fontsource/fonts/inter@5.2.8/

## JetBrains Mono 5.2.8

- 项目：https://github.com/JetBrains/JetBrainsMono
- 本地文件：`researcher/static/fonts/jetbrains-mono-latin-400.woff2`、`jetbrains-mono-latin-500.woff2`
- 许可证：SIL Open Font License 1.1
- 原文：https://github.com/JetBrains/JetBrainsMono/blob/master/OFL.txt
- 来源：https://cdn.jsdelivr.net/fontsource/fonts/jetbrains-mono@5.2.8/

## 研究与检索依赖

- LangGraph、LangChain Text Splitters：MIT，https://github.com/langchain-ai/langgraph 、https://github.com/langchain-ai/langchain
- sqlite-vec：MIT / Apache-2.0，https://github.com/asg017/sqlite-vec
- sentence-transformers：Apache-2.0，https://github.com/huggingface/sentence-transformers
- BGE-M3 模型：MIT，https://huggingface.co/BAAI/bge-m3
- bge-reranker-v2-m3 模型：Apache-2.0，https://huggingface.co/BAAI/bge-reranker-v2-m3
- Trafilatura：Apache-2.0，https://github.com/adbar/trafilatura
- pypdf：BSD-3-Clause，https://github.com/py-pdf/pypdf
- jieba：MIT，https://github.com/fxsjy/jieba
- httpx-sse：MIT，https://github.com/florimondmanca/httpx-sse

Python 依赖通过 requirements.lock 安装。检索模型下载到运行数据目录，用于本机推理。

## 调研去重设计参考

- GPT Researcher：共享已访问来源，https://github.com/assafelovic/gpt-researcher/blob/3582a6841fe7dc801e2a34e34ee408641aea9bce/gpt_researcher/skills/researcher.py
- Graphiti：相同事实、冲突和时间变化的判定，https://github.com/getzep/graphiti/blob/fb60e59d4a208c99e7d0186ee6335d2a257f17f2/graphiti_core/prompts/dedupe_edges.py
