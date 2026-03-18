# GCR-AI-Tour-2026

本仓库包含多个可运行 Lab，当前主要内容如下：

- `Lab-01-Tech-Insights/`：基于 RSS/Sitemap/HTML 的技术动态聚合与洞察 Lab
- `Lab-03-GitHub-Copilot/`：围绕 GitHub Copilot 与 Copilot SDK 的 PPT 生成 Lab

## Lab-01-Tech-Insights（你将做什么）

这是一个基于 RSS/Sitemap/HTML 的「技术动态聚合与洞察」Lab：抓取多源更新 → 归一为信号 → 聚类热点 → 生成洞察与 Markdown 报告。

你将得到：
- `report.md`：一份可阅读的技术洞察报告
- `raw_signals.json` / `clusters/hotspots.json` / `insights/insights.json`：可回放的中间产物（便于调试与复现实验）

- 入口文档：`Lab-01-Tech-Insights/README.md`
- 本地运行：先 `cd Lab-01-Tech-Insights`，再按文档执行 `./scripts/install_deps.sh` 或直接运行 `generated/tech_insight_workflow/run.py`

最短本地验证（mock，不消耗 Azure 额度）：

```bash
cd Lab-01-Tech-Insights
./scripts/install_deps.sh --python-only
python3 generated/tech_insight_workflow/run.py --mock-agents --non-interactive
```

> 说明：`.github/workflows` 仍保留在仓库根目录（GitHub Actions 规范要求）。

## Lab-03-GitHub-Copilot（你将做什么）

这是一个用于学习 GitHub Copilot 与 Copilot SDK 的 Lab，聚焦“把网页内容生成为 PowerPoint”。

你将体验两个场景：
- 在 VS Code 聊天窗口中，通过自然语言触发 `url2ppt` 和 `pptx` skill，把单个网页 URL 直接转换为 PPT
- 在 Next.js Web 应用中，由前端发起请求、后端通过 Copilot SDK 生成 PPT，并将进度流式返回给页面

你将得到：
- 一份由网页内容整理生成的 `.pptx` 演示文稿
- 一个可本地运行的 Copilot SDK 示例 Web 应用

- 入口文档：`Lab-03-GitHub-Copilot/README.md`
- 本地运行：先 `cd Lab-03-GitHub-Copilot`，复制 `.env.example` 为 `.env` 并填写 `COPILOT_GITHUB_TOKEN`，然后执行 `npm install` 和 `npm run dev`
- 默认访问地址：`http://localhost:3000`

最短本地启动：

```bash
cd Lab-03-GitHub-Copilot
cp .env.example .env
# 填写 COPILOT_GITHUB_TOKEN 后继续
npm install
npm run dev
```

