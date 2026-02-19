# Global Aviation Digest

面向航空公司职员的国际航空新闻日更自动化系统（GitHub Actions）。

## 核心能力
- 每日自动发布 1 篇日报到微信公众号。
- 每篇固定 10 条国际航空新闻。
- 质量闸门：总分 >= 80 才允许发布。
- 阻断规则：来源不可核验、事实冲突、夸张标题、敏感内容、幻觉风险。
- 支持多厂商 OpenAI 兼容 LLM（仅需配置 `LLM_API_KEY/LLM_BASE_URL/LLM_MODEL`）。
- 支持 `RSS + 网站列表页爬虫` 双采集通道（`type: rss/web`）。
- 网页爬虫支持 `requests / playwright / nodriver(+Xvfb)`，可按站点配置回退链路。
- 外国航空公司名称保留英文原名（如 Delta、United、Lufthansa、Emirates 等）。

## 快速开始
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run.py ingest
python run.py rank
python run.py compose
python run.py verify
python run.py publish
python run.py notify
```

## 环境变量
参见 `.env.example`。

## 采集源类型
- `type: rss`：标准 RSS/Atom 源。
- `type: web`：网站列表页爬虫。
  - `fetch_mode: requests | playwright | nodriver | auto`
  - `auto` 默认先 `requests`，失败或疑似空壳页时按 `fallback_order` 回退（例如 `playwright -> nodriver`）。
  - Linux 可启用 `xvfb: true` 配合 `nodriver` 使用虚拟显示（需安装 Xvfb）。
  - 通过 `link_patterns` / `exclude_patterns` / `article_include_keywords` 控制抓取质量。

## 数据契约
- `data/raw/YYYY-MM-DD.json`: `news_item[]`
- `data/processed/ranked_YYYY-MM-DD.json`: 排序结果
- `data/processed/composed_YYYY-MM-DD.json`: `daily_digest`
- `data/processed/quality_YYYY-MM-DD.json`: `quality_report`
- `data/output/publish_YYYY-MM-DD.json`: 发布结果

## 说明
默认开启 dry-run（无真实密钥可跑全流程）；配置微信密钥并关闭 dry-run 后可真实发布。
