# Global Aviation Digest

面向航空公司职员的国际航空新闻日更 + 播客自动化系统（自有服务器 + 宝塔计划任务）。

## 核心能力

### 新闻日报
- 每日自动发布 1 篇日报到微信公众号，不设默认条数，只保留通过高价值筛选的国际航空新闻。
- 质量闸门：总分 >= 80 才允许发布。
- 阻断规则：来源不可核验、事实冲突、夸张标题、敏感内容、幻觉风险。
- 支持多厂商 OpenAI 兼容 LLM（仅需配置 `LLM_API_KEY/LLM_BASE_URL/LLM_MODEL`）。
- 支持 `RSS + 网站列表页爬虫` 双采集通道。
- 外国航空公司名称保留英文原名（如 Delta、United、Lufthansa、Emirates 等）。

### 播客（PDF 转双人对话音频）
- 将民航法规 PDF 自动转为千羽（女）+ 虎机长（男）双人播客对话。
- 流程：PDF 文字提取 → LLM 生成对话脚本 → qwen-tts2api 语音合成 → ffmpeg 拼接 + 音量标准化 → 发布到自托管静态站 → 发布微信草稿。
- 音量标准化：EBU R128 广播标准（-16 LUFS）。

---

## 服务器自动化

GitHub Actions 已停用，workflow 文件保留在 `.github/workflows.disabled/` 作为历史参考。
生产运行迁移到宝塔管理的服务器计划任务，详见 `docs/server-deployment.md`。

### 新闻日报

**触发方式：** 每天北京时间 03:00 在服务器自动执行。

服务器脚本：

```bash
/www/wwwroot/flying-podcast/scripts/server/run_daily_digest.sh
```

流程：

```text
ingest -> rank -> compose -> verify -> publish -> publish-static -> notify
```

日志：

```bash
/www/wwwlogs/flying-podcast/daily_YYYY-MM-DD.log
```

### 播客

播客仍通过服务器 CLI 运行，必要时可在宝塔计划任务中添加独立 shell 任务。

手动处理单个 PDF：

```bash
cd /www/wwwroot/flying-podcast
.venv/bin/python run.py podcast --pdf path/to/file.pdf
```

批量处理 inbox：

```bash
cd /www/wwwroot/flying-podcast
.venv/bin/python run.py podcast-inbox
```

---

## 本地命令

```bash
# 安装依赖
python -m venv .venv
. .venv/Scripts/activate  # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 新闻日报全流程
python run.py all

# 新闻日报单阶段
python run.py ingest [--date YYYY-MM-DD]
python run.py rank [--date YYYY-MM-DD]
python run.py compose [--date YYYY-MM-DD]
python run.py verify [--date YYYY-MM-DD]
python run.py publish [--date YYYY-MM-DD]
python run.py notify [--date YYYY-MM-DD]

# 播客：单个 PDF
python run.py podcast --pdf path/to/file.pdf

# 播客：批量处理 inbox
python run.py podcast-inbox                # 从 CCAR-workflow 拉取 + 本地
python run.py podcast-inbox --local-only   # 只处理 data/podcast_inbox/pending/
python run.py podcast-inbox --dry-run      # 预览，不实际生成

# 播客：发布到微信草稿
python run.py publish-podcast [--date YYYY-MM-DD]
```

## 环境变量

参见 `.env.example`。播客相关的额外变量：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `QWEN_TTS_URL` | qwen-tts2api 服务地址 | `http://localhost:8825` |
| `DASHSCOPE_API_KEY` | DashScope API 密钥（TTS 付费 fallback） | 空 |
| `PODCAST_GREETING` | 播客对话额外提示词（如节日祝福） | 空 |

## 数据契约

- `data/raw/YYYY-MM-DD.json`: 采集的原始新闻
- `data/processed/ranked_YYYY-MM-DD.json`: 排序结果
- `data/processed/composed_YYYY-MM-DD.json`: LLM 摘要
- `data/processed/quality_YYYY-MM-DD.json`: 质量检查
- `data/output/publish_YYYY-MM-DD.json`: 发布结果
- `data/output/podcast/{date}_{name}/`: 播客输出（mp3 + metadata.json + dialogue.html + cover.jpg）

## 说明

默认开启 dry-run（无真实密钥可跑全流程）；配置微信密钥并关闭 dry-run 后可真实发布。
