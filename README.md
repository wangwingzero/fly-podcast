# Global Aviation Digest

面向航空公司职员的国际航空新闻日更 + 播客自动化系统（GitHub Actions）。

## 核心能力

### 新闻日报
- 每日自动发布 1 篇日报到微信公众号，固定 10 条国际航空新闻。
- 质量闸门：总分 >= 80 才允许发布。
- 阻断规则：来源不可核验、事实冲突、夸张标题、敏感内容、幻觉风险。
- 支持多厂商 OpenAI 兼容 LLM（仅需配置 `LLM_API_KEY/LLM_BASE_URL/LLM_MODEL`）。
- 支持 `RSS + 网站列表页爬虫` 双采集通道。
- 外国航空公司名称保留英文原名（如 Delta、United、Lufthansa、Emirates 等）。

### 播客（PDF 转双人对话音频）
- 将民航法规 PDF 自动转为千羽（女）+ 虎机长（男）双人播客对话。
- 流程：PDF 文字提取 → LLM 生成对话脚本 → qwen-tts2api 语音合成 → ffmpeg 拼接 + 音量标准化 → 上传 R2 → 发布微信草稿。
- 音量标准化：EBU R128 广播标准（-16 LUFS）。

---

## GitHub Actions Workflows

### 1. `daily-digest` — 每日新闻日报

**触发方式：** 每天北京时间 03:00 自动执行，也支持手动触发。

无需传参，自动运行全部 6 个阶段（ingest → rank → compose → verify → publish → notify）。

---

### 2. `podcast-from-pdf` — PDF 转播客（主力工作流）

**触发方式：**
- **自动触发：** 往 `podcast_pdfs/` 文件夹推送新 PDF 时自动运行（只处理新增的 PDF）。
- **手动触发：** GitHub Actions 页面点 "Run workflow"（处理 `podcast_pdfs/` 中所有 PDF）。

**完整流程：** PDF → 对话脚本 → 语音合成 → 上传 R2 → 发布微信草稿箱（全自动）。

#### 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `greeting` | 对话开头的额外提示词（如节日祝福）。会注入到 LLM prompt 中，让主持人在自我介绍后自然说出。 | 空（不加） |
| `pdf_filter` | 只处理文件名包含该关键词的 PDF。 | 空（处理全部） |

#### 使用示例

**场景 1：推送新 PDF 自动制作**
```bash
# 把 PDF 放进 podcast_pdfs/ 文件夹
cp 某个法规.pdf podcast_pdfs/

# 推送到 GitHub，自动触发制作 + 发布到微信草稿箱
git add podcast_pdfs/某个法规.pdf
git commit -m "add 某个法规"
git push
```

**场景 2：手动触发，处理所有 PDF**

在 GitHub Actions 页面 → `podcast-from-pdf` → "Run workflow"，参数留空即可。

**场景 3：只重新制作某一篇**

在 GitHub Actions 页面 → `podcast-from-pdf` → "Run workflow"：
- `pdf_filter`: `全天候`（只匹配文件名含"全天候"的 PDF）

**场景 4：节日特别版（带祝福语）**

在 GitHub Actions 页面 → `podcast-from-pdf` → "Run workflow"：
- `greeting`: `现在是春节期间，在开头自我介绍后，千羽和虎机长自然地互相拜年，祝听众蛇年大吉、飞行顺利、起降安妥，三四句话就好。`
- `pdf_filter`: `全天候`（可选，限定只处理某一篇）

**场景 5：用 gh CLI 触发**
```bash
# 带祝福 + 指定 PDF
gh workflow run podcast-from-pdf.yml \
  -f greeting="祝大家蛇年大吉，飞行顺利！" \
  -f pdf_filter="全天候"

# 重新制作全部
gh workflow run podcast-from-pdf.yml
```

---

### 3. `publish-podcast` — 单独发布播客到微信草稿

**触发方式：** 仅手动触发。用于将已上传到 R2 的播客重新发布到微信草稿箱（正常情况下不需要，`podcast-from-pdf` 已自动发布）。

#### 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `date` | 目标日期（YYYY-MM-DD），从 R2 下载该日期的播客文件。 | 当天（北京时间） |
| `episode_dirs` | 逗号分隔的播客目录名，限定只发布这些。 | 空（发布该日期全部） |

#### 使用示例

```bash
# 重新发布今天的所有播客到微信草稿
gh workflow run publish-podcast.yml

# 发布指定日期
gh workflow run publish-podcast.yml -f date="2025-02-21"
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
