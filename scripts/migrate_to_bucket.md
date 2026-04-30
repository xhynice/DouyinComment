# 抖音资源 → HF Bucket 迁移工具

将抖音 SQLite 数据库中的媒体文件（视频、封面、图片、头像、贴纸）迁移到 HuggingFace Bucket 存储。

## 工作原理

```
SQLite 数据库
    │
    ▼
提取 CDN URL（按 aweme_id 分批，URL 去重）
    │
    ▼
抖音 API 获取签名 URL（DouyinClient 翻页拉取）
    │  ↑
    │  └── --direct 模式跳过此步，直接用数据库原始 URL
    ▼
并发下载（CDN failover + 网络重试）
    │
    ▼
攒批上传 HF Bucket（每 50 个文件一批）
    │
    ▼
更新数据库（URL → MD5 文件名）+ 立即 commit
```

## 架构

```
scripts/
├── douyin_api.py           # API 层：签名算法 + DouyinClient
├── migrate_to_bucket.py    # 迁移层：下载/上传/数据库
├── cookie.txt              # 抖音 Cookie
└── requirements.txt
```

### 职责分离

| 文件 | 职责 | 核心内容 |
|---|---|---|
| `douyin_api.py` | 抖音 Web API | a_bogus 签名算法、`DouyinClient`（所有接口 + 错误处理）、CLI、Web 服务 |
| `migrate_to_bucket.py` | 迁移主程序 | 扫描 DB、URL 去重、并发下载、批量上传、数据库更新 |

`migrate_to_bucket.py` 通过 `DouyinClient` 调用 API，不直接处理签名和网络请求。

## 核心特性

- **分批处理**：按 aweme_id 分批，每批独立 commit，支持断点续传
- **URL 去重**：同一 URL 只下载一次，批量更新所有关联记录
- **CDN failover**：每个文件多个 CDN URL，自动切换
- **并发下载**：6 协程并发（可调）
- **攒批上传**：每 50 个文件一批调用 `batch_bucket_files`
- **上传重试**：失败自动重试 3 次
- **文件校验**：下载后检查文件大小，过滤错误响应
- **HEIC 转换**：自动将 HEIC 转为 JPEG
- **API 错误分类**：限流/过期/服务端错误/网络异常，分别处理
- **实时日志**：带运行时长前缀，实时刷新到磁盘，崩溃不丢失
- **失败追踪**：统一失败收集器，结束时按类型汇总打印
- **httpx 静默**：自动过滤 HTTP 库调试噪音，日志干净

## 安装依赖

```bash
pip install httpx huggingface_hub tqdm pillow pillow-heif

# 或使用 requirements.txt
pip install -r requirements.txt
```

## 数据目录结构

```
data/
├── MS4wLjABAAAA2F.../sqlite.db    # 作者数据库
├── MS4wLjABAAAA8K.../sqlite.db
└── ...

logs/
└── migrate/
    ├── migrate_20260427_165941.log  # 迁移日志（带时间戳 + 失败汇总）
    └── ...
```

### 数据库表结构

| 表 | 字段 | 说明 |
|---|---|---|
| videos | thumb, video, images | 视频封面、视频文件、图集 |
| comments | user_avatar, sticker, image_list | 评论头像、贴纸、图片 |
| replies | user_avatar, sticker, image_list | 回复头像、贴纸、图片 |

## Bucket 目录结构

```
sunset139/douyin/
└── {sec_uid}/
    ├── thumbs/       ← 视频封面（origin_cover）
    ├── videos/       ← 视频文件
    ├── images/       ← 图集 + 评论图片 + 回复图片
    ├── avatars/      ← 评论头像 + 回复头像
    └── stickers/     ← 评论贴纸 + 回复贴纸
```

### 目录映射

| API 类型 | Bucket 目录 | 来源 |
|---|---|---|
| origin_cover | thumbs/ | videos.thumb |
| video | videos/ | videos.video |
| images | images/ | videos.images |
| comment_avatar | avatars/ | comments.user_avatar |
| comment_sticker | stickers/ | comments.sticker |
| comment_image | images/ | comments.image_list |
| reply_avatar | avatars/ | replies.user_avatar |
| reply_sticker | stickers/ | replies.sticker |
| reply_image | images/ | replies.image_list |

## 数据库更新格式

上传成功后，数据库字段从原始 CDN URL 更新为 MD5 文件名：

```sql
-- 更新前
thumb = "['https://p3-pc-sign.douyinpic.com/...']"
video = "['https://v95-web-sz.douyinvod.com/...']"

-- 更新后
thumb = "['0b21a6d438f08dc7daf046c31278092b.jpeg']"
video = "['24342e3d766fb0445b640391ad59495c.mp4']"
```

---

## 使用方法

### 基本用法

```bash
# 设置 HF Token
export HF_TOKEN=hf_xxxxx

# 运行（交互选择数据库，处理所有表）
python migrate_to_bucket.py
```

### 数据库选择

启动时会显示数据库摘要表，输入编号选择要处理的作者：

```
  [SCAN] 发现 2 个作者数据库:

    #   sec_uid                                          videos   comments   replies       大小
    ─── ──────────────────────────────────────────────── ─────── ────────── ───────── ────────
    1   MS4wLjABAAAA2FYVN1jBWaTVasx7wXcsK0rCPfSY...         399      95297     20942    43.6M
    2   MS4wLjABAAAAWKIRK91nF8hLjuza0KVRL50-lcfNH...         146          0         0     0.6M

  输入编号选择（如 1,2 或 all），直接回车 = 全部处理:
```

只有 1 个数据库时自动跳过选择。也可以用 `--author` 跳过交互：

```bash
# 指定作者（前缀匹配）
python migrate_to_bucket.py --author MS4wLjABAAAA2FYVN1jB

# 多个作者
python migrate_to_bucket.py --author MS4wLjABAAAA2FYVN1jB MS4wLjABAAAAWKIRK91n

# 全部
python migrate_to_bucket.py --author all
```

### CLI 参数

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--author` | 指定要处理的作者（sec_uid 前缀匹配），或 `all` | 交互选择 |
| `--data-dir` | 数据目录 | `data` |
| `--tables` | 只处理指定表（`videos` / `comments` / `replies`） | 全部 |
| `--fields` | 只处理指定字段类型（见下方列表） | 全部 |
| `--max-tasks` | 每个作者最多处理多少个 task | 不限制 |
| `--batch-size` | 每批处理的 aweme_id 数量 | 20 |
| `--concurrency` | 并发下载数 | 6 |
| `--dry-run` | 只扫描不上传 | False |
| `--direct` | 跳过 API 签名，直接用数据库原始 URL 下载 | False |

`--fields` 可选值：`origin_cover` / `images` / `video` / `comment_avatar` / `comment_sticker` / `comment_image` / `reply_avatar` / `reply_sticker` / `reply_image`

### 示例

```bash
# 指定作者 + 只处理 videos 表
python migrate_to_bucket.py --author MS4wLjABAAAA2FYVN1jB --tables videos

# 多个作者，dry-run 预览
python migrate_to_bucket.py --author MS4wLjABAAAA2FYVN1jB MS4wLjABAAAAWKIRK91n --dry-run

# 全部作者，只处理 comments 和 replies
python migrate_to_bucket.py --author all --tables comments replies

# 只下载视频文件
python migrate_to_bucket.py --author all --fields video

# 跳过 API 签名，直接用数据库原始 URL 下载
python migrate_to_bucket.py --author all --direct

# 调并发数
python migrate_to_bucket.py --author all --concurrency 10

# 调整批次大小
python migrate_to_bucket.py --author all --tables comments --batch-size 5
```

---

## 分批处理机制

### 工作流程

```
扫描 DB → 提取所有 task → 按 URL 去重 → 按 aweme_id 分批
                                              │
                                              ▼
                              ┌─── 每批循环 ──────────────────┐
                              │                                │
                              │  1. 收集这批的 URL（跳过已处理）  │
                              │  2. 获取签名 URL（API 或 direct）│
                              │  3. 并发下载（CDN failover）     │
                              │  4. 攒批上传（每 50 个文件）     │
                              │  5. 更新 DB + commit            │
                              │  6. 标记已处理                   │
                              └────────────────────────────────┘
```

### 断点续传

- 每批处理完成后立即 commit
- 程序崩溃后重新运行，已处理的 URL 会跳过
- 日志显示进度：`已提交 X/Y 个 URL`

### URL 去重

- 按 `fallback_urls[0]`（首个 CDN URL）分组
- 同一 URL 只下载一次，更新所有关联的数据库记录
- 跨 aweme_id 共享的 URL 在第一批处理时更新所有记录

### 视频预拉取

- 按 sec_uid 只拉一次作品列表，所有批次共用缓存
- 避免每批重复调用视频 API
- `--direct` 模式跳过预拉取

---

## DouyinClient（API 层）

`douyin_api.py` 提供独立的 API 客户端，可单独使用：

### CLI

```bash
# 验证 Cookie
python douyin_api.py verify

# 获取作品详情
python douyin_api.py video 7630088455829370442

# 获取评论
python douyin_api.py comment 7630088455829370442 7630089456955245327

# 获取回复
python douyin_api.py replies 7630088455829370442 7630089456955245327

# 启动 Web 服务
python douyin_api.py serve --port 8080
```

### Python 调用

```python
from douyin_api import DouyinClient

async with DouyinClient(cookie) as client:
    # 单个作品
    result = await client.get_video("7630088455829370442")

    # 翻页拉取（返回原始 API 数据）
    videos = await client.fetch_all_videos(sec_uid)
    comments = await client.fetch_all_comments(aweme_id)
    replies = await client.fetch_all_replies(aweme_id, comment_id)

    # 验证 Cookie
    nickname = await client.verify()
```

### 异常类

| 异常 | 含义 | 触发条件 |
|---|---|---|
| `CookieExpiredError` | Cookie 过期或无效 | 401/403/返回 HTML/status_code=8 |
| `APIRateLimitError` | API 限流 | HTTP 429 |
| `APIServerError` | 服务端错误或网络异常 | 5xx / ConnectError / Timeout |
| `APIError` | 其他业务错误 | status_code != 0 且非上述情况 |

---

## 任务估算

| 表 | 典型任务数 | 主要瓶颈 |
|---|---|---|
| videos | ~1,000 | 下载大文件 |
| comments | ~100,000 | API 限速（~3秒/次） |
| replies | ~20,000 | API 限速 |

**全量预估**（12 万 task）：~5.5 小时

## 运行输出示例

```
2026-04-27 22:00:00 | 0s | ======================================================================
2026-04-27 22:00:00 | 0s |   [INIT] 抖音资源 → HF Bucket 迁移（v3）
2026-04-27 22:00:00 | 0s |   [INIT] 数据目录:    data
2026-04-27 22:00:00 | 0s |   [INIT] 作者过滤:    all
2026-04-27 22:00:00 | 0s |   [INIT] 限制表:      全部
2026-04-27 22:00:00 | 0s |   [INIT] Dry-run:     否
2026-04-27 22:00:00 | 0s | ======================================================================
2026-04-27 22:00:01 | 1s |
2026-04-27 22:00:01 | 1s |   [SCAN] 发现 2 个作者数据库:
2026-04-27 22:00:01 | 1s |
2026-04-27 22:00:01 | 1s |     #   sec_uid                                          videos   comments   replies       大小
2026-04-27 22:00:01 | 1s |   ─── ──────────────────────────────────────────────── ─────── ────────── ───────── ────────
2026-04-27 22:00:01 | 1s |     1   MS4wLjABAAAA2FYVN1jBWaTVasx7wXcsK0rCPfSY...         399      95297     20942    43.6M
2026-04-27 22:00:01 | 1s |     2   MS4wLjABAAAAWKIRK91nF8hLjuza0KVRL50-lcfNH...         146          0         0     0.6M
2026-04-27 22:00:05 | 5s |   [SCAN] 已选择 1 个数据库:
2026-04-27 22:00:05 | 5s |   [SCAN] 📁 MS4wLjABAAAA2FYVN1jB... (videos=399, comments=95297, replies=20942, 43.6 MB)
...
2026-04-27 22:30:47 | 1847s | ======================================================================
2026-04-27 22:30:47 | 1847s |   全部完成
2026-04-27 22:30:47 | 1847s | ======================================================================
2026-04-27 22:30:47 | 1847s |   作者数:        1
2026-04-27 22:30:47 | 1847s |   总 task:       104488
2026-04-27 22:30:47 | 1847s |   上传成功:      4687
2026-04-27 22:30:47 | 1847s |   失败:          24
2026-04-27 22:30:47 | 1847s |   总耗时:        1847s (30m47s)
2026-04-27 22:30:47 | 1847s | ======================================================================
```

## 错误处理

| 场景 | 行为 |
|---|---|
| API 限流 (429) | 程序立即终止，记录日志 + 失败汇总 |
| Cookie 过期 | 程序立即终止，记录日志 + 失败汇总 |
| 服务端错误 (5xx) | 程序立即终止，记录日志 + 失败汇总 |
| 网络异常（连接失败/超时） | 程序立即终止，记录日志 + 失败汇总 |
| 单个文件下载失败 | `record_failure` 记录原因，task 标记 failed，继续 |
| 单批上传失败 | 重试 3 次，失败后 `record_failure` 记录文件名+原因，继续 |
| DB commit 失败 | `record_failure` 记录，已上传文件成为孤儿 |
| 进程崩溃 | 每批已 commit 的数据不丢失，打印失败汇总 + 完整堆栈 |
| 用户中断 (Ctrl+C) | 记录日志 + 失败汇总后退出 |

## 日志文件

- 日志位置：`logs/migrate/migrate_YYYYMMDD_HHMMSS.log`
- 实时刷新：每次日志输出后立即写入磁盘
- 双重前缀：每行带时间戳 + 运行时长（`2026-04-27 22:00:00 | 5s |`）
- httpx 静默：自动过滤 HTTP 库调试输出，只保留 WARNING+

### 日志格式示例

```
2026-04-27 22:00:00 | 0s | [INIT] 日志文件: logs/migrate/migrate_20260427_220000.log
2026-04-27 22:00:01 | 1s | ✅ Cookie 有效，用户: 小黄鸭剪辑
2026-04-27 22:00:05 | 5s |   [SCAN] 总 task 数: 104488
2026-04-27 22:00:08 | 8s |   [BATCH] 📦 批次 1/20: 20 个 aweme_id, 1777 个 URL
2026-04-27 22:02:35 | 155s |   [BATCH] ✅ 批次完成，已提交 18994/39568 个 URL，耗时 142.3s
```

### 失败汇总

程序结束时自动打印失败汇总表：

```
──────────────────────────────────────────────────────────────────────
  失败汇总 (共 24 项)
──────────────────────────────────────────────────────────────────────
    签名缺失: 18
    下载失败: 4
    上传失败: 2
    • 签名缺失 | comments.id=82380 api_type=comment_image | 原因=cache_key=...
    • 下载失败 | comments.id=22157 api_type=comment_sticker | 原因=所有CDN均失败
──────────────────────────────────────────────────────────────────────
```

### 异常记录

`record_failure()` 统一收集所有失败，分类包括：

| 类型 | 含义 |
|---|---|
| 签名缺失 | API 未返回该资源的签名 URL |
| 下载失败 | 所有 CDN URL 均下载失败 |
| 上传失败 | HF Bucket 上传失败（含重试 3 次） |
| 数据库提交失败 | SQLite commit 异常 |

## 配置项

在 `migrate_to_bucket.py` 顶部修改：

```python
BUCKET_ID = "sunset139/douyin"   # Bucket ID
BATCH_SIZE = 20                  # 每批处理的 aweme_id 数量
CONCURRENCY = 6                  # 并发下载数
UPLOAD_BATCH_SIZE = 50           # 每批上传文件数
UPLOAD_BATCH_TIMEOUT = 5         # 攒批超时（秒）
```
