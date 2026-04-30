# douyin-comment

> 抖音用户作品与评论数据采集器 — API 分页拉取，评论/回复全量采集，CSV + SQLite 存储 + 媒体下载。

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)

## 目录

- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [核心机制](#核心机制)
  - [数据采集流程](#数据采集流程)
  - [双重存储与去重](#双重存储与去重)
  - [媒体下载与 URL 替换](#媒体下载与-url-替换)
  - [多用户采集](#多用户采集)
- [配置说明](#配置说明)
- [数据输出](#数据输出)
- [性能优化](#性能优化)
- [常见问题](#常见问题)
- [项目结构](#项目结构)

---

## 功能特性

- **作品采集** — 拉取用户全部作品（图片/视频/缩略图），支持数量限制
- **评论采集** — 按作品分页拉取全部评论，提取内容、用户、IP 归属地等字段
- **回复采集** — 自动识别有回复的评论，并发拉取全部回复
- **媒体下载** — 图片、头像、缩略图本地化，URL 自动回写
- **双重存储** — CSV（Excel 直接打开）+ SQLite（去重 + 查询）
- **Cookie 验证** — 启动时验证登录状态，过期自动报错退出
- **纯 Python 签名** — SM3 + RC4 + 自定义 Base64，无需 Node.js

---

## 快速开始

### 1. 环境要求

- Python 3.11+

### 2. 安装

```bash
git clone https://github.com/<your-username>/douyin-comment.git
cd douyin-comment
pip install -r requirements.txt
```

### 3. 配置

**步骤 1：配置用户**
编辑 `config.yaml`，填写目标用户的 `sec_uid`（从用户主页 URL 获取）

**步骤 2：配置 Cookie**
将登录 Cookie 粘贴到 `cookie.txt`

### 4. 运行

```bash
# 全量采集：作品 → 评论 → 回复
python main.py --all

# 限制采集最新 10 个作品的评论
python main.py --all --limit 10

# 跳过已采集数据（断点续采）
python main.py --all --skip-existing

# 单独采集
python main.py video
python main.py comment
python main.py reply

# 仅下载媒体（不采集新数据）
python main.py --all --download-only
python main.py video --download-only

# 指定用户（覆盖配置文件）
python main.py video --sec-uid MS4wLjABAAAA...
```

### 5. 命令行参数

| 参数 | 说明 |
|------|------|
| `video` / `comment` / `reply` | 采集类型 |
| `--all` | 全量采集：作品 → 评论 → 回复 |
| `--download-only` | 仅下载媒体，不采集数据 |
| `--limit N` | 限制采集视频数量 |
| `--skip-existing` | 跳过已有数据的视频/评论 |
| `--sec-uid ID` | 指定用户 sec_uid（覆盖配置文件） |

---

## 核心机制

### 数据采集流程

```
1. 加载配置       → config.yaml + cookie.txt
2. Cookie 验证    → 访问 live.douyin.com 检查 isLogin，失败则退出
3. 采集作品       → API 分页拉取 → 去重 (SQLite/CSV) → 双写
4. 采集评论       → 逐作品调用评论 API → 去重 → 双写
5. 采集回复       → 识别有回复的评论 → 串行拉取 → 去重 → 双写
6. 下载媒体       → 异步下载 → URL 回写 CSV + SQLite
```

### 双重存储与去重

**存储架构**：
```
SQLite（主存储）          CSV（辅助查看）
├─ 去重：INSERT OR IGNORE  ├─ 按视频分文件
├─ 索引：O(log n) 查询     ├─ Excel 直接打开
└─ 连接池：复用连接        └─ 按日期组织目录
```

**去重流程**：
```
1. 内存去重     → 从 SQLite 加载已有 ID 到缓存（首次采集时）
2. 过滤新数据   → 只保留未采集过的记录
3. 写入 SQLite  → INSERT OR IGNORE 兜底（幂等性保证）
4. 写入 CSV     → 按视频分文件追加写入
5. 更新缓存     → 同步更新内存缓存，提升后续写入性能
```

**批量采集优化**：
- 首次采集：全量加载已有 ID 到缓存（一次查询）
- 后续采集：直接使用内存缓存（O(1) 查找）
- 跨数据类型共享：Video/Comment/Reply 共享同一 SQLite 连接和缓存

### 媒体下载与 URL 替换

**下载策略**：
- **按需下载**：仅下载字段中包含 `http://` 或 `https://` 的记录
- **去重下载**：基于文件内容 MD5 去重，相同文件只保存一份
- **并发控制**：信号量限制最大并发数（默认 6 线程）
- **失败重试**：网络错误自动重试 2 次，其他错误直接跳过

**URL 替换流程**：
```
1. 加载数据     → 从 CSV 读取记录
2. 筛选记录     → 只处理包含未下载 URL 的记录
3. 下载媒体     → 异步下载 + 内容去重 + HEIC 自动转 JPEG
4. 批量更新     → 每 100 条批量回写 URL 到 CSV + SQLite
5. 进度保存     → 记录已下载视频，支持断点续传
```

**字段映射**：
```
images        → upload/{sec_uid}/images/{年份}/
video         → upload/{sec_uid}/videos/{年份}/
thumb         → upload/{sec_uid}/thumbs/{年份}/
user_avatar   → upload/{sec_uid}/avatars/{年份}/
sticker       → upload/{sec_uid}/stickers/{年份}/
```

### 多用户采集

**用户隔离**：
```
每个用户独立：
├─ 数据目录：data/{sec_uid}/
├─ SQLite 数据库：data/{sec_uid}/sqlite.db
├─ 媒体目录：upload/{sec_uid}/
└─ CSV 文件：按视频分文件存储
```

**资源复用**：
```
全局单例：
└─ API 实例 → 所有用户共享同一个 HTTP 客户端

按用户单例：
├─ 数据库连接 → 同一用户的 Video/Comment/Reply 共享连接池
└─ 下载器实例 → 同一用户的媒体下载共享信号量和客户端
```

**采集模式**：

1. **单用户模式**
   ```bash
   python main.py video --sec-uid "MS4wLjABAAAA..."
   ```

2. **多用户轮询模式**（config.yaml 配置多个用户）
   ```bash
   python main.py --all
   
   # 执行顺序：
   # 第一阶段：采集所有用户的作品
   #   → 用户 A → 用户 B → 用户 C
   # 第二阶段：采集所有用户的评论
   #   → 用户 A → 用户 B → 用户 C
   # 第三阶段：采集所有用户的回复
   #   → 用户 A → 用户 B → 用户 C
   ```

3. **断点续采**
   ```bash
   python main.py --all --skip-existing
   
   # 跳过逻辑：
   # - 作品：检查本地 videos.csv 是否存在
   # - 评论：跳过已有评论的视频（检查 comments.csv）
   # - 回复：跳过已有回复的评论（检查 replies.csv）
   ```

**Cookie 风控策略**：
- 阶梯式暂停：15 分钟 → 30 分钟 → 60 分钟 → 退出
- 错误计数全局累计（共享 API 实例）
- 建议多用户采集时设置合理的 `request_delay`（推荐 1.5-2 秒）

---

## 配置说明

### 用户配置（config.yaml）

```yaml
# 用户列表
users:
  - enabled: true
    sec_uid: "MS4wLjABAAAA..."
    nickname: "用户名"
    videos: true              # 是否采集作品
    comments: true            # 是否采集评论
    replies: true             # 是否采集回复

# SQLite 存储（true=CSV+SQLite 双写，false=仅 CSV）
sqlite: true

# 采集参数（page_size、timeout 和 max_retries 已改为硬编码：page_size=18, timeout=60, max_retries=3）
crawler:
  request_delay: 1            # 请求间隔（秒），自动加随机抖动
  download_threads: 6         # 媒体下载并发数
```

### 字段配置

`fields` 控制保存哪些字段到 CSV。以 `#` 开头的行会被忽略（不保存）：

```yaml
fields:
  comment:
    - aweme_id                # 作品 ID
    - cid                     # 评论 ID
    - text                    # 评论内容
    - image_list              # 图片列表
    - digg_count              # 点赞数
    - create_time             # 创建时间
    - user_nickname           # 用户昵称
    - user_unique_id          # 用户 ID
    - user_avatar             # 用户头像
    - sticker                 # 表情包
    - reply_comment_total     # 回复数
    - ip_label                # IP 归属地
    - #digg_count             # 点赞数（示例：如何忽略）
```

### 媒体下载配置

```yaml
media_download:
  video:
    images: true              # 作品图片
    videos: true              # 作品视频
    thumbs: true              # 缩略图
  comment:
    images: true              # 评论图片
    avatars: true             # 用户头像
    stickers: true            # 表情包
  reply:
    images: true              # 回复图片
    avatars: true             # 用户头像
    stickers: true            # 表情包
```

### Cookie 配置

登录 Cookie 是采集的必要条件，用于获取评论等需要登录态的数据。

**获取方式**：
1. 浏览器登录 [抖音](https://www.douyin.com)
2. 打开开发者工具 → Application → Cookies → `douyin.com`
3. 全选复制所有 Cookie，粘贴到项目根目录 `cookie.txt`

**格式**：
```
name1=value1; name2=value2; name3=value3
```

> ⚠️ Cookie 包含敏感信息，请勿分享或提交到版本控制。

---

## 数据输出

### 目录结构

```
data/{sec_uid}/
├── videos.csv                            # 作品列表
├── sqlite.db                             # SQLite 数据库
├── 2026-04/
│   ├── 7431130201108581692/
│   │   ├── comments.csv                  # 评论数据
│   │   └── replies.csv                   # 回复数据
│   └── 7597388357547118235/
│       ├── comments.csv
│       └── replies.csv

upload/{sec_uid}/                         # 仅 --download-only 时创建
├── images/{年份}/
├── videos/{年份}/
├── thumbs/{年份}/
├── avatars/{年份}/
└── stickers/{年份}/
```

### CSV 字段

**videos.csv**
```
aweme_id, desc, create_time, images, video, thumb
```

**comments.csv**
```
aweme_id, cid, text, create_time, user_nickname, user_avatar, 
reply_comment_total, ip_label
```

**replies.csv**
```
aweme_id, cid, reply_id, reply_to_reply_id, text, create_time, 
user_nickname, reply_to_username, ip_label
```

---

## 性能优化

### 内存缓存
- 首次采集时从 SQLite 加载已有 ID 到内存缓存
- 后续写入先检查缓存，避免重复查询数据库
- 缓存自动同步：CSV 和 DB 写入后同时更新两个缓存

### 批量操作
- 媒体下载：每 100 条批量回写 URL，减少 I/O 次数
- 数据库插入：使用 `executemany` 批量插入
- 评论采集：按视频批量获取，逐条处理

### 异步处理
- 头像下载：后台异步任务，不阻塞主流程
- 媒体下载：异步并发，信号量控制最大连接数
- 资源清理：程序退出时自动关闭所有连接

### 计数优化（P0 优化）
- CSV 和 DB 计数逻辑统一
- 二次去重检查：写入前再次过滤已有记录
- 日志输出准确，避免用户困惑

### 特殊场景处理

**HEIC 图片处理**：
- 自动检测 HEIC 格式（iPhone 拍摄）
- 使用 `pillow-heif` 转换为 JPEG
- 转换后删除原文件，节省空间

**空响应处理**：
- API 返回空数据时自动重试（最多 3 次）
- 首页空数据检测：Cookie 过期直接退出
- JSON 解析失败时记录响应内容

**并发控制**：
- 下载器：信号量限制最大并发数（默认 6）
- 数据库：连接池限制最大连接数（默认 5）
- API 请求：请求间隔 + 随机抖动（默认 1.5s ± 30%）

---

## 常见问题

### Cookie 过期

程序启动时会验证 Cookie。过期或无效时直接退出并提示更新 `cookie.txt`。

### 签名失效

`core/sign.py` 实现了抖音 Web 端的参数签名，随抖音版本更新可能失效。签名失败时日志会输出警告。

### 评论采集很慢

`config.yaml` 中 `request_delay` 控制请求间隔（默认 1 秒），过快可能被限流。回复采集采用串行方式逐条拉取。

### HEIC 图片无法打开

需要安装 `pillow-heif`（已在 requirements.txt），程序会自动将 HEIC 转换为 JPEG。

---

## 项目结构

```
├── main.py                  启动入口 + 命令行解析
├── config.yaml              运行配置
├── cookie.txt               登录 Cookie
├── requirements.txt         Python 依赖
├── core/
│   ├── api.py               抖音 API 封装（Cookie 验证 + 请求 + 签名调用）
│   ├── sign.py              纯 Python 签名（SM3 + RC4 + 自定义 Base64）
│   ├── database.py          SQLite 连接池 + 建表
│   ├── downloader.py        媒体文件异步下载
│   └── logger.py            系统日志
├── services/
│   ├── base_service.py      采集基类
│   ├── video_service.py     作品采集
│   ├── comment_service.py   评论采集
│   ├── reply_service.py     回复采集
│   └── storage.py           存储管理（CSV + SQLite 去重双写）
└── utils/
    ├── field_config.py      配置读取
    ├── helpers.py           工具函数（jitter_delay 等）
    └── printer.py           终端输出
```

---

## 依赖

| 包名 | 用途 |
|------|------|
| `httpx` | 异步 HTTP 请求 |
| `pyyaml` | YAML 配置解析 |
| `tqdm` | 进度条 |
| `pillow` | 图片处理 |
| `pillow-heif` | HEIC 格式支持 |

安装依赖：

```bash
pip install -r requirements.txt
```

---

## 免责声明

本项目仅供学习研究使用，请勿用于商业用途或违反平台规则的行为。采集的数据仅用于技术研究，请勿传播或用于非法目的。
