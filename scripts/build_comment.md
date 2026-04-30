# build_comment.py 使用文档

评论数据站点构建工具，支持 CSV 和 SQLite 两种数据源，支持本地和 CDN 两种资源模式。

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--sqlite` | 使用 SQLite 数据库作为数据源 | 否（使用 CSV） |
| `--cdn URL` | CDN 基础 URL，启用 CDN 模式 | 空（本地模式） |
| `--data-dir PATH` | 数据目录路径 | `data` |
| `--output-dir PATH` | 输出目录路径 | `docs` |
| `--upload-dir PATH` | 上传文件目录路径 | `upload` |

## 使用示例

### CSV 模式（默认）

```bash
python scripts/build_comment.py
```

数据源：`data/{sec_uid}/videos.csv` + `data/{sec_uid}/{年-月}/{aweme_id}/comments.csv`

### SQLite 模式

```bash
python scripts/build_comment.py --sqlite
```

数据源：`data/{sec_uid}/sqlite.db`

### CDN 模式

```bash
python scripts/build_comment.py --sqlite --cdn "https://huggingface.co/buckets/sunset139/douyin/resolve"
```

启用 CDN 后，所有媒体资源 URL 将指向 CDN 地址。

### 自定义目录

```bash
python scripts/build_comment.py --sqlite --data-dir ./mydata --output-dir ./output
```

## 输出结构

```
docs/data/comment/
├── index.json                          # 用户索引
├── {sec_uid}/
│   ├── avatar.jpg                      # 用户头像
│   ├── video_list.json                 # 视频列表
│   ├── summary.json                    # 统计摘要
│   └── comments/
│       └── {aweme_id}.json             # 评论数据
```

## 数据源结构

### CSV 结构

```
data/{sec_uid}/
├── videos.csv                          # 视频列表
├── avatar.jpg                          # 用户头像
├── sqlite.db                           # SQLite 数据库（可选）
└── {年-月}/
    └── {aweme_id}/
        ├── comments.csv                # 评论
        └── replies.csv                 # 回复
```

### SQLite 表结构

**videos 表**
| 字段 | 类型 | 说明 |
|------|------|------|
| aweme_id | TEXT | 视频ID |
| desc | TEXT | 视频描述 |
| create_time | INTEGER | 创建时间戳 |
| images | TEXT | 图片列表 JSON |
| video | TEXT | 视频列表 JSON |
| thumb | TEXT | 封面列表 JSON |
| sec_uid | TEXT | 用户ID |

**comments 表**
| 字段 | 类型 | 说明 |
|------|------|------|
| aweme_id | TEXT | 视频ID |
| cid | TEXT | 评论ID |
| text | TEXT | 评论内容 |
| image_list | TEXT | 图片列表 JSON |
| digg_count | INTEGER | 点赞数 |
| create_time | INTEGER | 创建时间戳 |
| user_nickname | TEXT | 用户昵称 |
| user_unique_id | TEXT | 用户唯一ID |
| user_avatar | TEXT | 用户头像 |
| sticker | TEXT | 表情包 |
| reply_comment_total | INTEGER | 回复数 |
| ip_label | TEXT | IP属地 |

**replies 表**
| 字段 | 类型 | 说明 |
|------|------|------|
| aweme_id | TEXT | 视频ID |
| cid | TEXT | 回复ID |
| reply_id | TEXT | 父评论ID |
| reply_to_reply_id | TEXT | 被回复的回复ID |
| text | TEXT | 回复内容 |
| image_list | TEXT | 图片列表 JSON |
| digg_count | INTEGER | 点赞数 |
| create_time | INTEGER | 创建时间戳 |
| user_nickname | TEXT | 用户昵称 |
| user_unique_id | TEXT | 用户唯一ID |
| user_avatar | TEXT | 用户头像 |
| sticker | TEXT | 表情包 |
| reply_to_username | TEXT | 被回复用户名 |
| ip_label | TEXT | IP属地 |

## URL 生成规则

### 本地模式

```
base_url: upload/{sec_uid}/
video: videos/2026/xxx.mp4
thumb: thumbs/2026/xxx.jpeg
image_list: images/2026/xxx.jpg
sticker: stickers/2026/xxx.png
user_avatar: avatars/2026/xxx.jpeg
```

完整 URL 示例：`upload/{sec_uid}/videos/2026/xxx.mp4`

### CDN 模式

```
base_url: {cdn_url}/{sec_uid}/
video: videos/xxx.mp4?download=true
thumb: thumbs/xxx.jpeg?download=true
image_list: images/xxx.jpg?download=true
sticker: stickers/xxx.png?download=true
user_avatar: avatars/xxx.jpeg?download=true
```

完整 URL 示例：`{cdn_url}/{sec_uid}/videos/xxx.mp4?download=true`

### 模式对比

| 特性 | 本地模式 | CDN 模式 |
|------|---------|---------|
| 年份目录 | ✅ 包含 | ❌ 不包含 |
| download参数 | ❌ 无 | ✅ 有 |
| base_url | 相对路径 | 完整URL |

## 输出文件说明

### index.json

```json
{
  "users": [
    {
      "sec_uid": "MS4wLjAB...",
      "nickname": "用户昵称",
      "avatar": "avatar.jpg",
      "total_videos": 399,
      "total_comments": 116239,
      "latest_video": {
        "date": "2026-04-26",
        "title": "视频标题"
      }
    }
  ],
  "generated_at": "2026-05-01 00:00:00"
}
```

### video_list.json

```json
{
  "sec_uid": "MS4wLjAB...",
  "base_url": "upload/MS4wLjAB.../",
  "videos": [
    {
      "aweme_id": "7632818045987846010",
      "desc": "视频描述",
      "create_time": 1745673600,
      "create_time_str": "2026-04-26 12:00",
      "media_type": "image",
      "images": ["images/2026/xxx.webp"],
      "thumb": ["thumbs/2026/xxx.jpeg"],
      "video": [],
      "comment_count": 100
    }
  ],
  "total_videos": 399,
  "total_comments": 116239
}
```

### summary.json

```json
{
  "total_videos": 399,
  "total_comments": 116239,
  "active_repliers": [
    {
      "nickname": "活跃用户",
      "avatar": "avatars/2026/xxx.jpeg",
      "count": 1314
    }
  ],
  "generated_at": "2026-05-01 00:00:00"
}
```

### comments/{aweme_id}.json

```json
{
  "aweme_id": "7632818045987846010",
  "video_title": "视频标题",
  "comments": [
    {
      "cid": "7138853520485073702",
      "text": "评论内容",
      "image_list": ["images/2026/xxx.jpg"],
      "digg_count": 10,
      "create_time": 1662146212,
      "create_time_str": "2022-09-03 01:16",
      "user_nickname": "用户昵称",
      "user_unique_id": "用户ID",
      "user_avatar": "avatars/2026/xxx.jpeg",
      "sticker": "stickers/2026/xxx.png",
      "reply_comment_total": 5,
      "ip_label": "四川",
      "reply_count": 5,
      "replies": [
        {
          "reply_id": "7138863306455106337",
          "text": "回复内容",
          "user_nickname": "回复者",
          "user_avatar": "avatars/2026/xxx.jpeg"
        }
      ]
    }
  ]
}
```

## 增量构建

程序支持增量构建，仅处理有变更的用户数据：

- 检测 `videos.csv` 或 `sqlite.db` 的修改时间
- 若输出文件较新则跳过构建
- 使用 `--force` 可强制重新构建（需修改代码添加此参数）

## 注意事项

1. **编码问题**：Windows 终端需设置 UTF-8 编码
   ```powershell
   $env:PYTHONIOENCODING='utf-8'
   ```

2. **数据源优先级**：使用 `--sqlite` 时优先读取 `sqlite.db`，否则读取 CSV 文件

3. **活跃用户统计**：基于回复数据计算，显示回复数最多的前 15 位用户

4. **媒体类型判断**：根据 `images` 字段判断，有图片则为 `image`，否则为 `video`
