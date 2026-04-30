#!/usr/bin/env python3
"""
抖音资源 → HF Bucket 迁移方案（v3）

核心改进：
1. 每个 task = 一个资源（不是每个 URL）
2. 每个 task 带 fallback_urls 列表，下载时 CDN failover
3. 嵌套结构（videos.images）拆分为独立 task
4. 按首个 URL 分组去重，下载一次批量更新所有关联行
5. 并发 6 协程
6. 路径格式：{sec_uid}/{subdir}/{md5}.{ext}
"""

import asyncio
import ast
import hashlib
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
import uuid
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import httpx
from huggingface_hub import batch_bucket_files
from tqdm import tqdm

HEIF_SUPPORT = False
HEIF_IMPORT_ERROR = ""

try:
    from PIL import Image as _PILImage
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        HEIF_SUPPORT = True
    except ImportError as e:
        HEIF_IMPORT_ERROR = f"pillow-heif 未安装: {e}"
except ImportError as e:
    HEIF_IMPORT_ERROR = f"Pillow 未安装: {e}"

sys.path.insert(0, os.path.dirname(__file__))
from douyin_api import (
    sign_request, DouyinClient,
    CookieExpiredError, APIRateLimitError, APIServerError, APIError,
)

# ============================================================
# 配置
# ============================================================

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")  # 数据目录（与 scripts 同级的 data/）
BUCKET_ID = "sunset139/douyin"
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookie.txt")
CONCURRENCY = 6

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "migrate")

# 过滤配置（可通过 CLI 覆盖）
TABLES = None                  # None = 全部表，或 ["videos", "comments", "replies"]
FIELDS = None                  # None = 全部字段，或 ["video", "images", "comment_avatar", ...]
MAX_TASKS = None               # None = 不限制，或整数如 100
DRY_RUN = False                # True = 只扫描不上传
DIRECT_MODE = False            # True = 跳过 API，直接用数据库中的原始 URL 下载
BATCH_SIZE = 20                # 每批处理的 aweme_id 数量

# ============================================================
# 日志
# ============================================================

# 全局计时 & 失败收集
_START_TIME: float = 0.0
_FAILURES: List[Dict] = []          # 收集所有失败项，结束时打印汇总
_MAX_FAILURE_LOG: int = 200         # 日志中最多打印多少条失败详情


def _elapsed() -> str:
    """返回已运行时长字符串，如 '3m12s'"""
    if _START_TIME <= 0:
        return "0s"
    secs = int(time.time() - _START_TIME)
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


class _ElapsedFilter(logging.Filter):
    """在每条日志前注入运行时长和时间戳"""
    def filter(self, record):
        record.elapsed = _elapsed()
        record.wall = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return True


def setup_log(log_dir: str) -> str:
    """创建日志文件，返回路径"""
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"migrate_{ts}.log")

    fmt = logging.Formatter(
        "%(wall)s | %(elapsed)s | %(message)s",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)
    file_handler.addFilter(_ElapsedFilter())

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)
    stream_handler.addFilter(_ElapsedFilter())

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
    )

    # 静默 httpx/httpcore 噪音（只保留 WARNING+）
    for noisy in ("httpx", "httpcore", "httpx._client", "httpcore._async"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_path


def log(msg: str = ""):
    """同时输出到控制台和日志文件（实时刷新）"""
    logging.info(msg)
    for handler in logging.root.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()


def record_failure(kind: str, detail: str, source_table: str = "",
                   aweme_id: str = "", field: str = "", reason: str = ""):
    """记录一条失败到全局列表，同时打印到日志"""
    # kind 中文映射
    kind_cn = {
        "cache_miss": "签名缺失",
        "download": "下载失败",
        "upload": "上传失败",
        "db_commit": "数据库提交失败",
    }
    kind_display = kind_cn.get(kind, kind)
    entry = {
        "kind": kind, "kind_display": kind_display, "detail": detail,
        "source_table": source_table, "aweme_id": aweme_id,
        "field": field, "reason": reason,
    }
    _FAILURES.append(entry)
    # 日志中只打印前 N 条详情，避免刷屏
    if len(_FAILURES) <= _MAX_FAILURE_LOG:
        log(f"  [FAIL] {kind_display} | {detail}"
            + (f" | 表={source_table}" if source_table else "")
            + (f" | aweme={aweme_id}" if aweme_id else "")
            + (f" | 字段={field}" if field else "")
            + (f" | 原因={reason}" if reason else ""))


def print_failure_summary():
    """结束时打印失败汇总表"""
    if not _FAILURES:
        return
    log(f"\n{'─' * 70}")
    log(f"  失败汇总 (共 {len(_FAILURES)} 项)")
    log(f"{'─' * 70}")

    # 按原因分类统计
    by_kind: Dict[str, int] = {}
    for f in _FAILURES:
        k = f["kind_display"]
        by_kind[k] = by_kind.get(k, 0) + 1
    for k, v in sorted(by_kind.items(), key=lambda x: -x[1]):
        log(f"    {k}: {v}")

    # 打印详情（最多 _MAX_FAILURE_LOG 条）
    if len(_FAILURES) <= _MAX_FAILURE_LOG:
        for f in _FAILURES:
            log(f"    • {f['kind_display']} | {f['detail']}"
                + (f" | {f['reason']}" if f['reason'] else ""))
    else:
        log(f"  （详情仅显示前 {_MAX_FAILURE_LOG} 条，完整列表见上方日志）")

    log(f"{'─' * 70}")

SUBDIR_MAP = {
    "origin_cover": "thumbs",
    "images": "images",
    "video": "videos",
    "comment_avatar": "avatars",
    "comment_sticker": "stickers",
    "comment_image": "images",
    "reply_avatar": "avatars",
    "reply_sticker": "stickers",
    "reply_image": "images",
}


# ============================================================
# 数据库操作
# ============================================================

class MediaDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._pending_images: Dict[Tuple, List] = {}

    def get_all_tasks(self) -> List[Dict]:
        """
        从三张表提取所有 task。
        每个 task = 一个资源，带 fallback_urls 列表。
        """
        tasks = []

        # aweme_id → sec_uid 映射
        cur = self.conn.execute("SELECT aweme_id, sec_uid, create_time FROM videos")
        video_meta = {}
        for row in cur.fetchall():
            video_meta[str(row[0])] = {"sec_uid": row[1] or "unknown", "create_time": row[2]}

        # ---- videos 表 ----
        cur = self.conn.execute(
            "SELECT id, aweme_id, thumb, images, video, sec_uid, create_time FROM videos"
        )
        for row in cur.fetchall():
            db_id, aweme_id, thumb, images, video, sec_uid, create_time = row
            meta = {"sec_uid": sec_uid or "unknown", "create_time": create_time}
            aid = str(aweme_id)

            # thumb: 1 task, 多个 CDN URL
            if thumb:
                urls = self._parse_flat_urls(thumb)
                if urls:
                    tasks.append({
                        "source_table": "videos", "source_id": db_id,
                        "field": "thumb", "array_index": -1,
                        "fallback_urls": urls, "aweme_id": aid,
                        "api_type": "origin_cover", **meta,
                    })

            # images: N 张图 = N 个 task，每个 task 多个 CDN URL
            if images:
                image_groups = self._parse_nested_urls(images)
                for i, group_urls in enumerate(image_groups):
                    if group_urls:
                        tasks.append({
                            "source_table": "videos", "source_id": db_id,
                            "field": "images", "array_index": i,
                            "fallback_urls": group_urls, "aweme_id": aid,
                            "api_type": "images", **meta,
                        })

            # video: 1 task, 多个 CDN URL
            if video:
                urls = self._parse_flat_urls(video)
                if urls:
                    tasks.append({
                        "source_table": "videos", "source_id": db_id,
                        "field": "video", "array_index": -1,
                        "fallback_urls": urls, "aweme_id": aid,
                        "api_type": "video", **meta,
                    })

        # ---- comments 表 ----
        cur = self.conn.execute(
            "SELECT id, aweme_id, cid, user_avatar, sticker, image_list, create_time FROM comments"
        )
        for row in cur.fetchall():
            db_id, aweme_id, cid, avatar, sticker, image_list, create_time = row
            aid = str(aweme_id)
            meta = {
                "sec_uid": video_meta.get(aid, {}).get("sec_uid", "unknown"),
                "create_time": create_time,
            }
            cid_str = str(cid)

            # avatar: 单 URL（跳过已处理的 MD5 文件名）
            if avatar and avatar.startswith("http"):
                tasks.append({
                    "source_table": "comments", "source_id": db_id,
                    "field": "user_avatar", "array_index": -1,
                    "fallback_urls": [avatar], "aweme_id": aid,
                    "api_type": "comment_avatar", "cid": cid_str, **meta,
                })

            # sticker: 单 URL（跳过已处理的 MD5 文件名）
            if sticker and sticker.startswith("http"):
                tasks.append({
                    "source_table": "comments", "source_id": db_id,
                    "field": "sticker", "array_index": -1,
                    "fallback_urls": [sticker], "aweme_id": aid,
                    "api_type": "comment_sticker", "cid": cid_str, **meta,
                })

            # image_list: 1 task, 多个 CDN URL
            if image_list:
                urls = self._parse_flat_urls(image_list)
                if urls:
                    tasks.append({
                        "source_table": "comments", "source_id": db_id,
                        "field": "image_list", "array_index": -1,
                        "fallback_urls": urls, "aweme_id": aid,
                        "api_type": "comment_image", "cid": cid_str, **meta,
                    })

        # ---- replies 表 ----
        cur = self.conn.execute(
            "SELECT id, aweme_id, cid, reply_id, user_avatar, sticker, image_list, create_time FROM replies"
        )
        for row in cur.fetchall():
            db_id, aweme_id, cid, reply_id, avatar, sticker, image_list, create_time = row
            aid = str(aweme_id)
            meta = {
                "sec_uid": video_meta.get(aid, {}).get("sec_uid", "unknown"),
                "create_time": create_time,
            }
            cid_str = str(cid)
            rid_str = str(reply_id)

            if avatar and avatar.startswith("http"):
                tasks.append({
                    "source_table": "replies", "source_id": db_id,
                    "field": "user_avatar", "array_index": -1,
                    "fallback_urls": [avatar], "aweme_id": aid,
                    "api_type": "reply_avatar", "cid": cid_str, "reply_id": rid_str, **meta,
                })

            if sticker and sticker.startswith("http"):
                tasks.append({
                    "source_table": "replies", "source_id": db_id,
                    "field": "sticker", "array_index": -1,
                    "fallback_urls": [sticker], "aweme_id": aid,
                    "api_type": "reply_sticker", "cid": cid_str, "reply_id": rid_str, **meta,
                })

            if image_list:
                urls = self._parse_flat_urls(image_list)
                if urls:
                    tasks.append({
                        "source_table": "replies", "source_id": db_id,
                        "field": "image_list", "array_index": -1,
                        "fallback_urls": urls, "aweme_id": aid,
                        "api_type": "reply_image", "cid": cid_str, "reply_id": rid_str, **meta,
                    })

        return tasks

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get_all_cids(self) -> set:
        """获取所有 comments 的 aweme_id:cid 集合"""
        result = set()
        for row in self.conn.execute("SELECT aweme_id, cid FROM comments"):
            result.add(f"{row[0]}:{row[1]}")
        return result

    def get_all_reply_ids(self) -> set:
        """获取所有 replies 的 aweme_id:cid:reply_id 集合"""
        result = set()
        for row in self.conn.execute("SELECT aweme_id, cid, reply_id FROM replies"):
            result.add(f"{row[0]}:{row[1]}:{row[2]}")
        return result

    # 数组字段：整体替换为 downloader.py 格式
    ARRAY_FIELDS = {
        ("videos", "thumb"), ("videos", "images"), ("videos", "video"),
        ("comments", "image_list"), ("replies", "image_list"),
    }

    def update_url(self, source_table: str, source_id: int, field: str,
                   array_index: int, new_url: str):
        """替换 URL，数组字段用 downloader.py 格式（str([...])）"""
        if (source_table, field) in self.ARRAY_FIELDS:
            if array_index >= 0:
                # 嵌套 images：先不写，收集到 _pending_images 最后统一写
                key = (source_table, source_id, field)
                self._pending_images.setdefault(key, []).append((array_index, new_url))
            else:
                # 平数组：整体替换为 str([url])，同 downloader.py
                self.conn.execute(
                    f"UPDATE [{source_table}] SET {field} = ? WHERE id = ?",
                    (str([new_url]), source_id)
                )
        else:
            # 单值字段：直接替换
            self.conn.execute(
                f"UPDATE [{source_table}] SET {field} = ? WHERE id = ?",
                (new_url, source_id)
            )

    def flush_pending_images(self):
        """把收集的嵌套 images 按行写入，格式同 downloader.py"""
        if not self._pending_images:
            return
        for (table, row_id, field), items in self._pending_images.items():
            items.sort(key=lambda x: x[0])
            result_list = [url for _, url in items]
            self.conn.execute(
                f"UPDATE [{table}] SET {field} = ? WHERE id = ?",
                (str(result_list), row_id)
            )
        self._pending_images.clear()

    @staticmethod
    def _parse_flat_urls(value: str) -> List[str]:
        """解析为扁平 URL 列表（JSON数组 / Python list / 单URL）"""
        if not value:
            return []
        if value.startswith("http"):
            return [value]
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [u for u in parsed if isinstance(u, str) and u.startswith("http")]
        except (json.JSONDecodeError, ValueError):
            pass
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [u for u in parsed if isinstance(u, str) and u.startswith("http")]
        except (ValueError, SyntaxError):
            pass
        return []

    @staticmethod
    def _parse_nested_urls(value: str) -> List[List[str]]:
        """
        解析嵌套列表：[['url1','url2'],['url3','url4']]
        每个子列表 = 一个资源的多个 CDN URL
        """
        if not value:
            return []
        try:
            parsed = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            try:
                parsed = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return []
        if not isinstance(parsed, list):
            return []

        result = []
        for item in parsed:
            if isinstance(item, list):
                urls = [u for u in item if isinstance(u, str) and u.startswith("http")]
                if urls:
                    result.append(urls)
            elif isinstance(item, str) and item.startswith("http"):
                result.append([item])
        return result

# ============================================================
# 文件处理工具
# ============================================================

def compute_md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def detect_ext(file_path: str, url: str) -> str:
    path = url.split("?")[0].lower()
    for ext in (".mp4", ".mp3", ".webp", ".jpg", ".jpeg", ".png", ".gif", ".webm", ".wav",
                ".heic", ".heif", ".avif", ".bmp", ".flac", ".ogg", ".aac"):
        if path.endswith(ext):
            return ext
    with open(file_path, 'rb') as f:
        header = f.read(20)
    if header[:2] == b'\xff\xd8':
        return ".jpg"
    elif header[:8] == b'\x89PNG\r\n\x1a\n':
        return ".png"
    elif header[:4] == b'GIF8':
        return ".gif"
    elif header[:4] == b'RIFF' and len(header) >= 12 and header[8:12] == b'WEBP':
        return ".webp"
    elif len(header) >= 12 and header[4:8] == b'ftyp':
        brand = header[8:12]
        if brand in (b'heic', b'heix', b'heim', b'heis', b'mif1', b'hevc', b'hevx'):
            return ".heic"
        elif brand in (b'avif', b'avis'):
            return ".avif"
        else:
            return ".mp4"
    elif header[:3] == b'ID3' or header[:2] == b'\xff\xfb' or header[:2] == b'\xff\xf3':
        return ".mp3"
    elif header[:4] == b'OggS':
        return ".ogg"
    elif header[:4] == b'fLaC':
        return ".flac"
    elif header[:4] == b'\x1a\x45\xdf\xa3':
        return ".webm"
    elif header[:4] == b'RIFF':
        return ".wav"
    return ".jpg"


def convert_heic_to_jpeg(file_path: str) -> Tuple[str, str]:
    """HEIC 转 JPEG，返回 (新路径, 新扩展名)"""
    if not HEIF_SUPPORT:
        return file_path, ".heic"
    try:
        img = _PILImage.open(file_path)
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        jpeg_path = file_path.rsplit('.', 1)[0] + '.jpg'
        img.save(jpeg_path, 'JPEG', quality=95)
        os.remove(file_path)
        return jpeg_path, ".jpg"
    except Exception as e:
        log(f"  [HEIC] ⚠️  转换失败 ({os.path.basename(file_path)}): {e}")
        return file_path, ".heic"


def detect_heic_stats(tasks: List[Dict]) -> Dict:
    """预检：统计 HEIC 文件数量，提前告警"""
    heic_count = 0
    for t in tasks:
        for url in t.get("fallback_urls", []):
            if url.split("?")[0].lower().endswith((".heic", ".heif")):
                heic_count += 1
                break
    return {"heic_total": heic_count}


# ============================================================
# URL 提取（从 API 原始数据中提取 CDN URL 列表）
# ============================================================

def _extract_video_urls(video: Dict) -> Dict[str, Any]:
    """从视频作品数据中提取 cover/video/images URL（返回完整 CDN 列表）"""
    result = {}

    # cover: 优先用 video.cover（720p），fallback 到 video.origin_cover（360p）
    vc = video.get("video", {})
    oc = vc.get("cover") or vc.get("origin_cover") or {}
    ul = oc.get("url_list", [])
    if ul:
        result["origin_cover"] = ul  # 返回完整 CDN 列表

    # video play_addr
    pa = vc.get("play_addr", {})
    vurls = pa.get("url_list", [])
    if vurls:
        result["video"] = vurls  # 返回完整 CDN 列表

    # images（图集）：返回全部图片，每张图片包含完整 CDN 列表
    imgs = video.get("images") or []
    image_list = []
    for img in imgs:
        if isinstance(img, dict):
            iul = img.get("url_list", [])
            image_list.append(iul if iul else None)  # 返回完整 CDN 列表
        elif isinstance(img, str) and img.startswith("http"):
            image_list.append([img])
        else:
            image_list.append(None)
    if image_list:
        result["images"] = image_list

    return result


def _extract_comment_urls(comment: Dict) -> Dict[str, List[str]]:
    """从评论/回复数据中提取 avatar/sticker/image URL（返回完整 CDN 列表）"""
    result = {}

    # avatar: user.avatar_thumb.url_list
    user = comment.get("user") or {}
    at = user.get("avatar_thumb") or {}
    ul = at.get("url_list", [])
    if ul:
        result["avatar"] = ul  # 返回完整 CDN 列表

    # sticker
    sticker = comment.get("sticker")
    if isinstance(sticker, dict):
        sul = sticker.get("static_url", {}).get("url_list", [])
        if sul:
            result["sticker"] = sul  # 返回完整 CDN 列表
    elif isinstance(sticker, str) and sticker.startswith("http"):
        result["sticker"] = [sticker]

    # image_list
    imgs = comment.get("image_list")
    if imgs and isinstance(imgs, list) and isinstance(imgs[0], dict):
        ou = imgs[0].get("origin_url") or {}
        iul = ou.get("url_list", [])
        if iul:
            result["image"] = iul  # 返回完整 CDN 列表

    return result


# ============================================================
# 批量 API 签名获取（替代逐条调用）
# ============================================================

async def fetch_api_urls_batch(
    client: DouyinClient, tasks: List[Dict], db: MediaDB,
    batch_aweme_ids: set = None,
    video_cache: Dict[str, Dict] = None,
) -> Dict[str, List[str]]:
    """
    用 DouyinClient 获取新签名 URL（每个资源返回完整 CDN 列表）。
    batch_aweme_ids: 当前批次要处理的 aweme_id 集合，用于限制 API 调用范围
    video_cache: 预拉取的视频 URL 缓存（由外部全局拉取一次），传入后跳过视频 API 调用
    正常返回 api_cache 字典。
    API 错误时抛异常，由上层终止程序。
    """
    api_cache: Dict[str, List[str]] = {}

    # 收集需要处理的 aweme_id，按 sec_uid 分组
    secuid_to_awemeids: Dict[str, set] = defaultdict(set)
    comment_aweme_ids = set()
    reply_comment_keys = set()
    for t in tasks:
        at = t["api_type"]
        aid = t["aweme_id"]
        if at in ("origin_cover", "images", "video"):
            secuid_to_awemeids[t.get("sec_uid", "unknown")].add(aid)
        elif at.startswith("comment_"):
            if batch_aweme_ids is None or aid in batch_aweme_ids:
                comment_aweme_ids.add(aid)
        elif at.startswith("reply_"):
            if batch_aweme_ids is None or aid in batch_aweme_ids:
                reply_comment_keys.add((aid, t.get("reply_id", "")))

    video_aweme_ids = set()
    for aids in secuid_to_awemeids.values():
        video_aweme_ids.update(aids)

    db_cids = db.get_all_cids()
    db_reply_ids = db.get_all_reply_ids()

    log(f"  [API] 视频 aweme_id: {len(video_aweme_ids)}, 评论 aweme_id: {len(comment_aweme_ids)}, 回复评论数: {len(reply_comment_keys)}")
    log(f"  [API] 涉及作者数: {len(secuid_to_awemeids)}")
    log(f"  [API] 数据库评论数: {len(db_cids)}, 回复数: {len(db_reply_ids)}")

    # ---- 1. 视频：使用预拉取的缓存，或按作者逐个翻页拉作品列表 ----
    if video_cache:
        for aid in set().union(*secuid_to_awemeids.values()) if secuid_to_awemeids else set():
            if aid in video_cache:
                urls = video_cache[aid]
                if "origin_cover" in urls:
                    api_cache[f"{aid}:origin_cover"] = urls["origin_cover"]
                if "video" in urls:
                    api_cache[f"{aid}:video"] = urls["video"]
                if "images" in urls:
                    for i, img_url in enumerate(urls["images"]):
                        if img_url:
                            api_cache[f"{aid}:images:{i}"] = img_url
    else:
        for idx, (sec_uid, aweme_ids) in enumerate(sorted(secuid_to_awemeids.items()), 1):
            log(f"  [API] [视频] ({idx}/{len(secuid_to_awemeids)}) 拉取 sec_uid={sec_uid[:20]}... ({len(aweme_ids)} 个作品)")
            videos = await client.fetch_all_videos(sec_uid)
            log(f"  [API] [视频]   获取 {len(videos)} 个作品，匹配 {len(aweme_ids)} 个")
            matched_aids = set()
            for v in videos:
                aid = str(v.get("aweme_id", ""))
                if aid not in aweme_ids:
                    continue
                matched_aids.add(aid)
                urls = _extract_video_urls(v)
                if "origin_cover" in urls:
                    api_cache[f"{aid}:origin_cover"] = urls["origin_cover"]
                if "video" in urls:
                    api_cache[f"{aid}:video"] = urls["video"]
                if "images" in urls:
                    for i, img_url in enumerate(urls["images"]):
                        if img_url:
                            api_cache[f"{aid}:images:{i}"] = img_url
            unmatched = aweme_ids - matched_aids
            if unmatched:
                log(f"  [API] [视频] ⚠️  {len(unmatched)} 个 aweme_id 未在 API 返回中找到: {list(unmatched)[:5]}{'...' if len(unmatched) > 5 else ''}")

    # ---- 2. 评论：按视频翻页拉评论列表 ----
    if comment_aweme_ids:
        comment_aweme_list = sorted(comment_aweme_ids)
        for i, aweme_id in enumerate(tqdm(comment_aweme_list, desc="获取评论URL", unit="个")):
            comments = await client.fetch_all_comments(aweme_id)
            matched, skipped = 0, 0
            for c in comments:
                cid = str(c.get("cid", ""))
                key = f"{aweme_id}:{cid}"
                if key not in db_cids:
                    skipped += 1
                    continue
                matched += 1
                urls = _extract_comment_urls(c)
                if "avatar" in urls:
                    api_cache[f"{aweme_id}:comment_avatar:{cid}"] = urls["avatar"]
                if "sticker" in urls:
                    api_cache[f"{aweme_id}:comment_sticker:{cid}"] = urls["sticker"]
                if "image" in urls:
                    api_cache[f"{aweme_id}:comment_image:{cid}"] = urls["image"]
            if not comments:
                log(f"  [API] [评论] aweme_id={aweme_id}: API 返回 0 条评论")
            elif skipped > 0:
                api_cids = [str(c.get("cid", "")) for c in comments[:5]]
                log(f"  [API] [评论] aweme_id={aweme_id}: API 返回 {len(comments)} 条, "
                    f"匹配 {matched}, 不在DB中 {skipped} "
                    f"(API样本: {api_cids})")

    # ---- 3. 回复：按评论翻页拉回复列表 ----
    if reply_comment_keys:
        for aweme_id, cid in tqdm(reply_comment_keys, desc="获取回复URL", unit="个"):
            replies = await client.fetch_all_replies(aweme_id, cid)
            matched, skipped = 0, 0
            for r in replies:
                rid = str(r.get("cid", ""))
                key = f"{aweme_id}:{rid}:{cid}"
                if key not in db_reply_ids:
                    skipped += 1
                    continue
                matched += 1
                urls = _extract_comment_urls(r)
                if "avatar" in urls:
                    api_cache[f"{aweme_id}:reply_avatar:{rid}:{cid}"] = urls["avatar"]
                if "sticker" in urls:
                    api_cache[f"{aweme_id}:reply_sticker:{rid}:{cid}"] = urls["sticker"]
                if "image" in urls:
                    api_cache[f"{aweme_id}:reply_image:{rid}:{cid}"] = urls["image"]
            if not replies:
                log(f"  [API] [回复] aweme_id={aweme_id} cid={cid}: API 返回 0 条回复")
            elif skipped > 0:
                api_cids = [str(r.get("cid", "")) for r in replies[:5]]
                log(f"  [API] [回复] aweme_id={aweme_id} cid={cid}: API 返回 {len(replies)} 条, "
                    f"匹配 {matched}, 不在DB中 {skipped} "
                    f"(API回复cid样本: {api_cids})")

    return api_cache


def get_cache_key(t: Dict) -> str:
    """生成 api_cache 的查找 key"""
    at = t["api_type"]
    aid = t["aweme_id"]
    if at in ("origin_cover", "video"):
        return f"{aid}:{at}"
    elif at == "images":
        return f"{aid}:images:{t.get('array_index', 0)}"
    elif at.startswith("comment_"):
        return f"{aid}:{at}:{t.get('cid', '')}"
    elif at.startswith("reply_"):
        return f"{aid}:{at}:{t.get('cid', '')}:{t.get('reply_id', '')}"
    return ""


# ============================================================
# 并发处理核心
# ============================================================

async def download_with_fallback(
    cdn_urls: List[str],
    http_client: httpx.AsyncClient,
    save_dir: str,
    max_retries: int = 2,
) -> Optional[str]:
    """
    流式下载到临时文件，带 CDN failover + 网络重试。
    cdn_urls: 完整的 CDN 签名 URL 列表（全部有效）
    返回临时文件路径，失败返回 None。
    """
    if not cdn_urls:
        log(f"  [DL] ❌ 下载失败：CDN URL 列表为空")
        record_failure("download", "(空URL列表)", reason="CDN列表为空")
        return None

    last_url = ""

    for url in cdn_urls:
        last_url = url
        for attempt in range(max_retries + 1):
            temp_path = os.path.join(save_dir, f"temp_{uuid.uuid4().hex}.tmp")
            try:
                async with http_client.stream("GET", url, timeout=30.0, follow_redirects=True) as resp:
                    if resp.status_code != 200:
                        log(f"  [DL] ⚠️  HTTP {resp.status_code}: {url[:80]}...")
                        break  # 非 200 不重试，换下一个 URL
                    with open(temp_path, 'wb') as f:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            if chunk:
                                f.write(chunk)

                # 校验文件：太小说明是错误响应
                file_size = os.path.getsize(temp_path)
                if file_size < 100:
                    os.remove(temp_path)
                    log(f"  [DL] ⚠️  文件太小 ({file_size}B): {url[:80]}...")
                    break  # 换下一个 URL

                return temp_path
            except (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError) as e:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                log(f"  [DL] ⚠️  网络错误 ({type(e).__name__}): {url[:80]}...")
                if attempt < max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                break
            except Exception as e:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                log(f"  [DL] ⚠️  未知错误 ({type(e).__name__}: {e}): {url[:80]}...")
                break

    log(f"  [DL] ❌ 下载失败，最后尝试的 URL: {last_url}")
    record_failure("download", last_url[:100] if last_url else "(无URL)", reason="所有CDN均失败")
    return None


async def process_task(
    url: str,
    all_tasks: List[Dict],
    valid_task: Dict,
    http_client: httpx.AsyncClient,
    db: MediaDB,
    semaphore: asyncio.Semaphore,
    stats: Dict,
    pbar: tqdm,
    api_cache: Dict[str, List[str]],
    temp_dir: str,
    upload_queue: asyncio.Queue,
):
    """
    处理一组共享同一 URL 的 tasks：
    1. 用 valid_task 获取签名 URL
    2. 流式下载到临时文件（带 CDN failover）
    3. 放入上传队列，更新所有 all_tasks
    """
    async with semaphore:
        cache_key = get_cache_key(valid_task)
        fresh_urls = api_cache.get(cache_key)

        if not fresh_urls:
            log(f"  [DL] ⚠️  无签名 URL（缓存未命中）: {get_cache_key(valid_task)}")
            for t in all_tasks:
                record_failure(
                    "cache_miss",
                    f"{t['source_table']}.id={t['source_id']} api_type={t['api_type']}",
                    source_table=t["source_table"],
                    aweme_id=t["aweme_id"],
                    field=t["field"],
                    reason=f"cache_key={get_cache_key(t)}"
                )
            stats["failed"] += len(all_tasks)
            pbar.update(len(all_tasks))
            return

        # 流式下载到临时文件（使用完整的 CDN 签名 URL 列表）
        temp_path = await download_with_fallback(
            fresh_urls, http_client, temp_dir
        )

        if not temp_path:
            for t in all_tasks:
                record_failure(
                    "download",
                    f"{t['source_table']}.id={t['source_id']} api_type={t['api_type']}",
                    source_table=t["source_table"],
                    aweme_id=t["aweme_id"],
                    field=t["field"],
                    reason="下载失败"
                )
            stats["failed"] += len(all_tasks)
            pbar.update(len(all_tasks))
            return

        try:
            # MD5 + 扩展名
            md5 = compute_md5(temp_path)
            ext = detect_ext(temp_path, fresh_urls[0] if fresh_urls else "")

            # HEIC → JPEG
            if ext == ".heic":
                original_ext = ext
                temp_path, ext = convert_heic_to_jpeg(temp_path)
                if ext == ".jpg" and original_ext == ".heic":
                    stats["heic_converted"] += 1
                md5 = compute_md5(temp_path)

            # Bucket 路径（完整路径）+ 文件名（只存 MD5+ext）
            sec_uid = valid_task.get("sec_uid", "unknown")
            subdir = SUBDIR_MAP.get(valid_task["api_type"], "other")
            bucket_path = f"{sec_uid}/{subdir}/{md5}{ext}"
            filename = f"{md5}{ext}"

            # 放入上传队列，由 upload_worker 批量处理
            # 注意：更新所有 all_tasks，不仅仅是 valid_task
            await upload_queue.put({
                "temp_path": temp_path,
                "bucket_path": bucket_path,
                "filename": filename,
                "tasks": all_tasks,
            })

        except Exception as e:
            # 处理失败，清理临时文件，记录错误
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            for t in all_tasks:
                record_failure(
                    "download",
                    f"{t.get('source_table', '?')}.id={t.get('source_id', '?')} api_type={t.get('api_type', '?')}",
                    source_table=t.get("source_table", ""),
                    aweme_id=t.get("aweme_id", ""),
                    field=t.get("field", ""),
                    reason=f"{type(e).__name__}: {e}"
                )
            stats["failed"] += len(all_tasks)
            pbar.update(len(all_tasks))


UPLOAD_BATCH_SIZE = 50   # 每批上传文件数
UPLOAD_BATCH_TIMEOUT = 5  # 最多等几秒凑批


async def upload_worker(
    upload_queue: asyncio.Queue,
    db: MediaDB,
    stats: Dict,
    pbar: tqdm,
):
    """
    从队列取出文件，攒批后一次性 batch_bucket_files 上传。
    每 UPLOAD_BATCH_SIZE 个文件或 UPLOAD_BATCH_TIMEOUT 秒触发一次。
    """
    batch_add = []       # [(temp_path, bucket_path), ...]
    batch_meta = []      # [{"tasks": [...], "filename": ...}, ...]

    async def flush_batch():
        """上传当前批次，更新数据库，立即 commit"""
        if not batch_add:
            return

        # 带重试的批量上传（最多 3 次）
        upload_ok = False
        last_error = None
        for retry in range(3):
            try:
                batch_bucket_files(BUCKET_ID, add=batch_add)
                upload_ok = True
                break
            except Exception as e:
                last_error = e
                if retry < 2:
                    log(f"  [UPLOAD] ⚠️  上传失败，重试 ({retry+1}/3): {e}")
                    await asyncio.sleep(2 ** retry)
                else:
                    log(f"  [UPLOAD] ❌ 上传失败，放弃 ({len(batch_add)} 个文件): {e}")

        if not upload_ok:
            for meta in batch_meta:
                for t in meta["tasks"]:
                    record_failure(
                        "upload",
                        f"{t['source_table']}.id={t['source_id']} → {meta['filename']}",
                        source_table=t["source_table"],
                        aweme_id=t["aweme_id"],
                        field=t["field"],
                        reason=f"上传失败3次: {last_error}"
                    )
            stats["failed"] += sum(len(m["tasks"]) for m in batch_meta)
            pbar.update(sum(len(m["tasks"]) for m in batch_meta))
            for local_path, _ in batch_add:
                if os.path.exists(local_path):
                    os.remove(local_path)
            batch_add.clear()
            batch_meta.clear()
            return

        # 上传成功，打印文件名
        for _, bucket_path in batch_add:
            log(f"  [UPLOAD] ↑ {bucket_path}")

        # 上传成功，批量更新数据库
        batch_updated = 0
        for meta in batch_meta:
            for t in meta["tasks"]:
                db.update_url(
                    t["source_table"], t["source_id"],
                    t["field"], t["array_index"], meta["filename"],
                )
            stats["uploaded"] += 1
            stats["updated_rows"] += len(meta["tasks"])
            batch_updated += len(meta["tasks"])
            pbar.update(len(meta["tasks"]))
        log(f"  [DB] 📝 待更新: {batch_updated} 行")

        # 立即提交，防止崩溃丢失
        try:
            db.flush_pending_images()
            db.commit()
            log(f"  [DB] ✅ commit 成功: {batch_updated} 行已写入")
        except Exception as e:
            log(f"  [DB] ❌ commit 失败: {e}")
            record_failure("db_commit", f"{batch_updated} 行", reason=str(e))
            # commit 失败，已上传的文件会成为孤儿，但不会数据不一致

        # 清理临时文件
        for local_path, _ in batch_add:
            if os.path.exists(local_path):
                os.remove(local_path)

        batch_add.clear()
        batch_meta.clear()

    while True:
        try:
            # 等待第一个文件，超时则 flush
            item = await asyncio.wait_for(
                upload_queue.get(), timeout=UPLOAD_BATCH_TIMEOUT
            )
        except asyncio.TimeoutError:
            # 超时，flush 已有的批次
            await flush_batch()
            continue

        if item is None:
            # 哨兵值，表示所有下载完成
            await flush_batch()
            break

        batch_add.append((item["temp_path"], item["bucket_path"]))
        batch_meta.append({"tasks": item["tasks"], "filename": item["filename"]})

        # 攒够一批就上传
        if len(batch_add) >= UPLOAD_BATCH_SIZE:
            await flush_batch()


def discover_databases(data_dir: str) -> List[Dict]:
    """
    扫描目录，发现所有 {sec_uid}/sqlite.db，附带统计信息。
    返回 [{"sec_uid": str, "db_path": str, "size_mb": float,
            "videos": int, "comments": int, "replies": int}, ...]
    """
    results = []
    if not os.path.isdir(data_dir):
        log(f"  [SCAN] ❌ 数据目录不存在: {data_dir}")
        return results

    for entry in sorted(os.listdir(data_dir)):
        entry_path = os.path.join(data_dir, entry)
        if not os.path.isdir(entry_path):
            continue
        db_file = os.path.join(entry_path, "sqlite.db")
        if not os.path.isfile(db_file):
            continue
        size_mb = os.path.getsize(db_file) / 1024 / 1024
        # 统计各表行数
        videos = comments = replies = 0
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            videos = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
            comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
            replies = conn.execute("SELECT COUNT(*) FROM replies").fetchone()[0]
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
        results.append({
            "sec_uid": entry, "db_path": db_file,
            "size_mb": size_mb, "videos": videos,
            "comments": comments, "replies": replies,
        })

    return results


def select_databases(
    databases: List[Dict], author_filter: Optional[List[str]],
) -> List[Dict]:
    """
    让用户选择要处理的数据库。
    - author_filter 有值：按 sec_uid 前缀匹配，跳过交互
    - author_filter 为 None：打印摘要表，交互选择
    返回筛选后的列表。
    """
    if not databases:
        return []

    # --- 非交互模式：按 --author 过滤 ---
    if author_filter is not None:
        if len(author_filter) == 1 and author_filter[0].lower() == "all":
            return databases
        selected = []
        seen = set()
        for prefix in author_filter:
            matched = [d for d in databases if d["sec_uid"].startswith(prefix)]
            if not matched:
                log(f"  [SCAN] ⚠️  未匹配到 sec_uid 前缀: {prefix}")
            for d in matched:
                if d["sec_uid"] not in seen:
                    seen.add(d["sec_uid"])
                    selected.append(d)
        return selected

    # --- 交互模式 ---
    # 只有 1 个库，自动选择
    if len(databases) == 1:
        d = databases[0]
        log(f"\n  [SCAN] 发现 1 个作者数据库，自动选择: {d['sec_uid'][:40]}...")
        return databases

    # 打印摘要表
    log(f"\n  [SCAN] 发现 {len(databases)} 个作者数据库:\n")
    header = f"  {'#':>3s}   {'sec_uid':<48s}  {'videos':>7s}  {'comments':>9s}  {'replies':>8s}  {'大小':>8s}"
    log(header)
    log(f"  {'─' * 3}   {'─' * 48}  {'─' * 7}  {'─' * 9}  {'─' * 8}  {'─' * 8}")
    for i, d in enumerate(databases, 1):
        log(f"  {i:>3d}   {d['sec_uid']:<48s}  {d['videos']:>7d}  {d['comments']:>9d}  {d['replies']:>8d}  {d['size_mb']:>7.1f}M")
    log("")

    # 等待用户输入
    while True:
        try:
            raw = input("  输入编号选择（如 1,2 或 all），直接回车 = 全部处理: ").strip()
        except (EOFError, KeyboardInterrupt):
            log("\n  [SCAN] 用户中断")
            return []
        if not raw:
            return databases
        if raw.lower() == "all":
            return databases
        try:
            indices = [int(x.strip()) for x in raw.split(",")]
            selected = []
            for idx in indices:
                if 1 <= idx <= len(databases):
                    selected.append(databases[idx - 1])
                else:
                    log(f"  [SCAN] ⚠️  编号 {idx} 超出范围 (1-{len(databases)})")
            if selected:
                return selected
        except ValueError:
            log(f"  [SCAN] ⚠️  无效输入，请输入编号（如 1,2）或 all")


async def process_one_db(
    sec_uid: str,
    db_path: str,
    client: DouyinClient,
    http_client: httpx.AsyncClient,
    global_stats: Dict,
):
    """处理单个作者的数据库（分批处理）"""
    log(f"\n{'─' * 70}")
    log(f"  [SCAN] 📁 作者: {sec_uid[:40]}...")
    log(f"  [SCAN] 📄 数据库: {db_path}")
    log(f"{'─' * 70}")

    db = MediaDB(db_path)

    # 1. 扫描
    all_tasks = db.get_all_tasks()
    if not all_tasks:
        log(f"  [SCAN] ⚠️  无资源，跳过")
        db.close()
        return

    # 1.5 按表过滤
    if TABLES:
        all_tasks = [t for t in all_tasks if t["source_table"] in TABLES]
        if not all_tasks:
            log(f"  [FILTER] ⚠️  过滤后无任务（限制表: {TABLES}）")
            db.close()
            return

    if FIELDS:
        all_tasks = [t for t in all_tasks if t["api_type"] in FIELDS]
        if not all_tasks:
            log(f"  [FILTER] ⚠️  过滤后无任务（限制字段: {FIELDS}）")
            db.close()
            return

    # 1.6 按数量限制
    if MAX_TASKS and len(all_tasks) > MAX_TASKS:
        log(f"  [FILTER] ⚠️  任务数 {len(all_tasks)} 超过限制 {MAX_TASKS}，截断")
        all_tasks = all_tasks[:MAX_TASKS]

    log(f"  [SCAN] 总 task 数: {len(all_tasks)}")
    type_counts = {}
    for t in all_tasks:
        type_counts[t["api_type"]] = type_counts.get(t["api_type"], 0) + 1
    for k, v in sorted(type_counts.items()):
        log(f"    {k}: {v}")

    # HEIF 检测
    heif_info = detect_heic_stats(all_tasks)
    if heif_info["heic_total"] > 0:
        log(f"  [SCAN] 📷 HEIC 文件: {heif_info['heic_total']} 个")

    # Dry-run 模式：只扫描不执行
    if DRY_RUN:
        log(f"\n  [SCAN] 🔍 DRY-RUN 模式，跳过下载和上传")
        db.close()
        return

    # 2. 全局去重
    url_to_tasks: Dict[str, List[Dict]] = defaultdict(list)
    for t in all_tasks:
        first_url = t["fallback_urls"][0] if t["fallback_urls"] else ""
        if first_url:
            url_to_tasks[first_url].append(t)

    unique_urls = len(url_to_tasks)
    log(f"  [SCAN] 唯一分组: {unique_urls}，去重节省: {len(all_tasks) - unique_urls} 次下载")

    # 3. 按 aweme_id 分组 URL
    aweme_to_urls: Dict[str, set] = defaultdict(set)
    for url, tasks in url_to_tasks.items():
        for t in tasks:
            aweme_to_urls[t["aweme_id"]].add(url)

    aweme_ids = list(aweme_to_urls.keys())
    total_batches = (len(aweme_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    log(f"  [BATCH] aweme_id 数: {len(aweme_ids)}，分 {total_batches} 批处理")

    # 3.5 全局预拉取视频 URL（按 sec_uid 只拉一次，所有批次共用）
    # direct 模式下跳过，直接用数据库中的原始 URL
    video_tasks = [t for t in all_tasks if t["api_type"] in ("origin_cover", "images", "video")]
    video_cache: Dict[str, Dict] = {}  # aweme_id → {origin_cover, video, images}
    if video_tasks and not DIRECT_MODE:
        secuid_to_video_awemeids: Dict[str, set] = defaultdict(set)
        for t in video_tasks:
            secuid_to_video_awemeids[t.get("sec_uid", "unknown")].add(t["aweme_id"])
        for idx, (sec_uid, v_aweme_ids) in enumerate(sorted(secuid_to_video_awemeids.items()), 1):
            log(f"\n  [API] 🎬 预拉取视频 ({idx}/{len(secuid_to_video_awemeids)}) sec_uid={sec_uid[:30]}... ({len(v_aweme_ids)} 个作品)")
            videos = await client.fetch_all_videos(sec_uid)
            log(f"  [API]    获取 {len(videos)} 个作品，匹配 {len(v_aweme_ids)} 个")
            matched = set()
            for v in videos:
                aid = str(v.get("aweme_id", ""))
                if aid not in v_aweme_ids:
                    continue
                matched.add(aid)
                urls = _extract_video_urls(v)
                video_cache[aid] = urls
            unmatched = v_aweme_ids - matched
            if unmatched:
                log(f"  [API]    ⚠️  {len(unmatched)} 个未找到: {list(unmatched)[:5]}{'...' if len(unmatched) > 5 else ''}")
        log(f"  [API] 🎬 视频缓存: {len(video_cache)} 个 aweme_id")

    # 4. 分批处理
    stats = {"uploaded": 0, "updated_rows": 0, "failed": 0, "heic_converted": 0}
    semaphore = asyncio.Semaphore(CONCURRENCY)
    processed_urls = set()

    for batch_idx in range(0, len(aweme_ids), BATCH_SIZE):
        batch_aweme_ids = aweme_ids[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1

        # 收集这批要处理的 URL（跳过已处理的）
        batch_urls = set()
        for aid in batch_aweme_ids:
            for url in aweme_to_urls[aid]:
                if url not in processed_urls:
                    batch_urls.add(url)

        if not batch_urls:
            continue

        # 收集这批的 tasks
        batch_tasks = []
        for url in batch_urls:
            batch_tasks.extend(url_to_tasks[url])

        log(f"\n  [BATCH] 📦 批次 {batch_num}/{total_batches}: {len(batch_aweme_ids)} 个 aweme_id, {len(batch_urls)} 个 URL")
        batch_start = time.time()

        # 获取签名 URL：direct 模式直接用数据库原始 URL，否则调 API
        if DIRECT_MODE:
            api_cache = {}
            for t in batch_tasks:
                key = get_cache_key(t)
                if key and t.get("fallback_urls"):
                    api_cache[key] = t["fallback_urls"]
            log(f"  [BATCH] direct 模式: 直接使用原始 URL {len(api_cache)} 个")
        else:
            api_cache = await fetch_api_urls_batch(client, batch_tasks, db, set(batch_aweme_ids), video_cache=video_cache)
            log(f"  [BATCH] 获取签名 URL: {len(api_cache)} 个")

        # 并发下载 + 批量上传
        pbar = tqdm(total=len(batch_tasks), desc=f"    批次{batch_num}", unit="个")

        pending = []
        for url in batch_urls:
            url_tasks = url_to_tasks[url]
            # 找到一个有效的 task 用于获取签名 URL
            valid_task = next((t for t in url_tasks if get_cache_key(t) in api_cache), None)
            if valid_task:
                # 保留所有 tasks，用 valid_task 获取签名 URL
                pending.append((url, url_tasks, valid_task))
            else:
                # 没有有效的 task，全部失败
                for t in url_tasks:
                    record_failure(
                        "cache_miss",
                        f"{t['source_table']}.id={t['source_id']} api_type={t['api_type']}",
                        source_table=t["source_table"],
                        aweme_id=t["aweme_id"],
                        field=t["field"],
                        reason=f"cache_key={get_cache_key(t)}"
                    )
                stats["failed"] += len(url_tasks)

        # 创建上传队列 + 启动上传 worker
        upload_queue = asyncio.Queue()
        worker_task = asyncio.create_task(
            upload_worker(upload_queue, db, stats, pbar)
        )

        temp_dir = tempfile.mkdtemp(prefix="migrate_")
        coros = [
            process_task(url, url_tasks, valid_task, http_client, db, semaphore, stats, pbar, api_cache, temp_dir, upload_queue)
            for url, url_tasks, valid_task in pending
        ]
        await asyncio.gather(*coros)

        # 所有下载完成，发哨兵值通知 worker 结束
        await upload_queue.put(None)
        await worker_task

        pbar.close()

        try:
            os.rmdir(temp_dir)
        except OSError:
            pass

        # 标记已处理
        processed_urls.update(batch_urls)

        # 提交这批
        db.flush_pending_images()
        db.commit()
        batch_elapsed = time.time() - batch_start
        log(f"  [BATCH] ✅ 批次完成，已提交 {len(processed_urls)}/{unique_urls} 个 URL，耗时 {batch_elapsed:.1f}s")

    db.close()

    # 汇总
    log(f"\n  [OK] 作者完成: 上传 {stats['uploaded']}，更新 {stats['updated_rows']} 行，"
          f"HEIC 转换 {stats['heic_converted']}，失败 {stats['failed']}")

    global_stats["total_tasks"] += len(all_tasks)
    global_stats["uploaded"] += stats["uploaded"]
    global_stats["updated_rows"] += stats["updated_rows"]
    global_stats["failed"] += stats["failed"]
    global_stats["heic_converted"] += stats["heic_converted"]
    global_stats["db_count"] += 1


async def main():
    global TABLES, FIELDS, MAX_TASKS, DATA_DIR, CONCURRENCY, DRY_RUN, BATCH_SIZE, DIRECT_MODE

    # CLI 参数
    import argparse
    parser = argparse.ArgumentParser(description="抖音资源 → HF Bucket 迁移")
    parser.add_argument("--data-dir", default=DATA_DIR, help=f"数据目录 (默认: {DATA_DIR})")
    parser.add_argument("--tables", nargs="+", default=None,
                        choices=["videos", "comments", "replies"],
                        help="只处理指定表 (默认: 全部)")
    parser.add_argument("--fields", nargs="+", default=None,
                        choices=["origin_cover", "images", "video",
                                 "comment_avatar", "comment_sticker", "comment_image",
                                 "reply_avatar", "reply_sticker", "reply_image"],
                        help="只处理指定字段类型 (默认: 全部)")
    parser.add_argument("--max-tasks", type=int, default=None,
                        help="每个作者最多处理多少个 task (默认: 不限制)")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"并发下载数 (默认: {CONCURRENCY})")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"每批处理的 aweme_id 数量 (默认: {BATCH_SIZE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="只扫描不上传，预览任务数量")
    parser.add_argument("--direct", action="store_true",
                        help="跳过 API 签名，直接用数据库中的原始 URL 下载")
    parser.add_argument("--author", nargs="+", default=None,
                        help="指定要处理的作者 sec_uid（前缀匹配），或 all (默认: 交互选择)")
    args = parser.parse_args()

    DATA_DIR = args.data_dir
    TABLES = args.tables
    FIELDS = args.fields
    MAX_TASKS = args.max_tasks
    CONCURRENCY = args.concurrency
    DRY_RUN = args.dry_run
    BATCH_SIZE = args.batch_size
    DIRECT_MODE = args.direct

    log_path = setup_log(LOG_DIR)
    global _START_TIME, _FAILURES
    _START_TIME = time.time()
    _FAILURES = []
    log(f"[INIT] 日志文件: {log_path}")

    with open(COOKIE_FILE) as f:
        cookie = f.read().strip()

    log("=" * 70)
    log("  [INIT] 抖音资源 → HF Bucket 迁移（v3）")
    log(f"  [INIT] 数据目录:    {DATA_DIR}")
    log(f"  [INIT] 日志目录:    {LOG_DIR}")
    log(f"  [INIT] 作者过滤:    {' '.join(args.author) if args.author else '交互选择'}")
    log(f"  [INIT] 限制表:      {TABLES or '全部'}")
    log(f"  [INIT] 限制字段:    {FIELDS or '全部'}")
    log(f"  [INIT] 最大任务数:  {MAX_TASKS or '不限'}")
    log(f"  [INIT] 批次大小:    {BATCH_SIZE}")
    log(f"  [INIT] 并发数:      {CONCURRENCY}")
    log(f"  [INIT] Dry-run:     {'是' if args.dry_run else '否'}")
    log(f"  [INIT] Direct:      {'是' if args.direct else '否'}")
    log("=" * 70)

    # 发现 & 选择数据库
    databases = discover_databases(DATA_DIR)
    if not databases:
        log(f"\n  [SCAN] ❌ 未找到任何 {{sec_uid}}/sqlite.db")
        log(f"  [SCAN]    请确认目录结构:")
        log(f"  [SCAN]    {DATA_DIR}/")
        log(f"  [SCAN]    ├── MS4wLjABAAAA2F.../sqlite.db")
        log(f"  [SCAN]    ├── MS4wLjABAAAA8K.../sqlite.db")
        log(f"  [SCAN]    └── ...")
        return

    databases = select_databases(databases, args.author)
    if not databases:
        log(f"\n  [SCAN] 未选择任何数据库，退出")
        return

    log(f"\n  [SCAN] 已选择 {len(databases)} 个数据库:")
    for d in databases:
        log(f"  [SCAN] 📁 {d['sec_uid'][:40]}... "
            f"(videos={d['videos']}, comments={d['comments']}, "
            f"replies={d['replies']}, {d['size_mb']:.1f} MB)")

    # HEIF 全局检测
    log(f"\n--- HEIF 格式检测 ---")
    if HEIF_SUPPORT:
        log(f"✅ pillow-heif 已就绪")
    else:
        log(f"⚠️  pillow-heif 不可用: {HEIF_IMPORT_ERROR}")
        log(f"   安装方法: pip install pillow-heif")

    # 逐作者处理
    global_stats = {
        "total_tasks": 0, "uploaded": 0, "updated_rows": 0,
        "failed": 0, "heic_converted": 0, "db_count": 0,
    }

    try:
        async with DouyinClient(cookie, timeout=30) as client:
            # 下载用独立的 httpx 客户端（流式下载需要）
            async with httpx.AsyncClient(
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.douyin.com/"},
                follow_redirects=True,
                timeout=httpx.Timeout(60.0, connect=30.0),
            ) as http_client:
                # 启动时验证 Cookie
                log("\n--- 验证 Cookie ---")
                try:
                    # 用 verify() 检查登录状态（访问 live.douyin.com）
                    nickname = await client.verify()
                    log(f"✅ Cookie 有效，用户: {nickname}")
                except CookieExpiredError as e:
                    log(f"❌ Cookie 无效: {e}")
                    log("请更新 cookie.txt 后重新运行")
                    return
                except (APIRateLimitError, APIServerError):
                    log("⚠️  验证请求失败（可能是限流），继续运行")

                for d in databases:
                    await process_one_db(d["sec_uid"], d["db_path"], client, http_client, global_stats)
    except APIRateLimitError as e:
        log()
        log("=" * 70)
        log(f"  ❌ API 限流，程序终止")
        log(f"  {e}")
        log(f"  已完成的数据已提交，未完成的不会更新数据库")
        log(f"  总耗时: {_elapsed()}")
        log("=" * 70)
        print_failure_summary()
        return
    except CookieExpiredError as e:
        log()
        log("=" * 70)
        log(f"  ❌ Cookie 过期或无效，程序终止")
        log(f"  {e}")
        log(f"  请更新 cookie.txt 后重新运行")
        log(f"  总耗时: {_elapsed()}")
        log("=" * 70)
        print_failure_summary()
        return
    except APIServerError as e:
        log()
        log("=" * 70)
        log(f"  ❌ 抖音服务端错误，程序终止")
        log(f"  {e}")
        log(f"  稍后重试即可")
        log(f"  总耗时: {_elapsed()}")
        log("=" * 70)
        print_failure_summary()
        return
    except KeyboardInterrupt:
        log()
        log("=" * 70)
        log(f"  ⚠️  用户中断 (Ctrl+C)")
        log(f"  已完成的数据已提交")
        log(f"  总耗时: {_elapsed()}")
        log("=" * 70)
        print_failure_summary()
        return

    # 最终汇总
    elapsed_total = time.time() - _START_TIME
    log()
    log("=" * 70)
    log("  全部完成")
    log("=" * 70)
    log(f"  作者数:        {global_stats['db_count']}")
    log(f"  总 task:       {global_stats['total_tasks']}")
    log(f"  上传成功:      {global_stats['uploaded']}")
    log(f"  更新行数:      {global_stats['updated_rows']}")
    log(f"  HEIC 转换:     {global_stats['heic_converted']}")
    log(f"  失败:          {global_stats['failed']}")
    log(f"  总耗时:        {elapsed_total:.0f}s ({_elapsed()})")
    log("=" * 70)

    print_failure_summary()


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("\n用户中断，程序退出")
        print_failure_summary()
    except Exception as e:
        logging.error(f"\n{'='*60}\n程序崩溃: {e}\n{traceback.format_exc()}{'='*60}")
        print_failure_summary()
        raise
