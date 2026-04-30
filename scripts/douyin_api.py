#!/usr/bin/env python3
"""
抖音 Web API — 完全独立版

零外部依赖（仅 stdlib + httpx），可直接运行。
内含：签名算法 + API 客户端 + Web 服务器 + CLI

用法:
  # Web 服务
  python douyin_api.py serve --port 8080 --cookie-file cookie.txt

  # CLI
  python douyin_api.py video 7630088455829370442
  python douyin_api.py comment 7630088455829370442 7630089456955245327
  python douyin_api.py replies 7630088455829370442 7630089456955245327
  python douyin_api.py verify

  # Python 调用
  from douyin_api import DouyinClient
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import random
import struct
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

# ============================================================
#  第一部分: a_bogus 签名算法 (SM3 + RC4 + 自定义 Base64)
# ============================================================

# ---------- RC4 ----------

def _rc4(plaintext: str, key: str) -> str:
    s = list(range(256))
    j = 0
    for i in range(256):
        j = (j + s[i] + ord(key[i % len(key)])) % 256
        s[i], s[j] = s[j], s[i]
    i = j = 0
    out = []
    for ch in plaintext:
        i = (i + 1) % 256
        j = (j + s[i]) % 256
        s[i], s[j] = s[j], s[i]
        out.append(chr(s[(s[i] + s[j]) % 256] ^ ord(ch)))
    return "".join(out)


# ---------- SM3 ----------

def _rotl(x: int, n: int) -> int:
    return ((x << (n % 32)) | (x >> (32 - n % 32))) & 0xFFFFFFFF


def _sm3_ff(j, x, y, z):
    return (x ^ y ^ z) & 0xFFFFFFFF if j < 16 else ((x & y) | (x & z) | (y & z)) & 0xFFFFFFFF


def _sm3_gg(j, x, y, z):
    return (x ^ y ^ z) & 0xFFFFFFFF if j < 16 else ((x & y) | ((~x) & z)) & 0xFFFFFFFF


def _sm3_p0(x):
    return (x ^ _rotl(x, 9) ^ _rotl(x, 17)) & 0xFFFFFFFF


def _sm3_p1(x):
    return (x ^ _rotl(x, 15) ^ _rotl(x, 23)) & 0xFFFFFFFF


def _sm3_compress(v, blk):
    w = [struct.unpack(">I", blk[i * 4:(i + 1) * 4])[0] for i in range(16)]
    for i in range(16, 68):
        a = _sm3_p1(w[i - 16] ^ w[i - 9] ^ _rotl(w[i - 3], 15))
        w.append((a ^ _rotl(w[i - 13], 7) ^ w[i - 6]) & 0xFFFFFFFF)
    w1 = [(w[j] ^ w[j + 4]) & 0xFFFFFFFF for j in range(64)]
    a, b, c, d, e, f, g, h = v
    for j in range(64):
        tj = 0x79CC4519 if j < 16 else 0x7A879D8A
        ss1 = _rotl((_rotl(a, 12) + e + _rotl(tj, j)) & 0xFFFFFFFF, 7)
        ss2 = (ss1 ^ _rotl(a, 12)) & 0xFFFFFFFF
        tt1 = (_sm3_ff(j, a, b, c) + d + ss2 + w1[j]) & 0xFFFFFFFF
        tt2 = (_sm3_gg(j, e, f, g) + h + ss1 + w[j]) & 0xFFFFFFFF
        d, c, b, a = c, _rotl(b, 9), a, tt1
        h, g, f, e = g, _rotl(f, 19), e, _sm3_p0(tt2)
    return [(v[i] ^ [a, b, c, d, e, f, g, h][i]) & 0xFFFFFFFF for i in range(8)]


def _sm3(data: bytes) -> list[int]:
    iv = [0x7380166F, 0x4914B2B9, 0x172442D7, 0xDA8A0600,
           0xA96F30BC, 0x163138AA, 0xE38DEE4D, 0xB0FB0E4E]
    ml = len(data)
    d = bytearray(data)
    d.append(0x80)
    while len(d) % 64 != 56:
        d.append(0)
    d.extend(struct.pack(">Q", ml * 8))
    v = iv[:]
    for i in range(0, len(d), 64):
        v = _sm3_compress(v, d[i:i + 64])
    out = []
    for x in v:
        out.extend([(x >> 24) & 0xFF, (x >> 16) & 0xFF, (x >> 8) & 0xFF, x & 0xFF])
    return out


def _sm3d(data: bytes) -> list[int]:
    return _sm3(bytes(_sm3(data)))


# ---------- 自定义 Base64 ----------

_B64 = {
    "s3": "ckdp1h4ZKsUB80/Mfvw36XIgR25+WQAlEi7NLboqYTOPuzmFjJnryx9HVGDaStCe",
    "s4": "Dkdpgh2ZmsQB80/MfvV36XI1R45-WUAlEixNLwoqYTOPuzKFjJnry79HbGcaStCe",
}
_C = {"0": 16515072, "1": 258048, "2": 4032}


def _b64enc(s: str, tbl: str) -> str:
    t, c, r, ln = _B64[tbl], _C, [], 0
    for i in range(len(s) // 3 * 4):
        if i // 4 != ln:
            ln += 1
        v = (ord(s[ln * 3]) << 16) | (ord(s[ln * 3 + 1]) << 8) | ord(s[ln * 3 + 2])
        k = i % 4
        r.append(t[(v & c["0"]) >> 18 if k == 0 else (v & c["1"]) >> 12 if k == 1 else (v & c["2"]) >> 6 if k == 2 else v & 63])
    return "".join(r)


def _b64std(s: str) -> str:
    return base64.b64encode(s.encode("latin-1")).decode()


# ---------- 签名组装 ----------

_WIN = "1536|747|1536|834|0|30|0|0|1536|834|1536|864|1525|747|24|24|Win32"


def _gen_rand() -> str:
    def gr(r, o):
        return [(r & 255 & 170) | o[0] & 85, (r & 255 & 85) | o[0] & 170,
                (r >> 8 & 255 & 170) | o[1] & 85, (r >> 8 & 255 & 85) | o[1] & 170]
    r = []
    for o in [[3, 45], [1, 0], [1, 5]]:
        r.extend(gr(random.randint(0, 9999), o))
    return "".join(chr(x) for x in r)


def _bb(url: str, ua: str, args: list[int]) -> str:
    suf = "cus"

    ul = _sm3d((url + suf).encode("utf-8"))
    cl = _sm3d(suf.encode("utf-8"))
    rk = chr(0) + chr(1) + chr(args[2])
    ue = _b64enc(_rc4(ua, rk), "s3")
    uh = _sm3(ue.encode("utf-8"))

    ts = int(time.time() * 1000)
    b: dict[int, Any] = {8: 3, 10: ts, 18: 44, 19: [1, 0, 1, 5]}
    b[15] = {"aid": 6383, "pageId": 6241, "boe": False, "ddrt": 7,
             "paths": {"include": [{}] * 7, "exclude": []},
             "track": {"mode": 0, "delay": 300, "paths": []}, "dump": True, "rpU": ""}
    b[16] = ts

    for k, sh in [(20, 24), (21, 16), (22, 8), (23, 0)]:
        b[k] = (b[16] >> sh) & 255
    b[24] = b[16] // 256**4
    b[25] = b[16] // 256**5

    for i, sh in enumerate([24, 16, 8, 0]):
        b[26 + i] = (args[0] >> sh) & 255
    b[30] = (args[1] // 256) & 255
    b[31] = args[1] % 256 & 255
    b[32] = (args[1] >> 24) & 255
    b[33] = (args[1] >> 16) & 255
    for i, sh in enumerate([24, 16, 8, 0]):
        b[34 + i] = (args[2] >> sh) & 255

    b[38], b[39] = ul[21], ul[22]
    b[40], b[41] = cl[21], cl[22]
    b[42], b[43] = uh[23], uh[24]

    for k, sh in [(44, 24), (45, 16), (46, 8), (47, 0)]:
        b[k] = (b[10] >> sh) & 255
    b[48] = b[8]
    b[49] = b[10] // 256**4
    b[50] = b[10] // 256**5

    pg = b[15]["pageId"]
    b[51] = pg
    for k, sh in [(52, 24), (53, 16), (54, 8), (55, 0)]:
        b[k] = (pg >> sh) & 255
    ad = b[15]["aid"]
    b[56] = ad
    b[57] = ad & 255
    b[58] = (ad >> 8) & 255
    b[59] = (ad >> 16) & 255
    b[60] = (ad >> 24) & 255

    wl = [ord(c) for c in _WIN]
    b[64] = len(wl)
    b[65] = len(wl) & 255
    b[66] = (len(wl) >> 8) & 255
    b[69], b[70], b[71] = 0, 0, 0

    b[72] = (b[18] ^ b[20] ^ b[26] ^ b[30] ^ b[38] ^ b[40] ^ b[42] ^
             b[21] ^ b[27] ^ b[31] ^ b[35] ^ b[39] ^ b[41] ^ b[43] ^
             b[22] ^ b[28] ^ b[32] ^ b[36] ^ b[23] ^ b[29] ^ b[33] ^
             b[37] ^ b[44] ^ b[45] ^ b[46] ^ b[47] ^ b[48] ^ b[49] ^
             b[50] ^ b[24] ^ b[25] ^ b[52] ^ b[53] ^ b[54] ^ b[55] ^
             b[57] ^ b[58] ^ b[59] ^ b[60] ^ b[65] ^ b[66] ^ b[70] ^ b[71])

    bb = [b[18], b[20], b[52], b[26], b[30], b[34], b[58], b[38],
          b[40], b[53], b[42], b[21], b[27], b[54], b[55], b[31],
          b[35], b[57], b[39], b[41], b[43], b[22], b[28], b[32],
          b[60], b[36], b[23], b[29], b[33], b[37], b[44], b[45],
          b[59], b[46], b[47], b[48], b[49], b[50], b[24], b[25],
          b[65], b[66], b[70], b[71]]
    bb.extend(wl)
    bb.append(b[72])

    return _rc4("".join(chr(x) for x in bb), chr(121))


def _sign(url: str, ua: str, args: list[int]) -> str:
    return _b64enc(_gen_rand() + _bb(url, ua, args), "s4") + "="


def sign_datail(params: str, ua: str) -> str:
    return _sign(params, ua, [0, 1, 14])


def sign_reply(params: str, ua: str) -> str:
    return _sign(params, ua, [0, 1, 8])


def sign_request(params: str, ua: str, method: str = "sign_datail") -> str:
    return sign_reply(params, ua) if method == "sign_reply" else sign_datail(params, ua)


# ============================================================
#  第二部分: API 客户端
# ============================================================

COMMON_PARAMS = {
    "device_platform": "webapp", "aid": "6383", "channel": "channel_pc_web",
    "update_version_code": "170400", "pc_client_type": "1",
    "version_code": "290100", "version_name": "29.1.0",
    "cookie_enabled": "true", "screen_width": "1920", "screen_height": "1080",
    "browser_language": "zh-CN", "browser_platform": "Win32",
    "browser_name": "Chrome", "browser_version": "132.0.0.0",
    "browser_online": "true", "platform": "PC",
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")

BASE = "https://www.douyin.com"

VIDEO_FLAT = {
    "author_nickname": ("author", "nickname"), "author_unique_id": ("author", "unique_id"),
    "author_sec_uid": ("author", "sec_uid"), "author_uid": ("author", "uid"),
    "author_avatar": ("author", "avatar_thumb", "url_list"),
    "video_uri": ("video", "play_addr", "uri"),
    "video_url_list": ("video", "play_addr", "url_list"),
    "video_width": ("video", "play_addr", "width"),
    "video_height": ("video", "play_addr", "height"),
    "video_duration": ("video", "duration"),
    "video_cover": ("video", "cover", "url_list"),
    "video_origin_cover": ("video", "origin_cover", "url_list"),
    "video_dynamic_cover": ("video", "dynamic_cover", "url_list"),
    "video_download_addr": ("video", "download_addr", "url_list"),
    "digg_count": ("statistics", "digg_count"),
    "comment_count": ("statistics", "comment_count"),
    "collect_count": ("statistics", "collect_count"),
    "share_count": ("statistics", "share_count"),
    "play_count": ("statistics", "play_count"),
}

USER_FLAT = {
    "user_uid": ("user", "uid"), "user_nickname": ("user", "nickname"),
    "user_unique_id": ("user", "unique_id"),
    "user_avatar": ("user", "avatar_thumb", "url_list"),
}


def _dg(obj, *ks):
    for k in ks:
        if isinstance(obj, dict):
            obj = obj.get(k)
        else:
            return None
    return obj


def _flatten(item, flat_map):
    for fk, path in flat_map.items():
        v = _dg(item, *path)
        if v is not None:
            item[fk] = v
    return item


def _filter(item, fields, flat_map):
    r = {}
    for f in fields:
        if f in item:
            r[f] = item[f]
    for fk, path in flat_map.items():
        if fk in fields:
            v = _dg(item, *path)
            if v is not None:
                r[fk] = v
    return r


class CookieExpiredError(Exception):
    """Cookie 过期或无效 (401/403/未登录/返回HTML)"""
    pass


class APIRateLimitError(Exception):
    """API 被限流 (429)"""
    pass


class APIServerError(Exception):
    """服务端错误 (5xx) 或网络错误"""
    pass


class APIError(Exception):
    """其他业务错误"""
    pass


@dataclass
class Result:
    code: int = 0
    msg: str = "ok"
    data: Any = None

    @property
    def ok(self):
        return self.code == 0


class DouyinClient:
    def __init__(self, cookie: str, timeout: int = 15):
        self.cookie = cookie.strip()
        self.timeout = timeout
        self._c: Optional[httpx.AsyncClient] = None

    async def _cli(self) -> httpx.AsyncClient:
        if not self._c or self._c.is_closed:
            self._c = httpx.AsyncClient(timeout=httpx.Timeout(self.timeout, connect=10), follow_redirects=True)
        return self._c

    async def close(self):
        if self._c and not self._c.is_closed:
            await self._c.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        await self.close()

    def _sign(self, params: dict, method="sign_datail") -> dict:
        m = {**COMMON_PARAMS, **params}
        q = "&".join(k + "=" + urllib.parse.quote(str(v)) for k, v in m.items())
        m["a_bogus"] = sign_request(q, UA, method)
        return m

    async def _get(self, path: str, params: dict, sm="sign_datail") -> dict:
        s = self._sign(params, sm)
        h = {**{"User-Agent": UA, "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9", "Referer": "https://www.douyin.com/",
                "sec-fetch-site": "same-origin", "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty"}, "Cookie": self.cookie}
        c = await self._cli()
        try:
            r = await c.get(BASE + path, params=s, headers=h)
        except (httpx.NetworkError, httpx.TimeoutException) as e:
            raise APIServerError(f"网络错误 ({type(e).__name__}): {path}") from e

        if r.status_code == 429:
            raise APIRateLimitError(f"429 Too Many Requests: {path}")
        if r.status_code in (401, 403):
            raise CookieExpiredError(f"{r.status_code} 认证失败: {path}")
        if r.status_code >= 500:
            raise APIServerError(f"{r.status_code} 服务端错误: {path}")
        r.raise_for_status()

        t = r.text
        if not t or t.strip().startswith("<"):
            raise CookieExpiredError("返回 HTML，Cookie 可能已过期")
        d = r.json()
        sc = d.get("status_code", -1)
        if sc == 8:
            raise CookieExpiredError("未登录")
        if sc != 0:
            raise APIError(f"status_code={sc}: {d.get('status_msg', '')}")
        return d

    # ---------- 作品 ----------

    async def get_video(self, aweme_id: str, fields=None, flatten=True) -> Result:
        d = await self._get("/aweme/v1/web/aweme/detail/",
                            {"aweme_id": aweme_id, "request_source": "600", "origin_type": "video_page"})
        det = d.get("aweme_detail")
        if not det:
            return Result(code=-1, msg="作品不存在")
        if fields:
            det = _filter(det, fields, VIDEO_FLAT)
        elif flatten:
            _flatten(det, VIDEO_FLAT)
        return Result(data=det)

    async def get_videos(self, sec_uid: str, cursor=0, count=20) -> Result:
        d = await self._get("/aweme/v1/web/aweme/post/",
                            {"sec_user_id": sec_uid, "max_cursor": str(cursor), "count": str(count),
                             "from_user_page": "1", "publish_video_strategy_type": "2"})
        return Result(data={"aweme_list": d.get("aweme_list", []),
                            "has_more": bool(d.get("has_more")), "max_cursor": d.get("max_cursor", 0)})

    # ---------- 评论 ----------

    async def get_comment(self, aweme_id: str, cid: str, fields=None, flatten=True) -> Result:
        d = await self._get("/aweme/v1/web/comment/list/",
                            {"aweme_id": aweme_id, "insert_ids": cid, "cursor": "0", "count": "1", "item_type": "0"})
        cs = d.get("comments", [])
        if not cs:
            return Result(code=-1, msg="评论不存在")
        c = cs[0]
        if fields:
            c = _filter(c, fields, USER_FLAT)
        elif flatten:
            _flatten(c, USER_FLAT)
        return Result(data=c)

    async def get_comments(self, aweme_id: str, cursor=0, count=20, insert_ids=None, fields=None, flatten=True) -> Result:
        p = {"aweme_id": aweme_id, "cursor": str(cursor), "count": str(count), "item_type": "0"}
        if insert_ids:
            p["insert_ids"] = insert_ids
        d = await self._get("/aweme/v1/web/comment/list/", p)
        cs = d.get("comments", [])
        if fields:
            cs = [_filter(c, fields, USER_FLAT) for c in cs]
        elif flatten:
            for c in cs:
                _flatten(c, USER_FLAT)
        return Result(data={"comments": cs, "has_more": bool(d.get("has_more")),
                            "cursor": d.get("cursor", 0), "total": d.get("total", 0)})

    # ---------- 回复 ----------

    async def get_reply(self, aweme_id: str, parent_cid: str, reply_cid: str, fields=None, flatten=True) -> Result:
        cur = 0
        while True:
            d = await self._get("/aweme/v1/web/comment/list/reply/",
                                {"item_id": aweme_id, "comment_id": parent_cid,
                                 "cursor": str(cur), "count": "20", "item_type": "0", "cut_version": "1"},
                                sm="sign_reply")
            for r in d.get("comments", []):
                if r.get("cid") == reply_cid:
                    if fields:
                        r = _filter(r, fields, USER_FLAT)
                    elif flatten:
                        _flatten(r, USER_FLAT)
                    return Result(data=r)
            if not d.get("has_more"):
                break
            cur = d.get("cursor", 0)
            if not cur:
                break
        return Result(code=-1, msg="回复不存在")

    async def get_replies(self, aweme_id: str, comment_id: str, cursor=0, count=20, fields=None, flatten=True) -> Result:
        d = await self._get("/aweme/v1/web/comment/list/reply/",
                            {"item_id": aweme_id, "comment_id": comment_id,
                             "cursor": str(cursor), "count": str(count), "item_type": "0", "cut_version": "1"},
                            sm="sign_reply")
        rs = d.get("comments", [])
        if fields:
            rs = [_filter(r, fields, USER_FLAT) for r in rs]
        elif flatten:
            for r in rs:
                _flatten(r, USER_FLAT)
        return Result(data={"replies": rs, "has_more": bool(d.get("has_more")),
                            "cursor": d.get("cursor", 0), "total": d.get("total", 0)})

    async def get_all_replies(self, aweme_id: str, comment_id: str, fields=None, flatten=True) -> Result:
        all_r, cur = [], 0
        while True:
            d = await self._get("/aweme/v1/web/comment/list/reply/",
                                {"item_id": aweme_id, "comment_id": comment_id,
                                 "cursor": str(cur), "count": "20", "item_type": "0", "cut_version": "1"},
                                sm="sign_reply")
            all_r.extend(d.get("comments", []))
            if not d.get("has_more"):
                break
            cur = d.get("cursor", 0)
            if not cur:
                break
        if fields:
            all_r = [_filter(r, fields, USER_FLAT) for r in all_r]
        elif flatten:
            for r in all_r:
                _flatten(r, USER_FLAT)
        return Result(data={"replies": all_r, "total": d.get("total", len(all_r))})

    # ---------- 翻页拉取（返回原始数据，供迁移工具使用）----------

    async def fetch_all_videos(self, sec_uid: str) -> List[Dict]:
        """翻页拉取全部作品列表，返回原始 API 数据"""
        all_videos, cursor = [], 0
        while True:
            d = await self._get("/aweme/v1/web/aweme/post/", {
                "sec_user_id": sec_uid, "max_cursor": str(cursor), "count": "30",
                "from_user_page": "1", "publish_video_strategy_type": "2",
            })
            items = d.get("aweme_list") or []
            all_videos.extend(items)
            if not d.get("has_more"):
                break
            cursor = d.get("max_cursor", 0)
            if not cursor:
                break
            await asyncio.sleep(1 + random.uniform(0.3, 0.7))
        return all_videos

    async def fetch_all_comments(self, aweme_id: str) -> List[Dict]:
        """翻页拉取某作品的全部评论，返回原始 API 数据"""
        all_comments, cursor = [], 0
        while True:
            d = await self._get("/aweme/v1/web/comment/list/", {
                "aweme_id": aweme_id, "cursor": str(cursor),
                "count": "50", "item_type": "0",
            })
            items = d.get("comments") or []
            all_comments.extend(items)
            if not d.get("has_more"):
                break
            cursor = d.get("cursor", 0)
            if not cursor:
                break
            await asyncio.sleep(1 + random.uniform(0.3, 0.7))
        return all_comments

    async def fetch_all_replies(self, aweme_id: str, comment_id: str) -> List[Dict]:
        """翻页拉取某评论的全部回复，返回原始 API 数据"""
        all_replies, cursor = [], 0
        while True:
            d = await self._get("/aweme/v1/web/comment/list/reply/", {
                "item_id": aweme_id, "comment_id": comment_id,
                "cursor": str(cursor), "count": "50",
                "item_type": "0", "cut_version": "1",
            }, sm="sign_reply")
            items = d.get("comments") or []
            all_replies.extend(items)
            if not d.get("has_more"):
                break
            cursor = d.get("cursor", 0)
            if not cursor:
                break
            await asyncio.sleep(1 + random.uniform(0.3, 0.7))
        return all_replies

    async def verify(self) -> str:
        import re
        c = await self._cli()
        r = await c.get("https://live.douyin.com/",
                        headers={"User-Agent": UA, "Accept": "text/html", "Cookie": self.cookie})
        m = re.search(r'defaultHeaderUserInfo.*?isLogin.*?(true|false).*?nickname\\?"[,:]\\?"([^"\\]+)', r.text)
        if m and m.group(1) == "true":
            return m.group(2)
        raise CookieExpiredError("Cookie 已过期")


# ============================================================
#  第三部分: Web 服务器 (内嵌，用于测试)
# ============================================================

def create_app(cookie: str):
    """创建 HTTP Handler"""
    from http.server import BaseHTTPRequestHandler
    from urllib.parse import urlparse, parse_qs
    import threading

    _loop = asyncio.new_event_loop()
    _client = DouyinClient(cookie)
    _thread = None

    def _start_loop():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    _thread = threading.Thread(target=_start_loop, daemon=True)
    _thread.start()

    def _run(coro):
        return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=20)

    class Handler(BaseHTTPRequestHandler):
        def _cors(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "*")

        def _json(self, data, status=200):
            body = json.dumps(data, ensure_ascii=False, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._cors()
            self.end_headers()
            self.wfile.write(body)

        def _err(self, msg, status=400):
            self._json({"error": msg}, status)

        def do_OPTIONS(self):
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path.strip("/")
            q = parse_qs(parsed.query)
            p = lambda k: q.get(k, [None])[0]

            try:
                if not path or path == "health":
                    self._json({"status": "ok", "endpoints": [
                        "/video", "/comment", "/comment/list", "/replies", "/verify"]})

                elif path == "video":
                    aid = p("aweme_id")
                    if not aid:
                        return self._err("缺少 aweme_id")
                    fields = p("fields")
                    r = _run(_client.get_video(aid, fields=fields.split(",") if fields else None))
                    self._json(r.data if r.ok else {"error": r.msg})

                elif path in ("comment", "comment/list"):
                    aid = p("aweme_id")
                    if not aid:
                        return self._err("缺少 aweme_id")
                    iid = p("insert_ids")
                    cur = int(p("cursor") or 0)
                    cnt = int(p("count") or 20)
                    fields = p("fields")
                    if iid:
                        r = _run(_client.get_comment(aid, iid, fields=fields.split(",") if fields else None))
                    else:
                        r = _run(_client.get_comments(aid, cur, cnt, fields=fields.split(",") if fields else None))
                    self._json(r.data if r.ok else {"error": r.msg})

                elif path in ("replies", "comment/list/reply"):
                    iid = p("item_id")
                    cid = p("comment_id")
                    if not iid or not cid:
                        return self._err("缺少 item_id 或 comment_id")
                    cur = int(p("cursor") or 0)
                    cnt = int(p("count") or 20)
                    fields = p("fields")
                    r = _run(_client.get_replies(iid, cid, cur, cnt, fields=fields.split(",") if fields else None))
                    self._json(r.data if r.ok else {"error": r.msg})

                elif path == "verify":
                    try:
                        nick = _run(_client.verify())
                        self._json({"logged_in": True, "nickname": nick})
                    except CookieExpiredError as e:
                        self._json({"logged_in": False, "error": str(e)})

                else:
                    self._err(f"未知路由: {path}", 404)

            except CookieExpiredError as e:
                self._json({"error": str(e)}, 401)
            except Exception as e:
                self._json({"error": str(e)}, 500)

        def log_message(self, fmt, *args):
            pass

    return Handler


def serve(host: str, port: int, cookie: str):
    from http.server import HTTPServer
    handler = create_app(cookie)
    server = HTTPServer((host, port), handler)
    print(f"🚀 抖音 API 服务已启动: http://{host}:{port}")
    print(f"   端点:")
    print(f"     GET /video?aweme_id=xxx")
    print(f"     GET /comment?aweme_id=xxx&insert_ids=cid")
    print(f"     GET /comment/list?aweme_id=xxx&cursor=0&count=20")
    print(f"     GET /replies?item_id=xxx&comment_id=xxx")
    print(f"     GET /verify")
    print(f"   按 Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
        server.server_close()


# ============================================================
#  第四部分: CLI
# ============================================================

def _cli():
    ap = argparse.ArgumentParser(description="抖音 Web API")
    sub = ap.add_subparsers(dest="cmd")

    # serve
    sp = sub.add_parser("serve", help="启动 Web 服务")
    sp.add_argument("--host", default="0.0.0.0")
    sp.add_argument("--port", type=int, default=8080)
    sp.add_argument("--cookie-file", default="cookie.txt")
    sp.add_argument("--cookie", default=None)

    # video
    vp = sub.add_parser("video", help="获取作品")
    vp.add_argument("aweme_id")
    vp.add_argument("--fields", default=None)

    # comment
    cp = sub.add_parser("comment", help="获取评论")
    cp.add_argument("aweme_id")
    cp.add_argument("cid")
    cp.add_argument("--fields", default=None)

    # replies
    rp = sub.add_parser("replies", help="获取回复")
    rp.add_argument("aweme_id")
    rp.add_argument("comment_id")
    rp.add_argument("--count", type=int, default=20)
    rp.add_argument("--fields", default=None)

    # verify
    sub.add_parser("verify", help="验证 Cookie")

    # 通用
    ap.add_argument("--cookie-file", default="cookie.txt")
    ap.add_argument("--cookie", default=None)

    args = ap.parse_args()

    if not args.cmd:
        ap.print_help()
        return

    # 获取 Cookie
    cookie = getattr(args, "cookie", None)
    if not cookie:
        cf = getattr(args, "cookie_file", "cookie.txt")
        if os.path.exists(cf):
            cookie = open(cf).read().strip()
        else:
            print(f"❌ 无 Cookie，请用 --cookie 或 --cookie-file 指定")
            sys.exit(1)

    if args.cmd == "serve":
        serve(args.host, args.port, cookie)
        return

    async def run():
        async with DouyinClient(cookie) as c:
            if args.cmd == "video":
                fields = args.fields.split(",") if args.fields else None
                r = await c.get_video(args.aweme_id, fields=fields)
                if r.ok:
                    print(json.dumps(r.data, ensure_ascii=False, indent=2, default=str))
                else:
                    print(f"❌ {r.msg}")

            elif args.cmd == "comment":
                fields = args.fields.split(",") if args.fields else None
                r = await c.get_comment(args.aweme_id, args.cid, fields=fields)
                if r.ok:
                    print(json.dumps(r.data, ensure_ascii=False, indent=2, default=str))
                else:
                    print(f"❌ {r.msg}")

            elif args.cmd == "replies":
                fields = args.fields.split(",") if args.fields else None
                r = await c.get_all_replies(args.aweme_id, args.comment_id, fields=fields)
                if r.ok:
                    print(f"共 {r.data['total']} 条回复：")
                    print(json.dumps(r.data["replies"], ensure_ascii=False, indent=2, default=str))

            elif args.cmd == "verify":
                try:
                    nick = await c.verify()
                    print(f"✅ Cookie 有效，用户: {nick}")
                except CookieExpiredError as e:
                    print(f"❌ {e}")

    asyncio.run(run())


if __name__ == "__main__":
    _cli()
