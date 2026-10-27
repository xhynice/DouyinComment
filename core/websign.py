"""x-secsdk-web-signature（secsdk webSign）纯 Python 实现。

来源与依据
----------
算法逻辑对应上游 DouYin_Spider 的 utils/secsdk_web_sign.py（2026-08-16
浏览器实录逆向）：签名明文为 ``{uifid}_{timestamp}_{CONST}_{canonical_query}``，
取 MD5 十六进制小写；CONST 是 SDK 常量池里的固定盐，与账号/会话无关。

为什么需要它
------------
``/aweme/v1/web/aweme/post/``（作品列表）在 secsdk 的 webSign 策略表里，
属于受保护接口。只发 a_bogus、不发 x-secsdk-web-signature 时，服务端会
返回 HTTP 403——2026-10-27 实测：同一 Cookie、同一用户，DouyinComment
旧版（无此签名）403，而上游（带此签名）成功返回 18 条作品。

使用约束（照抄上游注释，务必遵守）
----------------------------------
签名是对规范化后的 query 算的，服务端也按收到的 query 校验，所以必须
发送 ``sign_url()`` 返回的完整 URL，不能再把参数拆开用 httpx 的 params
重编码一遍发出去，否则签名输入与实际发送的 query 对不上。
"""

import hashlib
import time
import urllib.parse

# SDK 常量池里的固定盐（与账号 / 会话无关，上游已探针确认）
WEBSIGN_CONST = "A96D855A08C0A9707F8BEF0D9A527E4E"

# 走签名的接口路径（secsdk webSign 策略配置，www.douyin.com）
PROTECTED_PATHS_GET = (
    "/aweme/v1/web/aweme/detail/",
    "/aweme/v1/web/aweme/post/",
    "/aweme/v1/web/aweme/favorite/",
    "/aweme/v1/web/aweme/listcollection/",
    "/aweme/v1/web/mix/aweme/",
    "/aweme/v1/web/tab/feed/",
    "/aweme/v1/web/mix/list/",
    "/aweme/v1/web/music/aweme/",
    "/aweme/v1/web/music/list/",
    "/aweme/v1/web/mix/detail/",
    "/aweme/v1/web/mix/listcollection/",
    "/aweme/v1/web/music/detail/",
    "/aweme/v1/web/collects/list/",
    "/aweme/v1/web/collects/video/list/",
)

_ENCODE_SAFE = "!*'()"
_SIG_KEY = "x-secsdk-web-signature"
_TS_KEY = "timestamp"


def _encode_component(value):
    """等价于 JS 的 encodeURIComponent。"""
    return urllib.parse.quote(str(value), safe=_ENCODE_SAFE, encoding="utf-8")


def canonical_query(query):
    """按 SDK 规则规范化 query 字符串（顺序不变，value 重编码，key 只解码）。"""
    parts = []
    for pair in query.split("&"):
        if not pair:
            continue
        if "=" in pair:
            key, value = pair.split("=", 1)
        else:
            key, value = pair, ""
        key = urllib.parse.unquote_plus(key, encoding="utf-8", errors="replace")
        value = urllib.parse.unquote_plus(value, encoding="utf-8", errors="replace")
        parts.append("%s=%s" % (key, _encode_component(value)))
    return "&".join(parts)


def _split(url):
    """拆成 (base, query)，并剥掉已有的 timestamp / 签名字段，保证幂等。"""
    base, _, query = url.partition("?")
    kept = []
    for pair in query.split("&"):
        if not pair:
            continue
        name = pair.split("=", 1)[0]
        if name in (_TS_KEY, _SIG_KEY):
            continue
        kept.append(pair)
    return base, "&".join(kept)


def sign(url, ts=None, uifid=None):
    """计算 x-secsdk-web-signature，返回 (ts, signature, signed_query)。"""
    if ts is None:
        ts = int(time.time())
    ts = int(ts)

    _, raw_query = _split(url)
    canon = canonical_query(raw_query)

    names = [p.split("=", 1)[0] for p in canon.split("&") if p]
    if "uifid" not in names and uifid:
        canon = ("%s&uifid=%s" % (canon, _encode_component(uifid))
                 if canon else "uifid=%s" % _encode_component(uifid))

    signed_query = ("%s&%s=%d" % (canon, _TS_KEY, ts)) if canon \
        else "%s=%d" % (_TS_KEY, ts)

    uifid_value = ""
    for pair in signed_query.split("&"):
        if pair.startswith("uifid="):
            uifid_value = urllib.parse.unquote_plus(pair[6:], encoding="utf-8",
                                                    errors="replace")
            break
    if not uifid_value and uifid:
        uifid_value = uifid

    plain = "%s_%d_%s_%s" % (uifid_value, ts, WEBSIGN_CONST, signed_query)
    signature = hashlib.md5(plain.encode("utf-8")).hexdigest()
    return ts, signature, signed_query


def sign_url(url, ts=None, uifid=None):
    """返回可直接发送的完整 URL（规范化 query + timestamp + 签名）。"""
    base, _ = _split(url)
    ts, signature, signed_query = sign(url, ts=ts, uifid=uifid)
    return "%s?%s&%s=%s" % (base, signed_query, _SIG_KEY, signature)


def is_protected(path, method="GET"):
    """判断该 path 是否需要加签。"""
    return path in PROTECTED_PATHS_GET
