import asyncio
import httpx
import json as _json
import urllib.parse
import random
import os
import re
import sys
import time
from typing import Dict, Optional, Tuple
from core.logger import logger
from core.sign import sign_request


class CookieExpiredError(Exception):
    """Cookie过期或无效"""
    pass


class DouyinAPI:
    
    COMMON_PARAMS = {
        'device_platform': 'webapp',
        'aid': '6383',
        'channel': 'channel_pc_web',
        'update_version_code': '170400',
        'pc_client_type': '1',
        'pc_libra_divert': 'Windows',
        'version_code': '290100',
        'version_name': '29.1.0',
        'cookie_enabled': 'true',
        'screen_width': '1920',
        'screen_height': '1080',
        'browser_language': 'zh-CN',
        'browser_platform': 'Win32',
        'browser_name': 'Chrome',
        'browser_version': '132.0.0.0',
        'browser_online': 'true',
        'engine_name': 'Blink',
        'engine_version': '132.0.0.0',
        'os_name': 'Windows',
        'os_version': '10',
        'cpu_core_num': '16',
        'device_memory': '8',
        'platform': 'PC',
        'downlink': '10',
        'effective_type': '4g',
        'round_trip_time': '50',
    }
    
    COMMON_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "sec-ch-ua-platform": "Windows",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "referer": "https://www.douyin.com/?recommend=1",
        "priority": "u=1, i",
        "pragma": "no-cache",
        "cache-control": "no-cache",
        "accept-language": "zh-CN,zh;q=0.9",
        "accept": "application/json, text/plain, */*",
        "dnt": "1",
    }
    
    _instance = None

    def __new__(cls, cookie: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, cookie: str = None):
        if hasattr(self, '_initialized') and self._initialized:
            if cookie is not None:
                self.cookie = cookie
                self._parse_cookie()
            return
        
        self.cookie = cookie or ""
        self._cookie_dict = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._verified = False
        self._parse_cookie()
        self._initialized = True
        
        # 阶梯式暂停机制相关
        self._error_count = 0  # 错误次数
        self._pause_until = 0  # 暂停到某个时间点（时间戳）
        self._pause_durations = [
            15 * 60,   # 第 1 次错误：暂停 15 分钟
            30 * 60,   # 第 2 次错误：暂停 30 分钟
            60 * 60,   # 第 3 次错误：暂停 1 小时
        ]
        self._max_errors = 4  # 最多 4 次错误，之后退出

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=30.0),
                follow_redirects=True
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    def __del__(self):
        if self._client and not self._client.is_closed:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._client.aclose())
            except RuntimeError:
                try:
                    asyncio.run(self._client.aclose())
                except Exception:
                    pass
            except Exception:
                pass

    @classmethod
    async def close_instance(cls):
        if cls._instance:
            await cls._instance.close()

    def _parse_cookie(self):
        if not self.cookie:
            return
        try:
            content = self.cookie.strip()
            if not content:
                return
            content = content.replace('\n', ';').replace('\r', '')
            for item in content.split(';'):
                item = item.strip()
                if not item or '=' not in item:
                    continue
                name, value = item.split('=', 1)
                if name.strip():
                    self._cookie_dict[name.strip()] = value.strip()
        except Exception as e:
            logger.warning(f"[API] Cookie解析失败: {e}")

    async def verify_cookie(self):
        """验证cookie是否有效。无效则抛出CookieExpiredError。"""
        if not self.cookie:
            msg = "cookie.txt 为空，请从浏览器导出Cookie"
            logger.error(f"[Cookie] ❌ {msg}")
            raise CookieExpiredError(msg)

        try:
            client = await self._get_client()
            resp = await client.get(
                'https://live.douyin.com/',
                headers={
                    'User-Agent': self.COMMON_HEADERS['User-Agent'],
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                    'Cookie': self.cookie,
                },
            )
            body = resp.text
            m = re.search(
                r'defaultHeaderUserInfo.*?isLogin.*?(true|false).*?nickname\\?"[,:]\\?"([^"\\]+)',
                body, re.DOTALL
            )
            if m and m.group(1) == 'true':
                nickname = m.group(2)
                logger.info(f"[Cookie] ✓ 验证通过，登录用户: {nickname}")
                self._verified = True
                return
            elif m:
                msg = "Cookie已过期或未登录（isLogin=false），请重新从浏览器导出Cookie"
                logger.error(f"[Cookie] ❌ {msg}")
                raise CookieExpiredError(msg)
            else:
                m2 = re.search(r'defaultHeaderUserInfo.*?isLogin.*?(true|false)', body, re.DOTALL)
                if m2 and m2.group(1) == 'true':
                    logger.warning("[Cookie] ✓ 验证通过（未解析到昵称）")
                    self._verified = True
                    return
                msg = "无法解析登录状态（页面结构可能已更新），请检查Cookie是否有效"
                logger.error(f"[Cookie] ❌ {msg}")
                raise CookieExpiredError(msg)
        except CookieExpiredError:
            raise
        except Exception as e:
            msg = f"Cookie验证失败: {e}"
            logger.error(f"[Cookie] ❌ {msg}")
            raise CookieExpiredError(msg)

    def _get_ms_token(self, length: int = 120) -> str:
        chars = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789='
        return ''.join(random.choice(chars) for _ in range(length))

    async def _prepare_params(self, params: Dict, headers: Dict, sign_method: str = 'sign_datail') -> Tuple[Dict, Dict]:
        params = dict(params)
        params.update(self.COMMON_PARAMS)
        
        headers = dict(headers)
        headers.update(self.COMMON_HEADERS)
        
        if self.cookie:
            headers['cookie'] = self.cookie
        
        params['msToken'] = self._get_ms_token()
        
        if self._cookie_dict:
            params['screen_width'] = self._cookie_dict.get('dy_swidth', 1920)
            params['screen_height'] = self._cookie_dict.get('dy_sheight', 1080)
            params['cpu_core_num'] = self._cookie_dict.get('device_web_cpu_core', 16)
            params['device_memory'] = self._cookie_dict.get('device_web_memory_size', 8)
            
            s_v_web_id = self._cookie_dict.get('s_v_web_id')
            if s_v_web_id:
                params['verifyFp'] = s_v_web_id
                params['fp'] = s_v_web_id
            
            uifid = self._cookie_dict.get('UIFID_TEMP')
            if uifid:
                params['uifid'] = uifid
        
        # 纯 Python 签名 (无需 Node.js)
        try:
            query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])
            params["a_bogus"] = sign_request(query, headers["User-Agent"], sign_method)
        except Exception as e:
            logger.warning(f"[API] 签名异常 method={sign_method}: {e}")

        return params, headers

    async def _request(self, url: str, params: Dict, headers: Dict) -> Dict:
        while True:
            while self._pause_until > 0:
                remaining = self._pause_until - time.time()
                if remaining > 0:
                    minutes = int(remaining / 60) + 1
                    logger.warning(f"[API] ⏸ 暂停中，等待 {minutes} 分钟后恢复...")
                    await asyncio.sleep(min(remaining, 60))
                else:
                    self._pause_until = 0
                    self._error_count = 0
                    logger.info("[API] ▶ 暂停结束，恢复采集")
            
            if not self._verified:
                await self.verify_cookie()
            
            try:
                client = await self._get_client()
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                
                content = response.text
                if not content or content.strip() == '':
                    logger.warning(f"[API] 空响应 url={url}")
                    if self._handle_error("空响应"):
                        raise CookieExpiredError("API 错误次数达到上限")
                    continue
                
                try:
                    result = response.json()
                    self._error_count = 0
                    return result
                except json.JSONDecodeError as e:
                    logger.error(f"[API] JSON 解析失败 url={url}: {e}")
                    logger.error(f"[API] 响应内容：{content[:200]}")
                    
                    if content.strip().startswith('<!DOCTYPE') or content.strip().startswith('<html'):
                        logger.error(f"[API] 返回 HTML 页面，可能是 403/503 错误")
                        self._handle_error("HTML 页面", is_fatal=True)
                        raise CookieExpiredError(f"API 返回 HTML 页面，Cookie 可能已过期")
                    
                    if self._handle_error("JSON 解析失败"):
                        raise CookieExpiredError("API 错误次数达到上限")
                    continue
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 401):
                    msg = f"API 返回 {e.response.status_code}，Cookie 可能已过期"
                    logger.error(f"[Cookie] ❌ {msg}")
                    self._handle_error("HTTP 错误", is_fatal=True)
                    raise CookieExpiredError(msg)
                logger.error(f"[API] HTTP {e.response.status_code} url={url}")
                if self._handle_error("HTTP 错误"):
                    raise CookieExpiredError("API 错误次数达到上限")
                continue
            except CookieExpiredError:
                raise
            except httpx.TimeoutException:
                logger.warning(f"[API] 请求超时 url={url}")
                if self._handle_error("超时"):
                    raise CookieExpiredError("API 错误次数达到上限")
                continue
            except Exception as e:
                logger.error(f"[API] 请求异常 url={url}: {e}")
                if self._handle_error("异常"):
                    raise CookieExpiredError("API 错误次数达到上限")
                continue

    def _handle_error(self, error_type: str, is_fatal: bool = False) -> bool:
        """处理 API 错误，实现阶梯式暂停机制。返回 True 表示应退出，False 表示应继续"""
        self._error_count += 1
        
        if self._error_count >= self._max_errors:
            logger.error(f"[API] 错误次数达到 {self._error_count} 次，退出采集")
            return True
        
        pause_duration = self._pause_durations[min(self._error_count - 1, len(self._pause_durations) - 1)]
        self._pause_until = time.time() + pause_duration
        
        minutes = pause_duration // 60
        if is_fatal:
            logger.error(f"[API] 发生致命错误：{error_type}，暂停 {minutes} 分钟（第 {self._error_count} 次错误）")
        else:
            logger.warning(f"[API] 发生错误：{error_type}，暂停 {minutes} 分钟（第 {self._error_count} 次错误）")
        
        return False

    async def fetch_videos(self, sec_user_id: str, max_cursor: int = 0, count: int = 30) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/aweme/post/"
        params = {
            "sec_user_id": sec_user_id,
            "max_cursor": str(max_cursor),
            "count": str(count),
            "from_user_page": "1",
            "publish_video_strategy_type": "2",
            "show_live_replay_strategy": "1",
            "need_time_list": "1"
        }
        params, headers = await self._prepare_params(params, {})
        data = await self._request(url, params, headers)

        # 首页为空 → cookie大概率过期
        if not data and max_cursor == 0:
            msg = "作品接口返回空数据，Cookie可能已过期，请更新cookie.txt"
            logger.error(f"[Cookie] ❌ {msg}")
            raise CookieExpiredError(msg)

        return data

    async def fetch_comments(self, aweme_id: str, cursor: int = 0, count: int = 50) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/comment/list/"
        params = {
            "aweme_id": aweme_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0"
        }
        params, headers = await self._prepare_params(params, {})
        return await self._request(url, params, headers)

    async def fetch_replies(self, aweme_id: str, comment_id: str, cursor: int = 0, count: int = 50) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/comment/list/reply/"
        params = {
            "item_id": aweme_id,
            "comment_id": comment_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0"
        }
        params, headers = await self._prepare_params(params, {}, sign_method='sign_reply')
        return await self._request(url, params, headers)
