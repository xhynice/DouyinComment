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
        # 主站公共组（上游 2026-08-15/16 浏览器实录）：缺 support_* 会被部分
        # 接口静默拒绝；round_trip_time 主站实录为 0，旧值 50 是错的。
        'support_h265': '1',
        'support_dash': '1',
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
        'round_trip_time': '0',
    }
    
    UA_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    ]
    UA_COUNT = len(UA_LIST)

    COMMON_HEADERS = {
        "User-Agent": UA_LIST[0],
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
        self._state_lock = asyncio.Lock()
        self._ua_index = random.randint(0, self.UA_COUNT - 1)
        self._retry_method = 'sign_reply'

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

    def _rotate_ua(self):
        ua = self.UA_LIST[self._ua_index % self.UA_COUNT]
        self._ua_index += 1
        self.COMMON_HEADERS["User-Agent"] = ua
        m = re.search(r'Chrome/(\d+)', ua)
        if m:
            v = m.group(1)
            self.COMMON_HEADERS["sec-ch-ua"] = f'"Not A(Brand";v="8", "Chromium";v="{v}", "Google Chrome";v="{v}"'
        return ua

    def _get_webid(self) -> str:
        """webid：主站 44/44 接口都带。上游取登录者 user_unique_id（页面实录），
        这里用 Cookie 里的 uid_tt 近似（同为登录者数字 id）；取不到则用 19 位
        随机数兜底（上游 generate_fake_webid 同款做法）。缺 webid 本身就会
        让部分接口静默拒绝，所以宁可给一个兜底值也不能不带。"""
        uid = self._cookie_dict.get('uid_tt')
        if uid:
            return str(uid)
        return ''.join(random.choice('0123456789') for _ in range(19))

    async def _prepare_params(self, params: Dict, headers: Dict, sign_method: str = 'sign_datail',
                              a_bogus_excludes_verify: bool = False,
                              exclude_uifid: bool = False) -> Tuple[Dict, Dict]:
        """组装请求参数 + 签名。

        a_bogus 的签名输入范围逐接口不同（上游 2026-08-16 浏览器实录）：
        - 评论列表：verifyFp/fp 在 a_bogus 之前，参与签名（默认，旧行为）；
        - 作品列表、回复列表：verifyFp/fp 在 a_bogus 之后，不参与签名
          （a_bogus_excludes_verify=True）；回复列表还不带 uifid
          （exclude_uifid=True，回复是上游注明的严格校验接口之一）。
        顺序错一个，严格校验接口就会判人机验证/403。
        """
        params = dict(params)
        params.update(self.COMMON_PARAMS)

        self._rotate_ua()
        # 旧代码先 headers.update(COMMON_HEADERS)，会把调用方传的 referer 等
        # 覆盖掉；改为公共头打底、调用方头覆盖，调用方才能按接口设 referer。
        headers = dict(self.COMMON_HEADERS)
        headers.update(dict(headers))

        if self.cookie:
            headers['cookie'] = self.cookie

        params['msToken'] = self._get_ms_token()

        if self._cookie_dict:
            params['screen_width'] = self._cookie_dict.get('dy_swidth', 1920)
            params['screen_height'] = self._cookie_dict.get('dy_sheight', 1080)
            params['cpu_core_num'] = self._cookie_dict.get('device_web_cpu_core', 16)
            params['device_memory'] = self._cookie_dict.get('device_web_memory_size', 8)

            # webid：先于 uifid 加入（上游顺序：round_trip_time 后依次
            # webid、uifid、verifyFp、fp）。
            params['webid'] = self._get_webid()

            if not exclude_uifid:
                # 上游取 UIFID Cookie；旧代码只取 UIFID_TEMP，UIFID 优先、TEMP 兜底。
                uifid = self._cookie_dict.get('UIFID') or self._cookie_dict.get('UIFID_TEMP')
                if uifid:
                    params['uifid'] = uifid

        def _sign_a_bogus(p):
            try:
                query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in p.items()])
                p["a_bogus"] = sign_request(query, headers["User-Agent"], sign_method)
            except Exception as e:
                logger.warning(f"[API] 签名异常 method={sign_method}: {e}")

        if a_bogus_excludes_verify:
            # 先算 a_bogus，再补 verifyFp/fp（不参与签名）。
            _sign_a_bogus(params)
            s_v_web_id = self._cookie_dict.get('s_v_web_id')
            if s_v_web_id:
                params['verifyFp'] = s_v_web_id
                params['fp'] = s_v_web_id
        else:
            s_v_web_id = self._cookie_dict.get('s_v_web_id')
            if s_v_web_id:
                params['verifyFp'] = s_v_web_id
                params['fp'] = s_v_web_id
            _sign_a_bogus(params)

        return params, headers

    async def _request_signed(self, signed_url: str, headers: Dict) -> Dict:
        """发送已由 secsdk sign_url() 签好的完整 URL（作品列表专用）。

        必须直接发这个 URL，不能再传 params——httpx 重编码会改变 query，
        服务端按收到的 query 校验 secsdk 签名就会对不上（上游注释原话）。
        403 处理与 _request 一致，但不走空响应重签名逻辑（作品采集外层
        VideoService 已有 3 次重试 + 抖动等待）。
        """
        if not self._verified:
            await self.verify_cookie()
        client = await self._get_client()
        try:
            response = await client.get(signed_url, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 401):
                raise CookieExpiredError(self._forbidden_msg(e.response.status_code))
            logger.error(f"[API] HTTP {e.response.status_code} url={signed_url.split('?')[0]}")
            self._handle_error("HTTP 错误", is_fatal=True)
            raise CookieExpiredError(f"API HTTP {e.response.status_code}")
        content = response.text
        try:
            result = response.json()
        except Exception:
            logger.error(f"[API] JSON 解析失败，响应前 200 字：{content[:200]}")
            self._handle_error("JSON 解析失败", is_fatal=True)
            raise CookieExpiredError("API 返回非 JSON")
        self._error_count = 0
        return result

    def _forbidden_msg(self, status_code: int) -> str:
        """403/401 的文案：不要再一律写成“Cookie 可能已过期”。

        2026-10-27 实测：Cookie 验证通过、评论接口正常时作品接口仍 403，
        根因是签名/风控。验证没通过时才优先怀疑 Cookie。
        """
        if self._verified:
            return (f"API 返回 {status_code}，Cookie 验证已通过，"
                    f"优先排查签名/风控拦截（非 Cookie 失效）")
        return f"API 返回 {status_code}，Cookie 可能已过期"

    async def _request(self, url: str, params: Dict, headers: Dict, _retry: int = 0) -> Dict:
        while True:
            async with self._state_lock:
                if self._pause_until > 0:
                    remaining = self._pause_until - time.time()
                    if remaining > 0:
                        minutes = int(remaining / 60) + 1
                        logger.warning(f"[API] ⏸ 暂停中，等待 {minutes} 分钟后恢复...")
                        await asyncio.sleep(min(remaining, 60))
                        continue
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
                    if _retry < 2:
                        self._rotate_ua()
                        ua = self.COMMON_HEADERS["User-Agent"]
                        headers.update({
                            'User-Agent': ua,
                            'sec-ch-ua': self.COMMON_HEADERS["sec-ch-ua"],
                        })
                        params.pop('a_bogus', None)
                        params['msToken'] = self._get_ms_token()
                        query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])
                        sm = 'sign_datail' if _retry == 0 else 'sign_reply'
                        params['a_bogus'] = sign_request(query, ua, sm)
                        delay = random.uniform(2.0, 5.0)
                        logger.warning(f"[API] 空响应，切换UA+签名重试 ({_retry + 1}/2)，{delay:.1f}s后...")
                        await asyncio.sleep(delay)
                        return await self._request(url, params, headers, _retry + 1)
                    
                    logger.warning(f"[API] 空响应 url={url}")
                    async with self._state_lock:
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
                    
                    async with self._state_lock:
                        if self._handle_error("JSON 解析失败"):
                            raise CookieExpiredError("API 错误次数达到上限")
                    continue
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 401):
                    msg = self._forbidden_msg(e.response.status_code)
                    logger.error(f"[Cookie] ❌ {msg}")
                    self._handle_error("HTTP 错误", is_fatal=True)
                    raise CookieExpiredError(msg)
                logger.error(f"[API] HTTP {e.response.status_code} url={url}")
                async with self._state_lock:
                    if self._handle_error("HTTP 错误"):
                        raise CookieExpiredError("API 错误次数达到上限")
                continue
            except CookieExpiredError:
                raise
            except httpx.TimeoutException:
                logger.warning(f"[API] 请求超时 url={url}")
                async with self._state_lock:
                    if self._handle_error("超时"):
                        raise CookieExpiredError("API 错误次数达到上限")
                continue
            except Exception as e:
                logger.error(f"[API] 请求异常 url={url}: {e}")
                async with self._state_lock:
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

    async def fetch_videos(self, sec_user_id: str, max_cursor: int = 0, count: int = 18) -> Dict:
        """作品列表（/aweme/post/，secsdk 受保护接口）。

        对齐上游 DouYin_Spider.get_user_work_info（2026-08-16 浏览器实录）：
        count 取浏览器真实值 18（旧值 30）；补 locate_query、time_list_query、
        whale_cut_token（空值但必须发）、cut_version；need_time_list 只在
        首页为 1；referer 用用户主页；a_bogus 不含 verifyFp/fp；最后整条
        query 再过 secsdk webSign，直接发签好的 URL。
        """
        from core.websign import sign_url as _websign_url
        base = "https://www.douyin.com/aweme/v1/web/aweme/post/"
        params = {
            "sec_user_id": sec_user_id,
            "max_cursor": str(max_cursor),
            "locate_query": "false",
            "show_live_replay_strategy": "1",
            # 上游：首页 1、翻页 0；旧代码恒为 1。
            "need_time_list": "1" if str(max_cursor) == "0" else "0",
            "time_list_query": "0",
            "whale_cut_token": "",
            "cut_version": "1",
            "count": str(count),
            "publish_video_strategy_type": "2",
            # 采别人主页时为 1；采登录者本人主页时浏览器发 0。这里的采集对象
            # 都是他人账号，与上游“他人算 1”一致，不再区分本人分支。
            "from_user_page": "1",
        }
        params, headers = await self._prepare_params(
            params,
            {"referer": f"https://www.douyin.com/user/{sec_user_id}"},
            a_bogus_excludes_verify=True,
        )
        query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])
        signed = _websign_url(f"{base}?{query}",
                              uifid=self._cookie_dict.get('UIFID') or self._cookie_dict.get('UIFID_TEMP'))
        data = await self._request_signed(signed, headers)

        if not data and max_cursor == 0:
            msg = "作品接口返回空数据，请检查 Cookie / 签名是否有效"
            logger.error(f"[Cookie] ❌ {msg}")
            raise CookieExpiredError(msg)

        return data

    async def fetch_comments(self, aweme_id: str, cursor: int = 0, count: int = 50) -> Dict:
        """一级评论（/comment/list/，不在 secsdk 保护列表里，目前能跑）。

        对齐上游 webui 的 _request_out_comments：补 whale_cut_token（空值）、
        cut_version、rcFT，以及 referer 用作品页。count 保持 50 不动：
        服务端目前接受 50，且改成 20 只会让同一批评论多发 2.5 倍请求，
        没有收益；webid、support_*、round_trip_time 已由 _prepare_params
        和 COMMON_PARAMS 统一补上。
        """
        url = "https://www.douyin.com/aweme/v1/web/comment/list/"
        params = {
            "aweme_id": aweme_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0",
            "whale_cut_token": "",
            "cut_version": "1",
            "rcFT": "",
        }
        params, headers = await self._prepare_params(
            params,
            {"referer": f"https://www.douyin.com/video/{aweme_id}"},
        )
        return await self._request(url, params, headers)

    async def fetch_replies(self, aweme_id: str, comment_id: str, cursor: int = 0, count: int = 50) -> Dict:
        """回复（/comment/list/reply/，上游注明的严格校验接口）。

        对齐上游 get_work_inner_comment：a_bogus 不含 verifyFp/fp、不带
        uifid，referer 用作品页。count 保持 50（原因同 fetch_comments）。
        注意：这个接口的签名输入改动本身就有风险，改完必须实测（见测试
        记录），不能只凭“浏览器实录”就认为一定过。
        """
        url = "https://www.douyin.com/aweme/v1/web/comment/list/reply/"
        params = {
            "item_id": aweme_id,
            "comment_id": comment_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0",
            "cut_version": "1"
        }
        params, headers = await self._prepare_params(
            params,
            {"referer": f"https://www.douyin.com/video/{aweme_id}"},
            sign_method='sign_reply',
            a_bogus_excludes_verify=True,
            exclude_uifid=True,
        )
        return await self._request(url, params, headers)
