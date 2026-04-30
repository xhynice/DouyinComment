import asyncio
import os
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm
import httpx

from services.base_service import BaseService
from core.logger import logger
from core.api import CookieExpiredError
from utils.helpers import safe_str, safe_int, sleep_jitter


class VideoService(BaseService):
    data_type = "video"
    id_field = "aweme_id"
    table_name = "videos"
    csv_filename = "videos.csv"
    media_fields = {"images": "images", "video": "videos", "thumb": "thumbs"}
    has_avatar_sticker = False
    
    def __init__(self, sec_uid: str, cookie: str = ""):
        super().__init__(sec_uid, cookie)
    
    async def fetch(self, page_size: int = 30, delay: float = None,
                    limit: int = 0, **kwargs) -> List[Dict]:
        all_videos = []
        max_cursor = 0
        has_more = True
        retries = 0
        max_retries = 3
        
        pbar = tqdm(desc="采集作品", 
                    unit="条",
                    bar_format='{percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        
        try:
            while has_more and retries < max_retries:
                if limit > 0 and len(all_videos) >= limit:
                    break
                
                try:
                    response = await self.api.fetch_videos(self.sec_uid, max_cursor, page_size)
                    aweme_list = response.get("aweme_list", [])
                    has_more = response.get("has_more", False)
                    
                    logger.debug(f"[采集] API 响应：aweme_list={len(aweme_list)}, has_more={has_more}, max_cursor={response.get('max_cursor')}")
                    
                    if aweme_list:
                        if limit > 0:
                            remaining = limit - len(all_videos)
                            aweme_list = aweme_list[:remaining]
                        
                        all_videos.extend(aweme_list)
                        pbar.update(len(aweme_list))
                        max_cursor = response.get("max_cursor", 0)
                        has_more = response.get("has_more", False)
                        retries = 0
                    else:
                        retries += 1
                        await sleep_jitter(delay * retries)
                    
                    if has_more:
                        await sleep_jitter(delay)
                        
                except CookieExpiredError:
                    raise
                except Exception as e:
                    logger.error(f"[采集] 获取作品失败 用户={self.sec_uid}: {e}")
                    retries += 1
                    await sleep_jitter(delay * retries)
        finally:
            pbar.close()
        
        all_videos.reverse()
        return all_videos
    
    def process(self, raw_videos: List[Dict], **kwargs) -> List[Dict]:
        processed = []
        
        for video in raw_videos:
            aweme_id = video.get("aweme_id", "")
            if not aweme_id:
                continue
            
            images = []
            raw_images = video.get("images") or []
            for img in raw_images:
                url_list = img.get("url_list", [])
                if url_list:
                    images.append(url_list)
            
            video_urls = []
            video_obj = video.get("video") or {}
            play_addr = video_obj.get("play_addr") or {}
            if play_addr.get("url_list"):
                video_urls = play_addr["url_list"]
            
            thumb_urls = []
            origin_cover = video_obj.get("origin_cover") or {}
            if origin_cover.get("url_list"):
                thumb_urls = origin_cover["url_list"]
            
            processed.append({
                "aweme_id": aweme_id,
                "desc": safe_str(video.get("desc")),
                "create_time": safe_int(video.get("create_time")),
                "images": str(images) if images else None,
                "video": str(video_urls) if video_urls else None,
                "thumb": str(thumb_urls) if thumb_urls else None,
                "sec_uid": self.sec_uid,
            })
        
        return processed
    
    async def run(self, delay: float = 1.0, limit: int = 0, **kwargs) -> Dict:
        page_size = 18
        logger.info(f"[采集] 开始 用户={self.sec_uid} 类型=作品")
        
        # 在采集前完成 Cookie 验证
        await self.api.verify_cookie()
        
        start_time = datetime.now()
        raw_videos = await self.fetch(page_size, delay, limit)
        
        # 头像或昵称缺失时，后台更新
        avatar_task = None
        if raw_videos:
            need_update = False
            avatar_path = os.path.join('data', self.sec_uid, 'avatar.jpg')
            if not os.path.exists(avatar_path) or os.path.getsize(avatar_path) == 0:
                need_update = True
            if not self._get_current_nickname():
                need_update = True
            if need_update:
                avatar_task = asyncio.create_task(
                    self._download_avatar_and_update_nickname(raw_videos)
                )

        processed = self.process(raw_videos)

        with tqdm.external_write_mode():
            save_result = self.storage.save(processed)

        # 主流程完成，等待头像任务结束（不阻塞采集，只等这一个任务）
        if avatar_task is not None:
            try:
                await avatar_task
            except Exception as e:
                logger.warning(f"[采集] 头像/昵称后台任务失败：{e}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            'total': len(raw_videos),
            'new': save_result['csv'],
            'duration': f"{duration:.1f}秒"
        }

        return result
    
    def _get_current_nickname(self) -> str:
        """获取 config.yaml 中当前用户的 nickname，不存在或为空返回空字符串。"""
        for user in self.user_manager.get_active_users():
            if user.get('sec_uid') == self.sec_uid:
                return (user.get('nickname') or '').strip()
        return ''

    async def _download_avatar_and_update_nickname(self, raw_videos: List[Dict]):
        """后台任务：下载头像并更新昵称"""
        try:
            author = raw_videos[0].get('author', {})
            nickname = author.get('nickname', '')
            avatar_url = author.get('avatar_thumb', {}).get('url_list', [None])[0]
            avatar_path = os.path.join('data', self.sec_uid, 'avatar.jpg')
            
            # 1. 下载头像（不存在或为空时）
            if avatar_url and (not os.path.exists(avatar_path) or os.path.getsize(avatar_path) == 0):
                try:
                    async with httpx.AsyncClient(timeout=10) as client:
                        resp = await client.get(avatar_url)
                        if resp.status_code == 200:
                            os.makedirs(os.path.dirname(avatar_path), exist_ok=True)
                            with open(avatar_path, 'wb') as f:
                                f.write(resp.content)
                            logger.info(f"[采集] 头像已保存 {avatar_path}")
                        else:
                            logger.warning(f"[采集] 头像下载失败 HTTP {resp.status_code}")
                except httpx.TimeoutException:
                    logger.warning(f"[采集] 头像下载超时")
                except Exception as e:
                    logger.warning(f"[采集] 头像下载失败：{e}")
            
            # 2. 更新昵称（config 中为空时更新）
            if nickname:
                await asyncio.sleep(1)
                self.user_manager.update_nickname(self.sec_uid, nickname)
                
        except Exception as e:
            logger.warning(f"[采集] 头像/昵称后台任务失败：{e}")
