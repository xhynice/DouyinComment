import asyncio
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

from services.base_service import BaseService
from core.logger import logger
from core.api import CookieExpiredError
from utils.helpers import safe_str, safe_int, sleep_jitter


class CommentService(BaseService):
    data_type = "comment"
    id_field = "cid"
    table_name = "comments"
    csv_filename = "comments.csv"
    media_fields = {"image_list": "images"}
    has_avatar_sticker = True
    
    async def fetch(self, aweme_id: str, page_size: int = 50,
                    delay: float = None, **kwargs) -> List[Dict]:
        all_comments = []
        cursor = 0
        has_more = 1
        
        while has_more:
            try:
                response = await self.api.fetch_comments(aweme_id, cursor, page_size)
                comments = response.get("comments", [])
                
                if isinstance(comments, list):
                    all_comments.extend(comments)
                
                has_more = response.get("has_more", 0)
                cursor = response.get("cursor", 0)
                
                if has_more:
                    await sleep_jitter(delay)
                    
            except CookieExpiredError:
                raise
            except Exception as e:
                logger.error(f"[采集] 获取评论失败 aweme_id={aweme_id}: {e}")
                break
        
        return all_comments
    
    def process(self, raw_comments: List[Dict], aweme_id: str = None, **kwargs) -> List[Dict]:
        processed = []
        
        for c in raw_comments:
            cid = c.get('cid')
            if not cid:
                continue
            
            user_info = self._extract_user_info(c)
            
            processed.append({
                "aweme_id": aweme_id,
                "cid": str(cid),
                "text": safe_str(c.get('text')),
                "image_list": self._extract_image_urls(c),
                "digg_count": safe_int(c.get('digg_count')),
                "create_time": safe_int(c.get('create_time')),
                "user_nickname": user_info['user_nickname'],
                "user_unique_id": user_info['user_unique_id'],
                "user_avatar": user_info['user_avatar'],
                "sticker": self._extract_sticker_url(c),
                "reply_comment_total": safe_int(c.get('reply_comment_total')),
                "ip_label": safe_str(c.get('ip_label'))
            })
        
        return processed
    
    async def run(self, aweme_ids: List[str] = None,
                  delay: float = None, limit: int = 0,
                  skip_existing: bool = False, **kwargs) -> Dict:
        page_size = 18
        if aweme_ids is None:
            aweme_ids = self.storage.get_video_ids(limit)
        
        if skip_existing:
            existing = self.storage.get_videos_with_comments()
            aweme_ids = [vid for vid in aweme_ids if vid not in existing]
            if aweme_ids:
                logger.info(f"[采集] 跳过已采集的视频，剩余 {len(aweme_ids)} 个")
        
        if not aweme_ids:
            logger.warning(f"[采集] 无可采集数据 用户={self.sec_uid}")
            return {'total': 0, 'new': 0, 'videos': 0}
        
        logger.info(f"[采集] 开始 用户={self.sec_uid} 类型=评论 视频={len(aweme_ids)}")
        
        # 在进度条开始前完成 Cookie 验证，避免打断进度条
        await self.api.verify_cookie()
        
        start_time = datetime.now()
        stats = {'total': 0, 'new': 0, 'videos': len(aweme_ids)}
        
        video_timestamps = self.storage.get_video_timestamps()
        
        for aweme_id in tqdm(aweme_ids, 
                             desc="采集评论", 
                             unit="视频",
                             bar_format='{percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'):
            try:
                raw_comments = await self.fetch(aweme_id, page_size, delay)
            except CookieExpiredError:
                raise
            except Exception as e:
                logger.error(f"[采集] 异常：{e}")
                break
            
            processed = self.process(raw_comments, aweme_id=aweme_id)
            
            video_comment_count = len(processed)
            video_new_count = 0
            
            if processed:
                stats['total'] += video_comment_count
                video_ts = video_timestamps.get(aweme_id)
                
                with tqdm.external_write_mode():
                    save_result = self.storage.save(processed, aweme_id, video_timestamp=video_ts)
                    video_new_count = save_result['csv']
                    stats['new'] += video_new_count
            
            with tqdm.external_write_mode():
                if video_comment_count > 0:
                    logger.info(f"[采集] 视频 {aweme_id} 采集评论 {video_comment_count} 条，新增 {video_new_count} 条")
                else:
                    logger.info(f"[采集] 视频 {aweme_id} 无评论")
            
            await sleep_jitter(delay)
        
        stats['duration'] = f"{(datetime.now() - start_time).total_seconds():.1f}秒"

        return stats
    
    async def run_download_only(self, aweme_id: str = None,
                                 video_timestamp: int = None, quiet: bool = False) -> Dict:
        if aweme_id:
            return await super().run_download_only(aweme_id, video_timestamp, quiet=quiet)
        return await self.run_download_only_multi()
