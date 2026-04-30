import asyncio
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

from services.base_service import BaseService
from core.logger import logger
from core.api import CookieExpiredError
from utils.helpers import safe_str, safe_int, sleep_jitter


class ReplyService(BaseService):
    data_type = "reply"
    id_field = "cid"
    table_name = "replies"
    csv_filename = "replies.csv"
    media_fields = {"image_list": "images"}
    has_avatar_sticker = True
    
    async def fetch(self, aweme_id: str, comment_id: str, page_size: int = 50,
                    delay: float = None, **kwargs) -> List[Dict]:
        all_replies = []
        cursor = 0
        has_more = 1
        
        while has_more:
            try:
                response = await self.api.fetch_replies(aweme_id, comment_id, cursor, page_size)
                replies = response.get("comments", [])
                
                if isinstance(replies, list):
                    all_replies.extend(replies)
                
                has_more = response.get("has_more", 0)
                cursor = response.get("cursor", 0)
                
                if has_more:
                    await sleep_jitter(delay)
                    
            except CookieExpiredError:
                raise
            except Exception as e:
                logger.error(f"[采集] 获取回复失败 comment_id={comment_id}: {e}")
                break
        
        return all_replies
    
    def process(self, raw_replies: List[Dict], aweme_id: str = None,
                comment_id: str = None, **kwargs) -> List[Dict]:
        processed = []
        
        for r in raw_replies:
            cid = r.get('cid')
            if not cid:
                continue
            
            user_info = self._extract_user_info(r)
            
            processed.append({
                "aweme_id": aweme_id,
                "cid": str(cid),
                "reply_id": safe_str(r.get('reply_id')),
                "reply_to_reply_id": safe_str(r.get('reply_to_reply_id', '0')),
                "text": safe_str(r.get('text')),
                "image_list": self._extract_image_urls(r),
                "digg_count": safe_int(r.get('digg_count')),
                "create_time": safe_int(r.get('create_time')),
                "user_nickname": user_info['user_nickname'],
                "user_unique_id": user_info['user_unique_id'],
                "user_avatar": user_info['user_avatar'],
                "sticker": self._extract_sticker_url(r),
                "reply_to_username": safe_str(r.get('reply_to_username')),
                "ip_label": safe_str(r.get('ip_label'))
            })
        
        return processed
    
    async def _fetch_comment_reply(self, aweme_id: str, comment: Dict, 
                                   page_size: int, delay: float) -> List[Dict]:
        comment_id = comment.get('cid')
        if not comment_id:
            return []
        
        raw_replies = await self.fetch(aweme_id, comment_id, page_size, delay)
        processed = self.process(raw_replies, aweme_id=aweme_id, comment_id=comment_id)
        
        return processed
    
    async def run(self, delay: float = None,
                  limit: int = 0, skip_existing: bool = False, **kwargs) -> Dict:
        page_size = 18
        comments = self.storage.get_comment_ids(video_limit=limit)
        
        if skip_existing:
            existing = self.storage.get_comments_with_replies()
            comments = [c for c in comments if c['cid'] not in existing]
            if comments:
                logger.info(f"[采集] 跳过已采集的评论，剩余 {len(comments)} 条")
        
        if not comments:
            logger.warning(f"[采集] 无可采集数据 用户={self.sec_uid}")
            return {'total': 0, 'new': 0, 'comments': 0}
        
        from collections import defaultdict
        video_comments = defaultdict(list)
        for c in comments:
            video_comments[c['aweme_id']].append(c)
        
        logger.info(f"[采集] 开始 用户={self.sec_uid} 类型=回复 评论={len(comments)} 视频={len(video_comments)}")
        
        # 在进度条开始前完成 Cookie 验证，避免打断进度条
        await self.api.verify_cookie()
        
        start_time = datetime.now()
        stats = {'total': 0, 'new': 0, 'comments': len(comments), 'videos': len(video_comments)}
        
        video_timestamps = self.storage.get_video_timestamps()
        
        for aweme_id, video_comment_list in tqdm(video_comments.items(), 
                                                 desc="采集回复", 
                                                 unit="视频",
                                                 bar_format='{percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'):
            video_all_replies = []
            
            for comment in video_comment_list:
                try:
                    all_replies = await self._fetch_comment_reply(aweme_id, comment, page_size, delay)
                except CookieExpiredError:
                    raise
                except Exception as e:
                    logger.error(f"[采集] 异常：{e}")
                    continue
                
                if all_replies:
                    video_all_replies.extend(all_replies)
                
                await sleep_jitter(delay)
            
            video_reply_count = len(video_all_replies)
            video_new_count = 0
            
            if video_all_replies:
                stats['total'] += video_reply_count
                video_ts = video_timestamps.get(aweme_id)
                
                with tqdm.external_write_mode():
                    save_result = self.storage.save(video_all_replies, aweme_id, video_timestamp=video_ts)
                    video_new_count = save_result['csv']
                    stats['new'] += video_new_count
            
            with tqdm.external_write_mode():
                if video_reply_count > 0:
                    logger.info(f"[采集] 视频 {aweme_id} 采集回复 {video_reply_count} 条，新增 {video_new_count} 条")
                else:
                    logger.info(f"[采集] 视频 {aweme_id} 无回复")
            
            await sleep_jitter(delay)
        
        stats['duration'] = f"{(datetime.now() - start_time).total_seconds():.1f}秒"

        return stats
    
    async def run_download_only(self, aweme_id: str = None,
                                 video_timestamp: int = None, quiet: bool = False) -> Dict:
        if aweme_id:
            return await super().run_download_only(aweme_id, video_timestamp, quiet=quiet)
        return await self.run_download_only_multi()
