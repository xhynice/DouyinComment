#!/usr/bin/env python3
import os
import csv
import json
import ast
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import shutil


BEIJING_TZ = timezone(timedelta(hours=8))


def _ts_to_str(timestamp, fmt: str) -> str:
    """统一时间戳转换。fmt: '%Y' / '%Y-%m' / '%Y-%m-%d %H:%M' 等。"""
    if timestamp is None:
        return 'unknown'
    try:
        ts = int(timestamp)
        if ts <= 0:
            return 'unknown'
        return datetime.fromtimestamp(ts, BEIJING_TZ).strftime(fmt)
    except (ValueError, OSError, TypeError):
        return 'unknown'


class SiteBuilder:
    def __init__(self, data_dir: str = 'data', output_dir: str = 'docs', upload_dir: str = 'upload'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.upload_dir = upload_dir
        self.data_output_dir = os.path.join(output_dir, 'data', 'comment')
        self.current_sec_uid = None
        self.current_user_dir = None
        self.current_comments_dir = None
        self._config_cache = None
        self._config_path = None
        
    def _load_config(self):
        """懒加载配置并缓存。"""
        if self._config_cache is not None:
            return self._config_cache
        
        if self._config_path is None:
            self._config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                'config.yaml'
            )
        
        if not os.path.exists(self._config_path):
            self._config_cache = {}
            return self._config_cache
        
        try:
            import yaml
            with open(self._config_path, 'r', encoding='utf-8') as f:
                self._config_cache = yaml.safe_load(f) or {}
        except Exception:
            self._config_cache = {}
        
        return self._config_cache
    
    def _needs_rebuild(self, user_path: str, output_path: str) -> bool:
        """检查是否需要重新构建（增量构建）。"""
        if not os.path.exists(output_path):
            return True
        
        output_mtime = os.path.getmtime(output_path)
        
        videos_csv = os.path.join(user_path, 'videos.csv')
        if os.path.exists(videos_csv) and os.path.getmtime(videos_csv) > output_mtime:
            return True
        
        for entry in os.listdir(user_path):
            entry_path = os.path.join(user_path, entry)
            if os.path.isdir(entry_path) and '-' in entry:
                for root, dirs, files in os.walk(entry_path):
                    for f in files:
                        if f.endswith('.csv') and os.path.getmtime(os.path.join(root, f)) > output_mtime:
                            return True
        
        return False
        
    def build(self):
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.data_output_dir, exist_ok=True)
        
        existing_users = self._load_existing_users_index()
        all_users_data = []
        
        user_dirs = [
            d for d in os.listdir(self.data_dir)
            if os.path.isdir(os.path.join(self.data_dir, d))
            and os.path.exists(os.path.join(self.data_dir, d, 'videos.csv'))
        ]
        
        total_users = len(user_dirs)
        if total_users == 0:
            print("没有找到需要处理的用户数据")
            return
        
        print(f"\n找到 {total_users} 个用户数据")
        print("=" * 60)
        
        start_time = time.time()
        success_count = 0
        error_count = 0
        
        for idx, user_dir in enumerate(user_dirs, 1):
            user_path = os.path.join(self.data_dir, user_dir)
            
            print(f"\n[{idx}/{total_users}] 处理用户: {user_dir}")
            
            self.current_sec_uid = user_dir
            self.current_user_dir = os.path.join(self.data_output_dir, user_dir)
            self.current_comments_dir = os.path.join(self.current_user_dir, 'comments')
            
            video_list_file = os.path.join(self.current_user_dir, 'video_list.json')
            if not self._needs_rebuild(user_path, video_list_file):
                print(f"  数据未变化，跳过重建")
                if os.path.exists(video_list_file):
                    try:
                        with open(video_list_file, 'r', encoding='utf-8') as f:
                            cached_data = json.load(f)
                        all_users_data.append({
                            'sec_uid': user_dir,
                            'nickname': self._get_user_nickname(user_dir),
                            'total_videos': cached_data.get('total_videos', 0),
                            'total_comments': cached_data.get('total_comments', 0),
                            'author_replies': 0,
                            'participants_count': 0,
                            'latest_video': {'date': '', 'title': ''}
                        })
                        success_count += 1
                    except Exception:
                        pass
                continue
            
            try:
                os.makedirs(self.current_user_dir, exist_ok=True)
                os.makedirs(self.current_comments_dir, exist_ok=True)
                
                user_start = time.time()
                
                videos = self._load_videos(user_path)
                comments_data = self._load_all_comments(user_path)
                
                nickname = self._get_user_nickname(user_dir)
                active_repliers = self._calculate_active_repliers(comments_data)
                author_replies = self._count_author_replies(comments_data, nickname)
                participants_count = self._count_participants(user_dir)
                
                video_list = []
                total_reply_count = 0
                
                for video in videos:
                    aweme_id = video['aweme_id']
                    comments = comments_data.get(aweme_id, [])
                    video['comment_count'] = len(comments)
                    
                    reply_count = sum(len(c.get('replies', [])) for c in comments)
                    video['reply_count'] = reply_count
                    total_reply_count += reply_count
                    
                    year = _ts_to_str(video.get('create_time'), '%Y')
                    video['images'] = self._resolve_media_urls(video.get('images', ''), 'images', year)
                    video['thumb'] = self._resolve_media_urls(video.get('thumb', ''), 'thumbs', year)
                    video['video'] = self._resolve_media_urls(video.get('video', ''), 'videos', year)
                    
                    video_list.append({
                        'aweme_id': aweme_id,
                        'desc': video.get('desc', ''),
                        'create_time': video.get('create_time'),
                        'create_time_str': video.get('create_time_str', ''),
                        'media_type': video['media_type'],
                        'images': video['images'],
                        'thumb': video['thumb'],
                        'video': video['video'],
                        'comment_count': video['comment_count']
                    })
                    
                    self._save_comments_file(aweme_id, video.get('desc', ''), comments)
                
                video_list.sort(key=lambda x: x.get('create_time', 0) or 0, reverse=True)
                
                video_list_data = {
                    'sec_uid': user_dir,
                    'base_url': f'upload/{user_dir}/',
                    'videos': video_list,
                    'total_videos': len(video_list),
                    'total_comments': sum(v['comment_count'] for v in video_list) + total_reply_count
                }
                
                user_video_list_file = os.path.join(self.current_user_dir, 'video_list.json')
                with open(user_video_list_file, 'w', encoding='utf-8') as f:
                    json.dump(video_list_data, f, ensure_ascii=False, indent=2)
                
                self._generate_user_summary(video_list_data, active_repliers)
                self._copy_avatar(user_path)
                
                all_users_data.append({
                    'sec_uid': user_dir,
                    'nickname': nickname,
                    'total_videos': video_list_data['total_videos'],
                    'total_comments': video_list_data['total_comments'],
                    'author_replies': author_replies,
                    'participants_count': participants_count,
                    'latest_video': self._get_latest_video_info(video_list)
                })
                
                user_time = time.time() - user_start
                print(f"  ✓ 完成 ({user_time:.2f}s) - {len(video_list)} 视频, {video_list_data['total_comments']} 评论")
                success_count += 1
                
            except Exception as e:
                print(f"  ✗ 错误: {e}")
                error_count += 1
        
        users_index_file = os.path.join(self.data_output_dir, 'index.json')
        with open(users_index_file, 'w', encoding='utf-8') as f:
            json.dump({'users': all_users_data}, f, ensure_ascii=False, indent=2)
        
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"构建完成!")
        print(f"  成功: {success_count} 个用户")
        if error_count > 0:
            print(f"  失败: {error_count} 个用户")
        print(f"  耗时: {total_time:.2f}秒")
        print(f"  输出: {users_index_file}")
    
    # ==================== 数据加载 ====================
    
    def _load_videos(self, user_path: str) -> List[Dict]:
        """加载视频列表，处理媒体URL和时间转换。"""
        videos = []
        csv_path = os.path.join(user_path, 'videos.csv')
        
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                video = dict(row)
                
                has_video = bool(video.get('video', '').strip())
                has_images = bool(video.get('images', '').strip())
                video['media_type'] = 'image' if has_images else 'video'
                
                if video.get('create_time'):
                    try:
                        ts = int(video['create_time'])
                        dt = datetime.fromtimestamp(ts)
                        video['create_time_str'] = dt.strftime('%Y-%m-%d %H:%M')
                        year = dt.strftime('%Y')
                    except (ValueError, TypeError):
                        video['create_time_str'] = ''
                        year = 'unknown'
                    
                    for field, subdir in [('images', 'images'), ('video', 'videos'), ('thumb', 'thumbs')]:
                        raw = video.get(field, '').strip()
                        if raw:
                            urls = self._parse_json_list(raw)
                            
                            def _resolve_url(u, s=subdir, y=year):
                                if isinstance(u, str):
                                    return f"{s}/{y}/{u}" if not u.startswith('http') else u
                                return u
                            
                            if urls and isinstance(urls[0], list):
                                video[field] = str([[_resolve_url(u) for u in group] for group in urls])
                            else:
                                video[field] = str([_resolve_url(u) for u in urls]) if urls else raw
                videos.append(video)
        return videos
    
    def _load_all_comments(self, user_path: str) -> Dict[str, List[Dict]]:
        comments_data = {}
        
        for entry in os.listdir(user_path):
            year_month_path = os.path.join(user_path, entry)
            if not os.path.isdir(year_month_path) or '-' not in entry:
                continue
            
            for aweme_id in os.listdir(year_month_path):
                aweme_path = os.path.join(year_month_path, aweme_id)
                if not os.path.isdir(aweme_path):
                    continue
                
                comments_csv = os.path.join(aweme_path, 'comments.csv')
                replies_csv = os.path.join(aweme_path, 'replies.csv')
                
                if os.path.exists(comments_csv):
                    comments = self._load_comment_items(comments_csv)
                    
                    if os.path.exists(replies_csv):
                        replies = self._load_comment_items(replies_csv)
                        replies_by_cid = {}
                        for r in replies:
                            replies_by_cid.setdefault(r.get('reply_id'), []).append(r)
                        
                        for comment in comments:
                            cid = comment['cid']
                            comment['replies'] = replies_by_cid.get(cid, [])
                            comment['reply_count'] = len(comment['replies'])
                    
                    comments_data[aweme_id] = comments
        
        return comments_data
    
    def _load_comment_items(self, csv_path: str) -> List[Dict]:
        """统一加载评论/回复 CSV，处理 sticker、image_list、create_time、user_avatar。"""
        items = []
        year = self._extract_year_from_path(csv_path)
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                item = dict(row)
                
                if item.get('sticker', '').strip():
                    sticker_url = item['sticker'].strip()
                    item['sticker'] = sticker_url if sticker_url.startswith('http') else f"stickers/{year}/{sticker_url}"
                
                if item.get('image_list', '').strip():
                    item['image_list'] = self._parse_json_list(item['image_list'])
                
                if item.get('create_time'):
                    try:
                        ts = int(item['create_time'])
                        item['create_time_str'] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                        if item.get('user_avatar') and not item['user_avatar'].startswith('http'):
                            item['user_avatar'] = f"avatars/{year}/{item['user_avatar']}"
                    except (ValueError, TypeError):
                        item['create_time_str'] = ''
                items.append(item)
        return items
    
    # ==================== 文件操作 ====================
    
    def _save_comments_file(self, aweme_id: str, video_title: str, comments: List[Dict]):
        output_file = os.path.join(self.current_comments_dir, f'{aweme_id}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({'aweme_id': aweme_id, 'video_title': video_title, 'comments': comments},
                      f, ensure_ascii=False, indent=2)
    
    def _copy_avatar(self, user_path: str):
        src_avatar = os.path.join(user_path, 'avatar.jpg')
        if os.path.exists(src_avatar):
            dst_avatar = os.path.join(self.current_user_dir, 'avatar.jpg')
            shutil.copy2(src_avatar, dst_avatar)
            print(f"用户头像已复制到: {dst_avatar}")
    
    # ==================== 配置/索引 ====================
    
    def _load_existing_users_index(self) -> Dict:
        users_index_file = os.path.join(self.data_output_dir, 'index.json')
        if os.path.exists(users_index_file):
            try:
                with open(users_index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {u['sec_uid']: u for u in data.get('users', [])}
            except Exception as e:
                print(f"读取现有用户索引失败: {e}")
        return {}
    
    def _get_user_nickname(self, sec_uid: str) -> str:
        """从config.yaml读取用户nickname（使用缓存）。"""
        config = self._load_config()
        for user in config.get('users', []):
            if user.get('sec_uid') == sec_uid:
                return user.get('nickname') or ''
        return ''
    
    # ==================== 解析工具 ====================
    
    def _parse_json_list(self, json_str: str) -> list:
        """通用 JSON 列表解析器，兼容嵌套结构 [[urls]] 或扁平 [urls]。"""
        if not json_str:
            return []
        try:
            if json_str.startswith('['):
                parsed = ast.literal_eval(json_str)
                if isinstance(parsed, list):
                    return parsed
        except (ValueError, SyntaxError):
            pass
        return []
    
    def _resolve_media_urls(self, json_str: str, subdir: str, year: str) -> list:
        """解析媒体 URL 列表，本地文件名自动拼接子目录路径。"""
        urls = self._parse_json_list(json_str)
        if not urls:
            return urls
        
        def _resolve_one(url):
            if isinstance(url, str) and not url.startswith('http'):
                return f"{subdir}/{year}/{url}"
            return url
        
        if urls and isinstance(urls[0], list):
            return [[_resolve_one(u) for u in group] for group in urls]
        return [_resolve_one(u) for u in urls]
    
    def _extract_year_from_path(self, path: str) -> str:
        for part in path.replace('\\', '/').split('/'):
            if '-' in part and len(part) == 7 and part[:4].isdigit():
                return part[:4]
        return 'unknown'
    
    # ==================== 统计 ====================
    
    def _calculate_active_repliers(self, comments_data: Dict[str, List[Dict]]) -> List[Dict]:
        replier_count = {}
        for comments in comments_data.values():
            for comment in comments:
                for reply in comment.get('replies', []):
                    nickname = reply.get('user_nickname', '匿名')
                    if nickname not in replier_count:
                        replier_count[nickname] = {
                            'nickname': nickname,
                            'avatar': reply.get('user_avatar', ''),
                            'count': 0
                        }
                    replier_count[nickname]['count'] += 1
        
        return sorted(replier_count.values(), key=lambda x: x['count'], reverse=True)[:15]
    
    def _count_author_replies(self, comments_data: Dict[str, List[Dict]], author_nickname: str) -> int:
        count = 0
        for comments in comments_data.values():
            for comment in comments:
                if comment.get('user_nickname') == author_nickname:
                    count += 1
                count += sum(1 for r in comment.get('replies', []) if r.get('user_nickname') == author_nickname)
        return count
    
    def _count_participants(self, user_dir: str) -> int:
        avatars_dir = os.path.join(self.upload_dir, user_dir, 'avatars')
        if not os.path.exists(avatars_dir):
            return 0
        return sum(
            1 for year_dir in os.listdir(avatars_dir)
            if os.path.isdir(os.path.join(avatars_dir, year_dir))
            for f in os.listdir(os.path.join(avatars_dir, year_dir))
            if os.path.isfile(os.path.join(avatars_dir, year_dir, f))
        )
    
    def _get_latest_video_info(self, video_list: List[Dict]) -> Dict:
        if not video_list:
            return {'date': '', 'title': ''}
        latest = video_list[0]
        date_str = ''
        if latest.get('create_time'):
            try:
                date_str = datetime.fromtimestamp(int(latest['create_time']), BEIJING_TZ).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass
        return {
            'date': date_str,
            'title': latest.get('desc', '')[:100] if latest.get('desc') else '[作者偷懒 没有写标题]'
        }
    
    def _generate_user_summary(self, video_list_data: Dict, active_repliers: List[Dict]):
        output_file = os.path.join(self.current_user_dir, 'summary.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total_videos': video_list_data['total_videos'],
                'total_comments': video_list_data['total_comments'],
                'active_repliers': active_repliers,
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, f, ensure_ascii=False, indent=2)


def main():
    SiteBuilder().build()


if __name__ == '__main__':
    main()
