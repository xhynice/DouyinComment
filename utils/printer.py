from typing import Dict, List

from utils.field_config import UserManager


class Config:
    SERVICES = {}
    
    TYPE_NAMES: Dict[str, str] = {
        'video': '作品', 
        'comment': '评论', 
        'reply': '回复'
    }
    
    MEDIA_FIELDS: Dict[str, List[str]] = {
        'video': ['images', 'video', 'thumb'],
        'comment': ['image_list', 'avatars', 'stickers'],
        'reply': ['image_list', 'avatars', 'stickers']
    }
    
    FIELD_NAMES: Dict[str, str] = {
        'images': '图片', 'image_list': '图片', 'video': '视频', 'thumb': '缩略图',
        'videos': '视频', 'thumbs': '缩略图',
        'avatars': '头像', 'stickers': '表情'
    }
    
    @classmethod
    def init_services(cls, video_service, comment_service, reply_service):
        cls.SERVICES = {
            'video': video_service,
            'comment': comment_service,
            'reply': reply_service
        }


def user_header(user: dict, index: int, total: int):
    name = user.get('nickname') or user['sec_uid']
    print(f"\n{'#'*60}")
    print(f"进度: [{index}/{total}] 处理用户 {name}")
    print(f"{'#'*60}")


def task_info(user_manager: UserManager, data_type: str, users: list,
              limit: int = 0, download_only: bool = False):
    type_name = Config.TYPE_NAMES[data_type]
    action = "下载" if download_only else "采集"
    
    print(f"共有 {len(users)} 个用户需要{action}{type_name}")
    
    if download_only:
        media_config = user_manager.get_media_download_for_type(data_type)
        enabled = [Config.FIELD_NAMES.get(k, k) for k, v in media_config.items() if v]
        if enabled:
            print(f"媒体下载: {', '.join(enabled)}")
    
    if limit > 0:
        limit_desc = {
            'video': f"{limit} 条{type_name}",
            'comment': f"前 {limit} 个视频的{type_name}",
            'reply': f"前 {limit} 个视频的{type_name}"
        }
        print(f"限制{action}: {limit_desc[data_type]}")
    
    print()


def result(user: dict, stats: dict, data_type: str, is_download: bool = False):
    type_name = Config.TYPE_NAMES[data_type]
    name = user.get('nickname') or user['sec_uid']
    
    print(f"\n{'='*60}")
    print(f"用户: {name}")
    print(f"{'='*60}")
    
    if is_download:
        print(f"  下载{type_name}媒体:")
        fields = Config.MEDIA_FIELDS[data_type]
        items = [f"{Config.FIELD_NAMES.get(k, k)}: {stats.get(k, 0)}" for k in fields]
        print(f"    {' | '.join(items)}")
        print(f"    更新URL: {stats.get('updated', 0)} 条")
    else:
        print(f"  {type_name}采集: {stats.get('total', 0)} 条 (新增: {stats.get('new', 0)})")
        
        if stats.get('duration'):
            print(f"  耗时: {stats['duration']}")
    
    print(f"{'='*60}\n")


def total(stats: dict, data_type: str, is_download: bool = False):
    type_name = Config.TYPE_NAMES[data_type]
    action = "下载" if is_download else "采集"
    user_count = stats.get('users', 1)
    
    if user_count <= 1:
        print(f"\n{'='*60}")
        print(f"{type_name}{action}完成!")
        print(f"{'='*60}\n")
        return
    
    print(f"\n{'='*60}")
    print(f"{type_name}{action}完成!")
    
    if is_download:
        fields = Config.MEDIA_FIELDS[data_type]
        items = [f"{Config.FIELD_NAMES.get(k, k)}: {stats.get(k, 0)}" for k in fields]
        print(f"  {' | '.join(items)}")
        print(f"  更新 URL: {stats.get('updated', 0)} 条")
    else:
        print(f"  处理用户：{user_count} 个")
        print(f"  新增{type_name}: {stats.get('count', 0)}")
    
    print(f"{'='*60}\n")
