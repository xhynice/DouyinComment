import os
import ast
import hashlib
import asyncio
import httpx
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Union
from urllib.parse import urlparse
from tqdm import tqdm
from core.logger import logger

try:
    from PIL import Image
    from pillow_heif import register_heif_opener
    register_heif_opener()
    HEIF_SUPPORT = True
except ImportError:
    HEIF_SUPPORT = False

BEIJING_TZ = timezone(timedelta(hours=8))


def timestamp_to_year(timestamp: Union[int, float, str, None]) -> str:
    if timestamp is None:
        return 'unknown'
    try:
        ts = int(timestamp)
        if ts <= 0:
            return 'unknown'
        dt = datetime.fromtimestamp(ts, BEIJING_TZ)
        return dt.strftime('%Y')
    except (ValueError, OSError, TypeError):
        return 'unknown'


def timestamp_to_year_month(timestamp: Union[int, float, str, None]) -> str:
    if timestamp is None:
        return 'unknown'
    try:
        ts = int(timestamp)
        if ts <= 0:
            return 'unknown'
        dt = datetime.fromtimestamp(ts, BEIJING_TZ)
        return dt.strftime('%Y-%m')
    except (ValueError, OSError, TypeError):
        return 'unknown'


class MediaDownloader:
    ALLOWED_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.heic', '.heif', '.avif',
        '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv',
        '.mp3', '.wav', '.ogg', '.aac', '.flac'
    }
    
    _instances = {}

    def __new__(cls, upload_dir: str = None, sec_uid: str = None, max_workers: int = 3):
        upload_dir = upload_dir or 'upload'
        if sec_uid:
            upload_dir = os.path.join(upload_dir, sec_uid)
        instance_key = upload_dir
        
        if instance_key not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[instance_key] = instance
        
        return cls._instances[instance_key]

    def __init__(self, upload_dir: str = None, sec_uid: str = None, max_workers: int = 3):
        upload_dir = upload_dir or 'upload'
        if sec_uid:
            upload_dir = os.path.join(upload_dir, sec_uid)
        instance_key = upload_dir
        
        if hasattr(self, '_initialized') and self._initialized == instance_key:
            return
        
        self.upload_dir = upload_dir
        self.sec_uid = sec_uid
        self.max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)
        self._client: Optional[httpx.AsyncClient] = None
        self._url_cache = {}  # (url, year_month) → filename，URL 去重缓存
        
        os.makedirs(self.upload_dir, exist_ok=True)
        self._initialized = instance_key

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': '*/*',
                    'Connection': 'keep-alive'
                },
                follow_redirects=True,
                timeout=httpx.Timeout(60.0, connect=30.0)
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
        # 不推荐在 __del__ 中做异步清理，应该由调用者显式调用 close() 或使用上下文管理器
        if self._client and not self._client.is_closed:
            logger.warning(f"[下载] 检测到未关闭的下载器，请显式调用 close() 或使用 async with 上下文管理器")

    @classmethod
    async def close_all(cls):
        for instance in list(cls._instances.values()):
            try:
                await instance.close()
            except Exception:
                pass
        cls._instances.clear()

    def _detect_extension(self, file_path: str) -> str:
        try:
            with open(file_path, 'rb') as f:
                header = f.read(20)
            
            if header.startswith(b'\xff\xd8'):
                return '.jpg'
            elif header.startswith(b'\x89PNG'):
                return '.png'
            elif header.startswith(b'GIF8'):
                return '.gif'
            elif header.startswith(b'RIFF') and header[8:12] == b'WEBP':
                return '.webp'
            elif header.startswith(b'\x00\x00\x00') and b'ftyp' in header[:12]:
                ftyp_pos = header.find(b'ftyp')
                brand = header[ftyp_pos + 4:ftyp_pos + 8]
                if brand in (b'heic', b'mif1', b'heix'):
                    return '.heic'
                elif brand in (b'avif', b'avis'):
                    return '.avif'
                else:
                    return '.mp4'
            elif header.startswith(b'ID3') or header.startswith(b'\x49\x44\x33'):
                return '.mp3'
            return '.jpg'
        except (IOError, OSError):
            return '.jpg'

    def _convert_heic_to_jpeg(self, file_path: str) -> str:
        if not HEIF_SUPPORT:
            logger.warning("[下载] pillow-heif 未安装 HEIC 转换不可用")
            return file_path
        
        try:
            img = Image.open(file_path)
            jpeg_path = file_path.rsplit('.', 1)[0] + '.jpg'
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            img.save(jpeg_path, 'JPEG', quality=95)
            os.remove(file_path)
            logger.debug(f"[下载] HEIC 转换成功: {file_path} → {jpeg_path}")
            return jpeg_path
        except Exception as e:
            logger.warning(f"[下载] HEIC 转换失败: {e}")
            return file_path

    def _get_extension_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        for ext in self.ALLOWED_EXTENSIONS:
            if path.endswith(ext):
                return ext
        return ''

    def _generate_filename(self, content: bytes, ext: str) -> str:
        md5 = hashlib.md5(content).hexdigest()
        return f"{md5}{ext}"

    async def download(self, url: str, subdir: str = 'images', timeout: float = 30.0, 
                       max_retries: int = 2, year_month: str = None) -> Optional[str]:
        if not url:
            return None

        # URL 去重：同一 URL + 同一年月直接返回缓存
        cache_key = (url, year_month)
        if cache_key in self._url_cache:
            return self._url_cache[cache_key]

        save_dir = os.path.join(self.upload_dir, subdir, year_month) if year_month else os.path.join(self.upload_dir, subdir)
        os.makedirs(save_dir, exist_ok=True)
        
        async with self._semaphore:
            for attempt in range(max_retries + 1):
                temp_path = None
                try:
                    client = await self._get_client()
                    temp_path = os.path.join(save_dir, f"temp_{uuid.uuid4().hex}.tmp")
                    
                    async with client.stream("GET", url, timeout=timeout) as response:
                        if response.status_code != 200:
                            continue
                        
                        with open(temp_path, 'wb') as f:
                            async for chunk in response.aiter_bytes(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                    
                    with open(temp_path, 'rb') as f:
                        content = f.read()
                    
                    ext = self._get_extension_from_url(url) or self._detect_extension(temp_path)
                    
                    if ext == '.heic':
                        converted_path = self._convert_heic_to_jpeg(temp_path)
                        if converted_path != temp_path:
                            ext = '.jpg'
                            with open(converted_path, 'rb') as f:
                                content = f.read()
                            temp_path = converted_path
                    
                    filename = self._generate_filename(content, ext)
                    final_path = os.path.join(save_dir, filename)
                    
                    if os.path.exists(final_path):
                        os.remove(temp_path)
                    elif temp_path != final_path:
                        os.replace(temp_path, final_path)

                    self._url_cache[cache_key] = filename
                    return filename
                    
                except (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError) as e:
                    # 网络错误可重试
                    if attempt == max_retries:
                        logger.error(f"[下载] 网络错误 url={url}: {e}")
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
                    await asyncio.sleep(0.5 * (attempt + 1))
                except Exception as e:
                    # 其他错误不可重试，直接清理并退出
                    logger.error(f"[下载] 下载失败 url={url}: {e}")
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
                    break
        
        return None

    async def download_first_valid(self, urls: List[str], subdir: str = 'images',
                                   year_month: str = None) -> Optional[str]:
        for url in urls:
            if url:
                result = await self.download(url, subdir, year_month=year_month)
                if result:
                    return result
        if urls:
            logger.warning(f"[下载] 备用URL全部失败 url={urls[0]}")
        return None
    
    def _is_url(self, value: str) -> bool:
        return bool(value and (value.startswith('http://') or value.startswith('https://')))
    
    def _is_url_list(self, value: str) -> bool:
        if not value:
            return False
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list) and len(parsed) > 0:
                first = parsed[0]
                if isinstance(first, str):
                    return self._is_url(first)
                elif isinstance(first, list) and len(first) > 0:
                    return self._is_url(first[0])
        except (ValueError, SyntaxError):
            pass
        return False
    
    def _parse_url_list(self, value: str) -> Optional[List]:
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return None
    
    async def download_field(self, value: str, folder: str, year_month: str = None) -> Optional[str]:
        if not value:
            return None
        
        if self._is_url(value):
            result = await self.download(value, folder, year_month=year_month)
            return result if result else None
        
        if self._is_url_list(value):
            urls = self._parse_url_list(value)
            if not urls:
                return None
            
            if isinstance(urls[0], list):
                results = []
                for url_list in urls:
                    if url_list:
                        r = await self.download_first_valid(url_list, folder, year_month)
                        results.append(r if r else url_list)
                return str(results) if results else None
            else:
                result = await self.download_first_valid(urls, folder, year_month)
                return str([result]) if result else None
        
        return None
    
    async def download_items_media(self, items: List[Dict], id_field: str, 
                                   media_fields: Dict[str, str], 
                                   media_config: Dict = None,
                                   update_callback = None,
                                   batch_size: int = 100,
                                   time_field: str = 'create_time',
                                   video_timestamp: Union[int, float, None] = None) -> Dict:
        if media_config is None:
            media_config = {k: True for k in media_fields.keys()}
        
        stats = {field: 0 for field in media_fields.keys()}
        url_updates = {}
        
        year_month = timestamp_to_year(video_timestamp) if video_timestamp else None
        
        for item in tqdm(items, 
                         desc="下载媒体", 
                         unit="个",
                         bar_format='{percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'):
            item_id = item.get(id_field)
            if not item_id:
                continue
            
            item_year_month = year_month or timestamp_to_year(item.get(time_field))
            
            item_updates = {}
            
            for field, folder in media_fields.items():
                if not media_config.get(field, True):
                    continue
                
                value = item.get(field)
                if not value:
                    continue
                
                result = await self.download_field(value, folder, item_year_month)
                if result:
                    stats[field] += 1
                    item_updates[field] = result
            
            if item_updates:
                url_updates[item_id] = item_updates
                
                if update_callback and len(url_updates) >= batch_size:
                    with tqdm.external_write_mode():
                        update_callback(url_updates)
                    url_updates = {}
        
        return {'stats': stats, 'updates': url_updates}
    
    async def download_avatars_stickers(self, items: List[Dict], id_field: str,
                                        media_config: Dict = None,
                                        update_callback = None,
                                        batch_size: int = 100,
                                        time_field: str = 'create_time',
                                        video_timestamp: Union[int, float, None] = None) -> Dict:
        if media_config is None:
            media_config = {'avatars': True, 'stickers': True}
        
        stats = {'avatars': 0, 'stickers': 0}
        url_updates = {}
        
        year_month = timestamp_to_year(video_timestamp) if video_timestamp else None
        
        tasks = []
        
        for item in items:
            item_id = item.get(id_field)
            if not item_id:
                continue
            
            item_year_month = year_month or timestamp_to_year(item.get(time_field))
            
            if media_config.get('avatars', True):
                avatar = item.get('user_avatar')
                if avatar and self._is_url(avatar):
                    tasks.append(('avatars', item_id, avatar, item_year_month))
            
            if media_config.get('stickers', True):
                sticker = item.get('sticker')
                if sticker and self._is_url(sticker):
                    tasks.append(('stickers', item_id, sticker, item_year_month))
        
        if not tasks:
            return {'stats': stats, 'updates': url_updates}
        
        results = {}
        
        async def download_one(task_type: str, item_id: str, url: str, year_month: str):
            result = await self.download(url, task_type, year_month=year_month)
            return (task_type, item_id, result)
        
        download_tasks = [download_one(t[0], t[1], t[2], t[3]) for t in tasks]
        
        for coro in tqdm(asyncio.as_completed(download_tasks), 
                         total=len(download_tasks), 
                         desc="下载头像/表情", 
                         unit="个",
                         bar_format='{percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'):
            try:
                task_type, item_id, result = await coro
                if result:
                    stats[task_type] += 1
                    field = 'user_avatar' if task_type == 'avatars' else 'sticker'
                    if item_id not in results:
                        results[item_id] = {}
                    results[item_id][field] = result
                    
                    if update_callback and len(results) >= batch_size:
                        with tqdm.external_write_mode():
                            update_callback(results)
                        results.clear()
            except Exception as e:
                logger.error(f"[下载] 头像/表情下载失败：{e}")
                # 继续处理其他任务
        
        url_updates = results
        return {'stats': stats, 'updates': url_updates}
    
    def has_url(self, item: Dict, fields: List[str]) -> bool:
        for field in fields:
            value = item.get(field)
            if value and ('http://' in value or 'https://' in value):
                return True
        return False
