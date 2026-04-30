import os
import csv
from typing import List, Dict, Optional, Set, Union
from core.database import get_database
from core.logger import logger
from utils.field_config import UserManager
from core.downloader import timestamp_to_year_month


class StorageManager:
    
    def __init__(self, sec_uid: str, data_type: str, table_name: str,
                 id_field: str, csv_filename: str):
        self.sec_uid = sec_uid
        self.data_type = data_type
        self.table_name = table_name
        self.id_field = id_field
        self.csv_filename = csv_filename
        self.db = get_database(sec_uid=sec_uid)
        self.user_manager = UserManager()
        self.data_dir = os.path.join('data', sec_uid)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self._csv_cache = {}
        self._db_cache = None
        self._csv_path_cache: Dict[str, str] = {}
    
    def _get_existing_ids_from_csv(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None, force_refresh: bool = False) -> set:
        cache_key = aweme_id or '__root__'
        if force_refresh or cache_key not in self._csv_cache:
            filepath = self._get_csv_path(aweme_id, video_timestamp)
            if os.path.exists(filepath):
                with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                    self._csv_cache[cache_key] = {row.get(self.id_field) for row in csv.DictReader(f)}
            else:
                self._csv_cache[cache_key] = set()
        return self._csv_cache[cache_key]
    
    def _get_existing_ids_from_db(self) -> set:
        if not self.db:
            return set()
        if self._db_cache is None:
            if self.table_name == 'videos':
                rows = self.db.query(
                    f"SELECT {self.id_field} FROM {self.table_name} WHERE sec_uid = ?",
                    (self.sec_uid,)
                )
            else:
                rows = self.db.query(
                    f"SELECT t.{self.id_field} FROM {self.table_name} t JOIN videos v ON t.aweme_id = v.aweme_id WHERE v.sec_uid = ?",
                    (self.sec_uid,)
                )
            self._db_cache = {row[self.id_field] for row in rows}
        return self._db_cache

    def _normalize_id(self, value, sample_type: type):
        if value is None:
            return None
        if sample_type == str:
            return str(value)
        elif sample_type == int:
            if isinstance(value, str):
                return int(value) if value.isdigit() else value
            return int(value) if isinstance(value, (int, float)) else value
        return value

    def _normalize_existing_ids(self, existing_ids: set, sample_item: Dict) -> set:
        if not existing_ids or not sample_item:
            return existing_ids
        
        sample_value = sample_item.get(self.id_field)
        if sample_value is None:
            return existing_ids
        
        sample_type = type(sample_value)
        if sample_type not in (str, int):
            return existing_ids
        
        return {self._normalize_id(vid, sample_type) for vid in existing_ids}
    
    def _add_to_cache(self, item_id: str, aweme_id: str = None, update_db: bool = True):
        cache_key = aweme_id or '__root__'
        if cache_key in self._csv_cache:
            self._csv_cache[cache_key].add(item_id)
        if update_db and self._db_cache is not None:
            self._db_cache.add(item_id)
    
    def _get_csv_path(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> str:
        if aweme_id:
            year_month = timestamp_to_year_month(video_timestamp)
            return os.path.join(self.data_dir, year_month, str(aweme_id), self.csv_filename)
        return os.path.join(self.data_dir, self.csv_filename)
    
    def _find_csv_path(self, aweme_id: str, filename: str, video_timestamp: Union[int, float, None] = None) -> Optional[str]:
        """根据视频时间戳直接构造路径，不再遍历目录。"""
        cache_key = f"{aweme_id}:{filename}"
        
        if cache_key in self._csv_path_cache:
            cached_path = self._csv_path_cache[cache_key]
            if os.path.exists(cached_path):
                return cached_path
            del self._csv_path_cache[cache_key]
        
        if video_timestamp:
            year_month = timestamp_to_year_month(video_timestamp)
            path = os.path.join(self.data_dir, year_month, str(aweme_id), filename)
            if os.path.exists(path):
                self._csv_path_cache[cache_key] = path
                return path
        
        for entry in os.listdir(self.data_dir):
            year_month_path = os.path.join(self.data_dir, entry)
            if os.path.isdir(year_month_path) and '-' in entry:
                filepath = os.path.join(year_month_path, aweme_id, filename)
                if os.path.exists(filepath):
                    self._csv_path_cache[cache_key] = filepath
                    return filepath
        
        return None
    
    def save(self, items: List[Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> Dict:
        if not items:
            return {'csv': 0, 'db': 0}
        
        # DB 优先去重（有索引，更快）
        if self.db:
            existing_ids = self._normalize_existing_ids(
                self._get_existing_ids_from_db(), items[0]
            )
        else:
            existing_ids = self._normalize_existing_ids(
                self._get_existing_ids_from_csv(aweme_id, video_timestamp), items[0]
            )
        
        new_items = [item for item in items if item.get(self.id_field) not in existing_ids]
        if not new_items:
            return {'csv': 0, 'db': 0}
        
        csv_count = self._save_to_csv(new_items, aweme_id, video_timestamp)
        db_count = self._save_to_db(new_items) if self.db else 0
        
        return {'csv': csv_count, 'db': db_count}
    
    def _save_to_csv(self, items: List[Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        """写入 CSV，返回实际写入的新记录数（与 DB 计数逻辑一致）。"""
        # 写入时使用标准路径格式，不依赖缓存查找
        if aweme_id and video_timestamp:
            year_month = timestamp_to_year_month(video_timestamp)
            filepath = os.path.join(self.data_dir, year_month, str(aweme_id), self.csv_filename)
        else:
            filepath = os.path.join(self.data_dir, self.csv_filename)
        
        if aweme_id:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # 如果文件存在，进行二次去重检查（确保计数准确）
        truly_new_items = items
        if os.path.exists(filepath):
            # 强制刷新缓存，获取最新的已有 ID
            existing_ids = self._get_existing_ids_from_csv(aweme_id, video_timestamp, force_refresh=True)
            # 二次过滤，确保 truly_new_items 都是真正新的记录
            truly_new_items = [
                item for item in items 
                if item.get(self.id_field) not in existing_ids
            ]
        
        if not truly_new_items:
            logger.debug(f"[存储] CSV 无新记录 → {filepath}")
            return 0
        
        fields = self.user_manager.get_fields(self.data_type)
        file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
        with open(filepath, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(truly_new_items)
        
        # 同时更新 CSV 和 DB 缓存（P1 优化）
        for item in truly_new_items:
            self._add_to_cache(item.get(self.id_field), aweme_id, update_db=True)
        
        logger.info(f"[存储] CSV 写入 {len(truly_new_items)} 条 → {filepath}")
        return len(truly_new_items)
    
    def _save_to_db(self, items: List[Dict]) -> int:
        """写入 DB，items 已经是去重后的新数据。DB 侧仍用 INSERT OR IGNORE 兜底。"""
        if not self.db or not items:
            return 0
        
        fields = self.user_manager.get_fields(self.data_type)
        if self.data_type == 'video':
            fields = fields + ['sec_uid']
        filtered_items = [{k: v for k, v in item.items() if k in fields} for item in items]
        
        count = self.db.insert_many(self.table_name, filtered_items)
        for item in items:
            self._add_to_cache(item[self.id_field])
        logger.info(f"[存储] DB 写入 {count} 条 → {self.table_name}")
        return count
    
    def load(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> List[Dict]:
        filepath = self._get_csv_path(aweme_id, video_timestamp)
        if not os.path.exists(filepath):
            return []
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            return list(csv.DictReader(f))
    
    def update_urls(self, updates: Dict[str, Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        if not updates:
            return 0
        
        total_updated = self._update_csv_urls(updates, aweme_id, video_timestamp)
        if self.db:
            total_updated += self._update_db_urls(updates)
        
        return total_updated
    
    def _update_csv_urls(self, updates: Dict, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        filepath = self._get_csv_path(aweme_id, video_timestamp)
        if not os.path.exists(filepath):
            logger.warning(f"[存储] CSV文件不存在 filepath={filepath}")
            return 0
        
        try:
            rows = []
            updated_count = 0
            
            with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    item_id = row.get(self.id_field)
                    if item_id in updates:
                        for field, new_value in updates[item_id].items():
                            if field in row and new_value:
                                row[field] = new_value
                        updated_count += 1
                    rows.append(row)
            
            if updated_count > 0:
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(rows)
                logger.info(f"[存储] CSV 更新 {updated_count} 条 → {filepath}")
            
            return updated_count
        except Exception as e:
            logger.error(f"[存储] CSV更新失败 {filepath}: {e}")
            return 0
    
    def _update_db_urls(self, updates: Dict) -> int:
        if not self.db or not updates:
            return 0
        
        updated_count = 0
        errors = []
        
        with self.db.transaction() as cur:
            for item_id, update in updates.items():
                set_clauses = []
                values = []
                for field, new_value in update.items():
                    if new_value:
                        set_clauses.append(f"{field} = ?")
                        values.append(new_value)
                
                if set_clauses:
                    values.append(item_id)
                    sql = f"UPDATE {self.table_name} SET {', '.join(set_clauses)} WHERE {self.id_field} = ?"
                    try:
                        cur.execute(sql, tuple(values))
                        updated_count += 1
                    except Exception as e:
                        errors.append((item_id, str(e)))
            
            if errors:
                logger.warning(f"[存储] DB更新失败 count={len(errors)}")
                for item_id, err in errors[:5]:
                    logger.error(f"[存储] DB更新失败 id={item_id}: {err}")
        
        if updated_count > 0:
            logger.info(f"[存储] DB 更新 {updated_count} 条 → {self.table_name}")
        return updated_count
    
    def get_video_ids(self, limit: int = 0) -> List[str]:
        filepath = os.path.join(self.data_dir, 'videos.csv')
        if not os.path.exists(filepath):
            return []
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            ids = [row['aweme_id'] for row in csv.DictReader(f) if row.get('aweme_id')]
        return ids[-limit:] if limit > 0 else ids
    
    def get_video_timestamps(self) -> Dict[str, int]:
        filepath = os.path.join(self.data_dir, 'videos.csv')
        if not os.path.exists(filepath):
            return {}
        timestamps = {}
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                aweme_id = row.get('aweme_id')
                create_time = row.get('create_time', '0') or '0'
                if aweme_id:
                    timestamps[aweme_id] = int(create_time)
        return timestamps
    
    def get_comment_ids(self, video_limit: int = 0) -> List[Dict]:
        comments = []
        video_timestamps = self.get_video_timestamps()
        
        video_ids = self.get_video_ids(video_limit)
        
        for aweme_id in video_ids:
            video_ts = video_timestamps.get(aweme_id)
            filepath = self._find_csv_path(aweme_id, 'comments.csv', video_ts)
            if not filepath:
                continue
            try:
                with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                    for row in csv.DictReader(f):
                        reply_comment_total = row.get('reply_comment_total', '0') or '0'
                        if row.get('cid') and int(reply_comment_total) > 0:
                            comments.append({'cid': row['cid'], 'aweme_id': aweme_id})
            except (ValueError, IOError) as e:
                logger.warning(f"[存储] 读取评论 CSV 失败 {filepath}: {e}")
        return comments
    
    def get_videos_with_comments(self) -> Set[str]:
        videos_with_comments = set()
        video_timestamps = self.get_video_timestamps()
        for aweme_id in self.get_video_ids():
            video_ts = video_timestamps.get(aweme_id)
            if self._find_csv_path(aweme_id, 'comments.csv', video_ts):
                videos_with_comments.add(aweme_id)
        return videos_with_comments
    
    def get_comments_with_replies(self) -> Set[str]:
        comment_ids = set()
        video_timestamps = self.get_video_timestamps()
        for aweme_id in self.get_video_ids():
            video_ts = video_timestamps.get(aweme_id)
            filepath = self._find_csv_path(aweme_id, 'replies.csv', video_ts)
            if filepath:
                try:
                    with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                        for row in csv.DictReader(f):
                            if row.get('reply_id'):
                                comment_ids.add(row['reply_id'])
                except (ValueError, IOError):
                    pass
        return comment_ids
