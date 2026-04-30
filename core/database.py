import sqlite3
import os
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional, List, Dict, Tuple, Any, Set, Generator
from core.logger import logger


class BaseDatabase(ABC):
    _instances: Dict[str, 'BaseDatabase'] = {}
    _lock = threading.Lock()
    _pool: Queue
    _max_connections: int
    _created_connections: int
    _connection_lock: threading.Lock
    
    @abstractmethod
    def execute(self, sql: str, params: Tuple = None) -> int:
        pass
    
    @abstractmethod
    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        pass
    
    @abstractmethod
    def query_one(self, sql: str, params: Tuple = None) -> Optional[Dict]:
        pass
    
    @abstractmethod
    def insert_many(self, table: str, data: List[Dict]) -> int:
        pass
    
    @abstractmethod
    def close(self) -> None:
        pass
    
    @abstractmethod
    def table_exists(self, table: str) -> bool:
        pass
    
    @abstractmethod
    def _create_connection(self) -> Any:
        pass
    
    @abstractmethod
    def _validate_connection(self, conn: Any) -> bool:
        pass
    
    def _get_connection(self, timeout: float = 5.0) -> Optional[Any]:
        try:
            conn = self._pool.get(block=True, timeout=timeout)
            if self._validate_connection(conn):
                return conn
            self._safe_close(conn)
            with self._connection_lock:
                self._created_connections -= 1
            return self._create_new_connection()
        except Empty:
            return self._create_new_connection()
    
    def _create_new_connection(self) -> Optional[Any]:
        with self._connection_lock:
            if self._created_connections < self._max_connections:
                self._created_connections += 1
                try:
                    return self._create_connection()
                except Exception:
                    self._created_connections -= 1
                    raise
        return None
    
    def _release_connection(self, conn: Any) -> None:
        try:
            self._pool.put_nowait(conn)
        except Exception:
            self._safe_close(conn)
    
    def _safe_close(self, conn: Any) -> None:
        try:
            conn.close()
        except Exception:
            pass
        with self._connection_lock:
            self._created_connections = max(0, self._created_connections - 1)
    
    @contextmanager
    def cursor(self, autocommit: bool = True) -> Generator:
        conn = None
        cur = None
        try:
            conn = self._get_connection()
            if not conn:
                raise RuntimeError("数据库连接池已满")
            cur = conn.cursor()
            yield cur
            if autocommit:
                conn.commit()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise e
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                self._release_connection(conn)
    
    @contextmanager
    def transaction(self) -> Generator:
        conn = None
        cur = None
        try:
            conn = self._get_connection()
            if not conn:
                raise RuntimeError("数据库连接池已满")
            cur = conn.cursor()
            yield cur
            conn.commit()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise e
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                self._release_connection(conn)
    
    def get_existing_ids(self, table: str, id_field: str, ids: List[str]) -> Set[str]:
        if not ids:
            return set()
        
        result = set()
        batch_size = 500
        placeholder = self._get_placeholder()
        
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            placeholders = ', '.join([placeholder] * len(batch))
            sql = f"SELECT {id_field} FROM {table} WHERE {id_field} IN ({placeholders})"
            rows = self.query(sql, tuple(batch))
            result.update(row[id_field] for row in rows)
        
        return result
    
    @abstractmethod
    def _get_placeholder(self) -> str:
        pass
    
    @abstractmethod
    def _fetch_all_as_dict(self, cur) -> List[Dict]:
        pass
    
    @abstractmethod
    def _fetch_one_as_dict(self, cur) -> Optional[Dict]:
        pass

    def execute(self, sql: str, params: Tuple = None) -> int:
        with self.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.rowcount

    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        with self.cursor() as cur:
            cur.execute(sql, params or ())
            return self._fetch_all_as_dict(cur)

    def query_one(self, sql: str, params: Tuple = None) -> Optional[Dict]:
        with self.cursor() as cur:
            cur.execute(sql, params or ())
            return self._fetch_one_as_dict(cur)

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _close_pool(self) -> None:
        while True:
            try:
                conn = self._pool.get_nowait()
                self._safe_close(conn)
            except Empty:
                break
        with self._connection_lock:
            self._created_connections = 0
    
    @classmethod
    def close_all(cls) -> None:
        with cls._lock:
            for instance in list(cls._instances.values()):
                try:
                    instance.close()
                except Exception:
                    pass
            cls._instances.clear()


class SQLiteDatabase(BaseDatabase):
    db_path: str
    _initialized: bool = False

    def __new__(cls, db_path: str = None, sec_uid: str = None) -> 'SQLiteDatabase':
        if sec_uid and not db_path:
            db_path = os.path.join('data', sec_uid, 'sqlite.db')
        if db_path is None:
            db_path = os.path.join('data', 'database', 'sqlite.db')
        
        with cls._lock:
            if db_path not in cls._instances:
                instance = super().__new__(cls)
                instance.db_path = db_path
                instance._pool = Queue(maxsize=5)
                instance._max_connections = 5
                instance._created_connections = 0
                instance._connection_lock = threading.Lock()
                instance._initialized = False
                cls._instances[db_path] = instance
            
            instance = cls._instances[db_path]
            if not instance._initialized:
                instance.__init__(db_path, sec_uid)
            return instance

    def __init__(self, db_path: str = None, sec_uid: str = None):
        if self._initialized:
            return
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_tables()
        self._initialized = True

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn
    
    def _validate_connection(self, conn: sqlite3.Connection) -> bool:
        try:
            conn.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False
    
    def _get_placeholder(self) -> str:
        return '?'
    
    def _fetch_all_as_dict(self, cur) -> List[Dict]:
        return [dict(row) for row in cur.fetchall()]
    
    def _fetch_one_as_dict(self, cur) -> Optional[Dict]:
        row = cur.fetchone()
        return dict(row) if row else None

    def insert_many(self, table: str, data: List[Dict]) -> int:
        if not data:
            return 0
        
        columns = list(data[0].keys())
        placeholders = ', '.join(['?'] * len(columns))
        sql = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        values_list = [[row.get(col) for col in columns] for row in data]
        
        with self.cursor() as cur:
            cur.executemany(sql, values_list)
            return cur.rowcount

    def table_exists(self, table: str) -> bool:
        result = self.query_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        return result is not None

    def _init_tables(self) -> None:
        with self.cursor() as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aweme_id BIGINT UNIQUE NOT NULL,
                    desc TEXT,
                    create_time INTEGER,
                    images TEXT,
                    video TEXT,
                    thumb TEXT,
                    sec_uid VARCHAR(100)
                );
                
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aweme_id BIGINT NOT NULL,
                    cid BIGINT UNIQUE NOT NULL,
                    text TEXT,
                    image_list TEXT,
                    digg_count INTEGER DEFAULT 0,
                    create_time INTEGER,
                    user_nickname VARCHAR(50),
                    user_unique_id VARCHAR(50),
                    user_avatar TEXT,
                    sticker TEXT,
                    reply_comment_total INTEGER DEFAULT 0,
                    ip_label VARCHAR(10)
                );
                
                CREATE TABLE IF NOT EXISTS replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aweme_id BIGINT NOT NULL,
                    cid BIGINT UNIQUE NOT NULL,
                    reply_id BIGINT NOT NULL,
                    reply_to_reply_id BIGINT DEFAULT 0,
                    text TEXT,
                    image_list TEXT,
                    digg_count INTEGER DEFAULT 0,
                    create_time INTEGER,
                    user_nickname VARCHAR(50),
                    user_unique_id VARCHAR(50),
                    user_avatar TEXT,
                    sticker TEXT,
                    reply_to_username VARCHAR(50),
                    ip_label VARCHAR(10)
                );
                
                CREATE INDEX IF NOT EXISTS idx_videos_user ON videos(sec_uid);
                CREATE INDEX IF NOT EXISTS idx_videos_time ON videos(create_time);
                CREATE INDEX IF NOT EXISTS idx_comments_aweme ON comments(aweme_id);
                CREATE INDEX IF NOT EXISTS idx_comments_time ON comments(create_time);
                CREATE INDEX IF NOT EXISTS idx_replies_reply ON replies(reply_id);
                CREATE INDEX IF NOT EXISTS idx_replies_cid ON replies(cid);
                CREATE INDEX IF NOT EXISTS idx_replies_time ON replies(create_time); 
            """)
        
        logger.info(f"[DB] SQLite 初始化完成: {self.db_path}")

    def close(self) -> None:
        self._close_pool()
        logger.info(f"[DB] SQLite 连接已关闭: {self.db_path}")


def get_database(sec_uid: str = None) -> Optional['SQLiteDatabase']:
    try:
        from utils.field_config import UserManager
        config = UserManager()._config
        if not config.get('sqlite', False):
            return None
        return SQLiteDatabase(sec_uid=sec_uid)
    except Exception as e:
        logger.error(f"[DB] 读取配置失败: {e}")
    return None
