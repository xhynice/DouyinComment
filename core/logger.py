import logging
import os
from datetime import datetime


class DailyFileHandler(logging.FileHandler):
    """日志文件按日期命名：logs/2026-04-22.log，跨天自动切换。"""

    def __init__(self, log_dir, prefix='', encoding='utf-8'):
        self.log_dir = log_dir
        self.prefix = prefix
        self._file_encoding = encoding
        self._current_date = None
        os.makedirs(log_dir, exist_ok=True)
        super().__init__(self._get_filepath(), encoding=encoding, delay=True)

    def _get_filepath(self):
        date_str = datetime.now().strftime('%Y-%m-%d')
        name = f'{self.prefix}{date_str}.log' if self.prefix else f'{date_str}.log'
        return os.path.join(self.log_dir, name)

    def emit(self, record):
        today = datetime.now().strftime('%Y-%m-%d')
        if today != self._current_date:
            self._current_date = today
            if self.stream:
                self.stream.close()
                self.stream = None
            self.baseFilename = self._get_filepath()
        super().emit(record)


class Logger:
    _instance = None
    _initialized = False
    _log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if Logger._initialized:
            return
        Logger._initialized = True
        self._system_logger = None

    def _get_log_levels(self):
        """从配置文件和环境变量读取日志级别
        
        优先级：环境变量 > 配置文件 > 默认值
        """
        console_level = 'INFO'
        file_level = 'DEBUG'
        
        try:
            # 1. 优先读取环境变量（临时覆盖）
            console_level = os.getenv('LOG_LEVEL_CONSOLE', console_level)
            file_level = os.getenv('LOG_LEVEL_FILE', file_level)
            
            # 2. 读取配置文件
            from utils.field_config import FieldConfig
            config = FieldConfig()
            log_config = config._config.get('log', {})
            
            # 环境变量未设置时才使用配置文件
            if 'LOG_LEVEL_CONSOLE' not in os.environ:
                console_level = log_config.get('console_level', console_level)
            if 'LOG_LEVEL_FILE' not in os.environ:
                file_level = log_config.get('file_level', file_level)
                
        except Exception:
            # 配置读取失败时使用默认值
            pass
        
        return (
            self._log_levels.get(console_level.upper(), logging.INFO),
            self._log_levels.get(file_level.upper(), logging.DEBUG)
        )

    def _get_system_logger(self):
        if self._system_logger is not None:
            return self._system_logger

        os.makedirs('logs', exist_ok=True)

        self._system_logger = logging.getLogger('system')
        
        # 从配置读取级别
        console_level, file_level = self._get_log_levels()
        self._system_logger.setLevel(min(console_level, file_level))
        self._system_logger.handlers.clear()

        # 文件处理器
        file_handler = DailyFileHandler('logs', encoding='utf-8')
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        ))

        self._system_logger.addHandler(file_handler)
        self._system_logger.addHandler(console_handler)
        return self._system_logger

    def info(self, msg: str):
        self._get_system_logger().info(msg)

    def debug(self, msg: str):
        self._get_system_logger().debug(msg)

    def warning(self, msg: str):
        self._get_system_logger().warning(msg)

    def error(self, msg: str):
        self._get_system_logger().error(msg)


logger = Logger()
