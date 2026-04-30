import yaml
import os
import threading
from typing import List, Dict
from core.logger import logger


class UserManager:
    _instance = None
    _initialized = False
    _lock = threading.Lock()

    def __new__(cls, config_path: str = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            instance = cls._instance
            
            if not instance._initialized:
                instance.__init__(config_path)
            return instance

    def __init__(self, config_path: str = None):
        if self._initialized:
            return
        
        self.config_path = config_path or 'config.yaml'
        self._config = self._load_config()
        self._cookie_config = self._load_cookie_config()
        self._initialized = True

    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"[配置] 加载配置文件失败: {e}")
            return {}

    def reload_config(self):
        """重新加载配置文件"""
        self._config = self._load_config()
        self._cookie_config = self._load_cookie_config()

    def update_nickname(self, sec_uid: str, nickname: str) -> bool:
        """更新 config.yaml 中指定用户的 nickname（文本级替换，保留注释）。"""
        if not sec_uid or not nickname:
            return False
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            updated = False
            in_user_block = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith('sec_uid:') and sec_uid in stripped:
                    in_user_block = True
                    continue
                if in_user_block and stripped.startswith('nickname:'):
                    comment = ''
                    if '#' in line:
                        comment = '  ' + line[line.index('#'):]
                    indent = line[:len(line) - len(line.lstrip())]
                    
                    # 直接写入原始昵称（保留中文，不转义）
                    lines[i] = f'{indent}nickname: "{nickname}"{comment}\n'
                    
                    updated = True
                    in_user_block = False
                    break
                if in_user_block and stripped.startswith(('sec_uid:', '- enabled:')):
                    in_user_block = False
                    break

            if updated:
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)
                logger.info(f"[配置] 已更新 nickname: {nickname}")
                self._config = self._load_config()
            return updated
        except Exception as e:
            logger.error(f"[配置] 更新 nickname 失败: {e}")
            return False

    def _load_cookie_config(self) -> Dict:
        cookie_path = os.path.join(os.path.dirname(self.config_path), 'cookie.txt')
        try:
            if os.path.exists(cookie_path):
                with open(cookie_path, 'r', encoding='utf-8') as f:
                    return {'cookie': f.read().strip()}
        except Exception as e:
            logger.error(f"[配置] 加载cookie配置文件失败: {e}")
        return {}

    def get_active_users(self) -> List[Dict]:
        users = []
        for user in self._config.get('users', []):
            sec_uid = user.get('sec_uid', '')
            if sec_uid.startswith('#'):
                continue
            if not user.get('enabled', True):
                continue
            users.append(user)
        return users

    def get_cookie(self) -> str:
        return self._cookie_config.get('cookie', '')

    def get_crawler_config(self) -> Dict:
        return self._config.get('crawler', {
            'request_delay': 1.0,
            'download_threads': 6
        })

    def get_media_download_config(self) -> Dict:
        default = {
            'video': {'images': True, 'videos': True, 'thumbs': True},
            'comment': {'images': True, 'avatars': True, 'stickers': True},
            'reply': {'images': True, 'avatars': True, 'stickers': True}
        }
        return self._config.get('media_download', default)

    def get_media_download_for_type(self, data_type: str) -> Dict:
        config = self.get_media_download_config()
        return config.get(data_type, {})

    def get_fields(self, data_type: str) -> List[str]:
        fields = self._config.get('fields', {}).get(data_type, [])
        return [f for f in fields if f and not str(f).startswith('#')]