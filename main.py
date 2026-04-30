#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
抖音用户作品和评论下载系统

命令格式:
    python main.py <类型> [选项]
    
    类型: video | comment | reply
    
交互模式:
    python main.py          启动交互式菜单
    python main.py -i       启动交互式菜单
"""

import asyncio
import argparse
import sys
import os
import atexit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.logger import logger
from core.api import CookieExpiredError
from utils.field_config import UserManager
from utils.printer import Config, task_info, user_header, result, total
from services.video_service import VideoService
from services.comment_service import CommentService
from services.reply_service import ReplyService

_cleanup_registered = False


def cleanup():
    try:
        from core.database import SQLiteDatabase
        SQLiteDatabase.close_all()
    except Exception:
        pass
    
    try:
        from core.downloader import MediaDownloader
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(MediaDownloader.close_all())
        except RuntimeError:
            asyncio.run(MediaDownloader.close_all())
    except Exception:
        pass

    try:
        from core.api import DouyinAPI
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(DouyinAPI.close_instance())
        except RuntimeError:
            asyncio.run(DouyinAPI.close_instance())
    except Exception:
        pass
    
    logger.info("[清理] 资源释放完成")


def _ensure_cleanup():
    global _cleanup_registered
    if not _cleanup_registered:
        _cleanup_registered = True
        atexit.register(cleanup)

Config.init_services(VideoService, CommentService, ReplyService)


class TaskRunner:
    
    def __init__(self, data_type: str):
        self.data_type = data_type
        self.user_manager = UserManager()
        self.cookie = self.user_manager.get_cookie()
        self.crawler_config = self.user_manager.get_crawler_config()
    
    def create_service(self, sec_uid: str, user: dict = None):
        service_class = Config.SERVICES[self.data_type]
        return service_class(sec_uid, self.cookie)
    
    async def collect(self, users: list, limit: int = 0, skip_existing: bool = False):
        task_info(self.user_manager, self.data_type, users, limit, False)
        
        total_stats = {'users': 0, 'count': 0}
        
        for i, user in enumerate(users, 1):
            user_header(user, i, len(users))
            
            service = self.create_service(user['sec_uid'], user)
            
            params = {
                'delay': self.crawler_config.get('request_delay', 1.0),
                'limit': limit,
                'skip_existing': skip_existing
            }
            
            stats = await service.run(**params)
            result(user, stats, self.data_type)
            
            total_stats['users'] += 1
            total_stats['count'] += stats.get('new', 0)
        
        total(total_stats, self.data_type)
    
    async def download(self, users: list):
        task_info(self.user_manager, self.data_type, users, 0, True)
        
        total_stats = {'users': len(users), 'updated': 0}
        for field in Config.MEDIA_FIELDS[self.data_type]:
            total_stats[field] = 0
        
        for i, user in enumerate(users, 1):
            user_header(user, i, len(users))
            
            service = self.create_service(user['sec_uid'])
            stats = await service.run_download_only()
            
            result(user, stats, self.data_type, is_download=True)
            
            for field in Config.MEDIA_FIELDS[self.data_type] + ['updated']:
                total_stats[field] += stats.get(field, 0)
        
        total(total_stats, self.data_type, is_download=True)


def get_users(sec_uid: str = None) -> list:
    if sec_uid:
        return [{
            'sec_uid': sec_uid, 'nickname': '指定用户',
            'videos': True, 'comments': True, 'replies': True
        }]
    
    users = UserManager().get_active_users()
    if not users:
        print("没有活跃的用户需要采集")
    return users


async def run_all(users: list, limit: int = 0, download_only: bool = False, skip_existing: bool = False):
    types_order = ['video', 'comment', 'reply']
    
    for data_type in types_order:
        print(f"\n{'='*60}")
        print(f"开始处理: {Config.TYPE_NAMES[data_type]}")
        print(f"{'='*60}\n")
        
        runner = TaskRunner(data_type)
        
        if download_only:
            await runner.download(users)
        else:
            await runner.collect(users, limit, skip_existing)
    
    print(f"\n{'='*60}")
    print("全部操作完成!")
    print(f"{'='*60}\n")


# ==================== 交互式菜单 ====================

def _input_choice(prompt: str, choices: list, default: str = None) -> str:
    """获取用户选择，支持数字索引或直接输入"""
    while True:
        try:
            value = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value and default is not None:
            return default
        if value in choices:
            return value
        # 尝试作为数字索引解析
        try:
            idx = int(value)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        except ValueError:
            pass
        print(f"  无效输入，请重新输入")


def _input_int(prompt: str, default: int = 0, min_val: int = 0) -> int:
    """获取整数输入"""
    while True:
        try:
            value = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value:
            return default
        try:
            num = int(value)
            if num < min_val:
                print(f"  不能小于 {min_val}")
                continue
            return num
        except ValueError:
            print("  请输入有效数字")


def _input_yes_no(prompt: str, default: bool = False) -> bool:
    """获取是/否输入"""
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        try:
            value = input(prompt + suffix).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value:
            return default
        if value in ('y', 'yes', '是'):
            return True
        if value in ('n', 'no', '否'):
            return False
        print("  请输入 y 或 n")


def _print_menu(title: str, options: list):
    """打印菜单"""
    print(f"  {title}")
    for i, (key, desc) in enumerate(options, 1):
        print(f"  [{i}] {desc}")
    print(f"{'─'*41}")


def _select_users_interactive(all_users: list) -> list:
    """交互式选择用户"""
    if not all_users:
        return []
    if len(all_users) == 1:
        return all_users

    print(f"\n发现 {len(all_users)} 个用户:")
    for i, u in enumerate(all_users, 1):
        name = u.get('nickname') or u['sec_uid']
        print(f"  [{i}] {name}")
    print(f"  [0] 全部用户")

    while True:
        try:
            value = input("\n请选择用户编号（多个用逗号分隔，0=全部）: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value or value == '0':
            return all_users
        try:
            indices = [int(x.strip()) for x in value.split(',')]
            selected = []
            for idx in indices:
                if 1 <= idx <= len(all_users):
                    selected.append(all_users[idx - 1])
            if selected:
                return selected
            print("  无效的选择")
        except ValueError:
            print("  请输入数字编号")


def interactive_menu():
    """交互式主菜单"""
    print("                 _.")
    print("               <(o  )  _,,,°")
    print("---------------(__''___) ---------------")
    print("  作品评论采集器 04.27.2026 by NcieXHY'")
    print("-----------------------------------------")
    # print(f"\n{'='*41}")
    # print("  抖音用户作品和评论下载系统")
    # print(f"{'='*41}")

    all_users = UserManager().get_active_users()
    if not all_users:
        print("\n⚠️  config.yaml 中没有配置活跃用户")
        print("   请先配置用户后再运行")
        return

    # 1. 选择操作类型
    _print_menu("选择操作类型", [
        ('video', '采集作品'),
        ('comment', '采集评论'),
        ('reply', '采集回复'),
        ('all', '全量采集（作品→评论→回复）'),
    ])
    data_type = _input_choice("请输入选项: ", ['video', 'comment', 'reply', 'all'], default='all')

    # 2. 选择用户
    users = _select_users_interactive(all_users)
    if not users:
        print("未选择用户，退出")
        return

    # 3. 选择模式
    _print_menu("选择执行模式", [
        ('collect', '采集数据'),
        ('download', '仅下载媒体'),
    ])
    mode = _input_choice("请输入选项: ", ['collect', 'download'], default='collect')
    download_only = (mode == 'download')

    # 4. 高级选项
    limit = 0
    skip_existing = False
    if not download_only and data_type != 'video':
        limit = _input_int("\n限制采集数量（0=不限制）: ", default=0)
    
    if not download_only:
        skip_existing = _input_yes_no("跳过已采集的数据?", default=False)

    # 5. 确认执行
    print(f"\n{'─'*41}")
    print("  执行摘要:")
    print(f"  • 操作: {Config.TYPE_NAMES.get(data_type, '全量采集')}")
    print(f"  • 用户: {', '.join(u.get('nickname') or u['sec_uid'] for u in users)}")
    print(f"  • 模式: {'仅下载媒体' if download_only else '采集数据'}")
    if limit > 0:
        print(f"  • 限制: {limit}")
    print(f"{'─'*41}")

    if not _input_yes_no("确认执行?", default=True):
        print("已取消")
        return

    _ensure_cleanup()

    # 6. 执行
    if data_type == 'all':
        asyncio.run(run_all(users, limit, download_only, skip_existing))
    else:
        runner = TaskRunner(data_type)
        if download_only:
            asyncio.run(runner.download(users))
        else:
            asyncio.run(runner.collect(users, limit, skip_existing))


def main():
    parser = argparse.ArgumentParser(
        description='抖音用户作品和评论下载系统',
        usage='python main.py <类型> [选项] 或 python main.py --all [选项]',
        add_help=False
    )
    
    parser.add_argument('type', nargs='?', choices=['video', 'comment', 'reply'],
                        help='video | comment | reply')
    parser.add_argument('--all', action='store_true', dest='all_types',
                        help='全量采集：作品→评论→回复')
    parser.add_argument('--download-only', action='store_true',
                        help='仅下载媒体，不采集数据')
    parser.add_argument('--limit', type=int, default=0,
                        help='限制采集视频数量')
    parser.add_argument('--skip-existing', action='store_true', dest='skip_existing',
                        help='跳过已采集的数据')
    parser.add_argument('--sec-uid', type=str,
                        help='指定用户 sec_uid')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='强制进入交互式菜单')
    parser.add_argument('-h', '--help', action='store_true',
                        help='显示帮助信息')
    
    args = parser.parse_args()

    # 显示帮助
    if args.help:
        parser.print_help()
        print("\n交互模式:")
        print("  python main.py")
        print("  python main.py -i")
        return
    
    # 无参数 或 显式交互模式 → 进入交互菜单
    if args.interactive or (not args.type and not args.all_types):
        interactive_menu()
        return
    
    users = get_users(args.sec_uid)
    if not users:
        return
    
    _ensure_cleanup()
    
    if args.all_types:
        asyncio.run(run_all(users, args.limit, args.download_only, args.skip_existing))
        return
    
    runner = TaskRunner(args.type)
    
    if args.download_only:
        asyncio.run(runner.download(users))
    else:
        asyncio.run(runner.collect(users, args.limit, args.skip_existing))


if __name__ == "__main__":
    try:
        main()
    except CookieExpiredError as e:
        print(f"\n❌ Cookie验证失败: {e}")
        print("请从浏览器重新导出Cookie并更新 cookie.txt")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n程序已被用户中断")
        _ensure_cleanup()
    except Exception as e:
        logger.error(f"[主程序] 运行出错: {e}")
        import traceback
        traceback.print_exc()
        _ensure_cleanup()
        cleanup()
        sys.exit(1)
