import re
import base64
import logging
import feedparser
from datetime import datetime,timedelta
from typing import Any, List, Dict,Tuple, Optional
from app.plugins import _PluginBase
from app.log import logger
from p115client import P115Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.core.config import settings
import pytz


class P115Offline(_PluginBase):
    # --- 插件元数据 ---
    plugin_name = "115离线助手"
    plugin_desc = "支持 RSS 订阅自动离线到 115，并自动同步下载状态。"
    plugin_icon = "download"
    plugin_version = "1.0.5"
    plugin_author = "Gemini"

    # 私有属性
    _enabled = False
    _cookie = None
    _notify = False
    _onlyonce = False
    _cron = None
    _rss_url = None

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    _current_trigger_type = None  # 保存当前执行的触发类型

    # 内部变量
    _client = None

    def init_plugin(self, config: dict):
        # 停止现有任务
        self.stop_service()

        logger.info("============= P115Offline 初始化 =============")
        try:
            if config:
                self._enabled = config.get("enabled")
                self._cookie = config.get("cookie")
                self._notify = config.get("notify")
                self._cron = config.get("cron")
                self._onlyonce = config.get("onlyonce")
                self._rss_url = config.get("rss_url")

            if self._onlyonce:
                logger.info("执行一次订阅")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._manual_trigger = True
                self._scheduler.add_job(func=self.sync_rss, trigger='date',
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="订阅")
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "enabled": self._enabled,
                    "cookie": self._cookie,
                    "notify": self._notify,
                    "cron": self._cron,
                    "rss_url": self._rss_url,
                })

                # 启动任务
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()
        except Exception as e:
            logger.error(f"P115Offline初始化错误: {str(e)}", exc_info=True)


    def get_p115_client(self):
        """获取 115 客户端实例"""
        config = self.get_config()
        cookie = config.get("cookie", "")
        if not cookie:
            return None

        if not self._client:
            self._client = P115Client(cookie)
        return self._client

    # --- 核心业务逻辑 ---

    def sync_rss(self):
        """检查 RSS 订阅并提交离线"""
        config = self.get_config()
        rss_url = config.get("rss_url")
        dir_id = config.get("dir_id", "0")

        if not rss_url:
            return

        try:
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                magnet = self._extract_magnet(entry)
                if not magnet: continue

                info_hash = self._get_standard_info_hash(magnet)
                if not info_hash: continue

                # 调用自带的存储功能查重
                if self._check_history(info_hash):
                    continue

                client = self.get_p115_client()
                if client:
                    res = client.offline_add_url({"url": magnet, "wp_path_id": dir_id})
                    if res.get("state"):
                        # 记录历史
                        self._add_history(info_hash, entry.title, magnet)
                        logger.info(f"【115离线】成功添加任务：{entry.title}")
                        self.post_message(title="115 离线成功", text=f"任务：{entry.title}\n已添加到 115 离线下载。")
                        return
        except Exception as e:
            logger.error(f"【115离线】RSS 同步异常: {str(e)}")

    def sync_status(self):
        """查询 115 状态并更新历史记录"""
        incomplete = self._get_incomplete_tasks()
        if not incomplete: return

        client = self.get_p115_client()
        if not client: return

        try:
            res = client.offline_list()
            if not res.get("state"): return

            status_dict = {t.get("info_hash", "").upper(): t.get("status") for t in res.get("tasks", [])}

            for info_hash, magnet in incomplete:
                if info_hash in status_dict:
                    api_status = status_dict[info_hash]
                    if api_status == 11:
                        self._update_status(info_hash, 2)
                        logger.info(f"【115离线】任务下载完成：{info_hash}")
                    elif api_status == 9:
                        self._update_status(info_hash, -1)
                        logger.warn(f"【115离线】任务下载失败：{info_hash}")
        except Exception as e:
            logger.error(f"【115离线】状态同步异常: {str(e)}")

    # --- 数据存取专区 (使用 MoviePilot 原生方案) ---

    def _check_history(self, info_hash):
        """检查是否已经下载过"""
        # 取出所有历史记录（如果没有就返回空字典）
        history = self.get_data("history") or {}
        return info_hash in history

    def _add_history(self, info_hash, title, magnet):
        """新增下载记录"""
        history = self.get_data("history") or {}
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 把新任务塞进字典
        history[info_hash] = {
            "title": title,
            "magnet": magnet,
            "add_time": now,
            "status": 1  # 1 代表下载中
        }
        # 原封不动保存回去
        self.save_data("history", history)

    def clear_history(self):
        """
        清空历史记录的实际执行函数
        """
        self.save_data("history", {})
        # 返回 code: 0 代表成功，MoviePilot 前端会自动弹出绿色的 msg 提示
        return {"code": 0, "msg": "历史记录已彻底清空！刷新页面即可看到最新状态。"}

    def _get_incomplete_tasks(self):
        """找出所有状态为1（下载中）的任务"""
        history = self.get_data("history") or {}
        incomplete = []
        for info_hash, task in history.items():
            if task.get("status") == 1:
                incomplete.append((info_hash, task.get("magnet")))
        return incomplete

    def _update_status(self, info_hash, status):
        """更新某个任务的状态"""
        history = self.get_data("history") or {}
        if info_hash in history:
            history[info_hash]["status"] = status
            self.save_data("history", history)

    # --- 辅助方法 ---

    def _extract_magnet(self, entry):
        magnet_url = ""
        for link in entry.get('links', []):
            href = link.get('href', '')
            if 'magnet:' in href:
                magnet_url = href
                break
        if not magnet_url:
            magnet_url = entry.get('link', '')

        if magnet_url.startswith('magnet:'):
            return magnet_url.split('&')[0]
        return None

    def _get_standard_info_hash(self, magnet_url):
        match = re.search(r'urn:btih:([a-zA-Z2-70-9]+)', magnet_url, re.IGNORECASE)
        if not match: return None
        hash_str = match.group(1).upper()
        if len(hash_str) == 32:
            try:
                return base64.b32decode(hash_str).hex().upper()
            except:
                return hash_str
        return hash_str

    def get_state(self) -> bool:
        logger.info(f"p115Offline状态: {self._enabled}")
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            logger.info(f"注册定时服务: {self._cron}")
            return [{
                "id": "p115offlineRss",
                "name": "订阅115离线下载",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.sync_rss,
                "kwargs": {}
            },
                {
                    "id": "p115offlineSync",
                    "name": "同步115离线状态",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_status(),
                    "kwargs": {}
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        构建插件配置页面
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用 115 离线助手'}}
                            ]},
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '签到周期'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'cookie', 'label': '115 Cookie',
                                                                      'placeholder': '填入抓取到的 115 Cookie'}}
                            ]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 8}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'rss_url', 'label': 'RSS 订阅地址',
                                                                      'placeholder': '输入 M-Team 等站点的 RSS 链接'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'dir_id', 'label': '115 下载目录 ID',
                                                                      'placeholder': '默认传 0 代表根目录'}}
                            ]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 8 * * *",
            "cookie": "",
            "rss_url": "",
            "dir_id": "0"
        }

    def get_page(self) -> List[dict]:
        """
        构建插件详情页面，展示离线历史
        """
        history_dict = self.get_data("history") or {}
        history_list = sorted(history_dict.values(), key=lambda x: x.get("add_time", ""), reverse=True)

        history_rows = []
        for h in history_list:
            status = h.get("status")
            if status == 2:
                status_chip = {'component': 'VChip', 'props': {'color': 'success', 'size': 'small'}, 'text': '已完成'}
            elif status == -1:
                status_chip = {'component': 'VChip', 'props': {'color': 'error', 'size': 'small'}, 'text': '失败'}
            else:
                status_chip = {'component': 'VChip', 'props': {'color': 'primary', 'size': 'small'}, 'text': '下载中'}

            history_rows.append({
                'component': 'tr',
                'content': [
                    {'component': 'td', 'text': h.get("add_time", "")},
                    {'component': 'td', 'content': [status_chip]},
                    {'component': 'td', 'text': h.get("title", "未知任务")}
                ]
            })

        # 👇 这是从你找到的源码学来的“带点击事件的清空按钮”
        clear_button = {
            'component': 'VBtn',
            'props': {
                'color': 'error',
                'variant': 'elevated',
                'prepend-icon': 'mdi-delete-sweep',
                'class': 'mb-4'  # 底部留点边距
            },
            'text': '一键清空历史记录',
            'events': {
                'click': {
                    # 注意这里的路径：plugin/插件类名/api路径
                    'api': 'plugin/P115Offline/clear_history',
                    'method': 'post'
                }
            }
        }

        # 如果没有历史记录，就只展示一个提示
        if not history_list:
            return [
                clear_button,
                {'component': 'VAlert', 'props': {'type': 'info', 'text': '暂无离线下载记录。'}}
            ]

        # 正常展示按钮和表格
        return [
            clear_button,
            {
                'component': 'VCard',
                'props': {'variant': 'outlined'},
                'content': [
                    {'component': 'VCardTitle', 'text': '⚡ 115 离线下载历史'},
                    {
                        'component': 'VCardText',
                        'content': [{
                            'component': 'VTable',
                            'props': {'hover': True, 'density': 'compact'},
                            'content': [
                                {'component': 'thead', 'content': [{'component': 'tr', 'content': [
                                    {'component': 'th', 'text': '推送时间', 'props': {'width': '200px'}},
                                    {'component': 'th', 'text': '状态', 'props': {'width': '100px'}},
                                    {'component': 'th', 'text': '任务名称'}
                                ]}]},
                                {'component': 'tbody', 'content': history_rows}
                            ]
                        }]
                    }
                ]
            }
        ]

    def get_api(self) -> list:
        """
        对外暴露接口，系统会自动生成执行按钮
        """
        return [
            {
                "path": "/clear_history",
                "endpoint": self.clear_history,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "清空历史记录",
                "description": "一键清空所有的 115 离线下载历史。",
            }
        ]

    def stop_service(self):
        """
        停止服务
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止影巢签到服务失败: {str(e)}")