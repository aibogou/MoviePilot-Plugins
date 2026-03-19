import re
import base64
import logging
import feedparser
from datetime import datetime
from typing import Any, List, Dict,Tuple, Optional
from app.plugins import _PluginBase
from app.log import logger
from p115client import P115Client


class P115Offline(_PluginBase):
    # --- 插件元数据 ---
    plugin_name = "115离线助手"
    plugin_desc = "支持 RSS 订阅自动离线到 115，并自动同步下载状态。"
    plugin_icon = "download"
    plugin_version = "1.0.4"
    plugin_author = "Gemini"

    # 私有属性
    _enabled = False
    _cookie = None
    _notify = False
    _onlyonce = False
    _cron = None

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
        except Exception as e:
            logger.error(f"P115Offline初始化错误: {str(e)}", exc_info=True)

    def update_scheduler(self):
        """配置定时任务"""
        config = self.get_config()
        interval = int(config.get("interval", 30))
        enabled = config.get("enabled")

        # 先清理旧任务
        if self.scheduler.get_job("p115_rss_sync"):
            self.scheduler.remove_job("p115_rss_sync")
        if self.scheduler.get_job("p115_status_sync"):
            self.scheduler.remove_job("p115_status_sync")

        if enabled:
            # 任务 A：定期检查 RSS
            self.scheduler.add_job(
                func=self.sync_rss,
                trigger='interval',
                minutes=interval,
                id='p115_rss_sync',
                replace_existing=True
            )
            # 任务 B：定期同步下载状态
            self.scheduler.add_job(
                func=self.sync_status,
                trigger='interval',
                minutes=2,  # 稍微放宽到 2 分钟，避免请求过于频繁
                id='p115_status_sync',
                replace_existing=True
            )
            logger.info(f"【115离线】定时任务已启动，检查间隔：{interval}分钟")

    def get_p115_client(self):
        """获取 115 客户端实例"""
        config = self.get_config()
        cookie = config.get("cookie", "")
        if not cookie:
            return None

        if not self._client or self._client.cookie != cookie:
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
        logger.info(f"hdhivesign状态: {self._enabled}")
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        返回插件配置的表单
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
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
                                            'model': 'enabled',
                                            'label': '启用插件',
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '开启通知',
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cookie',
                                            'label': '站点Cookie',
                                            'placeholder': '请输入影巢站点Cookie值'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'username',
                                            'label': '用户名/邮箱（用于自动登录）',
                                            'placeholder': '例如：email@example.com'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'password',
                                            'label': '密码（用于自动登录）',
                                            'placeholder': '请输入密码',
                                            'type': 'password'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'base_url',
                                            'label': '站点地址',
                                            'placeholder': '例如：https://hdhive.online 或新域名',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
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
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'max_retries',
                                            'label': '最大重试次数',
                                            'type': 'number',
                                            'placeholder': '3'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'retry_interval',
                                            'label': '重试间隔(秒)',
                                            'type': 'number',
                                            'placeholder': '30'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'history_days',
                                            'label': '历史保留天数',
                                            'type': 'number',
                                            'placeholder': '30'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '【使用教程】\n1. 登录影巢站点（具体域名请在上方“站点地址”中填写），按F12打开开发者工具。\n2. 切换到"应用(Application)" -> "Cookie"，或"网络(Network)"选项卡，找到发往API的请求。\n3. 复制完整的Cookie字符串。\n4. 确保Cookie中包含 `token` 和 `csrf_access_token` 字段。\n5. 粘贴到上方输入框，启用插件并保存。\n\n⚠️ 影巢可能变更域名，若签到异常请先更新“站点地址”。插件会自动使用系统配置的代理。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cookie": "",
            "base_url": "https://hdhive.com",
            "cron": "0 8 * * *",
            "max_retries": 3,
            "retry_interval": 30,
            "history_days": 30,
            "username": "",
            "password": ""
        }

    def get_page(self) -> List[dict]:
        """
        构建插件详情页面，展示签到历史 (完全参照 qmjsign)
        """
        historys = self.get_data('sign_history') or []
        user = self.get_data('hdhive_user_info') or {}
        consecutive_days = self.get_data('consecutive_days') or 0

        info_card = []
        if user:
            avatar = user.get('avatar_url') or ''
            nickname = user.get('nickname') or '—'
            points = user.get('points') if user.get('points') is not None else '—'
            signin_days_total = user.get('signin_days_total') if user.get('signin_days_total') is not None else '—'
            created_at = user.get('created_at') or '—'
            info_card = [{
                'component': 'VCard',
                'props': {'variant': 'outlined', 'class': 'mb-4'},
                'content': [
                    {
                        'component': 'VCardTitle',
                        'props': {'class': 'd-flex align-center justify-space-between'},
                        'content': [
                            {
                                'component': 'div',
                                'content': [
                                    {'component': 'span', 'props': {'class': 'text-h6'}, 'text': '👤 影巢用户信息'},
                                    {'component': 'div', 'props': {'class': 'text-caption'}, 'text': f'加入时间：{created_at}'}
                                ]
                            },
                            {'component': 'VAvatar', 'props': {'size': 64}, 'content': [{'component': 'img', 'props': {'src': avatar, 'alt': nickname}}]}
                        ]
                    },
                    {'component': 'VDivider'},
                    {
                        'component': 'VCardText',
                        'content': [
                            {
                                'component': 'VRow',
                                'content': [
                                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VChip', 'props': {'variant': 'elevated', 'color': 'primary', 'class': 'mb-2'}, 'text': f'用户：{nickname}'}]},
                                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VChip', 'props': {'variant': 'elevated', 'color': 'amber-darken-2', 'class': 'mb-2'}, 'text': f'积分：{points}'}]},
                                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VChip', 'props': {'variant': 'elevated', 'color': 'success', 'class': 'mb-2'}, 'text': f'累计签到天数（站点）：{signin_days_total}'}]},
                                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VChip', 'props': {'variant': 'elevated', 'color': 'cyan-darken-2', 'class': 'mb-2'}, 'text': f'连续签到天数（插件）：{consecutive_days}'}]},
                                ]
                            },
                            {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'class': 'mt-2', 'text': '注：累计签到天数来自站点数据；插件统计的是连续天数，两者可能不同'}},
                        ]
                    }
                ]
            }]

        if not historys:
            return info_card + [{
                'component': 'VAlert',
                'props': {
                    'type': 'info', 'variant': 'tonal',
                    'text': '暂无签到记录，请等待下一次自动签到或手动触发一次。',
                    'class': 'mb-2'
                }
            }]

        historys = sorted(historys, key=lambda x: x.get("date", ""), reverse=True)

        history_rows = []
        for history in historys:
            status = history.get("status", "未知")
            if "成功" in status or "已签到" in status:
                status_color = "success"
            elif "失败" in status:
                status_color = "error"
            else:
                status_color = "info"

            history_rows.append({
                'component': 'tr',
                'content': [
                    {'component': 'td', 'props': {'class': 'text-caption'}, 'text': history.get("date", "")},
                    {
                        'component': 'td',
                        'content': [{
                            'component': 'VChip',
                            'props': {'color': status_color, 'size': 'small', 'variant': 'outlined'},
                            'text': status
                        }]
                    },
                    {'component': 'td', 'text': history.get('message', '—')},
                    {'component': 'td', 'text': str(history.get('points', '—'))},
                    {'component': 'td', 'text': str(history.get('days', '—'))},
                ]
            })

        return info_card + [{
            'component': 'VCard',
            'props': {'variant': 'outlined', 'class': 'mb-4'},
            'content': [
                {'component': 'VCardTitle', 'props': {'class': 'text-h6'}, 'text': '📊 影巢签到历史'},
                {
                    'component': 'VCardText',
                    'content': [{
                        'component': 'VTable',
                        'props': {'hover': True, 'density': 'compact'},
                        'content': [
                            {
                                'component': 'thead',
                                'content': [{
                                    'component': 'tr',
                                    'content': [
                                        {'component': 'th', 'text': '时间'},
                                        {'component': 'th', 'text': '状态'},
                                        {'component': 'th', 'text': '详情'},
                                        {'component': 'th', 'text': '奖励积分'},
                                        {'component': 'th', 'text': '连续天数'}
                                    ]
                                }]
                            },
                            {'component': 'tbody', 'content': history_rows}
                        ]
                    }]
                }
            ]
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

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