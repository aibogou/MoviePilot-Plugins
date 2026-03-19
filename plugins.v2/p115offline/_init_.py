import re
import base64
import logging
import feedparser
from datetime import datetime
from typing import Any, List, Dict, Optional

from app.plugins import _PluginBase
from app.log import logger
from p115client import P115Client


class P115Offline(_PluginBase):
    # --- 插件元数据 ---
    plugin_name = "115 离线助手"
    plugin_desc = "支持 RSS 订阅自动离线到 115，并自动同步下载状态。"
    plugin_icon = "download"
    plugin_version = "1.0.0"
    plugin_author = "Gemini"

    # 内部变量
    _client = None

    def init_plugin(self, config: dict):
        """插件初始化"""
        # 启动定时任务
        self.update_scheduler()

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

    def stop_plugin(self):
        """停止插件时清理定时器"""
        if self.scheduler.get_job("p115_rss_sync"):
            self.scheduler.remove_job("p115_rss_sync")
        if self.scheduler.get_job("p115_status_sync"):
            self.scheduler.remove_job("p115_status_sync")

    def get_page(self) -> List[dict]:
        """
        构建插件详情页面，展示离线历史
        """
        history_dict = self.get_data("history") or {}
        # 把字典转成列表，并按时间倒序排列
        history_list = sorted(history_dict.values(), key=lambda x: x.get("add_time", ""), reverse=True)

        if not history_list:
            return [{'component': 'VAlert', 'props': {'type': 'info', 'text': '暂无离线下载记录。'}}]

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

        return [{
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
        }]
