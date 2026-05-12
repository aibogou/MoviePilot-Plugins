import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.modules.qbittorrent import Qbittorrent
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.utils.string import StringUtils


lock = threading.Lock()


class SmartLimiter(_PluginBase):
    plugin_name = "下载器智能限速"
    plugin_desc = "按每日累计上传量统一限制已选下载器的上传速度，支持 qBittorrent 和 Transmission。"
    plugin_icon = "upload"
    plugin_version = "1.0.0"
    plugin_author = "aibogo"
    plugin_config_prefix = "smartlimiter_"
    plugin_order = 50
    auth_level = 2

    SUPPORTED_TYPES = {"qbittorrent", "transmission"}
    DEFAULT_LIMIT_GB = 150.0
    DEFAULT_SPEED_KBPS = 10

    _enabled = False
    _notify = False
    _onlyonce = False
    _clear_data = False
    _cron = None
    _downloaders = []
    _upload_limit_gb = DEFAULT_LIMIT_GB
    _upload_speed_kbps = DEFAULT_SPEED_KBPS

    def init_plugin(self, config: dict = None):

        self.stop_service()

        logger.info("============= SmartLimiter 初始化 =============")
        try:
            if config:
                self._enabled = bool(config.get("enabled", False))
                self._notify = bool(config.get("notify", False))
                self._onlyonce = bool(config.get("onlyonce", False))
                self._clear_data = bool(config.get("clear_data", False))
                self._cron = (config.get("cron") or "").strip()
                self._downloaders = self.__normalize_downloaders(config.get("downloaders"))
                self._upload_limit_gb = self.__safe_float(
                    config.get("upload_limit_gb"), self.DEFAULT_LIMIT_GB
                )
                self._upload_speed_kbps = max(
                    0,
                    self.__safe_int(config.get("upload_speed_kbps"), self.DEFAULT_SPEED_KBPS),
                )

            self.__clear_data()
            logger.info(
                f"SmartLimiter已加载，配置：enabled={self._enabled}, notify={self._notify}, "
                f"onlyonce={self._onlyonce}, cron={self._cron}, downloaders={self._downloaders}, "
                f"limit={self._upload_limit_gb}GB, speed={self._upload_speed_kbps}KB/s"
            )
        except Exception as e:
            logger.error(f"SmartLimiter初始化错误: {str(e)}", exc_info=True)

    def get_state(self) -> bool:
        return bool(
            self._enabled
            and self.__get_configured_downloaders()
            and (self._cron or self._onlyonce)
        )

    def get_service(self) -> List[Dict[str, Any]]:
        if not self.get_state():
            return []

        services: List[Dict[str, Any]] = []

        if self._cron:
            try:
                services.append(
                    {
                        "id": "smartlimiter_check",
                        "name": "全局上传限速检查",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self.run_limit_check,
                        "func_kwargs": {"triggered_once": False},
                    }
                )
            except Exception as e:
                logger.error(f"SmartLimiter Cron 表达式无效：{self._cron}，{str(e)}")

        if self._onlyonce:
            services.append(
                {
                    "id": "smartlimiter_once",
                    "name": "全局上传限速立即执行",
                    "trigger": DateTrigger(
                        run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                        + timedelta(seconds=3)
                    ),
                    "func": self.run_limit_check,
                    "func_kwargs": {"triggered_once": True},
                }
            )

        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        downloader_items = [
            {
                "title": f"{config.name} ({self.__type_label(config.type)})",
                "value": config.name,
            }
            for config in sorted(
                DownloaderHelper().get_configs().values(),
                key=lambda x: ((x.name or "").lower(), (x.type or "").lower()),
            )
            if config and config.name and self.__is_supported_type(config.type)
        ]

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即执行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear_data",
                                            "label": "清除统计数据",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "0 */12 * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_speed_kbps",
                                            "label": "限速值（KB/s）",
                                            "type": "number",
                                            "min": 0,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_limit_gb",
                                            "label": "每日总上传量（GB）",
                                            "type": "number",
                                            "min": 0,
                                            "step": "0.1",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "items": downloader_items,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "cron": "0 */12 * * *",
            "downloaders": [],
            "upload_limit_gb": self.DEFAULT_LIMIT_GB,
            "upload_speed_kbps": self.DEFAULT_SPEED_KBPS,
        }

    def get_page(self) -> List[dict]:
        state = self.get_data("state") or {}
        snapshot = state.get("downloaders_state") or {}
        total_today = self.__safe_int(state.get("last_total"), 0) or 0
        limit_bytes = self.__limit_bytes()

        if not snapshot:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无运行数据。",
                    },
                }
            ]

        status_text = "限速中" if state.get("limited") else "不限速"
        status_color = "warning" if state.get("limited") else "success"

        rows = []
        for name, item in snapshot.items():
            available = bool(item.get("available"))
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": name},
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "VChip",
                                    "props": {
                                        "size": "small",
                                        "variant": "outlined",
                                        "color": "primary",
                                    },
                                    "text": self.__type_label(item.get("type")),
                                }
                            ],
                        },
                        {
                            "component": "td",
                            "text": StringUtils.str_filesize(item.get("today", 0)),
                        },
                        {
                            "component": "td",
                            "text": (
                                StringUtils.str_filesize(item.get("current", 0))
                                if available
                                else "离线"
                            ),
                        },
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "VChip",
                                    "props": {
                                        "size": "small",
                                        "variant": "outlined",
                                        "color": "success" if available else "error",
                                    },
                                    "text": "在线" if available else "离线",
                                }
                            ],
                        },
                    ],
                }
            )

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "全局上传限速",
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": status_color,
                                                    "variant": "elevated",
                                                },
                                                "text": status_text,
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "primary",
                                                    "variant": "elevated",
                                                },
                                                "text": f"今日总上传：{StringUtils.str_filesize(total_today)}",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "secondary",
                                                    "variant": "elevated",
                                                },
                                                "text": f"阈值：{StringUtils.str_filesize(limit_bytes)}",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "info",
                                                    "variant": "elevated",
                                                },
                                                "text": f"限速：{self._upload_speed_kbps} KB/s",
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "grey-darken-1",
                                                    "variant": "outlined",
                                                },
                                                "text": f"统计日期：{state.get('date', '-')}",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "grey-darken-1",
                                                    "variant": "outlined",
                                                },
                                                "text": f"最后检查：{state.get('last_check_time', '-')}",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "grey-darken-1",
                                                    "variant": "outlined",
                                                },
                                                "text": f"运行下载器：{len(snapshot)}",
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VTable",
                                "props": {"hover": True, "density": "compact"},
                                "content": [
                                    {
                                        "component": "thead",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {"component": "th", "text": "下载器"},
                                                    {"component": "th", "text": "类型"},
                                                    {"component": "th", "text": "今日累计"},
                                                    {"component": "th", "text": "当前值"},
                                                    {"component": "th", "text": "状态"},
                                                ],
                                            }
                                        ],
                                    },
                                    {"component": "tbody", "content": rows},
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        try:
            with lock:
                state = self.get_data("state") or {}
                if not state.get("limited"):
                    return

                services = self.__get_selected_services()
                if not services:
                    return

                success_count, _ = self.__apply_limit_mode(services, limited=False)
                if success_count > 0:
                    state["limited"] = False
                    state["last_action"] = "unlimit"
                    state["last_action_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.save_data("state", state)
        except Exception as e:
            logger.error(f"停止SmartLimiter服务失败：{str(e)}")

    def run_limit_check(self, triggered_once: bool = False):
        if not self._enabled:
            return

        with lock:
            services = self.__get_selected_services()
            if not services:
                logger.warning("SmartLimiter 未找到可用的 qbittorrent/transmission 下载器，跳过本次检查")
                if triggered_once:
                    self.__disable_onlyonce()
                return

            state, total_today = self.__refresh_state(services)
            limit_bytes = self.__limit_bytes()
            previous_limited = bool(state.get("limited", False))
            desired_limited = total_today >= limit_bytes
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            action_limit_kbps = self._upload_speed_kbps if desired_limited else 0

            success_count, failed_names = self.__apply_limit_mode(services, desired_limited)

            if success_count > 0:
                state["limited"] = desired_limited
                state["last_action"] = "limit" if desired_limited else "unlimit"
                state["last_action_time"] = now_text
            else:
                state["last_action"] = "pending_limit" if desired_limited else "pending_unlimit"
                state["last_action_time"] = now_text
                logger.warning("SmartLimiter 本次未能处理任何下载器，保留上一次状态")

            state["last_total"] = total_today
            self.save_data("state", state)

            if (
                self._notify
                and success_count > 0
                and desired_limited != previous_limited
            ):
                self.__send_notify(
                    limited=desired_limited,
                    total_today=total_today,
                    limit_bytes=limit_bytes,
                    success_count=success_count,
                    failed_names=failed_names,
                )

            logger.info(
                f"SmartLimiter 检查完成：今日总上传 {StringUtils.str_filesize(total_today)}，"
                f"阈值 {StringUtils.str_filesize(limit_bytes)}，"
                f"状态 {'限速' if desired_limited else '不限速'}，"
                f"尝试限速值 {action_limit_kbps} KB/s"
            )

            if triggered_once:
                self.__disable_onlyonce()

    def __refresh_state(self, services: Dict[str, Any]) -> Tuple[dict, int]:
        stored_state = self.get_data("state") or {}
        today = datetime.now().strftime("%Y-%m-%d")
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_new_day = stored_state.get("date") != today
        previous_snapshot = stored_state.get("downloaders_state") or {}

        snapshot: Dict[str, Dict[str, Any]] = {}
        total_today = 0

        for name, service in services.items():
            prev_entry = previous_snapshot.get(name) or {}
            current_total = self.__get_downloader_total(service)
            available = current_total is not None
            today_bytes = 0 if is_new_day else self.__safe_int(prev_entry.get("today"), 0) or 0
            seen = prev_entry.get("seen")
            downloader_type = self.__downloader_type(service)

            if available:
                current_total = int(current_total)
                if is_new_day or seen is None:
                    seen = current_total
                    today_bytes = 0
                else:
                    seen_int = self.__safe_int(seen, current_total) or 0
                    if current_total >= seen_int:
                        today_bytes += current_total - seen_int
                    else:
                        today_bytes += current_total
                        logger.info(
                            f"SmartLimiter 检测到下载器 {name} 统计值回退，已按当前值重新计数"
                        )
                    seen = current_total
            elif seen is not None:
                seen = self.__safe_int(seen, None)

            today_bytes = max(0, int(today_bytes))
            snapshot[name] = {
                "name": name,
                "type": downloader_type,
                "today": today_bytes,
                "seen": seen,
                "current": current_total,
                "available": available,
            }
            total_today += today_bytes

        state = {
            "date": today,
            "limited": bool(stored_state.get("limited", False)),
            "downloaders_state": snapshot,
            "last_check_time": now_text,
            "last_total": total_today,
        }
        return state, total_today

    def __apply_limit_mode(self, services: Dict[str, Any], limited: bool) -> Tuple[int, List[str]]:
        success_count = 0
        failed_names: List[str] = []
        target_speed = self._upload_speed_kbps if limited else 0

        for name, service in services.items():
            if self.__set_upload_limit(service, target_speed):
                success_count += 1
            else:
                failed_names.append(name)

        return success_count, failed_names

    def __set_upload_limit(self, service: Any, upload_limit_kbps: int) -> bool:
        try:
            if not self.__ensure_service(service):
                return False

            current_download_limit = 0
            try:
                speed_limit = service.instance.get_speed_limit()
                if speed_limit:
                    current_download_limit = self.__safe_int(speed_limit[0], None)
            except Exception as e:
                logger.warning(f"SmartLimiter 获取下载器 {service.name} 当前下载限速失败：{str(e)}")
                current_download_limit = None

            if current_download_limit is None:
                current_download_limit = 0

            return bool(
                service.instance.set_speed_limit(
                    download_limit=current_download_limit,
                    upload_limit=upload_limit_kbps,
                )
            )
        except Exception as e:
            logger.error(f"SmartLimiter 设置下载器 {service.name} 上传限速失败：{str(e)}")
            return False

    def __get_downloader_total(self, service: Any) -> Optional[int]:
        try:
            if not self.__ensure_service(service):
                return None

            downloader_type = self.__downloader_type(service)

            if downloader_type == "qbittorrent":
                qb_service:Qbittorrent = service.instance
                maindata = qb_service.qbc.sync_maindata()
                return self.__safe_int(maindata.server_state.alltime_ul, None)

            if downloader_type == "transmission":
                info = service.instance.transfer_info()
                if not info:
                    return None
                uploaded = None
                cumulative_stats = getattr(info, "cumulative_stats", None)
                if cumulative_stats:
                    uploaded = getattr(cumulative_stats, "uploaded_bytes", None)
                if uploaded is None:
                    current_stats = getattr(info, "current_stats", None)
                    if current_stats:
                        uploaded = getattr(current_stats, "uploaded_bytes", None)
                return self.__safe_int(uploaded, None)

            return None
        except Exception as e:
            logger.error(f"SmartLimiter 获取下载器 {service.name} 上传量失败：{str(e)}")
            return None

    def __get_selected_services(self) -> Dict[str, Any]:
        configured_names = self.__get_configured_downloaders()
        if not configured_names:
            return {}

        services = DownloaderHelper().get_services(name_filters=configured_names) or {}
        ret: Dict[str, Any] = {}
        for name in configured_names:
            service = services.get(name)
            if not service or not service.config:
                continue
            if not self.__is_supported_type(service.config.type):
                continue
            ret[name] = service
        return ret

    def __get_configured_downloaders(self) -> List[str]:
        if not self._downloaders:
            return []

        configs = DownloaderHelper().get_configs() or {}
        ret: List[str] = []
        for name in self.__normalize_downloaders(self._downloaders):
            config = configs.get(name)
            if not config or not config.type:
                continue
            if not self.__is_supported_type(config.type):
                continue
            ret.append(name)
        return ret

    def __send_notify(
        self,
        limited: bool,
        total_today: int,
        limit_bytes: int,
        success_count: int,
        failed_names: List[str],
    ):
        action = "开启限速" if limited else "解除限速"
        title = f"【全局上传限速】{action}"
        text_lines = [
            f"今日累计上传：{StringUtils.str_filesize(total_today)}",
            f"阈值：{StringUtils.str_filesize(limit_bytes)}",
            f"限速值：{self._upload_speed_kbps} KB/s" if limited else "限速值：不限速",
            f"成功处理：{success_count} 个下载器",
        ]

        if failed_names:
            text_lines.append(f"未处理：{'、'.join(failed_names)}")

        self.post_message(
            mtype=NotificationType.SiteMessage,
            title=title,
            text="\n".join(text_lines),
        )

    def __disable_onlyonce(self):
        if not self._onlyonce:
            return

        self._onlyonce = False
        config = self.get_config() or {}
        config.update(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": False,
                "clear_data": self._clear_data,
                "cron": self._cron,
                "downloaders": self._downloaders,
                "upload_limit_gb": self._upload_limit_gb,
                "upload_speed_kbps": self._upload_speed_kbps,
            }
        )
        self.update_config(config)

    def __clear_data(self):
        if not self._clear_data:
            return
        if self._clear_data:
            self.del_data('state')

        self._clear_data = False
        config = self.get_config() or {}
        config.update(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": self._onlyonce,
                "clear_data": False,
                "cron": self._cron,
                "downloaders": self._downloaders,
                "upload_limit_gb": self._upload_limit_gb,
                "upload_speed_kbps": self._upload_speed_kbps,
            }
        )
        self.update_config(config)

    def __ensure_service(self, service: Any) -> bool:
        instance = getattr(service, "instance", None)
        if not instance:
            return False

        try:
            if hasattr(instance, "is_inactive") and instance.is_inactive():
                logger.info(f"SmartLimiter 下载器 {service.name} 连接断开，尝试重连 ...")
                instance.reconnect()
        except Exception as e:
            logger.warning(f"SmartLimiter 下载器 {service.name} 重连失败：{str(e)}")

        try:
            if hasattr(instance, "is_inactive") and instance.is_inactive():
                return False
        except Exception:
            return False

        return True

    def __downloader_type(self, service: Any) -> str:
        downloader_type = getattr(service, "type", None) or getattr(
            getattr(service, "config", None), "type", ""
        )
        return (downloader_type or "").lower()

    def __is_supported_type(self, downloader_type: Optional[str]) -> bool:
        return (downloader_type or "").lower() in self.SUPPORTED_TYPES

    def __type_label(self, downloader_type: Optional[str]) -> str:
        mapping = {
            "qbittorrent": "Qbittorrent",
            "transmission": "Transmission",
        }
        return mapping.get((downloader_type or "").lower(), downloader_type or "-")

    def __normalize_downloaders(self, downloaders: Any) -> List[str]:
        if not downloaders:
            return []
        if isinstance(downloaders, str):
            downloaders = [downloaders]

        ret: List[str] = []
        for item in downloaders:
            name = str(item).strip()
            if not name or name in ret:
                continue
            ret.append(name)
        return ret

    def __limit_bytes(self) -> int:
        try:
            return max(0, int(float(self._upload_limit_gb) * 1024 ** 3))
        except Exception:
            return int(self.DEFAULT_LIMIT_GB * 1024 ** 3)

    @staticmethod
    def __safe_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def __safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default
