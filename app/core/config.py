# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from dotenv import load_dotenv
import yaml


@dataclass
class AppConfig:
    telegram_token: str
    allowed_chat_id: int
    comfy_base_url: str
    comfy_api_key: Optional[str]
    workdir: str
    state_dir: str
    placeholder_path: Optional[str]
    limits_max_workers: int
    limits_per_topic: int
    # New: per-user pending tasks limit (waiting in queue, not started). 0 or negative -> disabled
    limits_per_user_pending: int
    timeout_ws: int
    timeout_run: int
    # i18n settings
    locale: str
    locales_dir: str
    # optional path to config yaml
    config_yaml_path: Optional[str] = None


def _get_int(env_name: str, default: int) -> int:
    """
    Read integer from environment or return default.
    """
    val = os.getenv(env_name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _load_yaml_config(path: str) -> Dict[str, Any]:
    """
    Load YAML config if path exists. Always returns a dict.
    """
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _dot_get(d: Mapping[str, Any], path: str) -> Any:
    """
    Safe dot-path getter for nested dictionaries.
    Returns None when the path is missing.
    """
    cur: Any = d
    for p in path.split("."):
        if not isinstance(cur, Mapping) or p not in cur:  # type: ignore[operator]
            return None
        cur = cur[p]  # type: ignore[index]
    return cur


def _yget_str(yml: Mapping[str, Any], path: str, default: str = "") -> str:
    """
    Get string value from YAML with fallback.
    If the value is not a string, tries to coerce simple primitives to str.
    """
    v = _dot_get(yml, path)
    if v is None:
        return default
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float, bool)):
        return str(v)
    return default


def _yget_int(yml: Mapping[str, Any], path: str, default: int) -> int:
    """
    Get int value from YAML with fallback.
    Accepts int or string that can be parsed to int.
    """
    v = _dot_get(yml, path)
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        try:
            return int(v)
        except ValueError:
            return default
    return default


def load_config() -> AppConfig:
    load_dotenv()

    # Optional config.yml overlay
    config_yaml_path_raw = os.getenv("CONFIG_YAML") or ""
    config_yaml_path = config_yaml_path_raw.strip()
    yml = _load_yaml_config(config_yaml_path)

    # Telegram settings
    telegram_token_env = os.getenv("TELEGRAM_TOKEN")
    telegram_token = telegram_token_env if (telegram_token_env is not None and telegram_token_env != "") else _yget_str(yml, "telegram.token", "")

    allowed_chat_id_env = os.getenv("ALLOWED_CHAT_ID")
    if allowed_chat_id_env is not None and allowed_chat_id_env != "":
        try:
            allowed_chat_id = int(allowed_chat_id_env)
        except ValueError:
            allowed_chat_id = 0
    else:
        allowed_chat_id = _yget_int(yml, "telegram.allowed_chat_id", 0)

    # Comfy settings
    comfy_base_url_env = os.getenv("COMFY_BASE_URL")
    comfy_base_url = comfy_base_url_env if (comfy_base_url_env is not None and comfy_base_url_env != "") else _yget_str(yml, "comfy.base_url", "")

    comfy_api_key_env = os.getenv("COMFY_API_KEY")
    comfy_api_key_val = comfy_api_key_env if (comfy_api_key_env is not None and comfy_api_key_env != "") else _yget_str(yml, "comfy.api_key", "")
    comfy_api_key: Optional[str] = comfy_api_key_val if comfy_api_key_val else None

    # Paths
    workdir = os.getenv("WORKDIR") or _yget_str(yml, "paths.workdir", "data/topics")
    state_dir = os.getenv("STATE_DIR") or _yget_str(yml, "paths.state_dir", "state")
    placeholder_path = os.getenv("PLACEHOLDER_PATH") or _yget_str(yml, "paths.placeholder_path", "assets/placeholder.png")

    # Limits
    limits_max_workers = _get_int("LIMITS_MAX_WORKERS", _yget_int(yml, "limits.max_workers", 2))
    limits_per_topic = _get_int("LIMITS_PER_TOPIC", _yget_int(yml, "limits.per_topic", 1))
    limits_per_user_pending = _get_int("LIMITS_PER_USER_PENDING", _yget_int(yml, "limits.per_user_pending", 3))

    # Timeouts
    timeout_ws = _get_int("TIMEOUT_WS", _yget_int(yml, "timeouts.ws", 120))
    timeout_run = _get_int("TIMEOUT_RUN", _yget_int(yml, "timeouts.run", 300))

    # i18n
    locale_raw = os.getenv("LOCALE") or _yget_str(yml, "i18n.locale", "en")
    locale = locale_raw.strip().lower()
    if locale not in ("ru", "en", "zh"):
        locale = "en"
    locales_dir = os.getenv("LOCALES_DIR") or _yget_str(yml, "i18n.dir", "locales")

    # Required fields validation
    if not telegram_token:
        raise RuntimeError("TELEGRAM_TOKEN is required")
    if not allowed_chat_id:
        raise RuntimeError("ALLOWED_CHAT_ID is required")
    if not comfy_base_url:
        raise RuntimeError("COMFY_BASE_URL is required")

    os.makedirs(state_dir, exist_ok=True)

    return AppConfig(
        telegram_token=telegram_token,
        allowed_chat_id=allowed_chat_id,
        comfy_base_url=comfy_base_url,
        comfy_api_key=comfy_api_key,
        workdir=workdir,
        state_dir=state_dir,
        placeholder_path=placeholder_path,
        limits_max_workers=limits_max_workers,
        limits_per_topic=limits_per_topic,
        limits_per_user_pending=limits_per_user_pending,
        timeout_ws=timeout_ws,
        timeout_run=timeout_run,
        locale=locale,
        locales_dir=locales_dir,
        config_yaml_path=config_yaml_path or None,
    )