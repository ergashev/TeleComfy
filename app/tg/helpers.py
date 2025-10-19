# -*- coding: utf-8 -*-
import io
import re
from typing import Any, Dict, Optional, Tuple

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.core.i18n import I18n
from app.domain.models import TopicConfig
from app.utils.images import get_image_size_from_bytes


def _parse_int_token(s: str) -> Optional[int]:
    """
    Extract first integer token from string s (tolerates trailing punctuation).
    Examples:
    - "1080," -> 1080
    - "1920." -> 1920
    - "'42'"  -> 42
    """
    if s is None:
        return None
    m = re.search(r"[-+]?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _parse_float_token(s: str) -> Optional[float]:
    """
    Extract first float token from string s (tolerates trailing punctuation).
    Also accepts comma as decimal separator (e.g., "16,5").
    Examples:
    - "16."   -> 16.0
    - "20,0," -> 20.0
    """
    if s is None:
        return None
    m = re.search(r"[-+]?\d+(?:[.,]\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except Exception:
        return None


def parse_inline_params(text: str) -> Tuple[str, Dict[str, Any]]:
    """
    Parse inline overrides "key=value" and optional long text blocks from the message.
    Supported scalar keys: steps, width, height, n, seed, model, fps, length
    Supported long text key: text
      - text can be provided as:
        a) text```<multiline text>```
        b) text: <text till end of message>
        c) text="<text with spaces>" or text='<text with spaces>'
    Returns (cleaned_prompt, params). 'cleaned_prompt' is the message text with technical parts removed.
    """
    params: Dict[str, Any] = {}
    working = text or ""

    # 1) Extract text as a fenced block: text``` ... ```
    m_code = re.search(r"(?is)\btext\s*```(.*?)```", working)
    text_val: Optional[str] = None
    if m_code:
        text_val = (m_code.group(1) or "").strip()
        # remove the block
        working = working[: m_code.start()] + working[m_code.end():]

    # 2) If not found, extract text as 'text: <till end>'
    if text_val is None:
        m_tail = re.search(r"(?is)\btext\s*:\s*(.+)$", working)
        if m_tail:
            text_val = (m_tail.group(1) or "").strip()
            # remove from the first char of 'text' till end
            working = working[: m_tail.start()]

    # 3) Parse key=value (with quotes) for supported keys, including text
    pattern = re.compile(
        r"(?P<key>steps|width|height|n|seed|model|fps|length|text)\s*=\s*(?P<val>\"[^\"]*\"|'[^']*'|[^\s]+)",
        re.IGNORECASE,
    )

    matches = list(pattern.finditer(working))
    for m in matches:
        k = m.group("key").lower()
        raw_v = m.group("val")
        # strip outer quotes if any
        if (raw_v.startswith('"') and raw_v.endswith('"')) or (raw_v.startswith("'") and raw_v.endswith("'")):
            v_clean = raw_v[1:-1]
        else:
            v_clean = raw_v

        if k in {"steps", "width", "height", "n", "seed"}:
            v_int = _parse_int_token(v_clean)
            if v_int is not None:
                params[k] = v_int
        elif k in {"fps", "length"}:
            v_float = _parse_float_token(v_clean)
            if v_float is not None:
                params[k] = v_float
        elif k in {"model", "text"}:
            params[k] = v_clean

    # Remove matched key=value fragments to produce cleaned prompt
    working = pattern.sub("", working)

    # Final cleanup: collapse extra spaces
    cleaned = working.strip()
    cleaned = re.sub(r"\s{2,}", " ", cleaned)

    if text_val is not None and text_val != "":
        params["text"] = text_val

    return cleaned, params


def merge_params(topic_cfg: TopicConfig, inline_params: Dict[str, Any], *, input_dims: Optional[Tuple[int, int]] = None) -> Dict[str, Any]:
    """
    Merge parameters in correct precedence with support of defaults width/height inheritance:
    nodes.defaults -> meta.defaults -> inherit from input (if default is 0) -> inline overrides (with enforcement):
    - If topic_cfg.inline_allowed is provided, only these inline keys are accepted;
    - If topic_cfg.inline_limits is provided, clamp numeric values to [min, max].
    - Special case for width/height:
      * If any default dimension equals 0 and input_dims are provided, the corresponding dimension is taken from input_dims.
      * Inline values still have priority over inherited defaults.
      * After values are decided, width/height are proportionally adjusted to satisfy inline_limits.
    """
    # 0) Base params (nodes.defaults -> meta.defaults)
    params: Dict[str, Any] = {}
    if topic_cfg.nodes_map and topic_cfg.nodes_map.defaults:
        params.update(topic_cfg.nodes_map.defaults)
    if topic_cfg.defaults:
        params.update(topic_cfg.defaults)

    # 0.1) Inheritance from input content when default dimension equals 0
    if input_dims is not None:
        iw, ih = input_dims
        if isinstance(params.get("width"), int) and params.get("width") == 0:
            params["width"] = int(iw)
        if isinstance(params.get("height"), int) and params.get("height") == 0:
            params["height"] = int(ih)

    # 1) Filter inline params by allowed list (if specified)
    allowed = topic_cfg.inline_allowed
    filtered_inline: Dict[str, Any] = {}
    for k, v in inline_params.items():
        key_norm = k.lower()
        if allowed is None or key_norm in allowed:
            filtered_inline[key_norm] = v

    # 2) Helpers for limits and clamping (numeric only)
    limits = topic_cfg.inline_limits or {}

    def _clamp_value(key: str, value: Any) -> Any:
        lim = limits.get(key)
        if not isinstance(lim, dict):
            return value
        if not isinstance(value, (int, float)):
            return value
        v = float(value)
        min_v = lim.get("min", None)
        max_v = lim.get("max", None)
        try:
            if isinstance(min_v, (int, float)):
                v = max(v, float(min_v))
            if isinstance(max_v, (int, float)):
                v = min(v, float(max_v))
        except Exception:
            return value
        return int(v) if isinstance(value, int) else v

    # 3) First, apply inline overrides for all keys except width/height (with single-value clamp)
    result: Dict[str, Any] = dict(params)
    for k, v in filtered_inline.items():
        if k in ("width", "height"):
            continue
        result[k] = _clamp_value(k, v)

    # 4) Width/Height proportional handling
    w_cur = result.get("width")
    h_cur = result.get("height")

    if "width" in filtered_inline:
        w_cur = filtered_inline["width"]
    if "height" in filtered_inline:
        h_cur = filtered_inline["height"]

    if isinstance(w_cur, (int, float)) and isinstance(h_cur, (int, float)):
        w0 = float(w_cur)
        h0 = float(h_cur)

        # Individual preliminary clamps
        w_pre = _clamp_value("width", w0)
        h_pre = _clamp_value("height", h0)
        w_pre_f = float(w_pre) if isinstance(w_pre, (int, float)) else w0
        h_pre_f = float(h_pre) if isinstance(h_pre, (int, float)) else h0

        scales = []
        if w_pre_f != w0 and w0 != 0:
            scales.append(w_pre_f / w0)
        if h_pre_f != h0 and h0 != 0:
            scales.append(h_pre_f / h0)

        if scales:
            downs = [s for s in scales if s < 1.0]
            ups = [s for s in scales if s > 1.0]
            if downs:
                scale = min(downs)
            elif ups:
                scale = max(ups)
            else:
                scale = 1.0

            w_scaled = w0 * scale
            h_scaled = h0 * scale

            w_final = _clamp_value("width", int(round(w_scaled)))
            h_final = _clamp_value("height", int(round(h_scaled)))
        else:
            w_final = _clamp_value("width", int(round(w0)) if isinstance(w_cur, int) else w_cur)
            h_final = _clamp_value("height", int(round(h0)) if isinstance(h_cur, int) else h_cur)

        if isinstance(w_final, float):
            w_final = int(round(w_final))
        if isinstance(h_final, float):
            h_final = int(round(h_final))
        result["width"] = w_final
        result["height"] = h_final
    else:
        if isinstance(w_cur, (int, float)):
            w_final = _clamp_value("width", w_cur)
            result["width"] = int(round(w_final)) if isinstance(w_final, (int, float)) else w_cur
        if isinstance(h_cur, (int, float)):
            h_final = _clamp_value("height", h_cur)
            result["height"] = int(round(h_final)) if isinstance(h_final, (int, float)) else h_cur

    return result


def _fmt_duration(i18n: I18n, sec: float) -> str:
    """
    Human-readable duration formatting with i18n:
    - < 60 sec: seconds
    - < 3600 sec: minutes and seconds
    - >= 3600 sec: hours and minutes
    """
    s = max(0.0, float(sec))
    if s < 60:
        if s < 10:
            return f"{s:.1f} {i18n.t('duration_second_short')}"
        return f"{int(round(s))} {i18n.t('duration_second_short')}"
    m = int(s // 60)
    rem_s = int(round(s - m * 60))
    if s < 3600:
        if rem_s and rem_s < 60:
            return i18n.t("duration_minutes_seconds_format", m=m, s=rem_s)
        return f"{m} {i18n.t('duration_minute_short')}"
    h = int(s // 3600)
    rem_m = int((s - h * 3600) // 60)
    if rem_m:
        return i18n.t("duration_hours_minutes_format", h=h, m=rem_m)
    return f"{h} {i18n.t('duration_hour_short')}"


def build_caption(i18n: I18n, prompt: str, bot_queue_s: float, comfy_queue_s: float, comfy_exec_s: float) -> str:
    """
    Build result caption with queue/generation timings.
    """
    total_queue = max(0.0, (bot_queue_s or 0.0) + (comfy_queue_s or 0.0))
    queue_str = _fmt_duration(i18n, total_queue)
    gen_str = _fmt_duration(i18n, comfy_exec_s)
    return i18n.t("caption_format", prompt=prompt, queue=queue_str, generation=gen_str)


def make_regen_kb(i18n: I18n, *, src_start_id: Optional[int] = None, src_count: Optional[int] = None) -> InlineKeyboardMarkup:
    """
    Build "Regenerate" inline keyboard.
    If src_start_id and src_count are provided, encode them into callback data as 'regen:<start>:<count>'.
    """
    if isinstance(src_start_id, int) and isinstance(src_count, int) and src_start_id > 0 and src_count > 0:
        cb = f"regen:{src_start_id}:{src_count}"
    else:
        cb = "regen"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=i18n.t("button_regen"), callback_data=cb)]]
    )


def make_cancel_kb(i18n: I18n) -> InlineKeyboardMarkup:
    """
    Build "Cancel" inline keyboard for queued tasks.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=i18n.t("button_cancel"), callback_data="cancel")]]
    )


async def download_attached_image(bot: Bot, message: Message) -> Optional[Tuple[bytes, str]]:
    """
    Download an image from a message (photo or document image/*).
    Returns (bytes, filename) or None if no image found.
    """
    # photo
    if message.photo:
        ps = message.photo[-1]
        try:
            f = await bot.get_file(ps.file_id)
            file_path = getattr(f, "file_path", None)
            if not file_path:
                return None
            buf = io.BytesIO()
            await bot.download_file(file_path, buf)
            ext = ".jpg"  # Telegram photo is typically jpeg
            fname = f"tg_{ps.file_unique_id}{ext}"
            return buf.getvalue(), fname
        except Exception:
            return None

    # document (image/*)
    if message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
        try:
            f = await bot.get_file(message.document.file_id)
            file_path = getattr(f, "file_path", None)
            if not file_path:
                return None
            buf = io.BytesIO()
            await bot.download_file(file_path, buf)
            fname = message.document.file_name or f"tg_{message.document.file_unique_id}.img"
            return buf.getvalue(), fname
        except Exception:
            return None

    return None


async def probe_attached_image_size(bot: Bot, message: Message) -> Optional[Tuple[int, int]]:
    """
    Determine attached image dimensions:
    - For photos: use PhotoSize (no download).
    - For document image/*: download bytes and parse dimensions via headers (PNG/JPEG/WEBP).
    Returns (width, height) or None if not found or not an image.
    """
    # photo — use Telegram-provided sizes
    if message.photo:
        ps = message.photo[-1]
        try:
            return int(ps.width), int(ps.height)
        except Exception:
            return None

    # document (image/*) — need to download to parse headers
    if message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
        try:
            f = await bot.get_file(message.document.file_id)
            file_path = getattr(f, "file_path", None)
            if not file_path:
                return None
            buf = io.BytesIO()
            await bot.download_file(file_path, buf)
            data = buf.getvalue()
            fname = message.document.file_name or ""
            mime = message.document.mime_type or None
            wh = get_image_size_from_bytes(data, filename=fname, mime=mime)
            return wh
        except Exception:
            return None

    return None