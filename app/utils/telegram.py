# -*- coding: utf-8 -*-
import base64
import logging
import os
from typing import Optional, List, Tuple

from aiogram import Bot
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup

log = logging.getLogger("utils.telegram")

# Built-in 1x1 PNG fallback in case assets/placeholder.png is missing
ONEPX_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO8p2cQAAAAASUVORK5CYII="


def load_placeholder_bytes(placeholder_path: Optional[str]) -> bytes:
    """
    Read placeholder bytes once on startup. Fallback to embedded 1x1 PNG if file missing or unreadable.
    """
    data: Optional[bytes] = None
    if placeholder_path and os.path.exists(placeholder_path):
        try:
            with open(placeholder_path, "rb") as f:
                data = f.read()
        except Exception as e:
            log.warning("Failed to read placeholder %s: %s", placeholder_path, e)
    if not data:
        data = base64.b64decode(ONEPX_PNG_B64)
    return data


def make_placeholder_inputfile(
    placeholder_path: Optional[str] = None,
    filename: str = "placeholder.png",
    data_bytes: Optional[bytes] = None,
) -> BufferedInputFile:
    """
    Build BufferedInputFile for placeholder.
    If data_bytes is provided, it will be used; otherwise read from placeholder_path or fallback to 1x1 PNG.
    """
    data: Optional[bytes] = data_bytes
    if data is None:
        if placeholder_path and os.path.exists(placeholder_path):
            try:
                with open(placeholder_path, "rb") as f:
                    data = f.read()
            except Exception as e:
                log.warning(f"Failed to read placeholder {placeholder_path}: {e}")
        if not data:
            # Hard fallback: built-in 1x1 PNG
            data = base64.b64decode(ONEPX_PNG_B64)
    return BufferedInputFile(file=data, filename=filename)


def make_image_inputfile(img_bytes: bytes, filename: str = "image.png") -> BufferedInputFile:
    return BufferedInputFile(file=img_bytes, filename=filename)


async def send_placeholder(
    bot: Bot,
    chat_id: int,
    thread_id: int,
    caption: str,
    placeholder_path: Optional[str] = None,
    placeholder_bytes: Optional[bytes] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> int:
    """
    Send a placeholder image to the given topic, return message_id.
    Uses cached placeholder_bytes if provided to avoid disk I/O.
    """
    ph = make_placeholder_inputfile(placeholder_path=placeholder_path, data_bytes=placeholder_bytes)
    m = await bot.send_photo(
        chat_id=chat_id,
        photo=ph,
        caption=caption,
        message_thread_id=thread_id,
        disable_notification=True,
        reply_to_message_id=reply_to_message_id,
        allow_sending_without_reply=True,
        reply_markup=reply_markup,
    )
    return m.message_id


async def edit_to_image(
    bot: Bot,
    chat_id: int,
    message_id: int,
    img_bytes: bytes,
    filename: str,
    caption: Optional[str],
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    Edit placeholder to an image result with original filename/extension.
    """
    from aiogram.types import InputMediaPhoto  # local import
    media = InputMediaPhoto(media=make_image_inputfile(img_bytes, filename), caption=caption or "")
    await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=media,
        reply_markup=reply_markup,
    )


async def edit_to_video(
    bot: Bot,
    chat_id: int,
    message_id: int,
    video_bytes: bytes,
    filename: str,
    caption: Optional[str],
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    Edit placeholder to a video result with original filename/extension.
    """
    from aiogram.types import InputMediaVideo  # local import
    media = InputMediaVideo(media=BufferedInputFile(video_bytes, filename=filename), caption=caption or "")
    await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=media,
        reply_markup=reply_markup,
    )


async def edit_to_audio(
    bot: Bot,
    chat_id: int,
    message_id: int,
    audio_bytes: bytes,
    filename: str,
    caption: Optional[str],
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    Edit placeholder to an audio file result with original filename/extension.
    """
    from aiogram.types import InputMediaAudio  # local import
    media = InputMediaAudio(media=BufferedInputFile(audio_bytes, filename=filename), caption=caption or "")
    await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=media,
        reply_markup=reply_markup,
    )


async def send_album(bot: Bot, chat_id: int, thread_id: int, images: List[Tuple[bytes, str]], caption: Optional[str] = None) -> None:
    """
    Send one or multiple images preserving original filenames/extensions.
    images: List of (bytes, filename)
    """
    from aiogram.types import InputMediaPhoto  # local import
    if not images:
        return
    MAX_GROUP = 10  # Telegram hard limit for one media group
    for start in range(0, len(images), MAX_GROUP):
        chunk = images[start:start + MAX_GROUP]
        media = []
        for i, (b, fn) in enumerate(chunk):
            cap = caption if (i == 0 and start == 0) else None
            media.append(InputMediaPhoto(media=make_image_inputfile(b, fn), caption=cap))
        await bot.send_media_group(
            chat_id=chat_id,
            media=media,
            message_thread_id=thread_id,
            disable_notification=True,
        )


async def send_videos(bot: Bot, chat_id: int, thread_id: int, videos: List[Tuple[bytes, str]], caption: Optional[str] = None) -> None:
    """
    Send one or multiple videos preserving original filenames/extensions.
    videos: List of (bytes, filename)
    """
    from aiogram.types import InputMediaVideo  # local import
    if not videos:
        return
    MAX_GROUP = 10  # Telegram hard limit for one media group

    # Single video — send as Video to allow caption
    if len(videos) == 1:
        vb, vfn = videos[0]
        await bot.send_video(
            chat_id=chat_id,
            video=BufferedInputFile(vb, filename=vfn),
            caption=caption,
            message_thread_id=thread_id,
            disable_notification=True,
        )
        return

    # Multiple videos — send via media groups (chunks of 10)
    for start in range(0, len(videos), MAX_GROUP):
        chunk = videos[start:start + MAX_GROUP]
        media = []
        for i, (b, fn) in enumerate(chunk):
            cap = caption if (i == 0 and start == 0) else None
            media.append(InputMediaVideo(media=BufferedInputFile(b, filename=fn), caption=cap))
        await bot.send_media_group(
            chat_id=chat_id,
            media=media,
            message_thread_id=thread_id,
            disable_notification=True,
        )


async def send_audios(bot: Bot, chat_id: int, thread_id: int, audios: List[Tuple[bytes, str]], caption: Optional[str] = None) -> None:
    """
    Send one or multiple audio files preserving original filenames/extensions.
    audios: List of (bytes, filename)
    """
    from aiogram.types import InputMediaAudio  # local import
    if not audios:
        return
    MAX_GROUP = 10

    if len(audios) == 1:
        ab, afn = audios[0]
        await bot.send_audio(
            chat_id=chat_id,
            audio=BufferedInputFile(ab, filename=afn),
            caption=caption,
            message_thread_id=thread_id,
            disable_notification=True,
        )
        return

    for start in range(0, len(audios), MAX_GROUP):
        chunk = audios[start:start + MAX_GROUP]
        media = []
        for i, (b, fn) in enumerate(chunk):
            cap = caption if (i == 0 and start == 0) else None
            media.append(InputMediaAudio(media=BufferedInputFile(b, filename=fn), caption=cap))
        await bot.send_media_group(
            chat_id=chat_id,
            media=media,
            message_thread_id=thread_id,
            disable_notification=True,
        )