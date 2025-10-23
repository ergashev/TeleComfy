# -*- coding: utf-8 -*-
import asyncio
import logging
import time
import uuid
from typing import Any, Dict, Optional, Tuple, List, DefaultDict
from collections import defaultdict

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app.comfy.client import ComfyClient, ComfyError
from app.core.config import AppConfig
from app.core.i18n import I18n
from app.domain.models import GenerateJob, TopicConfig
from app.infra.jobs_queue import JobsQueue
from app.infra.topics_repo import TopicsRepository
from app.utils.telegram import (
    send_placeholder,
    edit_to_image,
    edit_to_video,
    send_album,
    send_videos,
    edit_to_audio,
    send_audios,
    load_placeholder_bytes,
)
from app.tg.helpers import (
    parse_inline_params,
    merge_params,
    download_attached_image,
    build_caption,
    make_regen_kb,
    make_cancel_kb,
    probe_attached_image_size,
)

log = logging.getLogger("tg.bot")

REGEN_CB = "regen"
CANCEL_CB = "cancel"

async def create_bot_app(
    config: AppConfig,
    comfy_client: ComfyClient,
    topics_repo: TopicsRepository,
    jobs_queue: JobsQueue,
    i18n: I18n,
) -> Tuple[Dispatcher, Bot]:
    """
    Create and configure aiogram Dispatcher and Bot, wire handlers.
    """
    bot = Bot(token=config.telegram_token, default=DefaultBotProperties(parse_mode=None))
    dp = Dispatcher()

    # Cached placeholder bytes (avoid disk I/O for each generation)
    placeholder_bytes: bytes = load_placeholder_bytes(config.placeholder_path)

    # Processor for jobs_queue
    jobs_queue.set_processor(
        lambda job: _process_generate_job(config, bot, comfy_client, topics_repo, i18n, job)
    )

    # Media group collector (for topics requiring multiple input images) + thread-safety lock
    media_group_buffers: DefaultDict[str, List[Message]] = defaultdict(list)
    media_group_tasks: Dict[str, asyncio.Task] = {}
    media_group_lock = asyncio.Lock()

    async def _finish_media_group_after_delay(group_id: str) -> None:
        """
        Debounced finisher: waits a short time, then processes accumulated media group.
        Access to buffers is protected with a lock to avoid races/memory leaks.
        """
        try:
            await asyncio.sleep(0.9)  # slightly increased debounce
            # Snapshot and clear under lock
            async with media_group_lock:
                msgs = media_group_buffers.pop(group_id, [])
                media_group_tasks.pop(group_id, None)
            if not msgs:
                return
            # Sort by message_id ascending to ensure the first is the original with caption
            msgs.sort(key=lambda m: m.message_id)

            ref = msgs[0]
            if ref.chat is None:
                return
            if ref.chat.id != config.allowed_chat_id:
                return
            thread_id = ref.message_thread_id
            if thread_id is None:
                return

            topic_cfg: Optional[TopicConfig] = topics_repo.resolve_by_thread_id(thread_id)
            if not topic_cfg:
                try:
                    await ref.reply(i18n.t("topic_not_configured_run_scan"))
                except Exception:
                    pass
                return

            # Only process media groups for topics that require multiple input images
            needs_multi = any(r.type == "input_images" for r in topic_cfg.nodes_map.nodes)
            if not needs_multi:
                return

            await _handle_messages_group(topic_cfg, msgs, reply_to_override=None, source_ids_override=None)
        except Exception as e:
            log.exception("Media group finishing failed: %s", e)
        finally:
            # Ensure cleanup
            async with media_group_lock:
                media_group_buffers.pop(group_id, None)
                media_group_tasks.pop(group_id, None)

    async def _handle_messages_group(
        topic_cfg: TopicConfig,
        msgs: List[Message],
        *,
        reply_to_override: Optional[int] = None,
        source_ids_override: Optional[List[int]] = None,
        silent_on_limit: bool = False,
        acting_user_id: Optional[int] = None,
    ) -> bool:
        """
        Process a list of messages (media group or single) for generation.
        reply_to_override: if provided, use this message_id as reply target for placeholder/result.
        source_ids_override: if provided, use these as source message ids for regen button encoding.
        silent_on_limit: if True, do not send a chat message when user limit is hit (used for callback flows).
        acting_user_id: override user_id to attribute pending to the real initiator (e.g., callback user).
        Returns True if a job was enqueued, False otherwise.
        """
        if not msgs:
            return False

        # Ensure deterministic order for media groups
        if len(msgs) > 1:
            msgs.sort(key=lambda m: m.message_id)

        ref = msgs[0]
        if ref.chat is None:
            return False
        chat_id = ref.chat.id
        thread_id = ref.message_thread_id
        if thread_id is None:
            return False

        # Compose text (take first non-empty caption/text, respecting message order)
        text = ""
        for m in msgs:
            t = (m.caption or m.text or "").strip()
            if t:
                text = t
                break

        # If no text at all and no images — bail out
        has_any_image = any([
            bool(m.photo) or (m.document and m.document.mime_type and m.document.mime_type.startswith("image/"))
            for m in msgs
        ])
        if not text and not has_any_image:
            try:
                await ref.reply(i18n.t("prompt_or_image_required"))
            except Exception:
                pass
            return False

        # Detect input dimensions (use first message with an image)
        input_dims: Optional[Tuple[int, int]] = None
        for m in msgs:
            input_dims = await probe_attached_image_size(bot, m)
            if input_dims:
                break

        prompt, inline_params = parse_inline_params(text or "")
        params: Dict[str, Any] = merge_params(topic_cfg, inline_params, input_dims=input_dims)

        # Gather images (max 10)
        images: List[Tuple[bytes, str]] = []
        for m in msgs:
            pair = await download_attached_image(bot, m)
            if pair:
                images.append(pair)
            if len(images) >= 10:
                break

        needs_multi = any(r.type == "input_images" for r in topic_cfg.nodes_map.nodes)
        needs_single = any(r.type == "input_image" for r in topic_cfg.nodes_map.nodes)

        # Determine reply target
        reply_target_id: Optional[int] = reply_to_override if reply_to_override is not None else ref.message_id

        # Build source ids list for regen (sorted)
        source_ids: List[int] = [m.message_id for m in msgs]
        if source_ids_override is not None:
            source_ids = list(source_ids_override)

        # Attribute job to the real initiator if provided; else:
        # - If message is authored "as channel/group" (anonymous admin) — disable per-user limit (user_id=0);
        # - Otherwise use explicit from_user.
        if acting_user_id is not None:
            user_id_for_limits = acting_user_id
        else:
            is_anon_like = bool(ref.sender_chat and ref.chat and ref.sender_chat.id == ref.chat.id)
            if is_anon_like:
                user_id_for_limits = 0
            else:
                user_id_for_limits = ref.from_user.id if ref.from_user else 0

        if needs_multi:
            if not images:
                try:
                    await ref.reply(i18n.t("requires_input_image"))
                except Exception:
                    pass
                return False
            return await _send_and_enqueue(
                topic_cfg=topic_cfg,
                chat_id=chat_id,
                thread_id=thread_id,
                reply_to_message_id=reply_target_id,
                user_id=user_id_for_limits,
                prompt=prompt,
                params=params,
                input_img=None,
                input_images=images,
                source_message_ids=source_ids,
                ref_message=ref,
                placeholder_bytes=placeholder_bytes,
                silent_on_limit=silent_on_limit,
            )

        # Fallback: single-image flow
        input_img: Optional[Tuple[bytes, str]] = None
        if needs_single:
            if images:
                input_img = images[0]
            else:
                # If single image required but none present
                try:
                    await ref.reply(i18n.t("requires_input_image"))
                except Exception:
                    pass
                return False

        return await _send_and_enqueue(
            topic_cfg=topic_cfg,
            chat_id=chat_id,
            thread_id=thread_id,
            reply_to_message_id=reply_target_id,
            user_id=user_id_for_limits,
            prompt=prompt,
            params=params,
            input_img=input_img,
            input_images=None,
            source_message_ids=source_ids,
            ref_message=ref,
            placeholder_bytes=placeholder_bytes,
            silent_on_limit=silent_on_limit,
        )

    async def _send_and_enqueue(
        *,
        topic_cfg: TopicConfig,
        chat_id: int,
        thread_id: int,
        reply_to_message_id: Optional[int],
        user_id: int,
        prompt: str,
        params: Dict[str, Any],
        input_img: Optional[Tuple[bytes, str]],
        input_images: Optional[List[Tuple[bytes, str]]] = None,
        source_message_ids: Optional[List[int]] = None,
        ref_message: Optional[Message] = None,
        placeholder_bytes: Optional[bytes] = None,
        silent_on_limit: bool = False,
    ) -> bool:
        """
        Common helper:
        - checks global per-user pending limit (backpressure) BEFORE sending a placeholder
        - optionally reserves pending slot to avoid races
        - decides queueing state
        - sends placeholder (if allowed)
        - builds GenerateJob and enqueues (with 'reserved' flag)
        Returns True if enqueued, False otherwise.
        """
        per_user_limit = config.limits_per_user_pending

        # Check limit before sending placeholder
        if isinstance(per_user_limit, int) and per_user_limit > 0 and user_id > 0:
            allowed = await jobs_queue.can_enqueue(user_id, per_user_limit)
            if not allowed:
                if not silent_on_limit:
                    # Send plain text (no placeholder image)
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=i18n.t(
                                "user_pending_limit_reached",
                                count=jobs_queue.pending_count_by_user(user_id),
                                limit=per_user_limit,
                            ),
                            message_thread_id=thread_id,
                            reply_to_message_id=reply_to_message_id,
                            disable_notification=True,
                        )
                    except Exception:
                        pass
                return False
            # Try to reserve a slot (to be safer from race)
            reserved_ok = await jobs_queue.reserve_user_slot(user_id, per_user_limit)
            if not reserved_ok:
                if not silent_on_limit:
                    # Race: someone enqueued in between — still send text without placeholder
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=i18n.t(
                                "user_pending_limit_reached",
                                count=jobs_queue.pending_count_by_user(user_id),
                                limit=per_user_limit,
                            ),
                            message_thread_id=thread_id,
                            reply_to_message_id=reply_to_message_id,
                            disable_notification=True,
                        )
                    except Exception:
                        pass
                return False
            reserved = True
        else:
            reserved = False

        try:
            # Estimate whether we will wait
            will_wait = jobs_queue.will_queue(topic_cfg.alias)

            ph_caption = i18n.t("ph_waiting") if will_wait else i18n.t("ph_generating")
            ph_markup = make_cancel_kb(i18n) if will_wait else None

            # Send placeholder (only when not blocked by limit)
            placeholder_msg_id = await send_placeholder(
                bot=bot,
                chat_id=chat_id,
                thread_id=thread_id,
                caption=ph_caption,
                placeholder_path=config.placeholder_path,
                placeholder_bytes=placeholder_bytes,
                reply_to_message_id=reply_to_message_id,
                reply_markup=ph_markup,
            )
        except Exception:
            # Rollback reservation on failure to send placeholder
            if reserved and user_id > 0:
                await jobs_queue.release_user_slot(user_id)
            return False

        placeholder_ts = time.time()
        correlation_id = str(uuid.uuid4())[:8]

        job = GenerateJob(
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=placeholder_msg_id,
            prompt=prompt,
            user_id=user_id,
            params=params,
            topic_alias=topic_cfg.alias,
            correlation_id=correlation_id,
            input_image_bytes=(input_img[0] if input_img else None),
            input_image_filename=(input_img[1] if input_img else None),
            input_images=input_images,
            placeholder_ts=placeholder_ts,
            initially_waiting=will_wait,
            source_message_ids=(list(source_message_ids) if source_message_ids else None),
        )

        # Enqueue using reserved flag; if fails (unlikely), release reservation and update caption
        ok = await jobs_queue.enqueue_reserved(topic_cfg.alias, job, reserved=reserved)
        if not ok:
            if reserved and user_id > 0:
                await jobs_queue.release_user_slot(user_id)
            try:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=placeholder_msg_id,
                    caption=i18n.t(
                        "user_pending_limit_reached",
                        count=jobs_queue.pending_count_by_user(user_id),
                        limit=per_user_limit if isinstance(per_user_limit, int) and per_user_limit > 0 else 0,
                    ),
                    reply_markup=None,
                )
            except Exception:
                pass
            return False

        return True

    @dp.message(Command("topic_scan"))
    async def topic_scan(message: Message) -> None:
        if message.chat is None or message.chat.id != config.allowed_chat_id:
            return

        # Robust anonymous admin detection:
        # "Anonymous admin as group" -> sender_chat is the same as the chat itself.
        is_anon_like = bool(message.sender_chat and message.chat and message.sender_chat.id == message.chat.id)

        is_admin = False
        if is_anon_like:
            # Treat as admin (no user id to check)
            is_admin = True
        elif message.from_user:
            try:
                member = await bot.get_chat_member(message.chat.id, message.from_user.id)
                is_admin = member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
            except Exception:
                await message.reply(i18n.t("err_check_admin_rights"))
                return

        if not is_admin:
            await message.reply(i18n.t("err_admins_only"))
            return

        await message.reply(i18n.t("scan_start"))
        created, updated, deleted = await topics_repo.scan_and_sync(bot, message.chat.id)
        await topics_repo.reload_cache()
        await message.reply(
            i18n.t(
                "scan_summary",
                created=created,
                updated=updated,
                deleted=deleted,
                active=len(topics_repo.all_topics()),
            )
        )

    @dp.message(
        F.chat.type.in_({ChatType.SUPERGROUP}),
        F.message_thread_id,  # only messages in topics
    )
    async def on_topic_message(message: Message) -> None:
        if message.chat is None or message.chat.id != config.allowed_chat_id:
            return
        thread_id = message.message_thread_id
        if thread_id is None:
            return

        # Recognize "anonymous admin as group"
        is_anon_like = bool(message.sender_chat and message.chat and message.sender_chat.id == message.chat.id)

        # 1) Drop any "send as channel" or foreign sender_chat (channel-authored) message
        if message.sender_chat and not is_anon_like:
            # This is a message authored as some channel (or another chat) -> do not process
            return

        # 2) Drop bot-authored messages except the "anonymous admin as group" case
        if message.from_user and message.from_user.is_bot and not is_anon_like:
            return

        topic_cfg: Optional[TopicConfig] = topics_repo.resolve_by_thread_id(thread_id)
        if not topic_cfg:
            # Not associated, ask admin to run /topic_scan
            await message.reply(i18n.t("topic_not_configured_run_scan"))
            return

        # If topic supports multi-image input and this message is a media group — collect and defer
        needs_multi = any(r.type == "input_images" for r in topic_cfg.nodes_map.nodes)
        if needs_multi and message.media_group_id:
            gid = str(message.media_group_id)
            async with media_group_lock:
                media_group_buffers[gid].append(message)
                # If task doesn't exist or completed — create new
                t = media_group_tasks.get(gid)
                if t is None or t.done():
                    media_group_tasks[gid] = asyncio.create_task(_finish_media_group_after_delay(gid))
            return

        # Single message processing (or topics not requiring multi)
        await _handle_messages_group(topic_cfg, [message], reply_to_override=None, source_ids_override=None)

    @dp.callback_query(F.data.startswith(REGEN_CB))
    async def on_regen(cb: CallbackQuery) -> None:
        msg_union = cb.message
        if msg_union is None:
            await cb.answer(i18n.t("msg_not_found"), show_alert=True)
            return
        if not isinstance(msg_union, Message):
            await cb.answer(i18n.t("msg_not_found"), show_alert=True)
            return
        msg: Message = msg_union

        if msg.chat is None or msg.chat.id != config.allowed_chat_id:
            await cb.answer(i18n.t("not_available_in_this_chat"), show_alert=True)
            return
        thread_id = msg.message_thread_id
        if not thread_id:
            await cb.answer(i18n.t("button_only_in_topics"), show_alert=True)
            return

        # Debounce: require >= 5 seconds since last edit (or original date)
        try:
            last_dt = getattr(msg, "edit_date", None) or getattr(msg, "date", None)
            if last_dt:
                ts = float(getattr(last_dt, "timestamp")() if callable(getattr(last_dt, "timestamp", None)) else last_dt)
                if time.time() - ts < 5.0:
                    await cb.answer(i18n.t("wait_5_seconds_before_regen"), show_alert=True)
                    return
        except Exception:
            # Non-critical, continue if any error occurs
            pass

        topic_cfg: Optional[TopicConfig] = topics_repo.resolve_by_thread_id(thread_id)
        if not topic_cfg:
            await cb.answer(i18n.t("topic_not_configured_run_scan"), show_alert=True)
            return

        # Additional per-user pending limit check for regen button (prevents bypass via album forwarding)
        per_user_limit = config.limits_per_user_pending
        user_id = cb.from_user.id if cb.from_user else 0
        if isinstance(per_user_limit, int) and per_user_limit > 0 and user_id > 0:
            allowed = await jobs_queue.can_enqueue(user_id, per_user_limit)
            if not allowed:
                await cb.answer(
                    i18n.t(
                        "user_pending_limit_reached",
                        count=jobs_queue.pending_count_by_user(user_id),
                        limit=per_user_limit,
                    ),
                    show_alert=True,
                )
                return

        # Parse enhanced callback data: "regen[:start[:count]]"
        data = cb.data or REGEN_CB
        parts = data.split(":")
        src_start: Optional[int] = None
        src_count: Optional[int] = None
        if len(parts) == 3 and parts[0] == REGEN_CB:
            try:
                src_start = int(parts[1])
                src_count = int(parts[2])
            except Exception:
                src_start = None
                src_count = None

        # Forward-and-process flow only for albums (count > 1)
        if src_start and src_count and src_start > 0 and src_count > 1:
            # Forward all messages [start .. start+count-1] to this same chat/thread (silently)
            new_msgs: List[Message] = []
            for mid in range(src_start, src_start + src_count):
                try:
                    nm = await bot.forward_message(
                        chat_id=msg.chat.id,
                        from_chat_id=msg.chat.id,
                        message_id=mid,
                        message_thread_id=thread_id,
                        disable_notification=True,
                        protect_content=False,
                    )
                    new_msgs.append(nm)
                except Exception as e:
                    log.debug("Forward failed for message_id=%s: %s", mid, e)
                    # if any forward fails, continue with what we have

            if not new_msgs:
                await cb.answer(i18n.t("msg_not_found"), show_alert=True)
                return

            # Process forwarded messages with reply pointing to the original start message id
            # and encode original ids (not forwarded) into the job for future regen button.
            try:
                orig_ids = list(range(src_start, src_start + src_count))
                success = await _handle_messages_group(
                    topic_cfg,
                    new_msgs,
                    reply_to_override=src_start,
                    source_ids_override=orig_ids,
                    silent_on_limit=True,            # do not spam chat on limit from callback flow
                    acting_user_id=user_id,          # attribute pending to the user who clicked the button
                )
            finally:
                # Delete forwarded messages to keep thread clean
                for nm in new_msgs:
                    try:
                        await bot.delete_message(chat_id=nm.chat.id, message_id=nm.message_id)
                    except Exception:
                        pass

            if success:
                await cb.answer(i18n.t("regen_started"), show_alert=False)
            else:
                # In case of race (limit exceeded after pre-check) show popup (no chat spam)
                await cb.answer(
                    i18n.t(
                        "user_pending_limit_reached",
                        count=jobs_queue.pending_count_by_user(user_id),
                        limit=per_user_limit if isinstance(per_user_limit, int) else 0,
                    ),
                    show_alert=True,
                )
            return

        # Backward-compatible fallback (single image or pure text, or old buttons):
        # Use original message (the one the result is replying to) as source
        origin_union = msg.reply_to_message
        origin: Optional[Message] = origin_union if isinstance(origin_union, Message) else None

        # Prompt text is taken from the original message; fallback to current message text/caption
        src_text = ""
        if origin:
            src_text = (origin.caption or origin.text or "").strip()
        else:
            src_text = (msg.caption or msg.text or "").strip()

        # Detect input dimensions from the origin message (if any)
        input_dims: Optional[Tuple[int, int]] = None
        if origin:
            input_dims = await probe_attached_image_size(bot, origin)

        prompt, inline_params = parse_inline_params(src_text or "")
        params: Dict[str, Any] = merge_params(topic_cfg, inline_params, input_dims=input_dims)

        # If the topic requires input image(s) — try to take them from the origin message (single only)
        needs_input_images = any(r.type == "input_images" for r in topic_cfg.nodes_map.nodes)
        needs_input_image = any(r.type == "input_image" for r in topic_cfg.nodes_map.nodes)

        input_img: Optional[Tuple[bytes, str]] = None
        input_images: Optional[List[Tuple[bytes, str]]] = None

        if needs_input_images:
            # We can't restore whole album here (no ids embedded) and count<=1 (not an album) — try single image fallback
            if origin:
                pair = await download_attached_image(bot, origin)
                if pair:
                    input_images = [pair]
            if not input_images:
                await cb.answer(i18n.t("requires_input_image"), show_alert=True)
                return
        elif needs_input_image:
            if origin:
                input_img = await download_attached_image(bot, origin)
            if not input_img:
                await cb.answer(i18n.t("requires_input_image"), show_alert=True)
                return

        success_single = await _send_and_enqueue(
            topic_cfg=topic_cfg,
            chat_id=msg.chat.id,
            thread_id=thread_id,
            reply_to_message_id=(origin.message_id if origin else None),
            user_id=user_id,
            prompt=prompt,
            params=params,
            input_img=input_img,
            input_images=input_images,
            source_message_ids=([origin.message_id] if origin else None),
            ref_message=origin if origin else msg,
            placeholder_bytes=placeholder_bytes,
            silent_on_limit=True,  # do not spam chat from callback
        )
        if success_single:
            await cb.answer(i18n.t("regen_started"), show_alert=False)
        else:
            await cb.answer(
                i18n.t(
                    "user_pending_limit_reached",
                    count=jobs_queue.pending_count_by_user(user_id),
                    limit=per_user_limit if isinstance(per_user_limit, int) else 0,
                ),
                show_alert=True,
            )

    @dp.callback_query(F.data == CANCEL_CB)
    async def on_cancel(cb: CallbackQuery) -> None:
        msg_union = cb.message
        if msg_union is None or not isinstance(msg_union, Message):
            await cb.answer(i18n.t("msg_not_found"), show_alert=True)
            return
        msg: Message = msg_union
        if msg.chat is None or msg.chat.id != config.allowed_chat_id:
            await cb.answer(i18n.t("not_available_in_this_chat"), show_alert=True)
            return

        job = jobs_queue.get_job(msg.message_id)
        if not job:
            await cb.answer(i18n.t("cancel_unavailable_started"), show_alert=True)
            return

        # Permissions: author or admin can cancel
        user_id = cb.from_user.id if cb.from_user else 0
        is_author = (user_id == job.user_id and user_id != 0)

        is_admin = False
        try:
            if cb.from_user:
                member = await bot.get_chat_member(msg.chat.id, cb.from_user.id)
                is_admin = member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
        except Exception:
            is_admin = False

        if not (is_author or is_admin):
            await cb.answer(i18n.t("cancel_forbidden"), show_alert=True)
            return

        # Try to cancel; if already started — cannot cancel
        canceled = jobs_queue.cancel_job(msg.message_id, by_admin=(is_admin and not is_author))
        if not canceled:
            await cb.answer(i18n.t("cancel_unavailable_started"), show_alert=True)
            return

        # Update placeholder caption and remove inline keyboard
        caption = i18n.t("canceled_by_admin") if (is_admin and not is_author) else i18n.t("canceled_by_author")
        try:
            await bot.edit_message_caption(
                chat_id=msg.chat.id,
                message_id=msg.message_id,
                caption=caption,
                reply_markup=None,
            )
        except Exception:
            # non-critical
            pass

        await cb.answer(i18n.t("cancel_success"), show_alert=False)

    return dp, bot


async def _process_generate_job(
    config: AppConfig,
    bot: Bot,
    comfy_client: ComfyClient,
    topics_repo: TopicsRepository,
    i18n: I18n,
    job: GenerateJob,
) -> None:
    """
    JobsQueue processor. Runs in worker tasks.
    """
    # If job was canceled while waiting — skip silently
    if job.canceled:
        return

    log.info("Processing job corr=%s topic=%s user=%s", job.correlation_id, job.topic_alias, job.user_id)
    topic_cfg = topics_repo.all_topics().get(job.topic_alias)
    if not topic_cfg:
        try:
            await bot.edit_message_caption(
                chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("theme_not_found")
            )
        except Exception:
            pass
        return

    # Build workflow params
    params: Dict[str, Any] = dict(job.params)
    prompt: str = job.prompt

    # Input image(s) handling
    needs_input_images = any(r.type == "input_images" for r in topic_cfg.nodes_map.nodes)
    needs_input_image = any(r.type == "input_image" for r in topic_cfg.nodes_map.nodes)

    # Multiple images
    if needs_input_images:
        images = job.input_images or []
        if not images:
            await bot.edit_message_caption(
                chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("requires_input_image")
            )
            return
        saved_names: List[str] = []
        try:
            loop = asyncio.get_running_loop()
            # Upload each image sequentially to avoid overloading server
            for b, fn in images[:10]:
                name = await loop.run_in_executor(None, lambda bb=b, ff=fn: comfy_client.upload_image(bb, ff))
                saved_names.append(name)
            params["input_images"] = saved_names
        except Exception as e:
            log.exception("Upload multi images failed: %s", e)
            await bot.edit_message_caption(
                chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("uploading_input_image_failed")
            )
            return

    # Single image
    if needs_input_image and job.input_image_bytes:
        upload_name_hint = job.input_image_filename or f"tg_{job.correlation_id}.png"
        try:
            loop = asyncio.get_running_loop()
            saved_name = await loop.run_in_executor(
                None, lambda: comfy_client.upload_image(job.input_image_bytes or b"", upload_name_hint)
            )
            params["input_image"] = saved_name
        except Exception as e:
            log.exception("Upload image failed: %s", e)
            await bot.edit_message_caption(
                chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("uploading_input_image_failed")
            )
            return
    elif needs_input_image and not job.input_image_bytes:
        await bot.edit_message_caption(
            chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("requires_input_image")
        )
        return

    # If job was canceled right before starting (race) — stop early
    if job.canceled:
        return

    workflow = comfy_client.prepare_workflow(
        base_workflow=topic_cfg.workflow,
        nodes_map=topic_cfg.nodes_map,
        prompt=prompt,
        params=params,
    )

    # Timings: bot queue waiting time — from placeholder to worker start
    start_processing_ts = time.time()
    bot_queue_s = max(0.0, start_processing_ts - float(job.placeholder_ts or start_processing_ts))

    # Once real processing begins, if placeholder was "waiting", switch caption to "generating" and remove cancel button
    if job.initially_waiting:
        try:
            await bot.edit_message_caption(
                chat_id=job.chat_id,
                message_id=job.message_id,
                caption=i18n.t("ph_generating"),
                reply_markup=None,  # remove cancel button
            )
        except Exception as e:
            # Not critical, continue
            log.debug("Failed to switch placeholder caption to 'generating': %s", e)

    # Submit to ComfyUI in a thread to avoid blocking event loop
    loop = asyncio.get_running_loop()

    async def _run_comfy():
        return await loop.run_in_executor(None, lambda: comfy_client.submit_and_wait(workflow))

    try:
        gen_res = await asyncio.wait_for(_run_comfy(), timeout=config.timeout_run + 5)
    except asyncio.TimeoutError:
        err = i18n.t("generation_timeout")
        await bot.edit_message_caption(chat_id=job.chat_id, message_id=job.message_id, caption=err)
        return
    except ComfyError as e:
        await bot.edit_message_caption(chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("comfy_error_fmt", error=str(e)))
        return
    except Exception as e:
        log.exception("Comfy error: %s", e)
        await bot.edit_message_caption(
            chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("generation_failed")
        )
        return

    media_urls = gen_res.media
    if not media_urls:
        await bot.edit_message_caption(chat_id=job.chat_id, message_id=job.message_id, caption=i18n.t("no_media"))
        return

    # Download first media and edit placeholder
    first = media_urls[0]
    first_bytes = await loop.run_in_executor(None, lambda: comfy_client.download_image_bytes(first.url))
    caption = build_caption(i18n, job.prompt, bot_queue_s, gen_res.comfy_queue_s, gen_res.comfy_exec_s)

    # Build regen keyboard:
    # - only for albums (len > 1) encode start/count (to trigger forward-on-regen);
    # - for single image or pure text: keep old simple "regen" without forwarding.
    src_start_id: Optional[int] = None
    src_count: Optional[int] = None
    if job.source_message_ids:
        try:
            ids_sorted = sorted(job.source_message_ids)
            if ids_sorted:
                src_start_id = ids_sorted[0]
                src_count = len(ids_sorted)
        except Exception:
            pass
    kb = make_regen_kb(i18n, src_start_id=src_start_id if (src_count and src_count > 1) else None,
                       src_count=src_count if (src_count and src_count > 1) else None)

    if first.kind == "video":
        await edit_to_video(bot, job.chat_id, job.message_id, first_bytes, first.filename, caption, reply_markup=kb)
    elif first.kind == "audio":
        await edit_to_audio(bot, job.chat_id, job.message_id, first_bytes, first.filename, caption, reply_markup=kb)
    else:
        await edit_to_image(bot, job.chat_id, job.message_id, first_bytes, first.filename, caption, reply_markup=kb)

    # Send the rest of results
    rest = media_urls[1:]
    if rest:
        imgs: List[Tuple[bytes, str]] = []
        vids: List[Tuple[bytes, str]] = []
        auds: List[Tuple[bytes, str]] = []
        for m in rest:
            b = await loop.run_in_executor(None, lambda url=m.url: comfy_client.download_image_bytes(url))
            if m.kind == "video":
                vids.append((b, m.filename))
            elif m.kind == "audio":
                auds.append((b, m.filename))
            else:
                imgs.append((b, m.filename))
        if imgs:
            await send_album(bot, job.chat_id, job.thread_id, imgs, caption=None)
        if vids:
            await send_videos(bot, job.chat_id, job.thread_id, vids, caption=None)
        if auds:
            await send_audios(bot, job.chat_id, job.thread_id, auds, caption=None)

    log.info(
        "Job done corr=%s, media=%d, bot_queue=%.2fs, comfy_queue=%.2fs, comfy_exec=%.2fs",
        job.correlation_id,
        len(media_urls),
        bot_queue_s,
        gen_res.comfy_queue_s,
        gen_res.comfy_exec_s,
    )