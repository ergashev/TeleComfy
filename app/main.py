#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import signal
from app.core.config import load_config, AppConfig
from app.core.logging import setup_logging
from app.core.i18n import I18n
from app.comfy.client import ComfyClient
from app.infra.topics_repo import TopicsRepository
from app.infra.jobs_queue import JobsQueue
from app.tg.bot import create_bot_app

async def main():
    config: AppConfig = load_config()
    setup_logging()
    logging.getLogger("websocket").setLevel(logging.WARNING)
    log = logging.getLogger("main")

    log.info("Starting bot...")

    # Initialize ComfyUI client
    comfy_client = ComfyClient(
        base_url=config.comfy_base_url,
        api_key=config.comfy_api_key or "",
        ws_timeout=config.timeout_ws,
        run_timeout=config.timeout_run,
    )

    # Initialize topics repository
    topics_repo = TopicsRepository(
        workdir=config.workdir,
        state_dir=config.state_dir,
    )
    await topics_repo.reload_cache()
    loaded = topics_repo.all_topics()
    log.info("Topics loaded on startup: %d", len(loaded))

    # Jobs queue with global and per-topic limits
    jobs_queue = JobsQueue(
        max_workers=config.limits_max_workers,
        per_topic_limit=config.limits_per_topic,
    )

    # Initialize i18n
    i18n = I18n(locales_dir=config.locales_dir, locale=config.locale)

    # Create bot app
    dp, bot = await create_bot_app(config, comfy_client, topics_repo, jobs_queue, i18n)

    # Graceful shutdown
    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _signal_handler)
        except NotImplementedError:
            # Not supported on some platforms (e.g. Windows)
            pass

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await jobs_queue.shutdown()
        await bot.session.close()
        log.info("Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())