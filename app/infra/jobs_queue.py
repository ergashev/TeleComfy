# -*- coding: utf-8 -*-
import asyncio
import logging
from typing import Callable, Dict, Optional, Awaitable, List
from app.domain.models import GenerateJob

log = logging.getLogger("jobs_queue")

Processor = Callable[[GenerateJob], Awaitable[None]]


class JobsQueue:
    """
    Per-topic queues with per-topic workers and a global concurrency limit.

    Added:
    - Registry by message_id to support cancellation of queued tasks.
    - Cancel semantics: if a job is canceled before it starts, worker will skip it.
    - Job 'started' flag is set when the worker acquires a global slot.
    - Global per-user pending limit support: track number of awaiting (not started) jobs per user across all topics.
    """

    def __init__(self, max_workers: int = 2, per_topic_limit: int = 1):
        # Queue of tasks for each alias
        self._queues: Dict[str, asyncio.Queue[Optional[GenerateJob]]] = {}
        # Pools of workers per alias
        self._workers: Dict[str, List[asyncio.Task]] = {}
        # Global concurrency limit
        self._global_sema = asyncio.Semaphore(max_workers)
        # How many concurrent tasks allowed per topic
        self._per_topic_limit = max(1, per_topic_limit)
        # Processor callback (coroutine)
        self._processor: Optional[Processor] = None
        self._closed = False

        # Tracking active tasks per topic and globally
        self._active_per_topic: Dict[str, int] = {}
        self._active_global: int = 0
        self._max_workers: int = max_workers

        # Jobs registry: message_id -> job (queued or running)
        self._registry: Dict[int, GenerateJob] = {}

        # Global per-user pending counters (jobs queued but not started and not canceled)
        self._pending_by_user: Dict[int, int] = {}

        # Internal lock for atomic enqueue/limit checks and reservations
        self._lock = asyncio.Lock()

        log.info("JobsQueue init: max_workers=%d, per_topic_limit=%d", max_workers, self._per_topic_limit)

    def set_processor(self, processor: Processor) -> None:
        """
        Set coroutine processor which will handle GenerateJob items.
        Must be set before enqueue() is used.
        """
        self._processor = processor

    async def shutdown(self) -> None:
        """
        Gracefully stop all workers and clear internal structures.
        """
        self._closed = True
        # Put poison pills (None) for each worker
        for alias, q in self._queues.items():
            workers = self._workers.get(alias, [])
            n = len(workers) if workers else 1
            for _ in range(n):
                q.put_nowait(None)
        # Wait for all workers to stop
        for alias, tasks in self._workers.items():
            for t in tasks:
                try:
                    await t
                except Exception:
                    pass
        self._queues.clear()
        self._workers.clear()
        self._registry.clear()
        self._pending_by_user.clear()

    def _ensure_workers(self, alias: str) -> None:
        """
        Ensure worker pool is created for a given topic alias.
        """
        if alias in self._workers and self._workers[alias]:
            return
        q = self._queues.setdefault(alias, asyncio.Queue())
        # Spawn a pool of workers per topic
        self._workers[alias] = [
            asyncio.create_task(self._worker_loop(alias, q)) for _ in range(self._per_topic_limit)
        ]

    def _inc_pending(self, user_id: int) -> None:
        if user_id <= 0:
            return
        self._pending_by_user[user_id] = self._pending_by_user.get(user_id, 0) + 1

    def _dec_pending(self, user_id: int) -> None:
        if user_id <= 0:
            return
        cur = self._pending_by_user.get(user_id, 0)
        if cur <= 1:
            self._pending_by_user.pop(user_id, None)
        else:
            self._pending_by_user[user_id] = cur - 1

    def pending_count_by_user(self, user_id: int) -> int:
        """
        Return number of not-started (pending) jobs for given user.
        """
        return self._pending_by_user.get(user_id, 0)

    async def _worker_loop(self, alias: str, q: asyncio.Queue[Optional[GenerateJob]]) -> None:
        log.info("Worker started for topic: %s", alias)
        try:
            while True:
                job = await q.get()
                if job is None or self._closed:
                    break

                # If job was canceled before start — skip it
                if getattr(job, "canceled", False):
                    # Cleanup registry, mark done (pending was already decreased upon cancel, if any)
                    self._registry.pop(job.message_id, None)
                    q.task_done()
                    continue

                async with self._global_sema:
                    # Decrease 'pending' for user now (job starts)
                    if job.user_id:
                        self._dec_pending(job.user_id)
                    # Mark actual execution start (slot acquired in the global semaphore)
                    self._active_global += 1
                    self._active_per_topic[alias] = self._active_per_topic.get(alias, 0) + 1
                    # Mark started to prevent further cancellation
                    job.started = True
                    try:
                        processor = self._processor
                        if processor is None:
                            corr = getattr(job, "correlation_id", "?")
                            log.error("JobsQueue processor is not set; dropping job (topic=%s, corr=%s)", alias, corr)
                        else:
                            await processor(job)
                    except Exception as e:
                        corr = getattr(job, "correlation_id", "?")
                        log.exception("Job failed (topic=%s, corr=%s): %s", alias, corr, e)
                    finally:
                        # Mark execution end
                        self._active_global = max(0, self._active_global - 1)
                        self._active_per_topic[alias] = max(0, self._active_per_topic.get(alias, 1) - 1)
                q.task_done()
                # Remove from registry after processing (or skipping)
                self._registry.pop(job.message_id, None)
        finally:
            log.info("Worker stopped for topic: %s", alias)

    async def enqueue(self, alias: str, job: GenerateJob) -> None:
        """
        Enqueue a job for a given topic alias (no per-user limit check).
        """
        if self._closed:
            raise RuntimeError("JobsQueue is closed")
        if self._processor is None:
            raise RuntimeError("JobsQueue processor not set")
        self._ensure_workers(alias)
        # Register job for potential cancellation
        self._registry[job.message_id] = job
        # Increase pending for user
        if job.user_id:
            async with self._lock:
                self._inc_pending(job.user_id)
        await self._queues[alias].put(job)

    async def enqueue_limited(self, alias: str, job: GenerateJob, per_user_limit: Optional[int] = None) -> bool:
        """
        Atomically check per-user pending limit and enqueue if allowed.
        per_user_limit <= 0 or None -> limit disabled.
        Returns True if enqueued, False if rejected due to limit or queue closed/processor unset.
        """
        async with self._lock:
            if self._closed or self._processor is None:
                return False
            self._ensure_workers(alias)
            if per_user_limit and per_user_limit > 0 and job.user_id:
                pending = self._pending_by_user.get(job.user_id, 0)
                if pending >= per_user_limit:
                    return False
            # Register and enqueue
            self._registry[job.message_id] = job
            if job.user_id:
                self._inc_pending(job.user_id)
            await self._queues[alias].put(job)
            return True

    async def can_enqueue(self, user_id: int, per_user_limit: Optional[int]) -> bool:
        """
        Check (under lock) whether user can enqueue a new job according to global pending limit.
        Returns True if allowed, False if limit reached. If limit disabled or user_id invalid — returns True.
        """
        if user_id <= 0 or not per_user_limit or per_user_limit <= 0:
            return True
        async with self._lock:
            pending = self._pending_by_user.get(user_id, 0)
            return pending < per_user_limit

    async def reserve_user_slot(self, user_id: int, per_user_limit: Optional[int]) -> bool:
        """
        Try to reserve a pending slot for the user (increment counter) if limit allows.
        Returns True if reserved (counter incremented), False otherwise or if limit disabled/user invalid.
        """
        if user_id <= 0 or not per_user_limit or per_user_limit <= 0:
            return False
        async with self._lock:
            pending = self._pending_by_user.get(user_id, 0)
            if pending >= per_user_limit:
                return False
            self._inc_pending(user_id)
            return True

    async def release_user_slot(self, user_id: int) -> None:
        """
        Release (decrement) a previously reserved pending slot.
        Safe to call even if nothing was reserved (no-op).
        """
        if user_id <= 0:
            return
        async with self._lock:
            self._dec_pending(user_id)

    async def enqueue_reserved(self, alias: str, job: GenerateJob, *, reserved: bool) -> bool:
        """
        Enqueue a job assuming pending slot may be pre-reserved.
        If reserved=True — do not increment pending (already reserved).
        If reserved=False — increment pending here (no reservation path).
        Returns True if enqueued, False if queue is closed or processor is unset.
        """
        async with self._lock:
            if self._closed or self._processor is None:
                # On failure, caller is responsible to release reservation if any.
                return False
            self._ensure_workers(alias)
            self._registry[job.message_id] = job
            if job.user_id and not reserved:
                self._inc_pending(job.user_id)
            await self._queues[alias].put(job)
            return True

    def will_queue(self, alias: str) -> bool:
        """
        Best-effort estimation whether the task will be queued (wait) or start immediately.
        Heuristics:
        - there are already tasks in the per-topic queue; or
        - active tasks for this topic >= per-topic limit; or
        - global pool is exhausted (active >= max_workers).
        """
        self._ensure_workers(alias)
        qsize = self._queues[alias].qsize()
        if qsize > 0:
            return True
        active_topic = self._active_per_topic.get(alias, 0)
        if active_topic >= self._per_topic_limit:
            return True
        # Check global free slots
        global_free = max(0, self._max_workers - self._active_global)
        if global_free <= 0:
            return True
        return False

    def get_job(self, message_id: int) -> Optional[GenerateJob]:
        """
        Return job by placeholder message_id if it's still in queue or running.
        """
        return self._registry.get(message_id)

    def cancel_job(self, message_id: int, by_admin: bool = False) -> bool:
        """
        Mark job as canceled if it hasn't started yet.
        Returns True if cancellation succeeded, False if job not found or already started or already canceled.
        """
        job = self._registry.get(message_id)
        if job is None:
            return False
        if job.started:
            return False
        if job.canceled:
            return False
        job.canceled = True
        job.canceled_by_admin = bool(by_admin)
        # Free user's pending slot immediately
        if job.user_id:
            self._dec_pending(job.user_id)
        return True