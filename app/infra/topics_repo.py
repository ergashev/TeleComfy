# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Optional, Tuple, Any, List
import logging
from aiogram import Bot
from app.domain.models import TopicConfig, NodesMap, NodeRule

log = logging.getLogger("topics_repo")


class TopicsRepository:
    """
    File-based topics repository. Scans a workdir with topic directories and
    maintains a per-chat index mapping alias -> forum topic_id.

    Index file path (per chat):
      state/topics_index_<chat_id>.json
    """

    def __init__(self, workdir: str, state_dir: str, chat_id: int):
        self.workdir = workdir
        self.state_dir = state_dir
        self.chat_id = chat_id
        # Store index per chat id to avoid overwriting when ALLOWED_CHAT_ID changes
        self.index_path = os.path.join(self.state_dir, f"topics_index_{self.chat_id}.json")
        os.makedirs(self.workdir, exist_ok=True)
        os.makedirs(self.state_dir, exist_ok=True)
        self._cache_by_alias: Dict[str, TopicConfig] = {}
        self._cache_by_topic_id: Dict[int, TopicConfig] = {}
        # alias -> record dict
        self._index: Dict[str, Dict[str, object]] = self.index_load()
        # Cache of emoji (Unicode) -> custom_emoji_id from forum icon stickers
        self._icon_emoji_map: Optional[Dict[str, str]] = None

    def index_load(self) -> Dict[str, Dict[str, object]]:
        if not os.path.exists(self.index_path):
            return {}
        try:
            with open(self.index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
                return {}
        except Exception:
            return {}

    def index_save(self) -> None:
        """
        Atomically write index file to reduce risk of corruption.
        """
        tmp_path = self.index_path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._index, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.index_path)
        except Exception:
            # Fallback to direct write if atomic write fails for any reason
            try:
                with open(self.index_path, "w", encoding="utf-8") as f:
                    json.dump(self._index, f, ensure_ascii=False, indent=2)
            except Exception as e:
                log.error("Failed to write topics index: %s", e)

    def _list_topic_dirs(self) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if not os.path.exists(self.workdir):
            return result
        for name in os.listdir(self.workdir):
            path = os.path.join(self.workdir, name)
            if os.path.isdir(path):
                result[name] = path
        return result

    def _load_json(self, path: str) -> dict:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _parse_nodes_map(self, data: dict) -> NodesMap:
        nodes: List[NodeRule] = []
        for n in data.get("nodes", []):
            type_str = str(n["type"])
            node_ids = list(n["node_ids"])
            key = str(n["key"])
            param_val = n.get("param", None)
            param: Optional[str]
            if isinstance(param_val, (str, int, float, bool)):
                pv = str(param_val).strip()
                param = pv if pv else None
            else:
                param = None
            nodes.append(NodeRule(type=type_str, node_ids=node_ids, key=key, param=param))
        defaults = data.get("defaults", {}) or {}
        return NodesMap(nodes=nodes, defaults=defaults)

    def _validate_nodes_vs_workflow(self, wf: dict, nodes_map: NodesMap) -> None:
        # Minimal sanity check: referenced node_id exists and has "inputs"
        for rule in nodes_map.nodes:
            for nid in rule.node_ids:
                if nid not in wf:
                    raise ValueError(f"nodes.json references node_id {nid} absent in workflow.json")
                if "inputs" not in wf[nid]:
                    raise ValueError(f"workflow node {nid} has no 'inputs'")

    def scan(self) -> Dict[str, TopicConfig]:
        topic_dirs = self._list_topic_dirs()
        result: Dict[str, TopicConfig] = {}
        for alias, path in topic_dirs.items():
            meta_path = os.path.join(path, "meta.json")
            nodes_path = os.path.join(path, "nodes.json")
            workflow_path = os.path.join(path, "workflow.json")
            try:
                meta = self._load_json(meta_path)
                nodes = self._load_json(nodes_path)
                workflow = self._load_json(workflow_path)
            except Exception as e:
                log.error("Bad topic directory %s: %s", alias, e)
                continue
            nodes_map = self._parse_nodes_map(nodes)
            try:
                self._validate_nodes_vs_workflow(workflow, nodes_map)
            except Exception as e:
                log.error("Validation failed for topic %s: %s", alias, e)
                continue
            title = meta.get("title") or alias
            description = meta.get("description")
            permissions = meta.get("permissions", {}) or {}
            defaults = meta.get("defaults", {}) or {}
            # Emoji/icon fields
            emoji = meta.get("emoji") or None
            icon_custom_emoji_id = meta.get("icon_custom_emoji_id") or None

            # Inline params control
            inline_allowed_raw = meta.get("inline_allowed", None)
            inline_allowed: Optional[List[str]]
            if isinstance(inline_allowed_raw, list):
                inline_allowed = [str(x) for x in inline_allowed_raw if isinstance(x, (str, int, float, bool))]
                inline_allowed = [str(x).lower() for x in inline_allowed]
            else:
                inline_allowed = None  # None -> all supported inline params are allowed

            inline_limits_raw = meta.get("inline_limits", {})
            inline_limits: Dict[str, Any] = inline_limits_raw if isinstance(inline_limits_raw, dict) else {}

            idx_rec = self._index.get(alias) or {}
            topic_id = idx_rec.get("topic_id") if isinstance(idx_rec, dict) else None
            cfg = TopicConfig(
                alias=alias,
                title=title,
                description=description,
                topic_id=topic_id if isinstance(topic_id, int) else None,
                permissions=permissions,
                defaults=defaults,
                workflow_path=workflow_path,
                nodes_path=nodes_path,
                meta_path=meta_path,
                emoji=emoji,
                icon_custom_emoji_id=icon_custom_emoji_id,
                inline_allowed=inline_allowed,
                inline_limits=inline_limits,
                workflow=workflow,
                nodes_map=nodes_map,
            )
            result[alias] = cfg
        return result

    async def _ensure_icon_emoji_map(self, bot: Bot) -> None:
        if self._icon_emoji_map is not None:
            return
        try:
            stickers = await bot.get_forum_topic_icon_stickers()
            # Map unicode emoji -> custom_emoji_id
            mp: Dict[str, str] = {}
            for s in stickers:
                e = getattr(s, "emoji", None)
                cid = getattr(s, "custom_emoji_id", None)
                if e and cid and e not in mp:
                    mp[e] = cid
            self._icon_emoji_map = mp
            log.debug("Loaded forum icon stickers: %d", len(self._icon_emoji_map))
        except Exception as e:
            log.warning("Failed to load forum topic icon stickers: %s", e)
            self._icon_emoji_map = {}

    async def _resolve_icon_id(self, bot: Bot, cfg: TopicConfig) -> Optional[str]:
        """
        Returns custom_emoji_id to be applied for the forum topic:
        - If icon_custom_emoji_id is set in meta.json — use it;
        - Else, if emoji is set — try map it to one of default forum icon stickers.
        """
        if cfg.icon_custom_emoji_id:
            return cfg.icon_custom_emoji_id
        if cfg.emoji:
            await self._ensure_icon_emoji_map(bot)
            return (self._icon_emoji_map or {}).get(cfg.emoji)
        return None

    async def scan_and_sync(self, bot: Bot, chat_id: int) -> Tuple[int, int, int]:
        """
        Scan topics and sync with the forum for the given chat.
        Returns tuple: (created, updated, deleted).
        """
        discovered = self.scan()
        created = 0
        updated = 0
        deleted = 0

        # Create missing topics and update existing ones
        for alias, cfg in discovered.items():
            idx = self._index.get(alias) or {}
            tid_obj = idx.get("topic_id") if isinstance(idx, dict) else None
            topic_id: Optional[int] = tid_obj if isinstance(tid_obj, int) else None
            title_in_index = idx.get("title") if isinstance(idx, dict) else None
            icon_in_index = idx.get("icon_custom_emoji_id") if isinstance(idx, dict) else None

            desired_icon_id = await self._resolve_icon_id(bot, cfg)

            if topic_id is None:
                # Create a new forum topic
                try:
                    res = await bot.create_forum_topic(
                        chat_id=chat_id,
                        name=cfg.title,
                        icon_custom_emoji_id=desired_icon_id if desired_icon_id else None,
                    )
                    topic_id_new = res.message_thread_id
                    created += 1
                    self._index[alias] = {
                        "topic_id": topic_id_new,
                        "title": cfg.title,
                        "icon_custom_emoji_id": desired_icon_id,
                    }
                except Exception as e:
                    log.warning("Failed to create forum topic %s: %s", alias, e)
                    continue
            else:
                # Update title and/or icon if changed
                need_title_update = title_in_index != cfg.title
                need_icon_update = (icon_in_index or None) != (desired_icon_id or None)
                if need_title_update or need_icon_update:
                    try:
                        assert isinstance(topic_id, int)
                        await bot.edit_forum_topic(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            name=cfg.title if need_title_update else None,
                            icon_custom_emoji_id=desired_icon_id if need_icon_update else None,
                        )
                        updated += 1
                        self._index[alias]["title"] = cfg.title
                        self._index[alias]["icon_custom_emoji_id"] = desired_icon_id
                    except Exception as e:
                        log.warning(
                            "Failed to edit forum topic %s(%s): %s", alias, topic_id, e
                        )

        # Detect removed aliases and detach them from the index
        existing_aliases = set(self._index.keys())
        discovered_aliases = set(discovered.keys())
        removed = existing_aliases - discovered_aliases
        for alias in removed:
            self._index.pop(alias, None)
        deleted = len(removed)

        self.index_save()
        # Refresh caches
        await self.reload_cache()
        return created, updated, deleted

    async def reload_cache(self) -> None:
        self._cache_by_alias.clear()
        self._cache_by_topic_id.clear()
        discovered = self.scan()
        for alias, cfg in discovered.items():
            # Read topic_id from index
            idx = self._index.get(alias) or {}
            tid = idx.get("topic_id") if isinstance(idx, dict) else None
            cfg.topic_id = tid if isinstance(tid, int) else None
            self._cache_by_alias[alias] = cfg
            if cfg.topic_id:
                self._cache_by_topic_id[cfg.topic_id] = cfg

    def resolve_by_thread_id(self, thread_id: int) -> Optional[TopicConfig]:
        return self._cache_by_topic_id.get(thread_id)

    def all_topics(self) -> Dict[str, TopicConfig]:
        return dict(self._cache_by_alias)