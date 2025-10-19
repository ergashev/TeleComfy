# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

@dataclass
class NodeRule:
    type: str              # e.g., "prompt", "model", "width", "height", "steps", "seed", "n", "input_image", "input_images", "fps", "length", "text", "text:<param>"
    node_ids: List[str]    # e.g., ["45"]
    key: str               # e.g., "text", "unet_name", "width", ...
    # Optional: for generic text fields, allows mapping params['<param>'] to node input.
    # Examples in nodes.json:
    #   {"type": "text", "param": "text", "node_ids": ["11"], "key": "text"}
    #   {"type": "text:text", "node_ids": ["11"], "key": "text"}
    param: Optional[str] = None

@dataclass
class NodesMap:
    nodes: List[NodeRule] = field(default_factory=list)
    defaults: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TopicConfig:
    alias: str
    title: str
    description: Optional[str]
    topic_id: Optional[int]
    permissions: Dict[str, Any]
    defaults: Dict[str, Any]  # defaults merged from meta.json (can be merged with nodes defaults)
    workflow_path: str
    nodes_path: str
    meta_path: str
    emoji: Optional[str] = None
    icon_custom_emoji_id: Optional[str] = None

    # Inline params control
    # - inline_allowed: if None -> all supported inline params are allowed; else only listed keys are allowed
    # - inline_limits: per-param numeric limits, supports {"min": number, "max": number}
    inline_allowed: Optional[List[str]] = None
    inline_limits: Dict[str, Any] = field(default_factory=dict)

    workflow: Dict[str, Any] = field(default_factory=dict)
    nodes_map: NodesMap = field(default_factory=NodesMap)

@dataclass
class GenerateJob:
    chat_id: int
    thread_id: int
    message_id: int
    prompt: str
    user_id: int
    params: Dict[str, Any]
    topic_alias: str
    correlation_id: str
    input_image_bytes: Optional[bytes] = None
    input_image_filename: Optional[str] = None  # filename hint for ComfyUI upload
    input_images: Optional[List[Tuple[bytes, str]]] = None  # list of (bytes, filename)
    placeholder_ts: float = 0.0
    initially_waiting: bool = False
    canceled: bool = False
    canceled_by_admin: bool = False
    started: bool = False
    source_message_ids: Optional[List[int]] = None

@dataclass
class MediaURL:
    url: str
    filename: str
    subfolder: str
    kind: str  # "image" | "video" | "audio"
    mime_type: str = "application/octet-stream"

@dataclass
class GenerationResult:
    media: List[MediaURL]
    comfy_queue_s: float
    comfy_exec_s: float