# -*- coding: utf-8 -*-
import json
import uuid
import urllib.parse
import urllib.request
import logging
import time
import random
from typing import Dict, List, Any, Optional
from app.domain.models import NodesMap, MediaURL, GenerationResult
import websocket  # websocket-client

log = logging.getLogger("comfy_client")


class ComfyError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return self.message


class ComfyClient:
    """
    Minimal client for ComfyUI:
    - POST /prompt
    - WebSocket /ws
    - GET /history/{prompt_id}
    - GET /view?filename=...&subfolder=...&type=...
    - POST /upload/image (for input images)
    """

    def __init__(self, base_url: str, api_key: str = "", ws_timeout: int = 120, run_timeout: int = 300):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or ""
        self.ws_timeout = ws_timeout
        self.run_timeout = run_timeout

    def _headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {"User-Agent": "Mozilla/5.0"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _post_json(self, url: str, payload: dict) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={**self._headers(), "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.run_timeout) as resp:
            return json.loads(resp.read())

    def _get_json(self, url: str) -> dict:
        req = urllib.request.Request(url, headers=self._headers())
        with urllib.request.urlopen(req, timeout=self.run_timeout) as resp:
            return json.loads(resp.read())

    def _download_bytes(self, url: str) -> bytes:
        req = urllib.request.Request(url, headers=self._headers())
        with urllib.request.urlopen(req, timeout=self.run_timeout) as resp:
            return resp.read()

    def _ws_url(self, base_http_url: str) -> str:
        if base_http_url.startswith("https://"):
            return base_http_url.replace("https://", "wss://")
        return base_http_url.replace("http://", "ws://")

    def verify_server(self) -> bool:
        try:
            _ = self._get_json(f"{self.base_url}/object_info")
            return True
        except Exception as e:
            log.warning("ComfyUI /object_info failed: %s", e)
            return False

    def prepare_workflow(self, base_workflow: dict, nodes_map: NodesMap, prompt: str, params: Dict[str, Any]) -> dict:
        """
        Prepare a copy of the workflow with prompt and params applied according to nodes_map rules.
        Supports:
        - prompt (positive)
        - negative_prompt (via params['negative_prompt'] if provided)
        - generic text fields: type='text' with 'param', or type like 'text:<name>'
        - numeric and other scalar params: width/height/steps/seed/n/model/fps/length, etc.
        - input_image: single filename string in params['input_image']
        - input_images: list of filenames in params['input_images'], applied sequentially to rule.node_ids
        - Dynamic pruning: if input_images count < declared node_ids, remove unused LoadImage nodes and unlink inputs pointing to them.
        """
        wf = json.loads(json.dumps(base_workflow))  # deep copy
        eff_params: Dict[str, Any] = dict(params or {})
        if "seed" not in eff_params or eff_params["seed"] is None:
            eff_params["seed"] = random.randint(0, 2**48 - 1)
            log.debug("Generated random seed: %s", eff_params["seed"])

        # Helper: unlink references to specific node_ids across the workflow, then remove those nodes
        def _unlink_and_remove_nodes(workflow: Dict[str, Any], remove_ids: List[str]) -> None:
            if not remove_ids:
                return
            remove_set = set(remove_ids)
            for nid, node in list(workflow.items()):
                try:
                    inputs = node.get("inputs")
                    if not isinstance(inputs, dict):
                        continue
                    for k in list(inputs.keys()):
                        v = inputs[k]
                        # Standard edge is ["<node_id>", <index>]
                        if isinstance(v, list) and len(v) >= 1 and isinstance(v[0], str) and v[0] in remove_set:
                            # Remove the entire input key to detach the edge
                            del inputs[k]
                except Exception:
                    # Best-effort; continue on errors
                    continue
            # Finally, remove the nodes themselves
            for rid in remove_ids:
                workflow.pop(rid, None)

        # 1) Text-like mappings (prompt, negative_prompt, text:*)
        for rule in nodes_map.nodes:
            rtype = (rule.type or "").strip().lower()
            if not rtype:
                continue

            if rtype == "prompt":
                for nid in rule.node_ids:
                    wf[nid]["inputs"][rule.key] = prompt
                continue

            if rtype == "negative_prompt":
                neg = eff_params.get("negative_prompt", None)
                if neg is not None:
                    for nid in rule.node_ids:
                        wf[nid]["inputs"][rule.key] = neg
                continue

            # Generic text fields
            param_key: Optional[str] = None
            if rtype.startswith("text:") or rtype.startswith("string:"):
                try:
                    param_key = rtype.split(":", 1)[1].strip()
                except Exception:
                    param_key = None
            elif rtype in ("text", "string"):
                param_key = (rule.param or "").strip() if rule.param else None

            if param_key:
                val = eff_params.get(param_key, None)
                if val is not None:
                    for nid in rule.node_ids:
                        wf[nid]["inputs"][rule.key] = val

        # 1.1) Input image(s)
        # Single input image
        for rule in nodes_map.nodes:
            rtype = (rule.type or "").strip().lower()
            if rtype == "input_image":
                val = eff_params.get("input_image", None)
                if val is not None:
                    for nid in rule.node_ids:
                        wf[nid]["inputs"][rule.key] = val

        # Multiple input images + dynamic pruning of unused image nodes
        for rule in nodes_map.nodes:
            rtype = (rule.type or "").strip().lower()
            if rtype != "input_images":
                continue

            vals = eff_params.get("input_images", None)
            if isinstance(vals, list) and vals:
                # Apply sequentially to listed node_ids
                for idx, fname in enumerate(vals):
                    if idx >= len(rule.node_ids):
                        break
                    nid = rule.node_ids[idx]
                    try:
                        wf[nid]["inputs"][rule.key] = fname
                    except Exception:
                        pass

                # Prune unused LoadImage nodes and unlink references to them
                used_count = min(len(vals), len(rule.node_ids))
                if used_count < len(rule.node_ids):
                    to_remove = rule.node_ids[used_count:]
                    _unlink_and_remove_nodes(wf, to_remove)
            else:
                # No images provided — remove all declared image nodes and unlink
                _unlink_and_remove_nodes(wf, rule.node_ids)

        # 2) Other parameters (including model, width/height, steps, seed, n, fps, length, etc.)
        for rule in nodes_map.nodes:
            rtype = (rule.type or "").strip().lower()
            if rtype in ("prompt", "negative_prompt", "input_image", "input_images") or rtype.startswith("text") or rtype.startswith("string"):
                continue
            # Use lower-cased type as a key to match eff_params keys (also lower-cased)
            pkey = rtype
            if pkey in eff_params and eff_params[pkey] is not None:
                val = eff_params[pkey]
                for nid in rule.node_ids:
                    wf[nid]["inputs"][rule.key] = val

        if log.isEnabledFor(logging.DEBUG):
            try:
                classes = {nid: node.get("class_type") for nid, node in wf.items()}
                log.debug("Workflow prepared: nodes=%d, classes=%s", len(wf), classes)
            except Exception:
                pass

        return wf

    def submit_and_wait(self, workflow: dict) -> GenerationResult:
        """
        Submit workflow and wait using the WebSocket events until execution finishes.
        Then read /history and compose media view URLs.
        Also compute queue and execution timings.
        """
        client_id = str(uuid.uuid4())
        ws_url = f"{self._ws_url(self.base_url)}/ws?clientId={client_id}"
        headers: List[str] = []
        if self.api_key:
            headers.append(f"Authorization: Bearer {self.api_key}")
        ws = websocket.WebSocket()
        ws.settimeout(self.ws_timeout)
        start_ts = time.time()
        t_queued: Optional[float] = None
        t_exec_start: Optional[float] = None
        t_exec_done: Optional[float] = None

        if log.isEnabledFor(logging.DEBUG):
            log.debug("WS connecting: %s (client_id=%s)", ws_url, client_id)

        try:
            ws.connect(ws_url, header=headers)

            # Queue prompt
            payload = {"prompt": workflow, "client_id": client_id}
            res = self._post_json(f"{self.base_url}/prompt", payload)
            prompt_id = res["prompt_id"]
            t_queued = time.time()

            if log.isEnabledFor(logging.DEBUG):
                log.debug("Prompt queued: prompt_id=%s, t_queued=%.3f (dt=%.3fs from start)",
                            prompt_id, t_queued, t_queued - start_ts)

            # Wait events
            done = False
            while True:
                if time.time() - start_ts > self.run_timeout:
                    raise ComfyError("Generation timeout exceeded")
                try:
                    msg = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if isinstance(msg, (bytes, bytearray)):
                    # Preview frames, ignore them
                    continue
                try:
                    data = json.loads(msg)
                except Exception:
                    continue
                t = data.get("type")

                if t == "executing":
                    d = data.get("data", {})
                    if d.get("prompt_id") != prompt_id:
                        continue
                    node = d.get("node")

                    # First executing event with node != None — graph execution start
                    if node is not None and t_exec_start is None:
                        t_exec_start = time.time()
                        if log.isEnabledFor(logging.DEBUG):
                            log.debug("Execution start: t_exec_start=%.3f (queue=%.3fs since queued)",
                                        t_exec_start, (t_exec_start - (t_queued or t_exec_start)))
                    # executing with node=None — graph execution finished
                    if node is None:
                        t_exec_done = time.time()
                        done = True
                        if log.isEnabledFor(logging.DEBUG):
                            log.debug("Execution done: t_exec_done=%.3f (exec=%.3fs since start)",
                                        t_exec_done, (t_exec_done - (t_exec_start or t_exec_done)))
                        break

                elif t == "execution_error":
                    d = data.get("data", {})
                    err = d.get("exception_message") or "ComfyUI execution error"
                    log.debug("Execution error for prompt_id=%s: %s", prompt_id, err)
                    raise ComfyError(err)

            if not done:
                raise ComfyError("Unknown state: not completed")

            # Fetch history and build view URLs
            hist = self._get_json(f"{self.base_url}/history/{prompt_id}")
            entry = hist[prompt_id]

            # Debug log: history structure
            if log.isEnabledFor(logging.DEBUG):
                log.debug("History[%s] entry keys: %s", prompt_id, list(entry.keys()))
                outs = entry.get("outputs", {})
                log.debug("History[%s] outputs nodes: %d, ids: %s", prompt_id, len(outs), list(outs.keys()))

            # Collect Save* nodes from workflow
            saveimage_nodes = {nid for nid, node in workflow.items() if node.get("class_type") == "SaveImage"}
            savevideo_nodes = {nid for nid, node in workflow.items() if node.get("class_type") == "SaveVideo"}
            saveaudio_nodes = {nid for nid, node in workflow.items() if node.get("class_type") == "SaveAudio"}

            if log.isEnabledFor(logging.DEBUG):
                log.debug("Workflow SaveImage nodes: %s", list(saveimage_nodes))
                log.debug("Workflow SaveVideo nodes: %s", list(savevideo_nodes))
                log.debug("Workflow SaveAudio nodes: %s", list(saveaudio_nodes))

            media: List[MediaURL] = []
            outputs: Dict[str, Dict[str, Any]] = entry.get("outputs", {})

            # Helpers: mime types by extension
            def _guess_audio_mime(filename: str) -> str:
                fn = filename.lower()
                if fn.endswith(".flac"):
                    return "audio/flac"
                if fn.endswith(".wav"):
                    return "audio/wav"
                if fn.endswith(".mp3"):
                    return "audio/mpeg"
                if fn.endswith(".m4a") or fn.endswith(".aac"):
                    return "audio/aac"
                if fn.endswith(".ogg") or fn.endswith(".oga"):
                    return "audio/ogg"
                return "application/octet-stream"

            def _guess_video_mime(filename: str) -> str:
                fn = filename.lower()
                if fn.endswith(".mp4") or fn.endswith(".m4v"):
                    return "video/mp4"
                if fn.endswith(".webm"):
                    return "video/webm"
                if fn.endswith(".mov"):
                    return "video/quicktime"
                if fn.endswith(".mkv"):
                    return "video/x-matroska"
                if fn.endswith(".gif"):
                    return "image/gif"
                return "application/octet-stream"

            def _guess_image_mime(filename: str) -> str:
                fn = filename.lower()
                if fn.endswith(".png"):
                    return "image/png"
                if fn.endswith(".jpg") or fn.endswith(".jpeg"):
                    return "image/jpeg"
                if fn.endswith(".webp"):
                    return "image/webp"
                if fn.endswith(".bmp"):
                    return "image/bmp"
                if fn.endswith(".tiff") or fn.endswith(".tif"):
                    return "image/tiff"
                return "application/octet-stream"

            # 1) Videos
            for node_id, node_out in outputs.items():
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("Node %s outputs keys: %s", node_id, list(node_out.keys()))
                is_video_node = (node_id in savevideo_nodes) or bool(node_out.get("animated"))
                if not is_video_node:
                    continue
                files_key = "videos" if "videos" in node_out else ("images" if "images" in node_out else None)
                if not files_key:
                    continue
                files = node_out.get(files_key, [])  # type: ignore[assignment]
                for v in files:
                    params = urllib.parse.urlencode(
                        {"filename": v["filename"], "subfolder": v["subfolder"], "type": v["type"]}
                    )
                    url = f"{self.base_url}/view?{params}"
                    mime = _guess_video_mime(v["filename"])
                    media.append(MediaURL(url=url, filename=v["filename"], subfolder=v["subfolder"], kind="video", mime_type=mime))

            # 2) Images
            for node_id, node_out in outputs.items():
                if (node_id in savevideo_nodes) or bool(node_out.get("animated")):
                    continue
                if "images" not in node_out:
                    continue
                if saveimage_nodes and node_id not in saveimage_nodes:
                    continue
                files = node_out.get("images", [])
                for im in files:
                    params = urllib.parse.urlencode(
                        {"filename": im["filename"], "subfolder": im["subfolder"], "type": im["type"]}
                    )
                    url = f"{self.base_url}/view?{params}"
                    mime = _guess_image_mime(im["filename"])
                    media.append(MediaURL(url=url, filename=im["filename"], subfolder=im["subfolder"], kind="image", mime_type=mime))

            # 3) Audio
            for node_id, node_out in outputs.items():
                is_audio_node = (node_id in saveaudio_nodes)
                audio_key = None
                if "audio" in node_out:
                    audio_key = "audio"
                elif "audios" in node_out:
                    audio_key = "audios"
                if not (is_audio_node or audio_key):
                    pass
                if audio_key:
                    files = node_out.get(audio_key, [])
                    for a in files:
                        params = urllib.parse.urlencode(
                            {"filename": a["filename"], "subfolder": a["subfolder"], "type": a["type"]}
                        )
                        url = f"{self.base_url}/view?{params}"
                        media.append(MediaURL(url=url, filename=a["filename"], subfolder=a["subfolder"], kind="audio", mime_type=_guess_audio_mime(a["filename"])))

            # 4) Fallback if nothing matched
            if not media:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("No media collected via Save* heuristics, trying raw outputs fallback")
                for node_id, node_out in outputs.items():
                    for key in ("videos", "images", "audio", "audios"):
                        if key not in node_out:
                            continue
                        files_fallback = node_out[key]
                        for f in files_fallback:
                            params = urllib.parse.urlencode(
                                {"filename": f["filename"], "subfolder": f["subfolder"], "type": f["type"]}
                            )
                            url = f"{self.base_url}/view?{params}"
                            fn = f["filename"]
                            if key == "videos" or bool(node_out.get("animated")):
                                kind = "video"
                                mime = _guess_video_mime(fn)
                            elif key in ("audio", "audios"):
                                kind = "audio"
                                mime = _guess_audio_mime(fn)
                            else:
                                kind = "image"
                                mime = _guess_image_mime(fn)
                            media.append(MediaURL(url=url, filename=fn, subfolder=f["subfolder"], kind=kind, mime_type=mime))

            # Compute timings
            if t_queued is None:
                t_queued = start_ts
            if t_exec_start is None:
                comfy_queue_s = max(0.0, (t_exec_done or time.time()) - t_queued)
                comfy_exec_s = 0.0
            else:
                comfy_queue_s = max(0.0, t_exec_start - t_queued)
                comfy_exec_s = max(0.0, (t_exec_done or time.time()) - t_exec_start)

            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    "Timings: queued_at=%.3f, exec_start=%.3f, exec_done=%.3f, comfy_queue=%.3fs, comfy_exec=%.3fs",
                    t_queued, (t_exec_start or -1), (t_exec_done or -1), comfy_queue_s, comfy_exec_s
                )
                log.debug("Collected media: %d item(s): %s", len(media), [(m.kind, m.filename) for m in media])

            return GenerationResult(media=media, comfy_queue_s=comfy_queue_s, comfy_exec_s=comfy_exec_s)
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def download_image_bytes(self, url: str) -> bytes:
        """
        Download bytes by /view URL. Works for images, videos and audios.
        """
        return self._download_bytes(url)

    def upload_image(self, img_bytes: bytes, filename: str) -> str:
        """
        Upload an image to ComfyUI (to input folder) via /upload/image.
        Returns the saved filename to use in LoadImage node.
        """
        boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"

        def _part(name: str, content: bytes, filename: Optional[str] = None, content_type: Optional[str] = None) -> bytes:
            hdrs: List[str] = []
            disp = f'Content-Disposition: form-data; name="{name}"'
            if filename:
                hdrs.append(disp + f'; filename="{filename}"')
            else:
                hdrs.append(disp)
            if content_type:
                hdrs.append(f"Content-Type: {content_type}")
            headers_blob = ("\r\n".join(hdrs) + "\r\n\r\n").encode("utf-8")
            return b"--" + boundary.encode("utf-8") + b"\r\n" + headers_blob + content + b"\r\n"

        body = b""
        # file part
        ctype = "image/png"
        fn_lower = filename.lower()
        if fn_lower.endswith((".jpg", ".jpeg")):
            ctype = "image/jpeg"
        elif fn_lower.endswith(".webp"):
            ctype = "image/webp"
        body += _part("image", img_bytes, filename=filename, content_type=ctype)
        # type=input
        body += _part("type", b"input")
        # finalize
        body += b"--" + boundary.encode("utf-8") + b"--\r\n"

        req = urllib.request.Request(
            f"{self.base_url}/upload/image",
            data=body,
            headers={**self._headers(), "Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        with urllib.request.urlopen(req, timeout=self.run_timeout) as resp:
            data = json.loads(resp.read())
            # Expecting {"name": "<saved_name>", "subfolder": "", "type": "input"}
            name = data.get("name") or filename
            return str(name)