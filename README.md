# TeleComfy Bot

<u>**English**</u> • [Русский](/README.ru.md) • [中文](/README.zh.md)

**TeleComfy** is an intelligent Telegram bot for supergroups with forum topics: it generates images, videos, and audio via ComfyUI based on your messages in dedicated topics. Each topic corresponds to a separate, fully fledged ComfyUI workflow. The bot supports queued processing with limits, task cancelation, regeneration, a multilingual interface (en/ru/zh), and handling of image albums.

## Key features

- Generation via ComfyUI: POST /prompt + WebSocket /ws + /history → /view
- Operates strictly within a single supergroup (no private chats or other groups)
- Forum topics: a dedicated ComfyUI workflow per topic (txt2img, img2img, txt2video, txt2audio, etc.)
- Task queue:
  - Global worker limit and per‑topic limit
  - Per‑user limit for pending tasks
  - “Cancel” button is available while a task is still waiting
- One‑click regeneration with preserved original parameters/albums
- Album support (up to 10 images) for topics that require multiple input images (e.g., Qwen-Image-Edit-2509)
- Inline parameters in messages: steps, width, height, seed, fps, length, and more
  - Per‑topic configuration of the allowed parameter set
  - Per‑topic configuration of allowed value ranges per parameter
- Result captions show actual queue and generation timings
- Telegram UI localizations: en, ru, zh

## Requirements

- Python 3.11+
- A running ComfyUI (local or remote), reachable at COMFY_BASE_URL
- A Telegram bot token from BotFather
- A supergroup with Forum Topics enabled, where the bot is an administrator with topic management rights

## Quick start

1) Clone the repository and install dependencies:
```
pip install -r requirements.txt
```

2) Start ComfyUI
- Make sure ComfyUI is reachable at http://127.0.0.1:8000 (or your URL)

3) Prepare .env
- Copy .env.example to .env and fill the key parameters:
```
TELEGRAM_TOKEN=123456:ABCDEF-your-token
ALLOWED_CHAT_ID=-100xxxxxxxxxx
COMFY_BASE_URL=http://127.0.0.1:8000
# COMFY_API_KEY=  # set if your ComfyUI/proxy is protected
```
- How to get ALLOWED_CHAT_ID:
  - Create a supergroup, enable “Forum Topics”
  - Add the bot as an admin with “Manage Topics” rights
  - Get the chat ID via @getidsbot or similar tools

4) Topics (modes)
- The project includes example topics with workflows for: txt2img, img2img, album_img2img, txt2video, img2video, txt2audio
- On first run, simply perform topic synchronization (see below)

5) Run
```
python -m app.main
```

6) Synchronize topics
- In the configured supergroup (ALLOWED_CHAT_ID), as an administrator send:
```
/topic_scan
```
- The bot will create/update forum topics from data/topics and bind them by alias

## How to use

- Post in the relevant forum topic:
  - Text without an image — for txt2img/txt2video/txt2audio topics
  - A photo with a caption — for img2img/img2video
  - An album of multiple photos (up to 10) — for topics that require multiple input images (e.g., Qwen-Image-Edit-2509)
- The bot will reply with a placeholder (waiting image). When generation finishes:
  - The message will be edited with the final result (first media)
  - Remaining results (if any) will follow as an album
- Buttons under the result:
  - “Regenerate” — starts a new task with the same parameters
  - “Cancel” — available while the task is still in the queue

## Inline parameters

Add quick settings to your message:

- Scalars:
  - `steps=30 width=768 height=1344 seed=123`
  - For video/audio: `fps=24 length=5.0`
- Long text block (may be needed for song lyrics, etc.):
  - Option 1:
````text
text```Multiline text here
With line breaks
`` `
````
  - Option 2: `text: description continues until the end of the message`
  - Option 3: `text="any text in quotes"`

Examples:
- Simple prompt:
```
cinematic portrait, rim light, 85mm, f1.8
width=768 height=1152
```
- For img2img (in the img2img topic): send a photo with a caption:
```
watercolor style, high contrast
steps=30
```
- For a topic that supports multiple input images: send an album (up to 10 photos) and add a caption with parameters.

Notes:
- Parameter availability depends on the specific topic and its configuration
- In some topics (enabled by default), values are constrained (min/max); the bot neatly adjusts width/height proportionally to stay within bounds

## Useful settings

- Limits and timeouts (in .env):
  - `LIMITS_MAX_WORKERS` — global maximum of concurrent tasks
  - `LIMITS_PER_TOPIC` — per‑topic concurrency
  - `LIMITS_PER_USER_PENDING` — how many “pending” (not started yet) tasks a user may have
  - `TIMEOUT_WS`, `TIMEOUT_RUN` — WebSocket and end‑to‑end generation timeouts
- Localization:
  - `LOCALE=en|ru|zh`
- Securing ComfyUI:
  - `COMFY_API_KEY` — if ComfyUI/proxy enforces token checks

## Tips and common issues

- “This topic is not configured. Admin: run /topic_scan.”
  - Run `/topic_scan` as an administrator in the intended supergroup
- Placeholder takes too long:
  - Check connectivity to ComfyUI (COMFY_BASE_URL)
  - Increase `TIMEOUT_WS`/`TIMEOUT_RUN` if needed
- “You already have N pending task(s). Limit: N. Please wait until they finish.”
  - You hit the personal pending limit. Wait for some tasks to finish or cancel them
- Diagnostics:
  - Increase log verbosity: `LOG_LEVEL=DEBUG`

—

Happy generating!