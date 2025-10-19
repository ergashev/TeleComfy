# -*- coding: utf-8 -*-
import struct
from typing import Optional, Tuple


def _size_from_png(data: bytes) -> Optional[Tuple[int, int]]:
    # PNG signature: 8 bytes
    if len(data) < 24:
        return None
    if data[0:8] != b"\x89PNG\r\n\x1a\n":
        return None
    # IHDR chunk: 4 length + 4 'IHDR' + 13 data + 4 CRC
    # width/height are first 8 bytes of IHDR data (big-endian)
    try:
        # The first chunk should be IHDR
        if data[12:16] != b'IHDR':
            return None
        w = struct.unpack(">I", data[16:20])[0]
        h = struct.unpack(">I", data[20:24])[0]
        if w > 0 and h > 0:
            return int(w), int(h)
    except Exception:
        return None
    return None


def _size_from_jpeg(data: bytes) -> Optional[Tuple[int, int]]:
    # JPEG: scan for SOF0..SOF3, SOF5..SOF7, SOF9..SOF15 markers
    # 0xFF 0xC0..0xCF except 0xC4 (DHT), 0xC8 (JPG), 0xCC (DAC)
    if len(data) < 4:
        return None
    if not (data[0] == 0xFF and data[1] == 0xD8):  # SOI
        return None
    i = 2
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        # skip FF padding
        while i < len(data) and data[i] == 0xFF:
            i += 1
        if i >= len(data):
            break
        marker = data[i]
        i += 1
        # standalone markers without length
        if marker in (0xD8, 0xD9):  # SOI, EOI
            continue
        if i + 2 > len(data):
            break
        seg_len = struct.unpack(">H", data[i:i+2])[0]
        if seg_len < 2:
            return None
        if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
            # SOF: seg_len >= 7; layout: precision(1), height(2), width(2), ...
            if i + 5 < len(data):
                try:
                    precision = data[i+2]
                    # precision is unused here
                    h = struct.unpack(">H", data[i+3:i+5])[0]
                    w = struct.unpack(">H", data[i+5:i+7])[0]
                    if w > 0 and h > 0:
                        return int(w), int(h)
                except Exception:
                    return None
        i += seg_len
    return None


def _size_from_webp(data: bytes) -> Optional[Tuple[int, int]]:
    # WEBP (RIFF container)
    # Layout: 'RIFF' + size(4 LE) + 'WEBP' + chunks
    if len(data) < 16:
        return None
    if data[0:4] != b'RIFF' or data[8:12] != b'WEBP':
        return None

    def _le32(b: bytes) -> int:
        return struct.unpack("<I", b)[0]

    offset = 12
    total_size = len(data)
    while offset + 8 <= total_size:
        chunk_tag = data[offset:offset+4]
        chunk_size = _le32(data[offset+4:offset+8])
        payload_start = offset + 8
        payload_end = payload_start + chunk_size
        if payload_end > total_size:
            break

        # VP8X: extended format: 1b flags, 3b reserved, then 3b width-1, 3b height-1 (little-endian 24 bit)
        if chunk_tag == b'VP8X' and chunk_size >= 10:
            try:
                # flags(1) + reserved(3) + width(3) + height(3)
                w_minus_1 = data[payload_start+4] | (data[payload_start+5] << 8) | (data[payload_start+6] << 16)
                h_minus_1 = data[payload_start+7] | (data[payload_start+8] << 8) | (data[payload_start+9] << 16)
                w = int(w_minus_1) + 1
                h = int(h_minus_1) + 1
                if w > 0 and h > 0:
                    return w, h
            except Exception:
                return None

        # VP8 (lossy): look for 0x9d 0x01 0x2a signature
        if chunk_tag == b'VP8 ' and chunk_size >= 10:
            try:
                # Search signature inside the first 30 bytes of payload
                search_end = min(payload_end, payload_start + 30)
                sig_pos = -1
                sig = b'\x9d\x01\x2a'
                scan = payload_start
                while scan + 3 <= search_end:
                    if data[scan:scan+3] == sig:
                        sig_pos = scan
                        break
                    scan += 1
                if sig_pos != -1 and sig_pos + 7 <= payload_end:
                    w = struct.unpack("<H", data[sig_pos+3:sig_pos+5])[0] & 0x3FFF
                    h = struct.unpack("<H", data[sig_pos+5:sig_pos+7])[0] & 0x3FFF
                    if w > 0 and h > 0:
                        return int(w), int(h)
            except Exception:
                return None

        # VP8L (lossless): first byte is signature 0x2f, next 4 bytes store dims
        if chunk_tag == b'VP8L' and chunk_size >= 5:
            try:
                if data[payload_start] != 0x2F:
                    # Not a lossless signature
                    pass
                else:
                    b1 = data[payload_start+1]
                    b2 = data[payload_start+2]
                    b3 = data[payload_start+3]
                    b4 = data[payload_start+4]
                    width = ((b1 | ((b2 & 0x3F) << 8)) + 1)
                    height = ((((b2 >> 6) | (b3 << 2) | ((b4 & 0x0F) << 10)) + 1))
                    if width > 0 and height > 0:
                        return int(width), int(height)
            except Exception:
                return None

        # Chunk sizes are padded to even
        offset = payload_end + (chunk_size & 1)
    return None


def get_image_size_from_bytes(data: bytes, filename: Optional[str] = None, mime: Optional[str] = None) -> Optional[Tuple[int, int]]:
    """
    Try to determine (width, height) from image bytes.
    Supports PNG, JPEG, WEBP. Uses filename/mime heuristics to short-circuit type choice.
    """
    if not data:
        return None

    # Heuristic by mime or extension
    ext = ""
    if filename:
        fn = filename.lower()
        if "." in fn:
            ext = fn.rsplit(".", 1)[-1]
    mime_l = (mime or "").lower()

    # Try a guided order
    try_order = []
    if "png" in mime_l or ext == "png":
        try_order = ["png", "jpeg", "webp"]
    elif "jpeg" in mime_l or "jpg" in mime_l or ext in ("jpg", "jpeg"):
        try_order = ["jpeg", "png", "webp"]
    elif "webp" in mime_l or ext == "webp":
        try_order = ["webp", "png", "jpeg"]
    else:
        try_order = ["png", "jpeg", "webp"]

    for kind in try_order:
        if kind == "png":
            wh = _size_from_png(data)
        elif kind == "jpeg":
            wh = _size_from_jpeg(data)
        else:
            wh = _size_from_webp(data)
        if wh:
            return wh

    return None