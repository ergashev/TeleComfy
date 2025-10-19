# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Optional

class I18n:
    """
    Simple JSON-based key-value i18n loader.
    Loads {locale}.json from locales_dir and falls back to en.json and keys themselves.
    """

    def __init__(self, locales_dir: str = "locales", locale: str = "en"):
        self.locales_dir = locales_dir
        self.locale = (locale or "en").lower()
        self._cache: Dict[str, str] = {}
        self._fallback: Dict[str, str] = {}
        self._load_all()

    def _load_file(self, lang: str) -> Dict[str, str]:
        path = os.path.join(self.locales_dir, f"{lang}.json")
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        # Ensure keys are strings
        return {str(k): str(v) for k, v in data.items()}

    def _load_all(self):
        # Load primary locale
        self._cache = self._load_file(self.locale)
        # Load fallback 'en'
        self._fallback = self._load_file("en")

    def t(self, key: str, **kwargs) -> str:
        """
        Translate key using selected locale; fallback to 'en'; else return key.
        Applies str.format_map on kwargs if provided.
        """
        txt: Optional[str] = self._cache.get(key) or self._fallback.get(key) or key
        if kwargs:
            try:
                return txt.format_map(kwargs)
            except Exception:
                # If formatting fails, return without formatting
                return txt
        return txt