"""
storage/embeddings.py

Embedding providers (stdlib urllib only, no new pip deps).

Priority:
  1. ZHIPU_API_KEY  → embedding-3 (2048 dims, bigmodel.cn 免费)
  2. JINA_API_KEY   → jina-embeddings-v3 (1024 dims, jina.ai 免费)
  3. OPENAI_API_KEY → text-embedding-3-small (1536 dims)
  4. 都没有          → return None (graceful degradation)
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import List, Optional

_ZHIPU_URL   = "https://open.bigmodel.cn/api/paas/v4/embeddings"
_ZHIPU_MODEL = "embedding-3"
_ZHIPU_DIMS  = 2048

_JINA_URL    = "https://api.jina.ai/v1/embeddings"
_JINA_MODEL  = "jina-embeddings-v3"
_JINA_DIMS   = 1024

_OPENAI_URL   = "https://api.openai.com/v1/embeddings"
_OPENAI_MODEL = "text-embedding-3-small"
_OPENAI_DIMS  = 1536
_DEFAULT_TIMEOUT_SECONDS = 5.0


def _timeout_seconds() -> float:
    raw = str(os.environ.get("AMI_EMBEDDING_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except Exception:
        return _DEFAULT_TIMEOUT_SECONDS
    return max(1.0, value)


def _post(url: str, headers: dict, payload: dict) -> Optional[List[float]]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=_timeout_seconds()) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        return body["data"][0]["embedding"]
    except Exception:
        return None


def get_embedding(text: str) -> Optional[List[float]]:
    """Return an embedding vector, or None on any failure."""
    text = text[:8000]

    zhipu_key = os.environ.get("ZHIPU_API_KEY", "").strip()
    if zhipu_key:
        return _post(
            _ZHIPU_URL,
            {"Authorization": f"Bearer {zhipu_key}", "Content-Type": "application/json"},
            {"model": _ZHIPU_MODEL, "input": text},
        )

    jina_key = os.environ.get("JINA_API_KEY", "").strip()
    if jina_key:
        return _post(
            _JINA_URL,
            {"Authorization": f"Bearer {jina_key}", "Content-Type": "application/json"},
            {"model": _JINA_MODEL, "input": [text]},
        )

    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if openai_key:
        return _post(
            _OPENAI_URL,
            {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
            {"model": _OPENAI_MODEL, "input": text, "dimensions": _OPENAI_DIMS},
        )

    return None


def embedding_dims() -> int:
    if os.environ.get("ZHIPU_API_KEY", "").strip():
        return _ZHIPU_DIMS
    if os.environ.get("JINA_API_KEY", "").strip():
        return _JINA_DIMS
    return _OPENAI_DIMS
