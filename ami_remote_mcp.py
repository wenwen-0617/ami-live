#!/usr/bin/env python3
"""Remote MCP server for AmI Live memory tools.

This server exposes the core chat-memory tools over streamable HTTP so the
same Supabase-backed memory system can be used from Claude web/mobile via a
remote MCP connector.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import FastMCP

from storage.factory import get_backend


db = get_backend()
logger = logging.getLogger("ami_remote_mcp")
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

SERVER_ID = str(os.environ.get("AMI_REMOTE_SERVER_ID") or "ami-live-remote").strip()
MCP_HOST = str(os.environ.get("AMI_REMOTE_HOST") or "0.0.0.0").strip() or "0.0.0.0"
MCP_PORT = int(os.environ.get("PORT") or os.environ.get("AMI_REMOTE_PORT") or "8000")
MCP_PATH = str(os.environ.get("AMI_REMOTE_MCP_PATH") or "/mcp").strip() or "/mcp"
RECENT_READ_HOURS_DEFAULT = max(0, int(os.environ.get("AMI_RECENT_READ_HOURS") or "24"))

mcp = FastMCP(
    SERVER_ID,
    instructions=(
        "AmI Live remote memory connector. Use these tools to read and write "
        "the user's long-term memory stored in Supabase."
    ),
    host=MCP_HOST,
    port=MCP_PORT,
    streamable_http_path=MCP_PATH,
    json_response=True,
    stateless_http=True,
)


def _as_int(v: Any, d: int) -> int:
    try:
        return int(v)
    except Exception:
        return d


def _as_float(v: Any, d: float) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _as_bool(v: Any, d: bool = False) -> bool:
    if v is None:
        return d
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _brief(v: Any, n: int = 80) -> str:
    s = str(v or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[:n].rstrip() + "..."


def _ts(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return s[:16]


def _list_tags(tags: Any) -> list[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(item).strip() for item in tags if str(item).strip()]
    if isinstance(tags, str):
        text = tags.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            pass
        return [item.strip() for item in text.split(",") if item.strip()]
    return [str(tags).strip()] if str(tags).strip() else []


def _memory_payload_texts(content: str = "", text: str = "", context: str = "") -> tuple[str, str]:
    raw_content = str(content or "").strip()
    raw_text = str(text or "").strip()
    raw_context = str(context or "").strip()

    full_text = raw_context or raw_content or raw_text
    summary_text = raw_text

    if raw_content and raw_context and raw_content != raw_context:
        if len(raw_content) >= len(raw_context):
            full_text = raw_content
            if not summary_text:
                summary_text = raw_context
        else:
            full_text = raw_context
            if not summary_text:
                summary_text = raw_content

    return full_text, summary_text


def _summary_text_for_memory(m: dict[str, Any], n: int = 120) -> str:
    text = str(m.get("text") or m.get("context") or "").replace("\n", " ").strip()
    return text if len(text) <= n else text[:n].rstrip() + "..."


def _fmt_memory(m: dict[str, Any]) -> str:
    tag_list = _list_tags(m.get("tags"))
    tag_text = (" tags=" + ",".join(tag_list)) if tag_list else ""
    emotion = ""
    valence = _as_float(m.get("valence"), 0.0)
    arousal = _as_float(m.get("arousal"), 0.0)
    if valence != 0.0 or arousal != 0.0:
        emotion = f" v={valence:.2f} a={arousal:.2f}"
    return (
        f"id={m.get('id')} layer={m.get('layer')} imp={m.get('importance', 3)}"
        f"{emotion}{tag_text}\n"
        f"[{_ts(m.get('created_at'))}] {m.get('text', '')}"
    )


def _fmt_memories(rows: list[dict[str, Any]], limit: int = 20) -> str:
    if not rows:
        return "(empty)"
    show = rows[:limit]
    body = "\n\n---\n\n".join(_fmt_memory(row) for row in show)
    if len(rows) > len(show):
        body += f"\n\n... {len(rows) - len(show)} more"
    return body


def _fmt_search_summaries(rows: list[dict[str, Any]], limit: int = 20) -> str:
    if not rows:
        return "(empty)"
    show = rows[:limit]
    lines = []
    for row in show:
        lines.append(
            f"id={row.get('id')} layer={row.get('layer')} imp={row.get('importance', 3)} "
            f"[{_ts(row.get('created_at'))}] summary: {_summary_text_for_memory(row)}"
        )
    if len(rows) > len(show):
        lines.append(f"... {len(rows) - len(show)} more")
    return "\n".join(lines)


def _format_tool_result(tool: str, body: str) -> str:
    return f"{tool}:\n{body}" if body else f"{tool}: ok"


@mcp.tool(description="Open a compact chat briefing from recent memories, unread letters, and board messages.")
def session_brief(recent_hours: int = RECENT_READ_HOURS_DEFAULT) -> str:
    recent_hours = max(0, _as_int(recent_hours, RECENT_READ_HOURS_DEFAULT))
    recent_memories = db.get_recent_memories(hours=24, limit=6, min_importance=1)
    letters = db.get_unread_letters(recent_hours=recent_hours)
    consumed = db.mark_letters_read() if letters else 0
    board = db.read_board_messages(
        reader="user",
        limit=10,
        mark_read=True,
        include_recent_hours=recent_hours,
    )
    code_msgs = [
        row for row in board
        if str(row.get("author") or "").strip().lower() in {"code", "live", "claude", "assistant", "agent", "bot"}
        and "smoke" not in str(row.get("content") or "").lower()
    ]

    parts = [f"now: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]
    if recent_memories:
        parts.append(f"recent memories (24h) ({len(recent_memories)}):")
        for row in recent_memories:
            parts.append(
                f"  [{_ts(row.get('created_at'))}] "
                f"({row.get('layer') or 'memory'}/imp={_as_int(row.get('importance'), 0)}) "
                f"{_brief(row.get('text') or row.get('context'), 120)}"
            )
    if code_msgs:
        parts.append(f"board from code ({len(code_msgs)}):")
        for row in code_msgs[:6]:
            parts.append(f"  [{_ts(row.get('created_at'))}] {_brief(row.get('content'), 120)}")
    if letters:
        parts.append(f"letters ({consumed} consumed):")
        for row in letters[:8]:
            parts.append(f"  [{_ts(row.get('written_at') or row.get('created_at'))}] {_brief(row.get('content'), 120)}")
    return "\n\n".join(parts)


@mcp.tool(description="Search memories by query. mode=summary returns summaries, mode=full returns detailed rows.")
def search(query: str, limit: int = 15, mode: str = "summary", session_id: str | None = None) -> str:
    query = str(query or "").strip()
    if not query:
        return "search: missing query"
    rows = db.search_memories(query=query, limit=_as_int(limit, 15), session_id=session_id)
    normalized_mode = str(mode or "summary").strip().lower()
    if normalized_mode in {"summary", "surface", "brief"}:
        return _format_tool_result("search", _fmt_search_summaries(rows))
    if normalized_mode in {"full", "raw"}:
        return _format_tool_result("search", _fmt_memories(rows))
    return "search: unknown mode, use summary or full"


@mcp.tool(description="Read one memory by numeric id and return its full context.")
def memory_read(id: int, touch: bool = False) -> str:
    rid = _as_int(id, 0)
    if rid <= 0:
        return "memory_read: missing id"
    row = db.get_memory(rid, touch=_as_bool(touch, False), include_context=True)
    if not row:
        return f"memory_read: not found id={rid}"
    full_text = str(row.get("context") or row.get("content") or "").strip()
    return _format_tool_result("memory_read", full_text)


@mcp.tool(description="Save one memory to the Supabase-backed store.")
def memory_save(
    context: str = "",
    content: str = "",
    text: str = "",
    layer: str = "diary",
    importance: int = 3,
    valence: float = 0.0,
    arousal: float = 0.0,
    tags: list[str] | str | None = None,
    is_pending: int = 0,
) -> str:
    full_text, summary_text = _memory_payload_texts(content=content, text=text, context=context)
    if not full_text:
        return "memory_save: missing context (or content/text)"
    mem_id = db.write_memory(
        content=full_text,
        context=full_text,
        search_text=summary_text or None,
        layer=str(layer or "diary"),
        importance=_as_int(importance, 3),
        valence=_as_float(valence, 0.0),
        arousal=_as_float(arousal, 0.0),
        tags=_list_tags(tags),
        is_pending=_as_int(is_pending, 0),
    )
    row = db.get_memory(mem_id, touch=False, include_context=True)
    if not row:
        return f"memory_save: ok id={mem_id}"
    return _format_tool_result("memory_save", _fmt_memory(row))


@mcp.tool(description="Surface recent or breath memories. mode=recent or mode=breath.")
def memory_surface(
    mode: str = "recent",
    limit: int = 10,
    days: int = 3,
    min_importance: int = 3,
) -> str:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "breath":
        row = db.surface_breath(consume=True)
        if not row:
            return "memory_surface breath: no candidate"
        return _format_tool_result("memory_surface breath", _fmt_memory(row))
    if normalized_mode == "recent":
        rows = db.get_recent_important(
            days=_as_int(days, 3),
            min_importance=_as_int(min_importance, 3),
        )
        return _format_tool_result("memory_surface recent", _fmt_memories(rows, limit=_as_int(limit, 10)))
    return "memory_surface: unknown mode, use breath or recent"


@mcp.tool(description="Update a memory by numeric id.")
def memory_update(
    id: int,
    content: str | None = None,
    context: str | None = None,
    text: str | None = None,
    importance: int | None = None,
    valence: float | None = None,
    arousal: float | None = None,
    tags: list[str] | str | None = None,
    is_pending: int | None = None,
    pending_resolved: int | None = None,
    tier: str | None = None,
    status: str | None = None,
    layer: str | None = None,
) -> str:
    rid = _as_int(id, 0)
    if rid <= 0:
        return "memory_update: missing id"
    payload: dict[str, Any] = {}
    if content is not None:
        payload["content"] = content
    if context is not None:
        payload["context"] = context
    if text is not None:
        payload["text"] = text
    if importance is not None:
        payload["importance"] = _as_int(importance, 3)
    if valence is not None:
        payload["valence"] = _as_float(valence, 0.0)
    if arousal is not None:
        payload["arousal"] = _as_float(arousal, 0.0)
    if tags is not None:
        payload["tags"] = _list_tags(tags)
    if is_pending is not None:
        payload["is_pending"] = _as_int(is_pending, 0)
    if pending_resolved is not None:
        payload["pending_resolved"] = _as_int(pending_resolved, 0)
    if tier is not None:
        payload["tier"] = tier
    if status is not None:
        payload["status"] = status
    if layer is not None:
        payload["layer"] = layer
    if not payload:
        return "memory_update: no fields to update"
    ok = db.update_memory(rid, **payload)
    return f"memory_update: {'ok' if ok else 'failed'} id={rid}"


def main() -> None:
    logger.info(
        "Starting remote MCP server %s on %s:%s%s",
        SERVER_ID,
        MCP_HOST,
        MCP_PORT,
        MCP_PATH,
    )
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
