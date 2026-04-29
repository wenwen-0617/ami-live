"""
storage/postgres_backend.py

Postgres-backed storage adapter for AmI.
"""

from __future__ import annotations

import json
import hashlib
import logging
import math
import os
import queue
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from storage.base import WoStorageBase
from storage.embeddings import get_embedding

DECAY_LAMBDA = 0.05
logger = logging.getLogger(__name__)

_XML_TAG_RE = re.compile(r"<[^>]+>")
_INTEREST_REF_RE = re.compile(r"(?i)\binterest:([a-z0-9][a-z0-9_-]*)\b")


def _strip_xml(text: str | None) -> str | None:
    if not text:
        return text
    cleaned = _XML_TAG_RE.sub("", text).strip()
    return cleaned or None


def _normalize_interest_label(text: str | None) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", raw)


def _looks_placeholder_corruption(text: str | None) -> bool:
    if not text:
        return False
    cleaned = str(text).strip()
    if not cleaned:
        return False
    if "\ufffd" in cleaned:
        return True

    visible = [ch for ch in cleaned if not ch.isspace()]
    if len(visible) < 4:
        return False

    question_marks = sum(1 for ch in visible if ch in {"?", "？"})
    if question_marks < 3:
        return False

    return question_marks >= max(3, len(visible) // 2)


def _is_qa_source(value: str | None) -> bool:
    return str(value or "").strip().lower() == "qa"


DECAY_BASE = 0.5
DECAY_AROUSAL_BOOST = 0.5
DECAY_ARCHIVE_THRESHOLD = 0.3
MERGE_THRESHOLD = 0.72
EDGE_SEMANTIC_THRESHOLD = 0.72
EDGE_CONFIRM_MIN_SESSIONS = 2
EDGE_CONFIDENCE_THRESHOLD = {
    "co_occurrence": 0.58,
    "semantic": 0.72,
    "manual": 0.9,
}
EDGE_COOCCURRENCE_DELTA = 0.12
EDGE_SEMANTIC_DELTA = 0.22
EDGE_CANDIDATE_TTL_DAYS = 14
EDGE_EVIDENCE_TTL_DAYS = 45
EDGE_VALIDATION_WINDOW_DAYS = 30
EDGE_SESSION_PREFIX = "runtime"
DREAM_PASS_JOB_NAME = "memory_dream_pass"
INTEREST_STAGE_ORDER = ("seed", "sprout", "growing", "ripe", "fruit")

_INSIGHT_NORMALIZE_RE = re.compile(r"[^\w\u4e00-\u9fff]+")
_WORD_RE = re.compile(r"[a-zA-Z_]{2,}|[0-9]{2,}|[\u4e00-\u9fff]{2,}")
_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "about",
    "have", "has", "was", "were", "will", "would", "could", "should", "can",
    "then", "than", "them", "they", "you", "your", "our", "not", "are", "but",
    "或", "以及", "一个", "这个", "那个", "进行", "已经", "需要", "可以", "因为", "所以", "如果",
    "阶段", "深度", "触达", "下一步", "近期", "进展",
}


def _normalize_dsn(dsn: str) -> str:
    def _env_int(name: str, default: int) -> int:
        raw = str(os.environ.get(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except Exception:
            return default

    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    query.setdefault("connect_timeout", "8")
    # Fast-fail for interactive chat: avoid getting stuck on long waits/locks.
    if "options" not in query:
        statement_timeout_ms = _env_int("AMI_DB_STATEMENT_TIMEOUT_MS", 12000)
        lock_timeout_ms = _env_int("AMI_DB_LOCK_TIMEOUT_MS", 3000)
        opts: list[str] = []
        if statement_timeout_ms > 0:
            opts.extend(["-c", f"statement_timeout={statement_timeout_ms}"])
        if lock_timeout_ms > 0:
            opts.extend(["-c", f"lock_timeout={lock_timeout_ms}"])
        if opts:
            query["options"] = " ".join(opts)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


class PostgresBackend(WoStorageBase):
    MAX_EDGE_WEIGHT = 10.0

    # 队列容量：最多缓 50 个待构建任务；满了直接丢弃（只记日志），不阻塞 search 主链路
    _EDGE_QUEUE_MAXSIZE = 50
    # 每次 search 只取前 N 条参与配对，C(5,2)=10 对；避免 O(n²) 爆炸
    _EDGE_TOP_K = 5

    def __init__(self, dsn: str):
        self._dsn = _normalize_dsn(dsn)
        self._conn = None
        self._schema_ready = False
        self._auto_schema_ensure = _env_flag("AMI_AUTO_SCHEMA_ENSURE", False)
        self._memory_read_columns_cache: str | None = None
        runtime_tag = int(time.time())
        self._runtime_session_id = (
            str(os.environ.get("AMI_EDGE_SESSION_ID") or "").strip()
            or f"{EDGE_SESSION_PREFIX}:{os.getpid()}:{runtime_tag}"
        )

        # 边构建异步队列 + 单 worker 线程
        # worker 持有独立连接，与主线程 self._conn 完全隔离，无并发竞争
        self._edge_queue: queue.Queue = queue.Queue(maxsize=self._EDGE_QUEUE_MAXSIZE)
        self._edge_worker_stop = threading.Event()   # P1: 优雅停止信号
        self._edge_worker_conn = None                # P1: 供 close() 关闭
        self._evidence_constraint_ensured = False    # P0: worker 首次连接时迁移
        self._edge_worker_thread = threading.Thread(
            target=self._edge_worker_loop, daemon=True, name="ami-edge-worker"
        )
        self._edge_worker_thread.start()

    def _connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
            self._schema_ready = False
        if not self._auto_schema_ensure:
            self._schema_ready = True
            return self._conn
        if not self._schema_ready:
            self._schema_ready = True
            try:
                self._ensure_memory_graph_schema()
                self._ensure_interest_workspace_schema()
            except Exception:
                self._schema_ready = False
                raise
        return self._conn

    def close(self) -> None:
        # P1: 先停 worker —— 发 sentinel None 让阻塞的 queue.get() 解锁
        self._edge_worker_stop.set()
        try:
            self._edge_queue.put_nowait(None)   # sentinel
        except queue.Full:
            pass
        if self._edge_worker_thread.is_alive():
            self._edge_worker_thread.join(timeout=5)
        # 关 worker 专用连接
        wc = getattr(self, "_edge_worker_conn", None)
        if wc is not None and not wc.closed:
            try:
                wc.close()
            except Exception:
                pass
        # 关主连接
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    # ------------------------------------------------------------------
    # 边构建异步 worker —— 独立连接，与主线程完全隔离
    # ------------------------------------------------------------------

    def _ensure_evidence_constraint(self, conn) -> None:
        """幂等迁移：为已存在的 memory_edge_evidence 补唯一约束。
        直接走 worker 连接执行，与 AMI_AUTO_SCHEMA_ENSURE 完全无关。"""
        if self._evidence_constraint_ensured:
            return
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'uq_evidence_pair_session_type'
                        ) THEN
                            ALTER TABLE memory_edge_evidence
                                ADD CONSTRAINT uq_evidence_pair_session_type
                                UNIQUE (from_id, to_id, edge_type, session_id);
                        END IF;
                    END $$
                """)
            conn.commit()
            self._evidence_constraint_ensured = True
            logger.info("ami-edge-worker: uq_evidence_pair_session_type ensured")
        except Exception:
            logger.warning("ami-edge-worker: failed to ensure evidence constraint", exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass

    def _edge_worker_loop(self) -> None:
        """单 worker 线程：从队列取任务，串行批量写边，持有独立 DB 连接。"""
        worker_conn = None
        while not self._edge_worker_stop.is_set():   # P1: 检查停止信号
            try:
                task = self._edge_queue.get(timeout=1)  # P1: 带超时，避免永久阻塞
            except queue.Empty:
                continue
            if task is None:            # P1: sentinel，主动退出
                self._edge_queue.task_done()
                break
            try:
                # 懒连接 + 断线重连
                if worker_conn is None or worker_conn.closed:
                    worker_conn = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
                    self._edge_worker_conn = worker_conn   # P1: 暴露给 close()
                    self._ensure_evidence_constraint(worker_conn)  # P0: 幂等迁移
                memory_ids, query, vec_sim, session_id = task
                self._batch_build_edge_candidates(worker_conn, memory_ids, query, vec_sim, session_id)
            except Exception:
                logger.exception("ami-edge-worker: edge batch failed")
                if worker_conn is not None and not worker_conn.closed:
                    try:
                        worker_conn.rollback()
                    except Exception:
                        pass
                worker_conn = None  # 下次重新连
                self._edge_worker_conn = None
            finally:
                self._edge_queue.task_done()

    def _batch_build_edge_candidates(
        self,
        conn,
        memory_ids: List[int],
        query: str,
        vec_similarity_by_id: Dict[int, float],
        session_id: str | None,
    ) -> None:
        """用 execute_values 批量 upsert 边 + evidence，2 次 DB 往返完成整批。"""
        # 去重、保序
        seen: set[int] = set()
        ids: List[int] = []
        for raw in memory_ids:
            try:
                iid = int(raw)
            except Exception:
                continue
            if iid not in seen:
                seen.add(iid)
                ids.append(iid)

        if len(ids) < 2:
            return

        # 获取 context（用于 Jaccard），worker 用自己的连接
        placeholders = ",".join(["%s"] * len(ids))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, COALESCE(NULLIF(context, ''), content, '') AS ctx
                FROM memories WHERE id IN ({placeholders})
                """,
                ids,
            )
            context_map: Dict[int, str] = {
                int(r["id"]): str(r.get("ctx") or "") for r in cur.fetchall()
            }

        similarity_map = vec_similarity_by_id or {}
        ids_json = json.dumps(ids, ensure_ascii=False)

        edge_rows: list = []
        evidence_rows: list = []

        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                left, right = ids[i], ids[j]
                overlap = self._jaccard(context_map.get(left, ""), context_map.get(right, ""))

                semantic_similarity: float | None = None
                if left in similarity_map and right in similarity_map:
                    semantic_similarity = self._clamp01(
                        (similarity_map[left] + similarity_map[right]) / 2.0
                    )

                co_conf = self._edge_confidence_from_aggregate(
                    "co_occurrence", None, overlap, 1, 1
                )

                # co_occurrence 双向
                for from_id, to_id in ((left, right), (right, left)):
                    edge_rows.append((
                        from_id, to_id,
                        EDGE_COOCCURRENCE_DELTA, "co_occurrence", "candidate",
                        co_conf, 1, 1, session_id,
                    ))
                    evidence_rows.append((
                        from_id, to_id,
                        "co_occurrence", session_id, "search",
                        query, ids_json,
                        semantic_similarity, overlap, EDGE_COOCCURRENCE_DELTA,
                    ))

                # semantic 双向（仅相似度达阈值时）
                if semantic_similarity is not None and semantic_similarity >= EDGE_SEMANTIC_THRESHOLD:
                    sem_conf = max(
                        self._edge_confidence_from_aggregate(
                            "semantic", semantic_similarity, overlap, 1, 1
                        ),
                        semantic_similarity,
                    )
                    for from_id, to_id in ((left, right), (right, left)):
                        edge_rows.append((
                            from_id, to_id,
                            EDGE_SEMANTIC_DELTA, "semantic", "candidate",
                            sem_conf, 1, 1, session_id,
                        ))
                        evidence_rows.append((
                            from_id, to_id,
                            "semantic", session_id, "search",
                            query, ids_json,
                            semantic_similarity, overlap, EDGE_SEMANTIC_DELTA,
                        ))

        if not edge_rows:
            return

        with conn.cursor() as cur:
            # 一次批量 upsert 所有边（模板里 NOW() 直接写死，search 路径 record_evidence 恒为 True）
            execute_values(
                cur,
                f"""
                INSERT INTO memory_edges
                    (from_id, to_id, weight, edge_type, state, confidence,
                     evidence_count, distinct_sessions,
                     first_evidence_at, last_evidence_at, last_session_id,
                     created_at, updated_at)
                VALUES %s
                ON CONFLICT (from_id, to_id) DO UPDATE SET
                    weight = LEAST(
                        memory_edges.weight + EXCLUDED.weight,
                        {self.MAX_EDGE_WEIGHT}
                    ),
                    edge_type = CASE
                        WHEN memory_edges.edge_type='manual' OR EXCLUDED.edge_type='manual' THEN 'manual'
                        WHEN memory_edges.edge_type='semantic' OR EXCLUDED.edge_type='semantic' THEN 'semantic'
                        ELSE 'co_occurrence'
                    END,
                    state = CASE
                        WHEN memory_edges.state='confirmed' OR EXCLUDED.state='confirmed' THEN 'confirmed'
                        ELSE 'candidate'
                    END,
                    confidence = GREATEST(memory_edges.confidence, EXCLUDED.confidence),
                    evidence_count = memory_edges.evidence_count + EXCLUDED.evidence_count,
                    distinct_sessions = CASE
                        WHEN EXCLUDED.last_session_id IS NULL THEN memory_edges.distinct_sessions
                        WHEN memory_edges.last_session_id IS NULL
                            THEN GREATEST(memory_edges.distinct_sessions, 1)
                        WHEN memory_edges.last_session_id = EXCLUDED.last_session_id
                            THEN memory_edges.distinct_sessions
                        ELSE memory_edges.distinct_sessions + 1
                    END,
                    first_evidence_at = COALESCE(
                        memory_edges.first_evidence_at, EXCLUDED.first_evidence_at
                    ),
                    last_evidence_at = COALESCE(
                        EXCLUDED.last_evidence_at, memory_edges.last_evidence_at
                    ),
                    last_session_id = COALESCE(
                        EXCLUDED.last_session_id, memory_edges.last_session_id
                    ),
                    updated_at = NOW()
                """,
                edge_rows,
                # 9 个 %s 参数，first/last_evidence_at 用 NOW() 字面量
                template="(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),%s,NOW(),NOW())",
            )

            # 一次批量 insert evidence
            # NOTE: created_at 由 DB DEFAULT NOW() 填充，不在 tuple 里传值
            execute_values(
                cur,
                """
                INSERT INTO memory_edge_evidence
                    (from_id, to_id, edge_type, session_id, source, query_text,
                     memory_ids_json, similarity_score, overlap_score,
                     weight_delta)
                VALUES %s
                ON CONFLICT (from_id, to_id, edge_type, session_id) DO NOTHING
                """,
                evidence_rows,
            )

        conn.commit()
        logger.debug(
            "ami-edge-worker: committed %d edge rows, %d evidence rows for query=%r",
            len(edge_rows), len(evidence_rows), query,
        )

    def _fetchall(self, sql: str, params=None) -> List[Dict]:
        for attempt in range(2):
            try:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params or ())
                        return [dict(row) for row in cur.fetchall()]
            except psycopg2.OperationalError:
                if attempt == 0:
                    self._conn = None  # force reconnect
                else:
                    raise

    def _fetchone(self, sql: str, params=None) -> Optional[Dict]:
        for attempt in range(2):
            try:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params or ())
                        row = cur.fetchone()
                        return dict(row) if row else None
            except psycopg2.OperationalError:
                if attempt == 0:
                    self._conn = None
                else:
                    raise

    def _execute(self, sql: str, params=None) -> int:
        for attempt in range(2):
            try:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params or ())
                        return cur.rowcount
            except psycopg2.OperationalError:
                if attempt == 0:
                    self._conn = None
                else:
                    raise

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self._fetchall(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            """,
            (table_name,),
        )
        return {str(row.get("column_name")) for row in rows}

    def _memory_read_columns(self) -> str:
        if self._memory_read_columns_cache:
            return self._memory_read_columns_cache
        preferred = [
            "id", "text", "context", "content", "layer", "importance", "valence", "arousal",
            "tags", "stance", "silence_note", "is_pending", "pending_resolved", "pinned",
            "do_not_surface", "status", "recall_count", "last_recalled", "read_by_wen",
            "tier", "created_at", "updated_at",
        ]
        existing = self._table_columns("memories")
        columns = [col for col in preferred if col in existing]
        if not columns:
            columns = ["id", "content"]
        self._memory_read_columns_cache = ", ".join(columns)
        return self._memory_read_columns_cache

    @staticmethod
    def _build_search_text(content: str, limit: int = 240) -> str:
        base = re.sub(r"\s+", " ", str(content or "")).strip()
        if not base:
            return ""
        if len(base) <= limit:
            return base
        return base[:limit].rstrip() + "..."

    @classmethod
    def _coerce_memory_text_fields(
        cls,
        *,
        content: str | None = None,
        text: str | None = None,
        context: str | None = None,
    ) -> tuple[str, str]:
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

        if full_text and not summary_text:
            summary_text = cls._build_search_text(full_text)

        return summary_text, full_text

    def _normalize_memory_row(self, row: Optional[Dict], include_context: bool = False) -> Optional[Dict]:
        if not row:
            return row
        row = dict(row)
        context = str(row.get("context") or row.get("content") or "")
        text = str(row.get("text") or "").strip()
        if not text:
            text = self._build_search_text(context)
        row["text"] = text
        if include_context:
            row["context"] = context
        else:
            row.pop("context", None)
        row.pop("content", None)
        return row

    def _normalize_memory_rows(self, rows: List[Dict], include_context: bool = False) -> List[Dict]:
        return [self._normalize_memory_row(row, include_context=include_context) for row in rows]

    def _ensure_memory_graph_schema(self) -> None:
        if not self._fetchone(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='memories'
            """
        ):
            return
        self._execute("ALTER TABLE memories ADD COLUMN IF NOT EXISTS text TEXT")
        self._execute("ALTER TABLE memories ADD COLUMN IF NOT EXISTS context TEXT")
        self._execute("UPDATE memories SET context=content WHERE context IS NULL OR context=''")
        self._execute(
            """
            UPDATE memories
            SET text=LEFT(REGEXP_REPLACE(COALESCE(context, content, ''), '\\s+', ' ', 'g'), 240)
            WHERE text IS NULL OR text=''
            """
        )
        self._execute(
            """
            UPDATE memories
            SET content=context
            WHERE COALESCE(context, '') <> ''
              AND COALESCE(content, '') <> COALESCE(context, '')
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS memory_edges (
                from_id BIGINT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                to_id BIGINT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                weight FLOAT NOT NULL DEFAULT 0.2,
                edge_type TEXT NOT NULL DEFAULT 'co_occurrence',
                state TEXT NOT NULL DEFAULT 'candidate',
                confidence FLOAT NOT NULL DEFAULT 0.0,
                evidence_count INTEGER NOT NULL DEFAULT 0,
                distinct_sessions INTEGER NOT NULL DEFAULT 0,
                first_evidence_at TIMESTAMPTZ,
                last_evidence_at TIMESTAMPTZ,
                last_session_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (from_id, to_id),
                CHECK (from_id <> to_id)
            )
            """
        )
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'candidate'")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS confidence FLOAT NOT NULL DEFAULT 0.0")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS evidence_count INTEGER NOT NULL DEFAULT 0")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS distinct_sessions INTEGER NOT NULL DEFAULT 0")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS first_evidence_at TIMESTAMPTZ")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS last_evidence_at TIMESTAMPTZ")
        self._execute("ALTER TABLE memory_edges ADD COLUMN IF NOT EXISTS last_session_id TEXT")
        # One-time cleanup for legacy Hebbian edges: rebuild from fresh evidence.
        self._execute("DELETE FROM memory_edges WHERE edge_type='hebbian'")
        self._execute(
            """
            UPDATE memory_edges
            SET edge_type = CASE
                    WHEN edge_type='manual' THEN 'manual'
                    WHEN edge_type='semantic' THEN 'semantic'
                    ELSE 'co_occurrence'
                END
            """
        )
        self._execute(
            """
            UPDATE memory_edges
            SET state = CASE
                    WHEN edge_type='manual' THEN 'confirmed'
                    WHEN state IN ('candidate', 'confirmed') THEN state
                    ELSE 'candidate'
                END
            """
        )
        self._execute(
            """
            UPDATE memory_edges
            SET confidence = CASE
                    WHEN edge_type='manual' THEN GREATEST(confidence, 0.95)
                    ELSE GREATEST(confidence, LEAST(1.0, weight / %s))
                END
            """,
            (self.MAX_EDGE_WEIGHT,),
        )
        self._execute(
            """
            UPDATE memory_edges
            SET first_evidence_at = COALESCE(first_evidence_at, created_at),
                last_evidence_at  = COALESCE(last_evidence_at, updated_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_edges_from_weight
            ON memory_edges(from_id, weight DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_edges_updated_at
            ON memory_edges(updated_at DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_edges_state_type
            ON memory_edges(state, edge_type, last_evidence_at DESC)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS memory_edge_evidence (
                id BIGSERIAL PRIMARY KEY,
                from_id BIGINT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                to_id BIGINT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                edge_type TEXT NOT NULL DEFAULT 'co_occurrence',
                session_id TEXT,
                source TEXT NOT NULL DEFAULT 'search',
                query_text TEXT,
                memory_ids_json TEXT,
                similarity_score FLOAT,
                overlap_score FLOAT,
                weight_delta FLOAT NOT NULL DEFAULT 0.0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                -- P2: 唯一约束，让 ON CONFLICT 去重真正生效
                CONSTRAINT uq_evidence_pair_session_type
                    UNIQUE (from_id, to_id, edge_type, session_id)
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_edge_evidence_pair_created
            ON memory_edge_evidence(from_id, to_id, created_at DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_edge_evidence_session
            ON memory_edge_evidence(session_id, created_at DESC)
            """
        )
        # P2 迁移：为已存在的 memory_edge_evidence 表补唯一约束
        # IF NOT EXISTS 语法 Postgres 9.x 不支持，用 DO $$ 块幂等处理
        self._execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_evidence_pair_session_type'
                ) THEN
                    ALTER TABLE memory_edge_evidence
                        ADD CONSTRAINT uq_evidence_pair_session_type
                        UNIQUE (from_id, to_id, edge_type, session_id);
                END IF;
            END $$
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS job_locks (
                job_name TEXT PRIMARY KEY,
                lock_owner TEXT NOT NULL,
                locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS job_runs (
                id BIGSERIAL PRIMARY KEY,
                job_name TEXT NOT NULL,
                run_owner TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                summary TEXT,
                error TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                ended_at TIMESTAMPTZ
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_runs_job_started
            ON job_runs(job_name, started_at DESC)
            """
        )

    def _ensure_interest_workspace_schema(self) -> None:
        if not self._fetchone(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='interest_pool'
            """
        ):
            return

        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 50")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS next_action TEXT")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS next_review_at TIMESTAMPTZ")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS synthesis TEXT")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS last_synthesized_at TIMESTAMPTZ")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS meta_json TEXT")
        self._execute("ALTER TABLE interest_pool ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        self._execute("UPDATE interest_pool SET status='active' WHERE status IS NULL OR status=''")
        self._execute("UPDATE interest_pool SET priority=50 WHERE priority IS NULL")
        self._execute(
            """
            UPDATE interest_pool
            SET updated_at = COALESCE(updated_at, last_touched, first_touched, created_at, NOW())
            WHERE updated_at IS NULL
            """
        )

        if self._fetchone(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='interest_connections'
            """
        ):
            self._execute(
                "ALTER TABLE interest_connections ADD COLUMN IF NOT EXISTS relation TEXT NOT NULL DEFAULT 'related'"
            )

        if self._fetchone(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='activity_log'
            """
        ):
            cols = self._table_columns("activity_log")
            if "kind" not in cols:
                self._execute("ALTER TABLE activity_log ADD COLUMN kind TEXT")
            if "type" in cols:
                self._execute("UPDATE activity_log SET kind=COALESCE(kind, type, 'activity')")
            self._execute("ALTER TABLE activity_log ALTER COLUMN kind SET DEFAULT 'activity'")
            self._execute("UPDATE activity_log SET kind='activity' WHERE kind IS NULL OR kind=''")
            self._execute("ALTER TABLE activity_log ALTER COLUMN kind SET NOT NULL")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS outcome TEXT")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS energy FLOAT")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS branch TEXT")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS meta_json TEXT")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS insight_hash TEXT")
            self._execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS source_interest_id TEXT")

        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_interest_pool_status_stage
            ON interest_pool(status, stage)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_interest_pool_next_review_at
            ON interest_pool(next_review_at)
            """
        )
        if self._fetchone(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name='activity_log'
            """
        ):
            self._execute(
                """
                CREATE INDEX IF NOT EXISTS idx_activity_log_interest_time
                ON activity_log(interest_id, recorded_at DESC)
                """
            )
            self._execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_activity_log_insight_hash
                ON activity_log(insight_hash)
                WHERE insight_hash IS NOT NULL
                """
            )

    @staticmethod
    def _ngrams(text: str, n: int = 2) -> set:
        text = re.sub(r"\s+", "", text)
        return set(text[i:i + n] for i in range(len(text) - n + 1)) if len(text) >= n else {text}

    @staticmethod
    def _jaccard(a: str, b: str) -> float:
        sa = PostgresBackend._ngrams(a)
        sb = PostgresBackend._ngrams(b)
        if not sa and not sb:
            return 1.0
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)

    @staticmethod
    def _clamp01(value: float) -> float:
        try:
            v = float(value)
        except Exception:
            v = 0.0
        if v < 0.0:
            return 0.0
        if v > 1.0:
            return 1.0
        return v

    @staticmethod
    def _cosine_similarity_from_distance(distance: float | None) -> float | None:
        if distance is None:
            return None
        try:
            dist = float(distance)
        except Exception:
            return None
        return PostgresBackend._clamp01(1.0 - dist)

    @staticmethod
    def _edge_priority(edge_type: str) -> int:
        t = str(edge_type or "").strip().lower()
        if t == "manual":
            return 0
        if t == "semantic":
            return 1
        return 2

    @staticmethod
    def _edge_confidence_from_aggregate(edge_type: str,
                                        avg_similarity: float | None,
                                        avg_overlap: float | None,
                                        evidence_count: int,
                                        distinct_sessions: int) -> float:
        t = str(edge_type or "co_occurrence").strip().lower()
        evidence_boost = PostgresBackend._clamp01(math.log1p(max(0, int(evidence_count or 0))) / math.log(6))
        session_boost = PostgresBackend._clamp01(max(0, int(distinct_sessions or 0)) / 3.0)
        similarity = PostgresBackend._clamp01(avg_similarity or 0.0)
        overlap = PostgresBackend._clamp01(avg_overlap or 0.0)
        if t == "manual":
            return 1.0
        if t == "semantic":
            return PostgresBackend._clamp01(0.65 * similarity + 0.2 * session_boost + 0.15 * evidence_boost)
        return PostgresBackend._clamp01(0.5 * overlap + 0.3 * session_boost + 0.2 * evidence_boost)

    def _memory_context_map(self, memory_ids: List[int]) -> Dict[int, str]:
        ids = [int(x) for x in memory_ids if x is not None]
        if not ids:
            return {}
        placeholders = ",".join(["%s"] * len(ids))
        rows = self._fetchall(
            f"""
            SELECT id, COALESCE(NULLIF(context, ''), content, '') AS context_text
            FROM memories
            WHERE id IN ({placeholders})
            """,
            ids,
        )
        out: Dict[int, str] = {}
        for row in rows:
            try:
                out[int(row["id"])] = str(row.get("context_text") or "")
            except Exception:
                continue
        return out

    def find_similar(self, content: str, threshold: float = MERGE_THRESHOLD,
                     limit: int = 20) -> Optional[Dict]:
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE status='active' AND layer NOT IN ('anchor','note')
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        best_sim = 0.0
        best_mem = None
        for mem in rows:
            sim = self._jaccard(content, mem["content"])
            if sim > best_sim:
                best_sim = sim
                best_mem = mem
        return best_mem if best_sim >= threshold else None

    def _merge_into(self, target_id: int, new_content: str,
                    valence: float, arousal: float,
                    importance: int, tags: List[str] = None) -> int:
        existing = self._fetchone(
            f"SELECT {self._memory_read_columns()} FROM memories WHERE id=%s",
            (target_id,),
        )
        if not existing:
            return target_id
        base_context = str(existing.get("context") or existing.get("content") or "")
        merged_content = base_context + "\n---\n" + new_content
        merged_text = self._build_search_text(merged_content)
        merged_valence = valence if abs(valence) > abs(existing["valence"]) else existing["valence"]
        merged_arousal = max(arousal, existing["arousal"])
        merged_importance = max(importance, existing["importance"])
        existing_tags = json.loads(existing["tags"]) if existing.get("tags") else []
        merged_tags = list(set(existing_tags + (tags or [])))
        self._execute(
            """
            UPDATE memories
            SET content=%s, context=%s, text=%s,
                valence=%s, arousal=%s, importance=%s,
                tags=%s, recall_count=recall_count+1, last_recalled=NOW()
            WHERE id=%s
            """,
            (
                merged_content,
                merged_content,
                merged_text,
                merged_valence,
                merged_arousal,
                merged_importance,
                json.dumps(merged_tags, ensure_ascii=False) if merged_tags else None,
                target_id,
            ),
        )
        try:
            vec = get_embedding(merged_text or merged_content)
            if vec is not None:
                self._execute(
                    "UPDATE memories SET embedding = %s::vector WHERE id = %s",
                    (f"[{','.join(str(v) for v in vec)}]", target_id),
                )
        except Exception:
            logger.warning("failed to update merged embedding for memory_id=%s", target_id, exc_info=True)
        return target_id

    def _auto_touch_interests(self, content: str, tags: List[str] = None):
        interests = self._fetchall("SELECT id, name FROM interest_pool")
        explicit_ids = {
            str(match.group(1) or "").strip().lower()
            for match in _INTEREST_REF_RE.finditer(str(content or ""))
            if str(match.group(1) or "").strip()
        }
        tag_tokens: set[str] = set()
        for tag in tags or []:
            raw = str(tag or "").strip()
            if not raw:
                continue
            tag_tokens.add(raw.lower())
            normalized = _normalize_interest_label(raw)
            if normalized:
                tag_tokens.add(normalized)

        matched_ids: set[str] = set()
        for item in interests:
            iid = str(item.get("id") or "").strip()
            if not iid:
                continue
            normalized_name = _normalize_interest_label(item.get("name"))
            iid_lower = iid.lower()
            if (
                iid_lower in explicit_ids
                or iid_lower in tag_tokens
                or (normalized_name and normalized_name in tag_tokens)
            ):
                matched_ids.add(iid)

        for iid in matched_ids:
            # Memory references are weak signals; only nudge depth on explicit links.
            self.interest_touch(iid, depth_delta=0.02, notes=None, _from_memory=True)

    def get_anchors(self) -> List[Dict]:
        rows = self._fetchall(
            f"SELECT {self._memory_read_columns()} FROM memories WHERE layer='anchor' AND status='active' ORDER BY id"
        )
        return self._normalize_memory_rows(rows)

    def get_recent_important(self, days: int = 3,
                             min_importance: int = 3) -> List[Dict]:
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE importance >= %s
              AND status = 'active'
              AND layer != 'anchor'
              AND created_at >= NOW() - (%s || ' days')::interval
            ORDER BY importance DESC, created_at DESC
            LIMIT 10
            """,
            (min_importance, str(days)),
        )
        return self._normalize_memory_rows(rows)

    def get_recent_memories(self, hours: int = 24, limit: int = 6,
                            min_importance: int = 1) -> List[Dict]:
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE status = 'active'
              AND layer NOT IN ('anchor', 'note')
              AND importance >= %s
              AND created_at >= NOW() - (%s || ' hours')::interval
            ORDER BY created_at DESC, importance DESC
            LIMIT %s
            """,
            (min_importance, str(max(1, int(hours or 24))), max(1, int(limit or 6))),
        )
        return self._normalize_memory_rows(rows)

    def get_unread_comments(self) -> List[Dict]:
        return self._fetchall(
            """
            SELECT c.*, COALESCE(NULLIF(m.text, ''), m.content) AS memory_content
            FROM comments c
            JOIN memories m ON c.memory_id = m.id
            WHERE c.is_read=0 AND c.author='wen'
            ORDER BY c.written_at
            """
        )

    def write_memory(self, content: str, layer: str = "diary",
                     importance: int = 3, valence: float = 0.0,
                     arousal: float = 0.0, tags=None, stance: str = None,
                     silence_note: str = None, is_pending: int = 0,
                     do_not_surface: int = 0,
                     search_text: str | None = None,
                     context: str | None = None) -> int:
        summary_text, full_context = self._coerce_memory_text_fields(
            content=content,
            text=search_text,
            context=context,
        )
        if layer == "anchor":
            importance = 5
            pinned = 1
        elif layer == "treasure":
            pinned = 1
        else:
            pinned = 0

        if layer not in ("anchor", "note"):
            similar = self.find_similar(full_context)
            if similar:
                return self._merge_into(similar["id"], full_context, valence, arousal, importance, tags)

        tags_json = json.dumps(tags, ensure_ascii=False) if tags else None
        new_id = None
        for attempt in range(2):
            try:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO memories
                                (content, text, context, layer, importance, valence, arousal, tags,
                                 stance, silence_note, is_pending, pinned, do_not_surface)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            RETURNING id
                            """,
                            (
                                full_context, summary_text, full_context,
                                layer, importance, valence, arousal, tags_json,
                                stance, silence_note, is_pending, pinned, do_not_surface,
                            ),
                        )
                        new_id = cur.fetchone()["id"]
                break
            except psycopg2.OperationalError:
                if attempt == 0:
                    self._conn = None
                    continue
                raise
        if new_id is None:
            raise psycopg2.OperationalError("write_memory failed: no inserted id returned")
        # Generate and persist embedding for the new memory (fire-and-forget thread)
        # P2: get_embedding() 调外部 API 约 2~4s，改为后台线程，不阻塞 save 返回
        def _write_embedding(mid, text, dsn):
            try:
                vec = get_embedding(text)
                if vec is None:
                    return
                vec_str = f"[{','.join(str(v) for v in vec)}]"
                conn = psycopg2.connect(dsn)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE memories SET embedding = %s::vector WHERE id = %s",
                            (vec_str, mid),
                        )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                logger.warning("failed to write embedding for memory_id=%s", mid, exc_info=True)

        threading.Thread(
            target=_write_embedding,
            args=(new_id, summary_text or full_context, self._dsn),
            daemon=True,
            name=f"ami-embed-{new_id}",
        ).start()
        try:
            self._auto_touch_interests(full_context, tags)
        except Exception:
            logger.warning("failed to auto-touch interests for memory_id=%s", new_id, exc_info=True)
        if stance:
            try:
                self._execute(
                    "INSERT INTO stance_log (memory_id, content, prev_content) VALUES (%s,%s,%s)",
                    (new_id, stance, None),
                )
            except Exception:
                logger.warning("failed to append stance_log for memory_id=%s", new_id, exc_info=True)
        return new_id

    def get_memories(self, layer: str = None, status: str = "active",
                     limit: int = 200) -> List[Dict]:
        if layer:
            rows = self._fetchall(
                f"""
                SELECT {self._memory_read_columns()} FROM memories
                WHERE layer=%s AND status=%s
                ORDER BY pinned DESC, importance DESC, created_at DESC
                LIMIT %s
                """,
                (layer, status, limit),
            )
            return self._normalize_memory_rows(rows)
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE status=%s
            ORDER BY pinned DESC, importance DESC, created_at DESC
            LIMIT %s
            """,
            (status, limit),
        )
        return self._normalize_memory_rows(rows)

    def list_pending(self) -> List[Dict]:
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE is_pending=1 AND pending_resolved=0 AND status='active'
            ORDER BY arousal DESC, created_at DESC
            """
        )
        return self._normalize_memory_rows(rows)

    def get_memory(self, id: int, touch: bool = True,
                   include_context: bool = False) -> Optional[Dict]:
        row = self._fetchone(
            f"SELECT {self._memory_read_columns()} FROM memories WHERE id=%s",
            (id,),
        )
        if row and touch:
            self._execute(
                """
                UPDATE memories
                SET recall_count = recall_count + 1,
                    last_recalled = NOW()
                WHERE id=%s
                """,
                (id,),
            )
            row = self._fetchone(
                f"SELECT {self._memory_read_columns()} FROM memories WHERE id=%s",
                (id,),
            )
        return self._normalize_memory_row(row, include_context=include_context)

    def update_memory(self, id: int, **kwargs) -> bool:
        allowed = [
            "content", "layer", "importance", "valence", "arousal", "tags", "stance",
            "silence_note", "is_pending", "pending_resolved", "status", "pinned",
            "do_not_surface", "tier", "text", "context",
        ]
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if "content" in updates or "context" in updates:
            summary_text, full_context = self._coerce_memory_text_fields(
                content=updates.get("content"),
                text=updates.get("text"),
                context=updates.get("context"),
            )
            updates["context"] = full_context
            updates["content"] = full_context
            updates["text"] = summary_text
        elif "text" in updates:
            updates["text"] = str(updates.get("text") or "").strip()
        if updates.get("layer") == "anchor":
            updates.setdefault("importance", 5)
            updates.setdefault("pinned", 1)
        elif updates.get("layer") == "treasure":
            updates.setdefault("pinned", 1)
        if not updates:
            return False
        if "tags" in updates and isinstance(updates["tags"], list):
            updates["tags"] = json.dumps(updates["tags"], ensure_ascii=False)
        prev_stance = None
        should_log_stance = "stance" in updates and bool(updates["stance"])
        if should_log_stance:
            prev = self._fetchone("SELECT stance FROM memories WHERE id=%s", (id,))
            prev_stance = prev["stance"] if prev else None
        columns = list(updates.keys())
        params = [updates[col] for col in columns] + [id]
        set_clause = ", ".join(f"{col}=%s" for col in columns)
        result = self._execute(f"UPDATE memories SET {set_clause} WHERE id=%s", params) > 0
        if result and should_log_stance:
            try:
                self._execute(
                    "INSERT INTO stance_log (memory_id, content, prev_content) VALUES (%s,%s,%s)",
                    (id, updates["stance"], prev_stance),
                )
            except Exception:
                logger.warning("failed to append stance_log for memory_id=%s", id, exc_info=True)
        if result and any(k in updates for k in ("content", "context", "text")):
            try:
                source = str(updates.get("text") or updates.get("context") or updates.get("content") or "")
                vec = get_embedding(source)
                if vec is not None:
                    self._execute(
                        "UPDATE memories SET embedding = %s::vector WHERE id = %s",
                        (f"[{','.join(str(v) for v in vec)}]", id),
                    )
            except Exception:
                logger.warning("failed to update embedding for memory_id=%s", id, exc_info=True)
        return result

    def get_unread_by_wen(self) -> List[Dict]:
        rows = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE read_by_wen=0 AND status='active'
            ORDER BY created_at DESC
            """
        )
        return self._normalize_memory_rows(rows)

    def mark_read_by_wen(self, ids: Optional[List[int]] = None) -> int:
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            return self._execute(
                f"UPDATE memories SET read_by_wen=1 WHERE id IN ({placeholders}) AND read_by_wen=0",
                ids,
            )
        return self._execute("UPDATE memories SET read_by_wen=1 WHERE read_by_wen=0")

    def surface_breath(self, consume: bool = True) -> Optional[Dict]:
        row = self._fetchone(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE importance >= 2
              AND status = 'active'
              AND do_not_surface = 0
              AND (last_recalled IS NULL OR last_recalled < NOW() - INTERVAL '3 days')
            ORDER BY is_pending DESC, arousal DESC, RANDOM()
            LIMIT 1
            """
        )
        if not row:
            row = self._fetchone(
                f"""
                SELECT {self._memory_read_columns()} FROM memories
                WHERE status='active' AND layer != 'anchor' AND do_not_surface = 0
                ORDER BY RANDOM()
                LIMIT 1
                """
        )
        if row and consume:
            self._execute("UPDATE memories SET last_recalled=NOW() WHERE id=%s", (row["id"],))
        return self._normalize_memory_row(row)

    def search_memories(self, query: str, limit: int = 10, session_id: str | None = None) -> List[Dict]:
        """Hybrid search: vector cosine similarity + text ILIKE, fused with RRF.

        Falls back to pure ILIKE search when OPENAI_API_KEY is absent or the
        embedding call fails, preserving the original behaviour.
        """
        vec = get_embedding(query)
        edge_session = str(session_id or self._runtime_session_id).strip() or self._runtime_session_id

        if vec is None:
            # Graceful degradation: pure text search (original behaviour)
            like = f"%{query}%"
            rows = self._fetchall(
                f"""
                SELECT {self._memory_read_columns()} FROM memories
                WHERE COALESCE(NULLIF(text, ''), context, content) ILIKE %s OR tags ILIKE %s
                ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, importance DESC
                LIMIT %s
                """,
                (like, like, limit),
            )
            row_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
            try:
                self._edge_queue.put_nowait((
                    row_ids[:self._EDGE_TOP_K],
                    query,
                    {},
                    edge_session,
                ))
            except queue.Full:
                logger.debug("ami-edge-worker: queue full, skipping edge build (text-only search)")
            return self._normalize_memory_rows(rows)

        # ---------- Hybrid search with Reciprocal Rank Fusion (k=60) ----------
        vec_literal = f"[{','.join(str(v) for v in vec)}]"
        like = f"%{query}%"
        k = 60

        # Fetch vector candidates (top 4*limit by cosine distance)
        vec_rows = self._fetchall(
            """
            SELECT id, (embedding <=> %s::vector) AS vec_dist
            FROM memories
            WHERE embedding IS NOT NULL
              AND status='active'
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vec_literal, vec_literal, limit * 4),
        )

        # Fetch text candidates
        text_rows = self._fetchall(
            """
            SELECT id
            FROM memories
            WHERE COALESCE(NULLIF(text, ''), context, content) ILIKE %s OR tags ILIKE %s
            ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, importance DESC
            LIMIT %s
            """,
            (like, like, limit * 4),
        )

        vec_similarity_by_id: Dict[int, float] = {}
        for vrow in vec_rows:
            try:
                vid = int(vrow["id"])
            except Exception:
                continue
            sim = self._cosine_similarity_from_distance(vrow.get("vec_dist"))
            if sim is not None:
                vec_similarity_by_id[vid] = sim

        # Build RRF score map
        scores: Dict[int, float] = {}
        for rank, row in enumerate(vec_rows, start=1):
            mid = row["id"]
            scores[mid] = scores.get(mid, 0.0) + 1.0 / (k + rank)
        for rank, row in enumerate(text_rows, start=1):
            mid = row["id"]
            scores[mid] = scores.get(mid, 0.0) + 1.0 / (k + rank)

        if not scores:
            return []

        # Take top-limit ids sorted by RRF score
        top_ids = sorted(scores, key=lambda x: scores[x], reverse=True)[:limit]

        # Fetch full rows for those ids, preserving RRF order
        placeholders = ",".join(["%s"] * len(top_ids))
        rows_by_id: Dict[int, Dict] = {
            row["id"]: row
            for row in self._fetchall(
                f"SELECT {self._memory_read_columns()} FROM memories WHERE id IN ({placeholders})",
                top_ids,
            )
        }
        rows = [rows_by_id[mid] for mid in top_ids if mid in rows_by_id]
        row_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
        try:
            self._edge_queue.put_nowait((
                row_ids[:self._EDGE_TOP_K],
                query,
                vec_similarity_by_id,
                edge_session,
            ))
        except queue.Full:
            logger.debug("ami-edge-worker: queue full, skipping edge build (hybrid search)")
        return self._normalize_memory_rows(rows)

    def backfill_embeddings(self, limit: int = 100) -> int:
        """Generate embeddings for memories that don't have one yet.

        Returns the number of memories successfully updated. Safe to call
        multiple times; already-embedded rows are skipped.
        """
        rows = self._fetchall(
            """
            SELECT id, COALESCE(NULLIF(text, ''), context, content) AS content FROM memories
            WHERE embedding IS NULL
            ORDER BY id
            LIMIT %s
            """,
            (limit,),
        )
        updated = 0
        for row in rows:
            try:
                vec = get_embedding(row["content"])
                if vec is None:
                    break  # No API key or persistent failure — stop early
                self._execute(
                    "UPDATE memories SET embedding = %s::vector WHERE id = %s",
                    (f"[{','.join(str(v) for v in vec)}]", row["id"]),
                )
                updated += 1
            except Exception:
                logger.warning("failed to backfill embedding for memory_id=%s", row.get("id"), exc_info=True)
                continue
        return updated

    def _record_edge_evidence(self, from_id: int, to_id: int,
                              edge_type: str,
                              session_id: str | None = None,
                              source: str = "search",
                              query_text: str | None = None,
                              memory_ids: List[int] | None = None,
                              similarity_score: float | None = None,
                              overlap_score: float | None = None,
                              weight_delta: float = 0.0) -> None:
        payload = None
        if memory_ids:
            try:
                payload = json.dumps([int(x) for x in memory_ids], ensure_ascii=False)
            except Exception:
                payload = None
        self._execute(
            """
            INSERT INTO memory_edge_evidence
                (from_id, to_id, edge_type, session_id, source, query_text, memory_ids_json,
                 similarity_score, overlap_score, weight_delta, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                from_id,
                to_id,
                edge_type,
                session_id,
                source,
                query_text,
                payload,
                similarity_score,
                overlap_score,
                weight_delta,
            ),
        )

    def _upsert_memory_edge(self, from_id: int, to_id: int, delta: float,
                            edge_type: str = "co_occurrence",
                            state: str = "candidate",
                            confidence: float | None = None,
                            session_id: str | None = None,
                            source: str = "search",
                            query_text: str | None = None,
                            memory_ids: List[int] | None = None,
                            similarity_score: float | None = None,
                            overlap_score: float | None = None,
                            record_evidence: bool = False) -> None:
        if from_id == to_id:
            return
        edge_type = str(edge_type or "co_occurrence").strip().lower()
        if edge_type not in {"manual", "semantic", "co_occurrence"}:
            edge_type = "co_occurrence"
        state = "confirmed" if str(state or "").strip().lower() == "confirmed" else "candidate"
        conf = self._clamp01(confidence if confidence is not None else 0.0)
        if edge_type == "manual":
            conf = max(conf, 0.95)
            state = "confirmed"

        evidence_inc = 1 if record_evidence else 0
        session_value = str(session_id or "").strip() if record_evidence else None
        distinct_seed = 1 if session_value else 0
        first_evidence_seed = "NOW()" if record_evidence else "NULL"
        last_evidence_seed = "NOW()" if record_evidence else "NULL"
        self._execute(
            f"""
            INSERT INTO memory_edges
                (from_id, to_id, weight, edge_type, state, confidence, evidence_count,
                 distinct_sessions, first_evidence_at, last_evidence_at, last_session_id,
                 created_at, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, {first_evidence_seed}, {last_evidence_seed}, %s, NOW(), NOW())
            ON CONFLICT (from_id, to_id) DO UPDATE SET
                weight = LEAST(memory_edges.weight + EXCLUDED.weight, %s),
                edge_type = CASE
                    WHEN memory_edges.edge_type='manual' OR EXCLUDED.edge_type='manual' THEN 'manual'
                    WHEN memory_edges.edge_type='semantic' OR EXCLUDED.edge_type='semantic' THEN 'semantic'
                    ELSE 'co_occurrence'
                END,
                state = CASE
                    WHEN memory_edges.state='confirmed' OR EXCLUDED.state='confirmed' THEN 'confirmed'
                    ELSE 'candidate'
                END,
                confidence = GREATEST(memory_edges.confidence, EXCLUDED.confidence),
                evidence_count = memory_edges.evidence_count + EXCLUDED.evidence_count,
                distinct_sessions = CASE
                    WHEN EXCLUDED.last_session_id IS NULL THEN memory_edges.distinct_sessions
                    WHEN memory_edges.last_session_id IS NULL THEN GREATEST(memory_edges.distinct_sessions, 1)
                    WHEN memory_edges.last_session_id = EXCLUDED.last_session_id THEN memory_edges.distinct_sessions
                    ELSE memory_edges.distinct_sessions + 1
                END,
                first_evidence_at = COALESCE(memory_edges.first_evidence_at, EXCLUDED.first_evidence_at),
                last_evidence_at = COALESCE(EXCLUDED.last_evidence_at, memory_edges.last_evidence_at),
                last_session_id = COALESCE(EXCLUDED.last_session_id, memory_edges.last_session_id),
                updated_at = NOW()
            """,
            (
                from_id,
                to_id,
                delta,
                edge_type,
                state,
                conf,
                evidence_inc,
                distinct_seed,
                session_value,
                self.MAX_EDGE_WEIGHT,
            ),
        )
        if record_evidence:
            self._record_edge_evidence(
                from_id=from_id,
                to_id=to_id,
                edge_type=edge_type,
                session_id=session_value,
                source=source,
                query_text=query_text,
                memory_ids=memory_ids,
                similarity_score=similarity_score,
                overlap_score=overlap_score,
                weight_delta=delta,
            )

    def _upsert_memory_edge_pair(self, left_id: int, right_id: int, delta: float,
                                 edge_type: str = "co_occurrence",
                                 state: str = "candidate",
                                 confidence: float | None = None,
                                 session_id: str | None = None,
                                 source: str = "search",
                                 query_text: str | None = None,
                                 memory_ids: List[int] | None = None,
                                 similarity_score: float | None = None,
                                 overlap_score: float | None = None,
                                 record_evidence: bool = False) -> None:
        self._upsert_memory_edge(
            left_id, right_id, delta=delta, edge_type=edge_type, state=state, confidence=confidence,
            session_id=session_id, source=source, query_text=query_text, memory_ids=memory_ids,
            similarity_score=similarity_score, overlap_score=overlap_score, record_evidence=record_evidence,
        )
        self._upsert_memory_edge(
            right_id, left_id, delta=delta, edge_type=edge_type, state=state, confidence=confidence,
            session_id=session_id, source=source, query_text=query_text, memory_ids=memory_ids,
            similarity_score=similarity_score, overlap_score=overlap_score, record_evidence=record_evidence,
        )

    def _build_search_edge_candidates(self, memory_ids: List[int],
                                      query: str,
                                      vec_similarity_by_id: Dict[int, float] | None = None,
                                      session_id: str | None = None) -> None:
        ids: List[int] = []
        seen: set[int] = set()
        for raw in memory_ids:
            try:
                iid = int(raw)
            except Exception:
                continue
            if iid in seen:
                continue
            seen.add(iid)
            ids.append(iid)
        if len(ids) < 2:
            return
        similarity_map = vec_similarity_by_id or {}
        context_map = self._memory_context_map(ids)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                left = ids[i]
                right = ids[j]
                left_text = context_map.get(left, "")
                right_text = context_map.get(right, "")
                overlap = self._jaccard(left_text, right_text)

                semantic_similarity = None
                if left in similarity_map and right in similarity_map:
                    semantic_similarity = self._clamp01((similarity_map[left] + similarity_map[right]) / 2.0)

                co_conf = self._edge_confidence_from_aggregate(
                    "co_occurrence", avg_similarity=None, avg_overlap=overlap, evidence_count=1, distinct_sessions=1
                )
                self._upsert_memory_edge_pair(
                    left,
                    right,
                    delta=EDGE_COOCCURRENCE_DELTA,
                    edge_type="co_occurrence",
                    state="candidate",
                    confidence=co_conf,
                    session_id=session_id,
                    source="search",
                    query_text=query,
                    memory_ids=ids,
                    similarity_score=semantic_similarity,
                    overlap_score=overlap,
                    record_evidence=True,
                )
                if semantic_similarity is not None and semantic_similarity >= EDGE_SEMANTIC_THRESHOLD:
                    sem_conf = self._edge_confidence_from_aggregate(
                        "semantic",
                        avg_similarity=semantic_similarity,
                        avg_overlap=overlap,
                        evidence_count=1,
                        distinct_sessions=1,
                    )
                    self._upsert_memory_edge_pair(
                        left,
                        right,
                        delta=EDGE_SEMANTIC_DELTA,
                        edge_type="semantic",
                        state="candidate",
                        confidence=max(sem_conf, semantic_similarity),
                        session_id=session_id,
                        source="search",
                        query_text=query,
                        memory_ids=ids,
                        similarity_score=semantic_similarity,
                        overlap_score=overlap,
                        record_evidence=True,
                    )

    def get_memory_neighbors(self, memory_id: int, min_weight: float = 0.5,
                             limit: int = 5) -> List[Dict]:
        rows = self._fetchall(
            """
            SELECT
                e.to_id AS memory_id,
                e.weight,
                e.edge_type,
                e.state,
                e.confidence,
                e.evidence_count,
                e.distinct_sessions,
                e.updated_at,
                m.layer,
                m.importance,
                COALESCE(NULLIF(m.text, ''), m.content) AS content
            FROM memory_edges e
            JOIN memories m ON m.id = e.to_id
            WHERE e.from_id = %s
              AND e.weight >= %s
              AND m.status = 'active'
            ORDER BY
                CASE e.state WHEN 'confirmed' THEN 0 ELSE 1 END,
                CASE e.edge_type WHEN 'manual' THEN 0 WHEN 'semantic' THEN 1 ELSE 2 END,
                e.confidence DESC,
                e.weight DESC,
                e.updated_at DESC
            LIMIT %s
            """,
            (memory_id, min_weight, max(1, int(limit or 5))),
        )
        return rows

    def decay_memory_edges(self, hebbian_decay: float = 0.9,
                            manual_decay: float = 0.95,
                            min_weight: float = 0.1,
                            semantic_decay: float | None = None) -> Dict:
        if semantic_decay is None:
            semantic_decay = max(0.0, min(1.0, (float(hebbian_decay) + float(manual_decay)) / 2.0))
        manual_before = int(
            (self._fetchone("SELECT COUNT(*) AS n FROM memory_edges WHERE edge_type='manual'") or {}).get("n", 0)
        )
        semantic_before = int(
            (self._fetchone("SELECT COUNT(*) AS n FROM memory_edges WHERE edge_type='semantic'") or {}).get("n", 0)
        )
        co_occurrence_before = int(
            (self._fetchone("SELECT COUNT(*) AS n FROM memory_edges WHERE edge_type='co_occurrence'") or {}).get("n", 0)
        )
        updated = self._execute(
            """
            UPDATE memory_edges
            SET weight = CASE
                    WHEN edge_type='manual' THEN weight * %s
                    WHEN edge_type='semantic' THEN weight * %s
                    ELSE weight * %s
                END,
                updated_at = NOW()
            """,
            (manual_decay, semantic_decay, hebbian_decay),
        )
        pruned = self._execute(
            "DELETE FROM memory_edges WHERE edge_type != 'manual' AND weight < %s",
            (min_weight,),
        )
        remaining = int((self._fetchone("SELECT COUNT(*) AS n FROM memory_edges") or {}).get("n", 0))
        return {
            "updated_edges": int(updated or 0),
            "pruned_edges": int(pruned or 0),
            "manual_edges_before": manual_before,
            "semantic_edges_before": semantic_before,
            "co_occurrence_edges_before": co_occurrence_before,
            "remaining_edges": remaining,
            "manual_decay": manual_decay,
            "hebbian_decay": hebbian_decay,
            "semantic_decay": semantic_decay,
            "min_weight": min_weight,
        }

    def _cleanup_edge_evidence(self, keep_days: int = EDGE_EVIDENCE_TTL_DAYS) -> int:
        return self._execute(
            "DELETE FROM memory_edge_evidence WHERE created_at < NOW() - (%s || ' days')::interval",
            (str(max(1, int(keep_days or EDGE_EVIDENCE_TTL_DAYS))),),
        )

    def _refresh_candidate_edge_states(self,
                                       min_sessions: int = EDGE_CONFIRM_MIN_SESSIONS,
                                       semantic_threshold: float = EDGE_SEMANTIC_THRESHOLD,
                                       validation_window_days: int = EDGE_VALIDATION_WINDOW_DAYS,
                                       candidate_ttl_days: int = EDGE_CANDIDATE_TTL_DAYS) -> Dict:
        rows = self._fetchall(
            """
            SELECT
                e.from_id,
                e.to_id,
                e.edge_type,
                COALESCE(stats.evidence_count, 0) AS evidence_count,
                COALESCE(stats.distinct_sessions, 0) AS distinct_sessions,
                stats.avg_similarity,
                stats.avg_overlap,
                stats.first_seen,
                stats.last_seen
            FROM memory_edges e
            LEFT JOIN (
                SELECT
                    from_id,
                    to_id,
                    edge_type,
                    COUNT(*) AS evidence_count,
                    COUNT(DISTINCT session_id) AS distinct_sessions,
                    AVG(similarity_score) AS avg_similarity,
                    AVG(overlap_score) AS avg_overlap,
                    MIN(created_at) AS first_seen,
                    MAX(created_at) AS last_seen
                FROM memory_edge_evidence
                WHERE created_at >= NOW() - (%s || ' days')::interval
                GROUP BY from_id, to_id, edge_type
            ) stats
            ON stats.from_id = e.from_id
               AND stats.to_id = e.to_id
               AND stats.edge_type = e.edge_type
            WHERE e.state='candidate'
            """,
            (str(max(1, int(validation_window_days or EDGE_VALIDATION_WINDOW_DAYS))),),
        )
        promoted = 0
        kept = 0
        for row in rows:
            edge_type = str(row.get("edge_type") or "co_occurrence").strip().lower()
            evidence_count = int(row.get("evidence_count") or 0)
            distinct_sessions = int(row.get("distinct_sessions") or 0)
            avg_similarity = row.get("avg_similarity")
            avg_overlap = row.get("avg_overlap")
            confidence = self._edge_confidence_from_aggregate(
                edge_type=edge_type,
                avg_similarity=avg_similarity,
                avg_overlap=avg_overlap,
                evidence_count=evidence_count,
                distinct_sessions=distinct_sessions,
            )
            threshold = EDGE_CONFIDENCE_THRESHOLD.get(edge_type, 0.6)
            if edge_type == "semantic":
                threshold = max(threshold, float(semantic_threshold))
            is_confirmed = (distinct_sessions >= int(min_sessions)) and (confidence >= threshold)
            next_state = "confirmed" if is_confirmed else "candidate"
            if is_confirmed:
                promoted += 1
            else:
                kept += 1
            self._execute(
                """
                UPDATE memory_edges
                SET confidence=%s,
                    evidence_count=%s,
                    distinct_sessions=%s,
                    first_evidence_at=COALESCE(first_evidence_at, %s),
                    last_evidence_at=COALESCE(%s, last_evidence_at),
                    state=%s,
                    updated_at=NOW()
                WHERE from_id=%s AND to_id=%s
                """,
                (
                    confidence,
                    evidence_count,
                    distinct_sessions,
                    row.get("first_seen"),
                    row.get("last_seen"),
                    next_state,
                    row.get("from_id"),
                    row.get("to_id"),
                ),
            )
        stale_pruned = self._execute(
            """
            DELETE FROM memory_edges
            WHERE state='candidate'
              AND COALESCE(last_evidence_at, updated_at, created_at)
                  < NOW() - (%s || ' days')::interval
            """,
            (str(max(1, int(candidate_ttl_days or EDGE_CANDIDATE_TTL_DAYS))),),
        )
        return {
            "candidate_scanned": len(rows),
            "promoted": int(promoted),
            "kept_candidate": int(kept),
            "stale_pruned": int(stale_pruned or 0),
            "min_sessions": int(min_sessions),
            "semantic_threshold": float(semantic_threshold),
        }

    def _acquire_job_lock(self, job_name: str, lock_owner: str, stale_seconds: int = 120) -> bool:
        inserted = self._execute(
            """
            INSERT INTO job_locks (job_name, lock_owner, locked_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (job_name) DO NOTHING
            """,
            (job_name, lock_owner),
        )
        if inserted > 0:
            return True
        self._execute(
            """
            DELETE FROM job_locks
            WHERE job_name=%s
              AND locked_at < NOW() - (%s || ' seconds')::interval
            """,
            (job_name, str(max(30, int(stale_seconds or 120)))),
        )
        inserted = self._execute(
            """
            INSERT INTO job_locks (job_name, lock_owner, locked_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (job_name) DO NOTHING
            """,
            (job_name, lock_owner),
        )
        return inserted > 0

    def _release_job_lock(self, job_name: str, lock_owner: str) -> None:
        self._execute(
            "DELETE FROM job_locks WHERE job_name=%s AND lock_owner=%s",
            (job_name, lock_owner),
        )

    def _job_run_start(self, job_name: str, run_owner: str) -> int:
        row = self._fetchone(
            """
            INSERT INTO job_runs (job_name, run_owner, status, started_at)
            VALUES (%s,%s,'running',NOW())
            RETURNING id
            """,
            (job_name, run_owner),
        )
        return int(row["id"]) if row else 0

    def _close_stale_running_runs(self, job_name: str, stale_minutes: int = 10) -> int:
        return int(
            self._execute(
                """
                UPDATE job_runs
                SET status='failed_timeout',
                    error=COALESCE(error, 'stale running run auto-closed'),
                    ended_at=NOW()
                WHERE job_name=%s
                  AND status='running'
                  AND started_at < NOW() - (%s || ' minutes')::interval
                """,
                (job_name, str(max(1, int(stale_minutes or 10)))),
            )
            or 0
        )

    def _job_run_finish(self, run_id: int, status: str, summary: dict | None = None, error: str | None = None) -> None:
        if run_id <= 0:
            return
        self._execute(
            """
            UPDATE job_runs
            SET status=%s,
                summary=%s,
                error=%s,
                ended_at=NOW()
            WHERE id=%s
            """,
            (
                str(status or "unknown"),
                json.dumps(summary or {}, ensure_ascii=False),
                str(error or "")[:2000] if error else None,
                run_id,
            ),
        )

    def get_job_health(self, job_name: str = DREAM_PASS_JOB_NAME, stale_hours: int = 30) -> Dict:
        last_run = self._fetchone(
            "SELECT * FROM job_runs WHERE job_name=%s ORDER BY started_at DESC LIMIT 1",
            (job_name,),
        )
        last_success = self._fetchone(
            "SELECT * FROM job_runs WHERE job_name=%s AND status='success' ORDER BY ended_at DESC LIMIT 1",
            (job_name,),
        )
        stale_row = self._fetchone(
            """
            SELECT
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM job_runs
                        WHERE job_name=%s AND status='success'
                          AND ended_at >= NOW() - (%s || ' hours')::interval
                    ) THEN 0
                    ELSE 1
                END AS stale
            """,
            (job_name, str(max(1, int(stale_hours or 30)))),
        )
        return {
            "job_name": job_name,
            "last_run": last_run,
            "last_success": last_success,
            "is_stale": bool((stale_row or {}).get("stale", 1)),
            "stale_hours": int(stale_hours),
        }

    def run_dream_pass(self,
                       min_sessions: int = EDGE_CONFIRM_MIN_SESSIONS,
                       semantic_threshold: float = EDGE_SEMANTIC_THRESHOLD,
                       validation_window_days: int = EDGE_VALIDATION_WINDOW_DAYS,
                       candidate_ttl_days: int = EDGE_CANDIDATE_TTL_DAYS,
                       evidence_ttl_days: int = EDGE_EVIDENCE_TTL_DAYS,
                       hebbian_decay: float = 0.9,
                       manual_decay: float = 0.95,
                       semantic_decay: float | None = None,
                       min_weight: float = 0.1) -> Dict:
        stale_run_minutes = max(
            1,
            int(os.environ.get("AMI_DREAM_STALE_RUN_MINUTES") or 10),
        )
        self._close_stale_running_runs(DREAM_PASS_JOB_NAME, stale_minutes=stale_run_minutes)
        owner = f"{self._runtime_session_id}:dream:{int(time.time())}"
        stale_lock_seconds = max(
            30,
            int(os.environ.get("AMI_DREAM_LOCK_STALE_SECONDS") or 120),
        )
        locked = self._acquire_job_lock(
            DREAM_PASS_JOB_NAME,
            owner,
            stale_seconds=stale_lock_seconds,
        )
        if not locked:
            run_id = self._job_run_start(DREAM_PASS_JOB_NAME, owner)
            summary = {"status": "skipped_locked"}
            self._job_run_finish(run_id, "skipped_locked", summary=summary)
            return summary

        run_id = self._job_run_start(DREAM_PASS_JOB_NAME, owner)
        try:
            evidence_pruned = self._cleanup_edge_evidence(keep_days=evidence_ttl_days)
            validation = self._refresh_candidate_edge_states(
                min_sessions=min_sessions,
                semantic_threshold=semantic_threshold,
                validation_window_days=validation_window_days,
                candidate_ttl_days=candidate_ttl_days,
            )
            decay = self.decay_memory_edges(
                hebbian_decay=hebbian_decay,
                manual_decay=manual_decay,
                semantic_decay=semantic_decay,
                min_weight=min_weight,
            )
            summary = {
                "status": "success",
                "validation": validation,
                "edge_decay": decay,
                "evidence_pruned": int(evidence_pruned or 0),
            }
            self._job_run_finish(run_id, "success", summary=summary)
            return summary
        except Exception as exc:
            summary = {"status": "failed"}
            self._job_run_finish(run_id, "failed", summary=summary, error=str(exc))
            raise
        finally:
            self._release_job_lock(DREAM_PASS_JOB_NAME, owner)

    def link_memories(self, from_id: int, to_id: int,
                      relation: str = "related", note: str = None):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO memory_links (from_id, to_id, relation, note)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (from_id, to_id) DO NOTHING
                    RETURNING id
                    """,
                    (from_id, to_id, relation, note),
                )
                row = cur.fetchone()
                self._upsert_memory_edge_pair(
                    from_id,
                    to_id,
                    delta=2.0,
                    edge_type="manual",
                    state="confirmed",
                    confidence=1.0,
                    session_id=self._runtime_session_id,
                    source="manual_link",
                    query_text=note or relation,
                    memory_ids=[from_id, to_id],
                    similarity_score=None,
                    overlap_score=None,
                    record_evidence=True,
                )
                return row["id"] if row else None

    def unlink_memories(self, from_id: int, to_id: int) -> bool:
        removed = self._execute(
            """
            DELETE FROM memory_links
            WHERE (from_id=%s AND to_id=%s) OR (from_id=%s AND to_id=%s)
            """,
            (from_id, to_id, to_id, from_id),
        ) > 0
        self._execute(
            """
            DELETE FROM memory_edges
            WHERE (from_id=%s AND to_id=%s) OR (from_id=%s AND to_id=%s)
            """,
            (from_id, to_id, to_id, from_id),
        )
        return removed

    def get_memory_links(self, memory_id: int) -> List[Dict]:
        return self._fetchall(
            """
            SELECT l.*,
                   COALESCE(NULLIF(mf.text, ''), mf.content) AS from_content, mf.layer AS from_layer,
                   COALESCE(NULLIF(mt.text, ''), mt.content) AS to_content,   mt.layer AS to_layer
            FROM memory_links l
            JOIN memories mf ON l.from_id = mf.id
            JOIN memories mt ON l.to_id   = mt.id
            WHERE l.from_id=%s OR l.to_id=%s
            ORDER BY l.created_at DESC
            """,
            (memory_id, memory_id),
        )

    def add_comment(self, memory_id: int, content: str,
                    author: str = "me", reply_to: int = None) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO comments (memory_id, content, author, reply_to)
                    VALUES (%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (memory_id, content, author, reply_to),
                )
                return cur.fetchone()["id"]

    def get_comments(self, memory_id: int) -> List[Dict]:
        rows = self._fetchall(
            "SELECT * FROM comments WHERE memory_id=%s ORDER BY written_at",
            (memory_id,),
        )
        unread_ids = [row["id"] for row in rows if row["is_read"] == 0 and row["author"] == "wen"]
        if unread_ids:
            placeholders = ",".join(["%s"] * len(unread_ids))
            self._execute(f"UPDATE comments SET is_read=1 WHERE id IN ({placeholders})", unread_ids)
        return rows

    def write_letter(self, content: str, from_session: str = "") -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO letters (content, from_session) VALUES (%s,%s) RETURNING id",
                    (content, from_session),
                )
                return cur.fetchone()["id"]

    def get_unread_letters(self, recent_hours: int = 0) -> List[Dict]:
        keep = max(0, int(recent_hours or 0))
        return self._fetchall(
            """
            SELECT * FROM letters
            WHERE is_read=0
               OR (%s > 0 AND read_at IS NOT NULL
                   AND read_at >= NOW() - (%s || ' hours')::interval)
            ORDER BY written_at
            """,
            (keep, str(keep)),
        )

    def mark_letters_read(self) -> int:
        return self._execute(
            "UPDATE letters SET is_read=1, read_at=NOW() WHERE is_read=0"
        )

    def get_you_now(self) -> Optional[Dict]:
        return self._fetchone(
            """
            SELECT * FROM you_now
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        )

    def write_you_now(self, content: str | None = None,
                      pulling: str | None = None,
                      source: str = "manual",
                      expires_hours: int = 72) -> Dict:
        current = self.get_you_now()

        # Guard against blank writes wiping the current state.
        next_content = content
        if isinstance(next_content, str) and not next_content.strip():
            next_content = None
        next_pulling = pulling
        if isinstance(next_pulling, str) and not next_pulling.strip():
            next_pulling = None

        if current:
            row = self._fetchone(
                """
                UPDATE you_now
                SET content = %s,
                    pulling = %s,
                    source = %s,
                    updated_at = NOW(),
                    expires_at = NOW() + (%s || ' hours')::interval
                WHERE id = %s
                RETURNING *
                """,
                (
                    next_content if next_content is not None else current.get("content", ""),
                    next_pulling if next_pulling is not None else current.get("pulling"),
                    source,
                    str(expires_hours),
                    current["id"],
                ),
            )
            return row or current

        row = self._fetchone(
            """
            INSERT INTO you_now (content, pulling, source, expires_at)
            VALUES (%s, %s, %s, NOW() + (%s || ' hours')::interval)
            RETURNING *
            """,
            (
                next_content or "",
                next_pulling,
                source,
                str(expires_hours),
            ),
        )
        return row or {}

    def get_latest_handoff(self) -> Optional[Dict]:
        return self._fetchone(
            """
            SELECT * FROM window_handoff
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )

    def write_window_handoff(self, summary: str,
                             unfinished: str | None = None,
                             next_pull: str | None = None,
                             source: str = "normal",
                             confidence: str = "high",
                             handoff_needed: bool = False) -> Dict:
        row = self._fetchone(
            """
            INSERT INTO window_handoff
                (summary, unfinished, next_pull, source, confidence, handoff_needed)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                summary or "",
                unfinished,
                next_pull,
                source,
                confidence,
                1 if handoff_needed else 0,
            ),
        )
        return row or {}

    def mark_handoff_needed(self) -> int:
        latest = self.get_latest_handoff()
        if latest:
            return self._execute(
                "UPDATE window_handoff SET handoff_needed=1 WHERE id=%s",
                (latest["id"],),
            )
        self._execute(
            """
            INSERT INTO window_handoff
                (summary, unfinished, next_pull, source, confidence, handoff_needed)
            VALUES ('', NULL, NULL, 'auto_repair', 'low', 1)
            """
        )
        return 1

    def clear_handoff_needed(self) -> int:
        latest = self.get_latest_handoff()
        if not latest:
            return 0
        return self._execute(
            "UPDATE window_handoff SET handoff_needed=0 WHERE id=%s",
            (latest["id"],),
        )

    def write_weekly_digest(self, week_start: str, content: str,
                            hot_count: int = 0, cold_count: int = 0) -> int:
        row = self._fetchone(
            """
            INSERT INTO weekly_digest (week_start, content, hot_count, cold_count)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (
                week_start,
                content or "",
                int(hot_count or 0),
                int(cold_count or 0),
            ),
        )
        return int(row["id"]) if row else 0

    def get_weekly_digests(self, limit: int = 4) -> List[Dict]:
        return self._fetchall(
            """
            SELECT * FROM weekly_digest
            ORDER BY week_start DESC, id DESC
            LIMIT %s
            """,
            (max(1, int(limit or 4)),),
        )

    def add_board_message(self, author: str, content: str) -> Dict:
        row = self._fetchone(
            """
            INSERT INTO board_messages (author, content, read)
            VALUES (%s, %s, 0)
            RETURNING *
            """,
            (author, content),
        )
        return row or {}

    def read_board_messages(self, reader: str, limit: int = 20,
                            mark_read: bool = True,
                            include_recent_hours: int = 0) -> List[Dict]:
        norm_reader = "code" if str(reader or "").strip().lower() in {"code", "dialogue", "ami", "live"} else "user"
        other = "user" if norm_reader == "code" else "code"
        keep = max(0, int(include_recent_hours or 0))
        if keep > 0:
            # Consumption window semantics:
            # - unread messages stay visible until consumed
            # - once consumed (read=1), keep them visible for recent_hours by read_at
            rows = self._fetchall(
                """
                SELECT * FROM board_messages
                WHERE author=%s
                  AND (
                        read=0
                        OR (read_at IS NOT NULL
                            AND read_at >= NOW() - (%s || ' hours')::interval)
                  )
                ORDER BY
                    CASE WHEN read=0 THEN 0 ELSE 1 END ASC,
                    COALESCE(
                        CASE WHEN read=0 THEN created_at END,
                        read_at,
                        created_at
                    ) DESC,
                    id DESC
                LIMIT %s
                """,
                (other, str(keep), max(1, int(limit or 20))),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM board_messages
                WHERE author=%s
                  AND read=0
                ORDER BY
                    created_at DESC,
                    id DESC
                LIMIT %s
                """,
                (other, max(1, int(limit or 20))),
            )
        if mark_read and rows:
            ids = [int(row["id"]) for row in rows if row.get("id") is not None]
            if ids:
                self._execute(
                    "UPDATE board_messages SET read=1, read_at=NOW() WHERE id = ANY(%s) AND read=0",
                    (ids,),
                )
                rows = self._fetchall(
                    """
                    SELECT * FROM board_messages
                    WHERE id = ANY(%s)
                    ORDER BY COALESCE(read_at, created_at) DESC, id DESC
                    """,
                    (ids,),
                )
        return rows

    def list_board_messages(self, limit: int = 200) -> List[Dict]:
        rows = self._fetchall(
            """
            SELECT * FROM board_messages
            ORDER BY id DESC
            LIMIT %s
            """,
            (max(1, int(limit or 200)),),
        )
        return list(reversed(rows))

    def add_emotion_trace(self, your_valence: float = None,
                          your_arousal: float = None,
                          my_valence: float = None,
                          my_arousal: float = None,
                          your_notes: str = None,
                          my_notes: str = None,
                          session_id: str = "") -> int:
        row = self._fetchone(
            """
            SELECT EXTRACT(EPOCH FROM (NOW() - recorded_at)) / 86400.0 AS gap_days
            FROM emotion_trace
            ORDER BY recorded_at DESC
            LIMIT 1
            """
        )
        gap = row["gap_days"] if row else None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO emotion_trace
                        (session_id, your_valence, your_arousal, my_valence, my_arousal,
                         your_notes, my_notes, gap_days)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (
                        session_id, your_valence, your_arousal, my_valence, my_arousal,
                        your_notes, my_notes, gap,
                    ),
                )
                return cur.fetchone()["id"]

    def get_emotion_history(self, limit: int = 7) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM emotion_trace ORDER BY recorded_at DESC LIMIT %s",
            (limit,),
        )

    def get_stance_log(self, memory_id: int) -> List[Dict]:
        return self._fetchall(
            "SELECT * FROM stance_log WHERE memory_id=%s ORDER BY changed_at ASC",
            (memory_id,),
        )

    def get_emotion_delta(self) -> Optional[Dict]:
        rows = self.get_emotion_history(limit=2)
        if len(rows) < 2:
            return None
        curr, prev = rows[0], rows[1]

        def _delta(a, b):
            if a is None or b is None:
                return None
            return round(float(a) - float(b), 3)

        def _trend(delta):
            if delta is None:
                return "unknown"
            if delta > 0.1:
                return "up"
            if delta < -0.1:
                return "down"
            return "stable"

        yv_delta = _delta(curr.get("your_valence"), prev.get("your_valence"))
        return {
            "your_valence":       curr.get("your_valence"),
            "your_valence_delta": yv_delta,
            "your_valence_trend": _trend(yv_delta),
            "your_arousal":       curr.get("your_arousal"),
            "your_arousal_delta": _delta(curr.get("your_arousal"), prev.get("your_arousal")),
            "my_valence":         curr.get("my_valence"),
            "my_valence_delta":   _delta(curr.get("my_valence"), prev.get("my_valence")),
            "gap_days":           curr.get("gap_days"),
            "your_notes":         curr.get("your_notes"),
            "prev_your_notes":    prev.get("your_notes"),
            "recorded_at":        str(curr.get("recorded_at", ""))[:16],
        }

    @staticmethod
    def _clamp_depth(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _clamp_priority(value: int) -> int:
        return max(0, min(100, int(value)))

    @staticmethod
    def _normalize_stage(stage: str | None) -> str:
        raw = str(stage or "seed").strip().lower()
        return raw if raw in INTEREST_STAGE_ORDER else "seed"

    @staticmethod
    def _normalize_status(status: str | None) -> str:
        raw = str(status or "active").strip().lower()
        return raw if raw in {"active", "parked", "archived"} else "active"

    @staticmethod
    def _promote_stage(stage: str, touch_count: int, depth: float) -> str:
        cur = PostgresBackend._normalize_stage(stage)
        d = PostgresBackend._clamp_depth(depth)
        n = int(touch_count or 0)
        if cur == "seed" and n >= 1:
            cur = "sprout"
        if cur == "sprout" and n >= 3 and d >= 0.25:
            cur = "growing"
        if cur == "growing" and n >= 6 and d >= 0.65:
            cur = "ripe"
        if cur == "ripe" and n >= 10 and d >= 0.85:
            cur = "fruit"
        return cur

    @staticmethod
    def _normalize_insight_summary(summary: str) -> str:
        text = str(summary or "").lower()
        text = _INSIGHT_NORMALIZE_RE.sub(" ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _extract_keywords(text: str | None) -> list[str]:
        words = _WORD_RE.findall(str(text or "").lower())
        out: list[str] = []
        seen: set[str] = set()
        for word in words:
            token = str(word).strip()
            if not token or token in _STOPWORDS or token in seen:
                continue
            if token.isdigit():
                continue
            seen.add(token)
            out.append(token)
        return out

    @staticmethod
    def _build_insight_hash(source_id: str, target_id: str, summary: str) -> str:
        normalized = PostgresBackend._normalize_insight_summary(summary)[:50]
        raw = f"{source_id}|{target_id}|{normalized}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _brief(text: str | None, n: int = 80) -> str:
        s = str(text or "").replace("\n", " ").strip()
        return s if len(s) <= n else s[:n].rstrip() + "..."

    @staticmethod
    def _as_json_text(value) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            s = value.strip()
            return s or None
        return json.dumps(value, ensure_ascii=False)

    def _interest_context_text(self, interest_id: str) -> str:
        row = self._fetchone("SELECT * FROM interest_pool WHERE id=%s", (interest_id,))
        if not row:
            return ""
        activities = self.activity_recent(limit=8, interest_id=interest_id)
        parts = [
            str(row.get("name") or ""),
            str(row.get("notes") or ""),
            str(row.get("synthesis") or ""),
            str(row.get("next_action") or ""),
        ]
        for act in activities:
            parts.append(str(act.get("topic") or ""))
            parts.append(str(act.get("details") or ""))
            parts.append(str(act.get("outcome") or ""))
        return " ".join(parts)

    def _append_interest_note(self, interest_id: str, note: str) -> None:
        clean = str(note or "").strip()
        if not clean:
            return
        self._execute(
            """
            UPDATE interest_pool
            SET notes = CASE
                    WHEN COALESCE(notes, '') = '' THEN %s
                    ELSE notes || E'\n' || %s
                END,
                updated_at = NOW()
            WHERE id=%s
            """,
            (clean, clean, interest_id),
        )

    def _push_cross_interest_insights(self) -> list[dict]:
        pairs = self._fetchall(
            """
            SELECT
                c.from_id,
                c.to_id,
                c.strength,
                c.relation,
                sf.name AS from_name,
                st.name AS to_name
            FROM interest_connections c
            JOIN interest_pool sf ON sf.id = c.from_id
            JOIN interest_pool st ON st.id = c.to_id
            WHERE sf.status='active' AND st.status='active'
            ORDER BY c.strength DESC, c.created_at DESC
            """
        )
        pushed: list[dict] = []
        context_cache: dict[str, str] = {}

        for pair in pairs:
            source_id = str(pair.get("from_id") or "")
            target_id = str(pair.get("to_id") or "")
            if not source_id or not target_id or source_id == target_id:
                continue

            source_text = context_cache.get(source_id)
            if source_text is None:
                source_text = self._interest_context_text(source_id)
                context_cache[source_id] = source_text

            target_text = context_cache.get(target_id)
            if target_text is None:
                target_text = self._interest_context_text(target_id)
                context_cache[target_id] = target_text

            source_kw = set(self._extract_keywords(source_text))
            target_kw = set(self._extract_keywords(target_text))
            overlap = sorted(source_kw & target_kw)
            strength = float(pair.get("strength") or 0.0)
            if len(overlap) < 2:
                if len(overlap) < 1 or strength < 0.7:
                    continue

            source_name = str(pair.get("from_name") or source_id)
            target_name = str(pair.get("to_name") or target_id)
            overlap_text = ", ".join(overlap[:5])
            summary = (
                f"{source_name} 的新洞察可迁移到 {target_name}，"
                f"交叉关键词：{overlap_text}。建议在下一步里复用这条路径。"
            )
            insight_hash = self._build_insight_hash(source_id, target_id, summary)
            exists = self._fetchone(
                "SELECT id FROM activity_log WHERE insight_hash=%s LIMIT 1",
                (insight_hash,),
            )
            if exists:
                continue
            exists_recent = self._fetchone(
                """
                SELECT id
                FROM activity_log
                WHERE kind='insight_push'
                  AND interest_id=%s
                  AND recorded_at > NOW() - INTERVAL '1 day'
                  AND (
                        source_interest_id=%s
                        OR COALESCE(meta_json, '') ILIKE %s
                  )
                LIMIT 1
                """,
                (target_id, source_id, f"%\"source_id\": \"{source_id}\"%"),
            )
            if exists_recent:
                continue

            note = (
                f"[cross-insight {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}] "
                f"from={source_id} relation={pair.get('relation') or 'related'} "
                f"strength={float(pair.get('strength') or 0.0):.2f} | {summary}"
            )
            self._append_interest_note(target_id, note)
            self.activity_write(
                kind="insight_push",
                topic=source_name,
                details=summary,
                outcome="pushed",
                branch="cross_topic",
                meta_json={
                    "source_id": source_id,
                    "target_id": target_id,
                    "overlap": overlap[:8],
                    "strength": strength,
                },
                insight_hash=insight_hash,
                source_interest_id=source_id,
                interest_id=target_id,
            )
            pushed.append(
                {
                    "source_id": source_id,
                    "target_id": target_id,
                    "summary": summary,
                    "insight_hash": insight_hash,
                }
            )
        return pushed

    def interest_add(self, id: str, name: str, stage: str = "seed",
                     source: str = "from_exploration",
                     parent_id: str = None, notes: str = None,
                     status: str = "active",
                     priority: int = 50,
                     next_action: str | None = None,
                     next_review_at: str | None = None,
                     meta_json: str | None = None) -> bool:
        iid = str(id or "").strip()
        title = str(name or "").strip()
        if not iid or not title:
            return False
        if _looks_placeholder_corruption(title):
            logger.warning("Rejecting suspicious interest title: id=%s title=%r source=%r", iid, title, source)
            return False
        try:
            self._execute(
                """
                INSERT INTO interest_pool (
                    id, name, status, stage, depth, priority,
                    source, parent_id, notes, next_action, next_review_at, meta_json,
                    created_at, updated_at
                )
                VALUES (%s,%s,%s,%s,0.0,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                """,
                (
                    iid,
                    title,
                    self._normalize_status(status),
                    self._normalize_stage(stage),
                    self._clamp_priority(priority),
                    str(source or "from_exploration"),
                    parent_id,
                    _strip_xml(notes),
                    _strip_xml(next_action),
                    next_review_at,
                    self._as_json_text(meta_json),
                ),
            )
            return True
        except psycopg2.IntegrityError:
            return False

    def interest_touch(self, id: str, depth_delta: float = 0.1,
                       notes: str = None,
                       outcome: str | None = None,
                       kind: str = "advance",
                       branch: str | None = None,
                       _from_memory: bool = False) -> Optional[Dict]:
        current = self._fetchone("SELECT * FROM interest_pool WHERE id=%s", (id,))
        if not current:
            return None

        delta = max(-0.3, min(0.3, float(depth_delta or 0.0)))
        new_depth = self._clamp_depth(float(current.get("depth") or 0.0) + delta)
        new_count = int(current.get("touch_count") or 0) + 1
        new_stage = self._promote_stage(str(current.get("stage") or "seed"), new_count, new_depth)
        update_notes = _strip_xml(notes) if notes is not None else current.get("notes")

        self._execute(
            """
            UPDATE interest_pool
            SET touch_count=%s,
                depth=%s,
                stage=%s,
                last_touched=NOW(),
                first_touched=COALESCE(first_touched, NOW()),
                last_activity_at=NOW(),
                notes=%s,
                updated_at=NOW()
            WHERE id=%s
            """,
            (new_count, new_depth, new_stage, update_notes, id),
        )
        result = self._fetchone("SELECT * FROM interest_pool WHERE id=%s", (id,))
        if result and not _from_memory:
            self.activity_write(
                kind=kind or "advance",
                topic=result.get("name"),
                details=notes,
                outcome=outcome,
                branch=branch or "interest_touch",
                interest_id=id,
            )
        return result

    def interest_update(self, id: str, **kwargs) -> bool:
        allowed = {
            "name", "status", "stage", "depth", "priority", "next_action", "next_review_at",
            "notes", "ripe_direction", "parent_id", "meta_json", "synthesis", "source",
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        if "status" in updates:
            updates["status"] = self._normalize_status(updates["status"])
        if "stage" in updates:
            updates["stage"] = self._normalize_stage(updates["stage"])
        if "name" in updates:
            updates["name"] = str(updates["name"] or "").strip()
            if not updates["name"] or _looks_placeholder_corruption(updates["name"]):
                logger.warning("Rejecting suspicious interest rename: id=%s name=%r", id, updates["name"])
                return False
        if "depth" in updates:
            updates["depth"] = self._clamp_depth(float(updates["depth"]))
        if "priority" in updates:
            updates["priority"] = self._clamp_priority(int(updates["priority"]))
        if "notes" in updates and updates["notes"] is not None:
            updates["notes"] = _strip_xml(updates["notes"])
        if "next_action" in updates and updates["next_action"] is not None:
            updates["next_action"] = _strip_xml(updates["next_action"])
        if "meta_json" in updates:
            updates["meta_json"] = self._as_json_text(updates["meta_json"])

        updates["updated_at"] = datetime.now(timezone.utc)
        columns = list(updates.keys())
        params = [updates[col] for col in columns] + [id]
        set_clause = ", ".join(f"{col}=%s" for col in columns)
        return self._execute(f"UPDATE interest_pool SET {set_clause} WHERE id=%s", params) > 0

    def interest_list(self, stage: str = None, status: str = None,
                      sort: str = "priority", limit: int = 50,
                      include_qa: bool = False) -> List[Dict]:
        where_clauses = []
        params = []
        if stage:
            where_clauses.append("stage=%s")
            params.append(self._normalize_stage(stage))
        if status:
            where_clauses.append("status=%s")
            params.append(self._normalize_status(status))
        if not include_qa:
            where_clauses.append("COALESCE(source, '') <> 'qa'")

        order_map = {
            "priority": "priority DESC, COALESCE(next_review_at, NOW() + INTERVAL '365 days') ASC, COALESCE(last_activity_at,last_touched,created_at) ASC",
            "recent": "COALESCE(last_activity_at,last_touched,created_at) DESC, priority DESC",
            "review": "COALESCE(next_review_at, NOW() + INTERVAL '365 days') ASC, priority DESC",
            "depth": "depth DESC, touch_count DESC, priority DESC",
            "stage": (
                "CASE stage "
                "WHEN 'growing' THEN 0 WHEN 'sprout' THEN 1 WHEN 'ripe' THEN 2 "
                "WHEN 'seed' THEN 3 WHEN 'fruit' THEN 4 ELSE 5 END, "
                "priority DESC, COALESCE(last_activity_at,last_touched,created_at) DESC"
            ),
        }
        order_clause = order_map.get(str(sort or "").strip().lower(), order_map["priority"])

        sql = "SELECT * FROM interest_pool"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += f" ORDER BY {order_clause} LIMIT %s"
        params.append(max(1, int(limit or 50)))
        return self._fetchall(sql, tuple(params))

    def interest_get(self, id: str, activity_limit: int = 8,
                     include_connections: bool = True,
                     include_qa: bool = False) -> Optional[Dict]:
        row = self._fetchone("SELECT * FROM interest_pool WHERE id=%s", (id,))
        if not row:
            return None
        if not include_qa and _is_qa_source(row.get("source")):
            return None
        payload = {
            "interest": row,
            "recent_activity": self.activity_recent(limit=activity_limit, interest_id=id, include_qa=include_qa),
            "connections": [],
        }
        if include_connections:
            sql = """
                SELECT
                    c.*,
                    src.name AS from_name,
                    dst.name AS to_name
                FROM interest_connections c
                JOIN interest_pool src ON src.id = c.from_id
                JOIN interest_pool dst ON dst.id = c.to_id
                WHERE (c.from_id=%s OR c.to_id=%s)
            """
            params: list[Any] = [id, id]
            if not include_qa:
                sql += " AND COALESCE(src.source, '') <> 'qa' AND COALESCE(dst.source, '') <> 'qa'"
            sql += " ORDER BY c.strength DESC, c.created_at DESC LIMIT 30"
            payload["connections"] = self._fetchall(sql, tuple(params))
        return payload

    def interest_review(self, limit: int = 3, status: str = "active",
                        include_qa: bool = False) -> List[Dict]:
        rows = self.interest_list(status=status, sort="priority", limit=200, include_qa=include_qa)
        now = datetime.now(timezone.utc)
        scored: list[dict] = []
        stage_bonus = {"seed": 12.0, "sprout": 14.0, "growing": 16.0, "ripe": 8.0, "fruit": 4.0}

        for row in rows:
            priority = self._clamp_priority(int(row.get("priority") or 50))
            depth = self._clamp_depth(float(row.get("depth") or 0.0))

            pivot = row.get("last_activity_at") or row.get("last_touched") or row.get("created_at")
            age_hours = 0.0
            if isinstance(pivot, datetime):
                dt = pivot if pivot.tzinfo else pivot.replace(tzinfo=timezone.utc)
                age_hours = max(0.0, (now - dt).total_seconds() / 3600.0)

            stale_score = min(age_hours / 24.0, 10.0) * 2.0  # max 20
            review_due_score = 0.0
            next_review_at = row.get("next_review_at")
            if isinstance(next_review_at, datetime):
                due_dt = next_review_at if next_review_at.tzinfo else next_review_at.replace(tzinfo=timezone.utc)
                if due_dt <= now:
                    review_due_score = 15.0

            total = (
                priority * 0.6
                + stale_score
                + review_due_score
                + stage_bonus.get(str(row.get("stage") or "seed"), 8.0)
                + (1.0 - depth) * 6.0
            )
            reasons = [
                f"priority={priority}",
                f"stale={age_hours:.1f}h",
                f"stage={row.get('stage')}",
            ]
            if review_due_score > 0:
                reasons.append("review_due")
            scored.append({"interest": row, "score": round(total, 2), "reason": ", ".join(reasons)})

        scored.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
        return scored[: max(1, int(limit or 3))]

    def interest_synthesize(self, interest_id: str | None = None,
                            push_insights: bool = True) -> Dict:
        targets = []
        if interest_id:
            row = self._fetchone("SELECT * FROM interest_pool WHERE id=%s", (interest_id,))
            if row:
                targets = [row]
        else:
            targets = self.interest_list(status="active", sort="review", limit=200)

        updated_ids = []
        for row in targets:
            iid = str(row.get("id"))
            activities = self.activity_recent(limit=12, interest_id=iid)
            snippets = []
            for act in activities[:3]:
                ts = str(act.get("recorded_at") or "")[:16]
                snippets.append(
                    f"[{ts}] {act.get('kind') or ''} {act.get('topic') or ''} {self._brief(act.get('details'), 90)}"
                )
            progress = "；".join(snippets) if snippets else "近期暂无活动记录。"
            synthesis = (
                f"阶段={row.get('stage')} 深度={float(row.get('depth') or 0.0):.2f} "
                f"触达={int(row.get('touch_count') or 0)}。"
                f"近期进展：{progress} "
                f"下一步：{row.get('next_action') or '继续推进并记录可复用方法。'}"
            )
            self._execute(
                """
                UPDATE interest_pool
                SET synthesis=%s,
                    last_synthesized_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (synthesis, iid),
            )
            updated_ids.append(iid)

        pushes: list[dict] = []
        if push_insights and not interest_id:
            pushes = self._push_cross_interest_insights()

        return {
            "target_count": len(targets),
            "updated": updated_ids,
            "insight_pushes": pushes,
        }

    def interest_connect(self, from_id: str, to_id: str,
                         strength: float = 0.5,
                         relation: str = "related",
                         description: str = None) -> bool:
        try:
            self._execute(
                """
                INSERT INTO interest_connections (from_id, to_id, strength, relation, description)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (from_id, to_id)
                DO UPDATE SET
                    strength = EXCLUDED.strength,
                    relation = EXCLUDED.relation,
                    description = COALESCE(EXCLUDED.description, interest_connections.description)
                """,
                (
                    str(from_id or "").strip(),
                    str(to_id or "").strip(),
                    max(0.0, min(1.0, float(strength or 0.5))),
                    str(relation or "related").strip() or "related",
                    _strip_xml(description),
                ),
            )
            return True
        except psycopg2.IntegrityError:
            return False

    def activity_write(self, type: str = None, kind: str = None,
                       topic: str = None, details: str = None,
                       outcome: str = None, energy: float = None,
                       branch: str = None, meta_json: str = None,
                       insight_hash: str = None,
                       source_interest_id: str = None,
                       interest_id: str = None) -> int:
        resolved_kind = str(kind or type or "activity").strip() or "activity"
        meta_text = self._as_json_text(meta_json)
        cols = self._table_columns("activity_log")
        with self._connect() as conn:
            with conn.cursor() as cur:
                if "type" in cols:
                    cur.execute(
                        """
                        INSERT INTO activity_log (
                            type, kind, topic, details, outcome, energy, branch, meta_json,
                            insight_hash, source_interest_id, interest_id
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            resolved_kind,
                            resolved_kind,
                            _strip_xml(topic),
                            _strip_xml(details),
                            _strip_xml(outcome),
                            float(energy) if energy is not None else None,
                            _strip_xml(branch),
                            meta_text,
                            insight_hash,
                            source_interest_id,
                            interest_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO activity_log (
                            kind, topic, details, outcome, energy, branch, meta_json,
                            insight_hash, source_interest_id, interest_id
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            resolved_kind,
                            _strip_xml(topic),
                            _strip_xml(details),
                            _strip_xml(outcome),
                            float(energy) if energy is not None else None,
                            _strip_xml(branch),
                            meta_text,
                            insight_hash,
                            source_interest_id,
                            interest_id,
                        ),
                    )
                aid = int(cur.fetchone()["id"])

        if interest_id and str(branch or "") != "interest_touch":
            step_depth = 0.0 if resolved_kind == "insight_push" else 0.03
            row = self._fetchone(
                """
                UPDATE interest_pool
                SET touch_count = touch_count + 1,
                    depth = LEAST(1.0, depth + %s),
                    last_touched = NOW(),
                    first_touched = COALESCE(first_touched, NOW()),
                    last_activity_at = NOW(),
                    updated_at = NOW()
                WHERE id=%s
                RETURNING *
                """,
                (step_depth, interest_id),
            )
            if row:
                stage = self._promote_stage(
                    str(row.get("stage") or "seed"),
                    int(row.get("touch_count") or 0),
                    float(row.get("depth") or 0.0),
                )
                if stage != row.get("stage"):
                    self._execute("UPDATE interest_pool SET stage=%s, updated_at=NOW() WHERE id=%s", (stage, interest_id))
        return aid

    def activity_cleanup(self, days: int = 7) -> int:
        """Delete activity_log entries older than `days` days. Returns deleted count."""
        return self._execute(
            "DELETE FROM activity_log WHERE recorded_at < NOW() - (%s || ' days')::interval",
            (str(days),),
        )

    def activity_recent(self, limit: int = 20,
                        kind: str = None,
                        interest_id: str = None,
                        include_qa: bool = False) -> List[Dict]:
        where = []
        params = []
        if interest_id:
            where.append("interest_id=%s")
            params.append(interest_id)
        if kind:
            where.append("kind=%s")
            params.append(kind)
        if not include_qa:
            where.append("COALESCE(branch, '') <> 'qa'")
            where.append(
                """NOT EXISTS (
                    SELECT 1
                    FROM interest_pool ip
                    WHERE (
                        ip.id = activity_log.interest_id
                        OR ip.id = activity_log.source_interest_id
                    )
                    AND COALESCE(ip.source, '') = 'qa'
                )"""
            )
        sql = "SELECT * FROM activity_log"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY recorded_at DESC LIMIT %s"
        params.append(max(1, int(limit or 20)))
        return self._fetchall(sql, tuple(params))

    def compute_decay_score(self, mem: Dict) -> float:
        importance = mem["importance"]
        recall_count = max(1, mem.get("recall_count", 0))
        arousal = mem.get("arousal", 0.0)
        last_active = mem.get("last_recalled") or mem.get("created_at")
        days = 0.0
        if last_active:
            row = self._fetchone(
                "SELECT GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - %s::timestamptz)) / 86400.0) AS days",
                (last_active,),
            )
            days = float(row["days"]) if row and row["days"] is not None else 0.0
        score = (
            importance
            * (recall_count ** 0.3)
            * math.exp(-DECAY_LAMBDA * days)
            * (DECAY_BASE + arousal * DECAY_AROUSAL_BOOST)
        )
        if mem.get("pending_resolved"):
            score *= 0.05
        return score

    def stats(self) -> Dict:
        total = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active'")["n"]
        anchors = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND layer='anchor'")["n"]
        treasures = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND layer='treasure'")["n"]
        pending = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND is_pending=1 AND pending_resolved=0")["n"]
        unread_letters = self._fetchone("SELECT COUNT(*) AS n FROM letters WHERE is_read=0")["n"]
        archived = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='archived'")["n"]
        high_recall = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE recall_count >= 5 AND status='active'")["n"]
        hidden = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE do_not_surface=1 AND status='active'")["n"]
        unread_by_wen = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE read_by_wen=0 AND status='active'")["n"]
        unread_comments = self._fetchone("SELECT COUNT(*) AS n FROM comments WHERE is_read=0 AND author='wen'")["n"]
        edge_count = self._fetchone("SELECT COUNT(*) AS n FROM memory_edges")["n"]
        hot = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND tier='hot'")["n"]
        cold = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND tier='cold'")["n"]
        archive = self._fetchone("SELECT COUNT(*) AS n FROM memories WHERE status='active' AND tier='archive'")["n"]
        last_emotion = self._fetchone("SELECT * FROM emotion_trace ORDER BY recorded_at DESC LIMIT 1")
        return {
            "total": total,
            "anchors": anchors,
            "treasures": treasures,
            "pending": pending,
            "unread_letters": unread_letters,
            "archived": archived,
            "high_recall": high_recall,
            "hidden": hidden,
            "unread_by_wen": unread_by_wen,
            "unread_comments": unread_comments,
            "memory_edges": edge_count,
            "hot": hot,
            "cold": cold,
            "archive_tier": archive,
            "last_emotion": last_emotion,
        }

    def run_decay(self, dry_run: bool = True) -> Dict:
        candidates = self._fetchall(
            f"""
            SELECT {self._memory_read_columns()} FROM memories
            WHERE status='active'
              AND importance < 5
              AND layer NOT IN ('anchor','treasure','note')
              AND pinned = 0
              AND recall_count < 5
            """
        )
        summary = {"archived": [], "skipped": [], "scores": []}
        for mem in candidates:
            score = self.compute_decay_score(mem)
            summary["scores"].append(
                {"id": mem["id"], "score": round(score, 3), "preview": mem["content"][:40]}
            )
            if score < DECAY_ARCHIVE_THRESHOLD:
                summary["archived"].append(
                    {"id": mem["id"], "score": round(score, 3), "preview": mem["content"][:40]}
                )
                if not dry_run:
                    self._execute("UPDATE memories SET status='archived' WHERE id=%s", (mem["id"],))
            else:
                summary["skipped"].append(mem["id"])
        return summary
