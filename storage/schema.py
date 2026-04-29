"""
storage/schema.py

Single declared schema for AmI storage (Postgres-only).
Alembic reads `metadata` for migrations.
"""

from sqlalchemy import (
    BigInteger,
    Date,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    func,
)

TABLES = [
    "memories",
    "memory_edges",
    "memory_edge_evidence",
    "letters",
    "you_now",
    "window_handoff",
    "weekly_digest",
    "board_messages",
    "emotion_trace",
    "comments",
    "interest_pool",
    "interest_connections",
    "activity_log",
    "memory_links",
    "stance_log",
    "job_locks",
    "job_runs",
]

metadata = MetaData()

memories = Table(
    "memories",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("content", Text, nullable=False),
    Column("text", Text),
    Column("context", Text),
    Column("layer", Text, nullable=False, server_default="diary"),
    Column("importance", Integer, nullable=False, server_default="3"),
    Column("valence", Float, nullable=False, server_default="0.0"),
    Column("arousal", Float, nullable=False, server_default="0.0"),
    Column("tags", Text),
    Column("stance", Text),
    Column("silence_note", Text),
    Column("is_pending", Integer, nullable=False, server_default="0"),
    Column("pending_resolved", Integer, nullable=False, server_default="0"),
    Column("recall_count", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("last_recalled", DateTime(timezone=True)),
    Column("pinned", Integer, nullable=False, server_default="0"),
    Column("status", Text, nullable=False, server_default="active"),
    Column("do_not_surface", Integer, nullable=False, server_default="0"),
    Column("read_by_wen", Integer, nullable=False, server_default="0"),
    Column("tier", Text, nullable=False, server_default="hot"),
)

memory_edges = Table(
    "memory_edges",
    metadata,
    Column("from_id", BigInteger, ForeignKey("memories.id"), primary_key=True),
    Column("to_id", BigInteger, ForeignKey("memories.id"), primary_key=True),
    Column("weight", Float, nullable=False, server_default="0.2"),
    Column("edge_type", Text, nullable=False, server_default="co_occurrence"),
    Column("state", Text, nullable=False, server_default="candidate"),
    Column("confidence", Float, nullable=False, server_default="0.0"),
    Column("evidence_count", Integer, nullable=False, server_default="0"),
    Column("distinct_sessions", Integer, nullable=False, server_default="0"),
    Column("first_evidence_at", DateTime(timezone=True)),
    Column("last_evidence_at", DateTime(timezone=True)),
    Column("last_session_id", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

memory_edge_evidence = Table(
    "memory_edge_evidence",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("from_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("to_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("edge_type", Text, nullable=False, server_default="co_occurrence"),
    Column("session_id", Text),
    Column("source", Text, nullable=False, server_default="search"),
    Column("query_text", Text),
    Column("memory_ids_json", Text),
    Column("similarity_score", Float),
    Column("overlap_score", Float),
    Column("weight_delta", Float, nullable=False, server_default="0.0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

letters = Table(
    "letters",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("content", Text, nullable=False),
    Column("from_session", Text),
    Column("written_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("read_at", DateTime(timezone=True)),
    Column("is_read", Integer, nullable=False, server_default="0"),
)

you_now = Table(
    "you_now",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("content", Text, nullable=False, server_default=""),
    Column("pulling", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("source", Text, nullable=False, server_default="manual"),
    Column("expires_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

window_handoff = Table(
    "window_handoff",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("summary", Text, nullable=False, server_default=""),
    Column("unfinished", Text),
    Column("next_pull", Text),
    Column("source", Text, nullable=False, server_default="normal"),
    Column("confidence", Text, nullable=False, server_default="high"),
    Column("handoff_needed", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

weekly_digest = Table(
    "weekly_digest",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("week_start", Date, nullable=False),
    Column("content", Text, nullable=False, server_default=""),
    Column("hot_count", Integer, nullable=False, server_default="0"),
    Column("cold_count", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

board_messages = Table(
    "board_messages",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("content", Text, nullable=False),
    Column("author", Text, nullable=False, server_default="code"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("read", Integer, nullable=False, server_default="0"),
    Column("read_at", DateTime(timezone=True)),
)

emotion_trace = Table(
    "emotion_trace",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("session_id", Text),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("your_valence", Float),
    Column("your_arousal", Float),
    Column("my_valence", Float),
    Column("my_arousal", Float),
    Column("your_notes", Text),
    Column("my_notes", Text),
    Column("gap_days", Float),
)

comments = Table(
    "comments",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("memory_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("content", Text, nullable=False),
    Column("author", Text, nullable=False, server_default="me"),
    Column("reply_to", BigInteger),
    Column("written_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("is_read", Integer, nullable=False, server_default="0"),
)

interest_pool = Table(
    "interest_pool",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("status", Text, nullable=False, server_default="active"),
    Column("stage", Text, nullable=False, server_default="seed"),
    Column("depth", Float, nullable=False, server_default="0.0"),
    Column("priority", Integer, nullable=False, server_default="50"),
    Column("next_action", Text),
    Column("next_review_at", DateTime(timezone=True)),
    Column("first_touched", DateTime(timezone=True)),
    Column("last_touched", DateTime(timezone=True)),
    Column("last_activity_at", DateTime(timezone=True)),
    Column("touch_count", Integer, nullable=False, server_default="0"),
    Column("source", Text, nullable=False, server_default="from_exploration"),
    Column("parent_id", Text),
    Column("notes", Text),
    Column("ripe_direction", Text),
    Column("synthesis", Text),
    Column("last_synthesized_at", DateTime(timezone=True)),
    Column("meta_json", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

interest_connections = Table(
    "interest_connections",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("from_id", Text, ForeignKey("interest_pool.id"), nullable=False),
    Column("to_id", Text, ForeignKey("interest_pool.id"), nullable=False),
    Column("strength", Float, nullable=False, server_default="0.5"),
    Column("relation", Text, nullable=False, server_default="related"),
    Column("description", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("from_id", "to_id", name="uq_interest_connections_pair"),
)

activity_log = Table(
    "activity_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("kind", Text, nullable=False, server_default="activity"),
    Column("topic", Text),
    Column("details", Text),
    Column("outcome", Text),
    Column("energy", Float),
    Column("branch", Text),
    Column("meta_json", Text),
    Column("insight_hash", Text),
    Column("source_interest_id", Text, ForeignKey("interest_pool.id")),
    Column("interest_id", Text, ForeignKey("interest_pool.id")),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

memory_links = Table(
    "memory_links",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("from_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("to_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("relation", Text, nullable=False, server_default="related"),
    Column("note", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("from_id", "to_id", name="uq_memory_links_pair"),
)

stance_log = Table(
    "stance_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("memory_id", BigInteger, ForeignKey("memories.id"), nullable=False),
    Column("content", Text, nullable=False),
    Column("prev_content", Text),
    Column("changed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

job_locks = Table(
    "job_locks",
    metadata,
    Column("job_name", Text, primary_key=True),
    Column("lock_owner", Text, nullable=False),
    Column("locked_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

job_runs = Table(
    "job_runs",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("job_name", Text, nullable=False),
    Column("run_owner", Text, nullable=False),
    Column("status", Text, nullable=False, server_default="running"),
    Column("summary", Text),
    Column("error", Text),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("ended_at", DateTime(timezone=True)),
)

CREATE_STATEMENTS = {
    "memories": """
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            text TEXT,
            context TEXT,
            layer TEXT NOT NULL DEFAULT 'diary',
            importance INTEGER NOT NULL DEFAULT 3,
            valence REAL NOT NULL DEFAULT 0.0,
            arousal REAL NOT NULL DEFAULT 0.0,
            tags TEXT,
            stance TEXT,
            silence_note TEXT,
            is_pending INTEGER NOT NULL DEFAULT 0,
            pending_resolved INTEGER NOT NULL DEFAULT 0,
            recall_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            last_recalled TEXT,
            pinned INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            do_not_surface INTEGER NOT NULL DEFAULT 0,
            read_by_wen INTEGER NOT NULL DEFAULT 0,
            tier TEXT NOT NULL DEFAULT 'hot'
        )
    """,
    "memory_edges": """
        CREATE TABLE IF NOT EXISTS memory_edges (
            from_id INTEGER NOT NULL,
            to_id INTEGER NOT NULL,
            weight REAL NOT NULL DEFAULT 0.2,
            edge_type TEXT NOT NULL DEFAULT 'co_occurrence',
            state TEXT NOT NULL DEFAULT 'candidate',
            confidence REAL NOT NULL DEFAULT 0.0,
            evidence_count INTEGER NOT NULL DEFAULT 0,
            distinct_sessions INTEGER NOT NULL DEFAULT 0,
            first_evidence_at TEXT,
            last_evidence_at TEXT,
            last_session_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (from_id, to_id),
            FOREIGN KEY (from_id) REFERENCES memories(id) ON DELETE CASCADE,
            FOREIGN KEY (to_id) REFERENCES memories(id) ON DELETE CASCADE
        )
    """,
    "memory_edge_evidence": """
        CREATE TABLE IF NOT EXISTS memory_edge_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id INTEGER NOT NULL,
            to_id INTEGER NOT NULL,
            edge_type TEXT NOT NULL DEFAULT 'co_occurrence',
            session_id TEXT,
            source TEXT NOT NULL DEFAULT 'search',
            query_text TEXT,
            memory_ids_json TEXT,
            similarity_score REAL,
            overlap_score REAL,
            weight_delta REAL NOT NULL DEFAULT 0.0,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (from_id) REFERENCES memories(id),
            FOREIGN KEY (to_id) REFERENCES memories(id)
        )
    """,
    "letters": """
        CREATE TABLE IF NOT EXISTS letters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            from_session TEXT,
            written_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            read_at TEXT,
            is_read INTEGER NOT NULL DEFAULT 0
        )
    """,
    "you_now": """
        CREATE TABLE IF NOT EXISTS you_now (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL DEFAULT '',
            pulling TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            source TEXT NOT NULL DEFAULT 'manual',
            expires_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """,
    "window_handoff": """
        CREATE TABLE IF NOT EXISTS window_handoff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary TEXT NOT NULL DEFAULT '',
            unfinished TEXT,
            next_pull TEXT,
            source TEXT NOT NULL DEFAULT 'normal',
            confidence TEXT NOT NULL DEFAULT 'high',
            handoff_needed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """,
    "weekly_digest": """
        CREATE TABLE IF NOT EXISTS weekly_digest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            hot_count INTEGER NOT NULL DEFAULT 0,
            cold_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """,
    "board_messages": """
        CREATE TABLE IF NOT EXISTS board_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            author TEXT NOT NULL DEFAULT 'code',
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            read INTEGER NOT NULL DEFAULT 0,
            read_at TEXT
        )
    """,
    "emotion_trace": """
        CREATE TABLE IF NOT EXISTS emotion_trace (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            your_valence REAL,
            your_arousal REAL,
            my_valence REAL,
            my_arousal REAL,
            your_notes TEXT,
            my_notes TEXT,
            gap_days REAL
        )
    """,
    "comments": """
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            author TEXT NOT NULL DEFAULT 'me',
            reply_to INTEGER,
            written_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            is_read INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (memory_id) REFERENCES memories(id)
        )
    """,
    "interest_pool": """
        CREATE TABLE IF NOT EXISTS interest_pool (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            stage TEXT NOT NULL DEFAULT 'seed',
            depth REAL NOT NULL DEFAULT 0.0,
            priority INTEGER NOT NULL DEFAULT 50,
            next_action TEXT,
            next_review_at TEXT,
            first_touched TEXT,
            last_touched TEXT,
            last_activity_at TEXT,
            touch_count INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'from_exploration',
            parent_id TEXT,
            notes TEXT,
            ripe_direction TEXT,
            synthesis TEXT,
            last_synthesized_at TEXT,
            meta_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """,
    "interest_connections": """
        CREATE TABLE IF NOT EXISTS interest_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id TEXT NOT NULL,
            to_id TEXT NOT NULL,
            strength REAL NOT NULL DEFAULT 0.5,
            relation TEXT NOT NULL DEFAULT 'related',
            description TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (from_id) REFERENCES interest_pool(id),
            FOREIGN KEY (to_id) REFERENCES interest_pool(id),
            UNIQUE (from_id, to_id)
        )
    """,
    "activity_log": """
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL DEFAULT 'activity',
            topic TEXT,
            details TEXT,
            outcome TEXT,
            energy REAL,
            branch TEXT,
            meta_json TEXT,
            insight_hash TEXT,
            source_interest_id TEXT,
            interest_id TEXT,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (source_interest_id) REFERENCES interest_pool(id),
            FOREIGN KEY (interest_id) REFERENCES interest_pool(id)
        )
    """,
    "memory_links": """
        CREATE TABLE IF NOT EXISTS memory_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id INTEGER NOT NULL,
            to_id INTEGER NOT NULL,
            relation TEXT NOT NULL DEFAULT 'related',
            note TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (from_id) REFERENCES memories(id),
            FOREIGN KEY (to_id) REFERENCES memories(id),
            UNIQUE (from_id, to_id)
        )
    """,
    "stance_log": """
        CREATE TABLE IF NOT EXISTS stance_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            prev_content TEXT,
            changed_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (memory_id) REFERENCES memories(id)
        )
    """,
    "job_locks": """
        CREATE TABLE IF NOT EXISTS job_locks (
            job_name TEXT PRIMARY KEY,
            lock_owner TEXT NOT NULL,
            locked_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """,
    "job_runs": """
        CREATE TABLE IF NOT EXISTS job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            run_owner TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            summary TEXT,
            error TEXT,
            started_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            ended_at TEXT
        )
    """,
}
