"""
storage/base.py

Shared storage interface for AmI backends.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class WoStorageBase(ABC):
    @abstractmethod
    def get_anchors(self) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_recent_important(self, days: int = 3,
                             min_importance: int = 3) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_recent_memories(self, hours: int = 24, limit: int = 6,
                            min_importance: int = 1) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_unread_comments(self) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def write_memory(self, content: str, layer: str = "diary",
                     importance: int = 3, valence: float = 0.0,
                     arousal: float = 0.0, tags=None, stance: str = None,
                     silence_note: str = None, is_pending: int = 0,
                     do_not_surface: int = 0,
                     search_text: str | None = None,
                     context: str | None = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_memories(self, layer: str = None, status: str = "active",
                     limit: int = 200) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def list_pending(self) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_memory(self, id: int, touch: bool = True,
                   include_context: bool = False) -> Optional[Dict]:
        raise NotImplementedError

    def get_memory_neighbors(self, memory_id: int, min_weight: float = 0.5,
                             limit: int = 5) -> List[Dict]:
        return []

    def decay_memory_edges(self, hebbian_decay: float = 0.9,
                           manual_decay: float = 0.95,
                           min_weight: float = 0.1) -> Dict:
        return {
            "updated_edges": 0,
            "pruned_edges": 0,
            "manual_edges_before": 0,
            "hebbian_edges_before": 0,
            "remaining_edges": 0,
        }

    @abstractmethod
    def update_memory(self, id: int, **kwargs) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_unread_by_wen(self) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def mark_read_by_wen(self, ids: Optional[List[int]] = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def surface_breath(self, consume: bool = True) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def search_memories(self, query: str, limit: int = 10, session_id: str | None = None) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def link_memories(self, from_id: int, to_id: int,
                      relation: str = "related", note: str = None):
        raise NotImplementedError

    @abstractmethod
    def unlink_memories(self, from_id: int, to_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_memory_links(self, memory_id: int) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def add_comment(self, memory_id: int, content: str,
                    author: str = "me", reply_to: int = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_comments(self, memory_id: int) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def write_letter(self, content: str, from_session: str = "") -> int:
        raise NotImplementedError

    @abstractmethod
    def get_unread_letters(self, recent_hours: int = 0) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def mark_letters_read(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_you_now(self) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def write_you_now(self, content: str | None = None,
                      pulling: str | None = None,
                      source: str = "manual",
                      expires_hours: int = 72) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def get_latest_handoff(self) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def write_window_handoff(self, summary: str,
                             unfinished: str | None = None,
                             next_pull: str | None = None,
                             source: str = "normal",
                             confidence: str = "high",
                             handoff_needed: bool = False) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def mark_handoff_needed(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def clear_handoff_needed(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def write_weekly_digest(self, week_start: str, content: str,
                            hot_count: int = 0, cold_count: int = 0) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_weekly_digests(self, limit: int = 4) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def add_board_message(self, author: str, content: str) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def read_board_messages(self, reader: str, limit: int = 20,
                            mark_read: bool = True,
                            include_recent_hours: int = 0) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def list_board_messages(self, limit: int = 200) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def add_emotion_trace(self, your_valence: float = None,
                          your_arousal: float = None,
                          my_valence: float = None,
                          my_arousal: float = None,
                          your_notes: str = None,
                          my_notes: str = None,
                          session_id: str = "") -> int:
        raise NotImplementedError

    @abstractmethod
    def get_emotion_history(self, limit: int = 7) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_stance_log(self, memory_id: int) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def get_emotion_delta(self) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def interest_add(self, id: str, name: str, stage: str = "seed",
                     source: str = "from_exploration",
                     parent_id: str = None, notes: str = None,
                     status: str = "active",
                     priority: int = 50,
                     next_action: str | None = None,
                     next_review_at: str | None = None,
                     meta_json: str | None = None) -> bool:
        raise NotImplementedError

    @abstractmethod
    def interest_touch(self, id: str, depth_delta: float = 0.1,
                       notes: str = None,
                       outcome: str | None = None,
                       kind: str = "advance",
                       branch: str | None = None) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def interest_update(self, id: str, **kwargs) -> bool:
        raise NotImplementedError

    @abstractmethod
    def interest_list(self, stage: str = None, status: str = None,
                      sort: str = "priority", limit: int = 50,
                      include_qa: bool = False) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def interest_get(self, id: str, activity_limit: int = 8,
                     include_connections: bool = True,
                     include_qa: bool = False) -> Optional[Dict]:
        raise NotImplementedError

    @abstractmethod
    def interest_review(self, limit: int = 3, status: str = "active",
                        include_qa: bool = False) -> List[Dict]:
        raise NotImplementedError

    @abstractmethod
    def interest_synthesize(self, interest_id: str | None = None,
                            push_insights: bool = True) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def interest_connect(self, from_id: str, to_id: str,
                         strength: float = 0.5,
                         relation: str = "related",
                         description: str = None) -> bool:
        raise NotImplementedError

    @abstractmethod
    def activity_write(self, type: str = None, kind: str = None,
                       topic: str = None, details: str = None,
                       outcome: str = None, energy: float = None,
                       branch: str = None, meta_json: str = None,
                       insight_hash: str = None,
                       source_interest_id: str = None,
                       interest_id: str = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def activity_recent(self, limit: int = 20,
                        kind: str = None,
                        interest_id: str = None,
                        include_qa: bool = False) -> List[Dict]:
        raise NotImplementedError

    def activity_cleanup(self, days: int = 7) -> int:
        """Delete activity_log entries older than `days` days. Returns deleted count.
        Default no-op for backends that don't implement cleanup (e.g. SQLite fallback)."""
        return 0

    @abstractmethod
    def stats(self) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def run_decay(self, dry_run: bool = True) -> Dict:
        raise NotImplementedError

    def run_dream_pass(self) -> Dict:
        return {
            "status": "noop",
            "updated_edges": 0,
            "promoted_edges": 0,
            "pruned_edges": 0,
        }
