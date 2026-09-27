"""FarmSync 저장소 (메시지 이력 + 세션 + 읽음 표시).

핵심 세 가지를 sqlite 로 보장한다.
1. **전역 순번**  : seq AUTOINCREMENT — 중앙 서버가 대화 순서를 확정한다 (L5 세션 / L7 응용)
2. **멱등성**     : UNIQUE(room, cid) — 같은 메시지가 두 번 도착해도 한 번만 저장한다 (L7 응용)
3. **수신 오프셋** : since(room, after_seq) — 클라이언트가 못 받은 구간만 정확히 돌려준다 (L5 세션)
"""

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    room     TEXT NOT NULL,
    cid      TEXT NOT NULL,
    sender   TEXT NOT NULL,
    text     TEXT NOT NULL,
    ts       REAL NOT NULL,
    UNIQUE(room, cid)
);
CREATE INDEX IF NOT EXISTS idx_messages_room_seq ON messages(room, seq);

CREATE TABLE IF NOT EXISTS receipts (
    seq    INTEGER NOT NULL,
    reader TEXT NOT NULL,
    ts     REAL NOT NULL,
    PRIMARY KEY(seq, reader)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT PRIMARY KEY,
    room            TEXT NOT NULL,
    user            TEXT NOT NULL,
    client_id       TEXT NOT NULL,
    connected_at    REAL NOT NULL,
    disconnected_at REAL,
    resynced        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_room ON sessions(room);
"""


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        if path != ":memory:":
            Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            if path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)
            self._conn.commit()
        # 실증 지표 (중복 0 / 유실 0 / 순서 오류 0 을 증명하는 카운터)
        self.counters: Dict[str, int] = {
            "appended": 0,
            "duplicates_ignored": 0,
            "resyncs": 0,
            "resync_messages": 0,
            "delivered": 0,
            "sessions_opened": 0,
        }

    # ------------------------------------------------------------------ 쓰기
    def append(self, room: str, cid: str, sender: str, text: str) -> Tuple[int, bool]:
        """메시지를 저장하고 (seq, duplicate) 를 돌려준다.

        이미 같은 cid 가 있으면 **새 행을 만들지 않고** 기존 seq 를 반환한다.
        이것이 중복 주문을 막는 멱등성 구현이다.
        """
        now = time.time()
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO messages(room, cid, sender, text, ts) VALUES(?,?,?,?,?)",
                    (room, cid, sender, text, now),
                )
                self._conn.commit()
                self.counters["appended"] += 1
                return int(cur.lastrowid), False
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT seq FROM messages WHERE room=? AND cid=?", (room, cid)
                ).fetchone()
                self.counters["duplicates_ignored"] += 1
                return (int(row["seq"]) if row else 0), True

    def add_receipt(self, seq: int, reader: str) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO receipts(seq, reader, ts) VALUES(?,?,?)", (seq, reader, time.time())
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    # ------------------------------------------------------------------ 읽기
    def since(self, room: str, after_seq: int, limit: int = 1000) -> List[Dict[str, Any]]:
        """after_seq 이후의 메시지만 순번 순서로 반환한다 (수신 오프셋 기반 재동기화)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, cid, sender, text, ts FROM messages "
                "WHERE room=? AND seq>? ORDER BY seq ASC LIMIT ?",
                (room, after_seq, limit),
            ).fetchall()
            receipts = self._receipt_map(room)
        return [self._row_to_dict(r, receipts) for r in rows]

    def latest_seq(self, room: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(seq) AS m FROM messages WHERE room=?", (room,)
            ).fetchone()
        return int(row["m"]) if row and row["m"] is not None else 0

    def history(self, room: str, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM (SELECT seq, cid, sender, text, ts FROM messages "
                "WHERE room=? ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC",
                (room, limit),
            ).fetchall()
            receipts = self._receipt_map(room)
        return [self._row_to_dict(r, receipts) for r in rows]

    def _receipt_map(self, room: str) -> Dict[int, List[str]]:
        rows = self._conn.execute(
            "SELECT r.seq, r.reader FROM receipts r "
            "JOIN messages m ON m.seq = r.seq WHERE m.room=?",
            (room,),
        ).fetchall()
        out: Dict[int, List[str]] = {}
        for r in rows:
            out.setdefault(int(r["seq"]), []).append(r["reader"])
        return out

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, receipts: Dict[int, List[str]]) -> Dict[str, Any]:
        seq = int(row["seq"])
        return {
            "seq": seq,
            "cid": row["cid"],
            "from": row["sender"],
            "text": row["text"],
            "ts": row["ts"],
            "read_by": receipts.get(seq, []),
        }

    # ------------------------------------------------------------------ 세션 (L5)
    def open_session(self, session_id: str, room: str, user: str, client_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO sessions(session_id, room, user, client_id, connected_at, disconnected_at, resynced) "
                "VALUES(?,?,?,?,?,NULL,0)",
                (session_id, room, user, client_id, time.time()),
            )
            self._conn.commit()
            self.counters["sessions_opened"] += 1

    def close_session(self, session_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sessions SET disconnected_at=? WHERE session_id=?", (time.time(), session_id)
            )
            self._conn.commit()

    def mark_resync(self, session_id: str, count: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sessions SET resynced=1 WHERE session_id=?", (session_id,)
            )
            self._conn.commit()
        self.counters["resyncs"] += 1
        self.counters["resync_messages"] += count

    def online_users(self, room: str) -> List[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT user FROM sessions WHERE room=? AND disconnected_at IS NULL ORDER BY user",
                (room,),
            ).fetchall()
        return [r["user"] for r in rows]

    def session_summary(self, room: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT session_id, user, client_id, connected_at, disconnected_at, resynced "
                "FROM sessions WHERE room=? ORDER BY connected_at DESC LIMIT 20",
                (room,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ 종료
    def close(self) -> None:
        with self._lock:
            self._conn.close()
