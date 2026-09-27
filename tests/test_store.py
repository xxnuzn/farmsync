"""저장소 단위 테스트 — 멱등성·전역 순번·수신 오프셋."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.store import Store  # noqa: E402


@pytest.fixture()
def store():
    s = Store(":memory:")
    yield s
    s.close()


def test_append_assigns_increasing_global_seq(store):
    seq1, dup1 = store.append("room", "cid-1", "농장주", "상추 300kg 가능합니다")
    seq2, dup2 = store.append("room", "cid-2", "기업", "확정할게요")
    assert (dup1, dup2) == (False, False)
    assert seq2 == seq1 + 1


def test_duplicate_cid_is_not_stored_twice(store):
    """같은 cid 재전송 → 새 행이 생기지 않고 기존 seq 가 그대로 반환된다."""
    first, dup_first = store.append("room", "cid-abc", "농장주", "확정")
    second, dup_second = store.append("room", "cid-abc", "농장주", "확정")
    assert dup_first is False
    assert dup_second is True
    assert first == second
    assert len(store.history("room")) == 1
    assert store.counters["duplicates_ignored"] == 1


def test_cid_is_scoped_per_room(store):
    a, _ = store.append("room-a", "cid-x", "u", "hello")
    b, dup = store.append("room-b", "cid-x", "u", "hello")
    assert dup is False
    assert a != b


def test_since_returns_only_missing_range(store):
    for i in range(1, 6):
        store.append("room", f"cid-{i}", "u", f"m{i}")
    missing = store.since("room", 2)
    assert [m["text"] for m in missing] == ["m3", "m4", "m5"]
    assert store.since("room", 5) == []
    assert store.latest_seq("room") == 5


def test_history_is_ordered_and_limited(store):
    for i in range(1, 11):
        store.append("room", f"cid-{i}", "u", f"m{i}")
    hist = store.history("room", limit=3)
    assert [m["seq"] for m in hist] == [8, 9, 10]


def test_receipts_are_deduplicated(store):
    seq, _ = store.append("room", "cid-1", "농장주", "확인")
    assert store.add_receipt(seq, "기업") is True
    assert store.add_receipt(seq, "기업") is False
    msgs = store.history("room")
    assert msgs[0]["read_by"] == ["기업"]


def test_sessions_track_online_users(store):
    store.open_session("SID-1", "room", "농장주", "client-a")
    store.open_session("SID-2", "room", "기업", "client-b")
    assert store.online_users("room") == ["기업", "농장주"]
    store.close_session("SID-1")
    assert store.online_users("room") == ["기업"]


def test_messages_survive_process_restart(tmp_path):
    """서버를 재시작해도 이력이 남아 있어야 한다 (이력 저장소)."""
    db = str(tmp_path / "farmsync.db")
    s1 = Store(db)
    s1.append("room", "cid-1", "농장주", "재시작 전 메시지")
    s1.close()

    s2 = Store(db)
    assert s2.latest_seq("room") == 1
    assert s2.history("room")[0]["text"] == "재시작 전 메시지"
    s2.close()
