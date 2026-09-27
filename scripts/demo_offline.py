"""종단 간 시연 + 검증 스크립트.

실제로 uvicorn 을 띄우고 두 클라이언트를 붙여 다음을 재현한다.

    1) 정상 대화 (양쪽 모두 실시간 수신)
    2) 클라이언트 A 의 회선 단절 (연결 강제 종료)
    3) A 가 끊긴 동안 A 는 3건 작성(로컬 큐), B 는 2건 발신
    4) A 재접속 → HELLO 에 마지막 수신 오프셋을 실어 **밀린 구간만** 복구
    5) A 의 로컬 큐 3건 전송 → 같은 cid 로 의도적 재전송 → 서버가 중복 차단
    6) 서버 이력과 클라이언트 상태를 대조해 유실 0 / 중복 0 / 순서 오류 0 판정

실행
    python -u scripts/demo_offline.py
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
PORT = os.environ.get("DEMO_PORT", "8899")
BASE = f"http://127.0.0.1:{PORT}"
WS = f"ws://127.0.0.1:{PORT}/ws"
ROOM = "demo-room"


class Client:
    """클라이언트 대역 — 앱과 같은 방식으로 로컬 큐와 수신 오프셋을 들고 있는다."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.ws = None
        self.outbox = []          # 아직 서버에 못 보낸 메시지 (로컬 큐)
        self.inbox = []           # 서버 순번을 받은 메시지
        self.last_seq = 0         # 수신 오프셋
        self.session = None
        self.server_latest = 0
        self.duplicates = 0       # 서버가 "이미 저장됨"이라고 답한 횟수
        self.resyncs = []         # (from_seq, to_seq, count)

    # ------------------------------------------------------------- 수신
    def _dispatch(self, m: dict) -> None:
        t = m.get("type")
        if t == "WELCOME":
            self.session = m["session_id"]
            self.server_latest = m["latest_seq"]
        elif t == "SYNC":
            for rec in m["messages"]:
                self._merge(rec)
            self.resyncs.append((m["from_seq"], m["to_seq"], m["count"]))
        elif t == "MSG":
            self._merge(m)
        elif t == "DELIVERED" and m.get("duplicate"):
            self.duplicates += 1

    def _merge(self, rec: dict) -> None:
        if any(x["seq"] == rec["seq"] for x in self.inbox):
            return
        self.inbox.append({"seq": rec["seq"], "cid": rec.get("cid"), "from": rec.get("from"), "text": rec["text"]})
        self.inbox.sort(key=lambda x: x["seq"])
        self.last_seq = max(self.last_seq, rec["seq"])

    async def _recv_until(self, wanted: set, timeout: float = 8.0) -> dict:
        end = time.time() + timeout
        while True:
            remain = end - time.time()
            if remain <= 0:
                raise TimeoutError(f"{self.name}: {wanted} 대기 시간 초과")
            raw = await asyncio.wait_for(self.ws.recv(), timeout=remain)
            m = json.loads(raw)
            self._dispatch(m)
            if m.get("type") in wanted:
                return m

    async def connect(self) -> "Client":
        self.ws = await websockets.connect(WS, max_size=4 * 1024 * 1024)
        await self.ws.send(json.dumps({
            "type": "HELLO", "room": ROOM, "user": self.name,
            "client_id": f"cli-{self.name}", "last_seq": self.last_seq, "ts": time.time(),
        }, ensure_ascii=False))
        w = await self._recv_until({"WELCOME"})
        # 밀린 구간이 있으면 SYNC 가 이어서 온다. 여기서 반드시 소비해야 한다.
        if w["latest_seq"] > self.last_seq:
            await self._recv_until({"SYNC"})
        return self

    # ------------------------------------------------------------- 발신
    async def send(self, text: str) -> str:
        cid = str(uuid.uuid4())
        await self.ws.send(json.dumps({"type": "SEND", "cid": cid, "text": text, "ts": time.time()}, ensure_ascii=False))
        return cid

    def queue(self, text: str) -> str:
        """회선이 끊긴 상태에서 작성 — 전송하지 않고 로컬 큐에만 넣는다."""
        cid = str(uuid.uuid4())
        self.outbox.append({"cid": cid, "text": text, "sent": False})
        return cid

    async def flush(self, force: bool = False) -> int:
        sent = 0
        for item in self.outbox:
            if item["sent"] and not force:
                continue
            await self.ws.send(json.dumps({"type": "SEND", "cid": item["cid"], "text": item["text"], "ts": time.time()}, ensure_ascii=False))
            item["sent"] = True
            sent += 1
        return sent

    async def drain(self, seconds: float = 1.0) -> None:
        end = time.time() + seconds
        while True:
            remain = end - time.time()
            if remain <= 0:
                return
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=remain)
            except asyncio.TimeoutError:
                return
            self._dispatch(json.loads(raw))

    async def cut(self) -> None:
        await self.ws.close()
        self.ws = None


def api(path: str) -> dict:
    with urllib.request.urlopen(f"{BASE}{path}", timeout=5) as r:
        return json.loads(r.read())


async def main() -> int:
    db = ROOT / "data" / "demo.db"
    for f in (db, Path(str(db) + "-wal"), Path(str(db) + "-shm")):
        if f.exists():
            f.unlink()
    env = dict(os.environ, FARMSYNC_DB=str(db))
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "server.app:app", "--host", "127.0.0.1", "--port", PORT, "--log-level", "warning"],
        cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        try:
            api("/healthz")
            break
        except Exception:
            time.sleep(0.3)
    else:
        proc.terminate()
        print("서버 기동 실패")
        return 1

    print("=" * 74)
    print(" FarmSync 오프라인 재동기화 시연")
    print("=" * 74)

    a, b = Client("농장주"), Client("기업")
    await a.connect()
    await b.connect()
    print("\n[1] 정상 대화 — 양쪽 모두 실시간 수신")
    await a.send("다음 주 상추 300kg 가능하실까요?")
    await a.send("400kg까지 가능합니다. 3,200원/kg으로 드릴게요.")
    await b.send("좋습니다. 300kg 확정할게요.")
    await asyncio.sleep(0.6)
    await a.drain(0.6)
    await b.drain(0.6)
    print(f"    서버 최신 seq {api(f'/api/stats?room={ROOM}')['latest_seq']} · A 수신 오프셋 seq {a.last_seq} · B 수신 오프셋 seq {b.last_seq}")

    print("\n[2] A 의 회선 단절 (연결 강제 종료)")
    await a.cut()
    print("    A 연결 끊김 — 이제 A 가 보내려는 메시지는 전송 계층이 책임지지 않는다")

    print("\n[3] 끊긴 동안 양쪽이 작성")
    queued = [
        a.queue("300kg 확정하겠습니다."),
        a.queue("화요일 오전 출하로 잡을게요."),
        a.queue("송장은 출하 직후에 올려드리겠습니다."),
    ]
    await b.send("네, 출하 일정 확인했습니다.")
    await b.send("검수는 창고에서 진행합니다.")
    await asyncio.sleep(0.5)
    await b.drain(0.6)
    print(f"    A 로컬 큐 {len(queued)}건 적재 (전송되지 않음) · B 수신 오프셋 seq {b.last_seq}")
    print(f"    → 서버에는 B 의 메시지 2건이 이미 저장되어 있다")

    print("\n[4] A 재접속 — 수신 오프셋을 실어 보내고 밀린 구간만 복구")
    await a.connect()
    print(f"    A 재접속 완료 · 복구된 메시지 {len(a.inbox)}건 · 수신 오프셋 seq {a.last_seq}")
    for f, t, c in a.resyncs:
        print(f"    재동기화: seq {f} → {t} · 밀린 {c}건 수신")

    print("\n[5] A 의 로컬 큐 전송 → 같은 cid 로 의도적 재전송")
    first = await a.flush()
    await asyncio.sleep(0.5)
    await a.drain(0.6)
    again = await a.flush(force=True)
    await asyncio.sleep(0.6)
    await a.drain(0.8)
    await b.drain(0.8)   # B 도 수신 루프를 돌려야 A 의 메시지를 받는다
    print(f"    최초 전송 {first}건 → 동일 cid 로 {again}건 재전송")
    print(f"    서버가 중복으로 처리한 건수: {a.duplicates}건 (기대 {len(queued)}건)")

    print("\n[6] 서버 이력과 클라이언트 상태 대조")
    stats = api(f"/api/stats?room={ROOM}")
    history = api(f"/api/history?room={ROOM}&limit=500")["messages"]
    seqs = [m["seq"] for m in history]
    unique_cids = len({m["cid"] for m in history})
    expected_total = 3 + 2 + 3  # 1단계 3건 + 3단계 B 2건 + 5단계 A 3건

    a_seqs = [m["seq"] for m in a.inbox]
    b_seqs = [m["seq"] for m in b.inbox]
    a_missing = [s for s in seqs if s not in a_seqs]
    b_missing = [s for s in seqs if s not in b_seqs]

    print(f"    서버 저장 메시지 : {len(history)}건 (기대 {expected_total}건, seq {seqs[0] if seqs else '-'} ~ {seqs[-1] if seqs else '-'})")
    print(f"    고유 cid 수      : {unique_cids}건 (중복 저장이 없으면 저장 건수와 같아야 함)")
    print(f"    순번 단조 증가   : {'OK' if seqs == sorted(seqs) else 'FAIL'}")
    print(f"    서버 중복 차단   : {stats['counters']['duplicates_ignored']}건 (기대 {len(queued)}건)")
    print(f"    재동기화 횟수    : {stats['counters']['resyncs']}회 · 복구 메시지 {stats['counters']['resync_messages']}건")
    print(f"    A 가 못 받은 순번: {a_missing if a_missing else '없음 (유실 0)'}")
    print(f"    B 가 못 받은 순번: {b_missing if b_missing else '없음 (유실 0)'}")

    checks = {
        "저장 건수가 기대와 일치": len(history) == expected_total,
        "고유 cid = 저장 건수 (중복 저장 0)": unique_cids == len(history),
        "순번 단조 증가 (순서 오류 0)": seqs == sorted(seqs),
        "중복 차단이 기대와 일치": stats["counters"]["duplicates_ignored"] == len(queued),
        "A 유실 0": not a_missing,
        "B 유실 0": not b_missing,
        "부분 재동기화가 실제로 일어남": bool(a.resyncs),
    }
    print("\n" + "=" * 74)
    for name, ok in checks.items():
        print(f"  [{'통과' if ok else '실패'}] {name}")
    verdict = all(checks.values())
    print("-" * 74)
    print(" 판정:", "통과 — 유실 0 · 중복 0 · 순서 오류 0" if verdict else "실패 — 위 항목을 확인하세요")
    print("=" * 74)

    await a.cut()
    await b.cut()
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
    return 0 if verdict else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
