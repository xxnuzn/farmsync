"""FarmSync 중앙 서버 (클라이언트-서버 구조의 서버 측).

역할
    ① 세션 관리자    : 접속/재접속 처리, session_id 발급 (L5)
    ② 이력 저장소    : 모든 대화를 영구 보관 (L7 + 저장소)
    ③ 전역 순번 발급 : 대화의 순서를 확정하는 유일한 권위 (L5)
    ④ 동기화 관리자  : 클라이언트의 수신 오프셋을 받아 밀린 구간만 전송 (L5 + L6)

실행
    uvicorn server.app:app --host 0.0.0.0 --port 8000
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .protocol import P, decode, encode_text, event, msg, validate_hello, validate_send
from .store import Store

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
DB_PATH = os.environ.get("FARMSYNC_DB", str(ROOT / "data" / "farmsync.db"))
MAX_SYNC_BATCH = int(os.environ.get("FARMSYNC_SYNC_BATCH", "1000"))

store: Store = None  # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    store = Store(DB_PATH)
    yield
    store.close()


app = FastAPI(title="FarmSync", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class Connection:
    """접속 하나. 순번이 뒤섞이지 않도록 전송에 잠금을 둔다."""

    def __init__(self, ws: WebSocket, room: str, user: str, client_id: str, session_id: str):
        self.ws = ws
        self.room = room
        self.user = user
        self.client_id = client_id
        self.session_id = session_id
        self.lock = asyncio.Lock()
        self.closed = False

    async def send(self, payload: Dict[str, Any]) -> None:
        if self.closed:
            return
        try:
            async with self.lock:
                await self.ws.send_text(encode_text(payload))
        except Exception:
            self.closed = True


class Hub:
    """방(room) 단위 접속 관리 + 브로드캐스트."""

    def __init__(self) -> None:
        self.rooms: Dict[str, Set[Connection]] = {}

    def join(self, conn: Connection) -> None:
        self.rooms.setdefault(conn.room, set()).add(conn)

    def leave(self, conn: Connection) -> None:
        self.rooms.get(conn.room, set()).discard(conn)

    def peers(self, room: str) -> List[Connection]:
        return list(self.rooms.get(room, set()))

    async def broadcast(self, room: str, payload: Dict[str, Any], exclude: Connection = None) -> int:
        targets = [c for c in self.peers(room) if c is not exclude]
        if not targets:
            return 0
        await asyncio.gather(*(c.send(payload) for c in targets), return_exceptions=True)
        return len(targets)

    async def log(self, conn: Connection, layer: int, text: str, broadcast: bool = False, **extra: Any) -> None:
        payload = event(layer, text, **extra)
        if broadcast:
            await self.broadcast(conn.room, payload)
        else:
            await conn.send(payload)


hub = Hub()


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"status": "ok", "service": "farmsync", "db": DB_PATH}


@app.get("/api/stats")
async def api_stats(room: str = "default") -> Dict[str, Any]:
    return {
        "room": room,
        "latest_seq": store.latest_seq(room),
        "online": store.online_users(room),
        "counters": store.counters,
        "history_count": len(store.history(room, limit=1000)),
    }


@app.get("/api/history")
async def api_history(room: str = "default", limit: int = 200) -> Dict[str, Any]:
    return {"room": room, "messages": store.history(room, limit=limit)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """WebSocket 핸들러.

    L4 전송 계층 관점에서 이 소켓은 TCP 위에서 동작한다(순서 보장·재전송).
    다만 TCP는 '연결이 끊긴 구간'을 모르므로, 그 구간의 복구는 L5 세션과 L7 응용이 담당한다.
    """
    await ws.accept()
    conn: Connection = None  # type: ignore[assignment]
    try:
        # ---- 1) HELLO 수신 : 접속 + 수신 오프셋 알림
        # 바이트/텍스트 어느 쪽으로 와도 처리한다.
        # 한쪽을 먼저 시도해 실패하면 그 프레임은 이미 소비되어
        # 다른 쪽으로 기다리다 교착이 생기므로 receive() 를 한 번만 쓴다.
        first = await ws.receive()
        if first.get("bytes") is not None:
            hello = decode(first["bytes"])
        elif first.get("text") is not None:
            hello = decode(first["text"].encode("utf-8"))
        else:
            return
    except WebSocketDisconnect:
        return
    except Exception:
        await ws.close(code=1003)
        return

    errors = validate_hello(hello)
    if hello.get("type") != P.HELLO or errors:
        await ws.send_text(
            encode_text(msg(P.ERROR, code="BAD_HELLO", detail="; ".join(errors) or "HELLO 필요"))
        )
        await ws.close(code=1008)
        return

    room = hello["room"]
    user = hello["user"]
    client_id = hello["client_id"]
    last_seq = int(hello.get("last_seq", 0) or 0)

    session_id = f"SID-{uuid.uuid4().hex[:8]}"
    store.open_session(session_id, room, user, client_id)
    conn = Connection(ws, room, user, client_id, session_id)
    hub.join(conn)

    # ---- 2) WELCOME : 세션 발급
    latest = store.latest_seq(room)
    await conn.send(
        msg(
            P.WELCOME,
            session_id=session_id,
            room=room,
            user=user,
            latest_seq=latest,
            online=store.online_users(room),
        )
    )
    await hub.log(conn, 5, f"세션 발급 {session_id} · 접속자 {store.online_users(room)}")
    await hub.log(conn, 4, f"TCP 연결 수립 후 WebSocket 핸드셰이크 완료 · 방 {room}")

    # ---- 3) SYNC : 밀린 구간만 전송 (수신 오프셋 기반 재동기화)
    if last_seq < latest:
        missing = store.since(room, last_seq, limit=MAX_SYNC_BATCH)
        store.mark_resync(session_id, len(missing))
        await conn.send(
            msg(
                P.SYNC,
                room=room,
                from_seq=last_seq,
                to_seq=latest,
                count=len(missing),
                messages=missing,
                compressed=False,
            )
        )
        await hub.log(
            conn, 5,
            f"재동기화: 수신 오프셋 seq {last_seq} → {latest} · 밀린 메시지 {len(missing)}건 전송",
        )
        await hub.log(
            conn, 6,
            f"표현 계층: JSON 직렬화 + UTF-8, {len(missing)}건 묶음 전송",
        )
        await hub.broadcast(
            room,
            event(5, f"{user} 님이 재접속 · {len(missing)}건 동기화 복구"),
        )
    else:
        await hub.log(conn, 5, f"재동기화 불필요 (수신 오프셋 seq {last_seq} = 서버 최신)")

    await hub.broadcast(
        room,
        msg(P.PRESENCE, user=user, online=True, users=store.online_users(room)),
        exclude=conn,
    )

    # ---- 4) 메시지 루프
    try:
        while True:
            raw = await ws.receive_text()
            payload = decode(raw.encode("utf-8"))
            mtype = payload.get("type")

            if mtype == P.SEND:
                await handle_send(conn, payload)

            elif mtype == P.READ:
                seq = int(payload.get("seq", 0))
                if seq and store.add_receipt(seq, user):
                    await hub.broadcast(
                        conn.room, msg(P.READ, seq=seq, reader=user), exclude=None
                    )
                    await hub.log(conn, 7, f"읽음 표시 seq {seq} · {user}")

            elif mtype == P.PING:
                await conn.send(msg(P.PONG, ts=payload.get("ts")))

            else:
                await conn.send(
                    msg(P.ERROR, code="UNKNOWN_TYPE", detail=f"알 수 없는 타입: {mtype}")
                )
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        await conn.send(msg(P.ERROR, code="SERVER_ERROR", detail=str(exc)))
    finally:
        # ---- 5) 접속 종료 : 세션 닫기 + 접속 상태 방송
        conn.closed = True
        hub.leave(conn)
        store.close_session(conn.session_id)
        await hub.broadcast(
            conn.room,
            msg(P.PRESENCE, user=conn.user, online=False, users=store.online_users(conn.room)),
        )
        await hub.broadcast(conn.room, event(5, f"{conn.user} 님 세션 종료 ({conn.session_id})"))


async def handle_send(conn: Connection, payload: Dict[str, Any]) -> None:
    """SEND 처리 — 멱등성 + 전역 순번 발급 + 브로드캐스트."""
    errors = validate_send(payload)
    if errors:
        await conn.send(msg(P.ERROR, code="BAD_SEND", detail="; ".join(errors)))
        return

    cid = payload["cid"]
    text = payload["text"]

    # L7 응용: 메시지 수신
    await hub.log(conn, 7, f"메시지 수신 cid={cid[:12]}… '{text[:40]}'")

    # 저장 + 전역 순번 발급 (중복이면 기존 순번 반환, 새 행 없음)
    seq, duplicate = store.append(conn.room, cid, conn.user, text)

    # L6 표현: 직렬화 크기
    await hub.log(conn, 6, f"JSON 직렬화 + UTF-8 · 본문 {len(text.encode('utf-8'))}바이트")

    if duplicate:
        # 같은 cid 재전송 → 저장하지 않았음을 명확히 알린다 (중복 주문 방지)
        await conn.send(msg(P.DELIVERED, cid=cid, seq=seq, duplicate=True))
        await hub.log(
            conn, 7, f"중복 감지: cid={cid[:12]}… 이미 seq {seq} 로 저장됨 · 새로 저장하지 않음",
            duplicate=True, cid=cid, seq=seq,
        )
        return

    # 전송 계층: 브로드캐스트 (순서 보장은 아래 순번으로 확정)
    record = {
        "seq": seq,
        "cid": cid,
        "from": conn.user,
        "text": text,
        "ts": payload.get("ts"),
        "read_by": [],
    }
    await conn.send(msg(P.DELIVERED, cid=cid, seq=seq, duplicate=False))
    delivered = await hub.broadcast(conn.room, msg(P.MSG, **record))
    await hub.log(
        conn, 4,
        f"브로드캐스트 완료 → {delivered}명에게 전달 · 전역 순번 seq {seq} 확정",
    )


# ---------------------------------------------------------------- 정적 파일 (홈페이지 + 앱)
if WEB_DIR.exists():
    assets = WEB_DIR / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/")
    async def home() -> FileResponse:
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/app")
    async def chat_app() -> FileResponse:
        return FileResponse(WEB_DIR / "app.html")
