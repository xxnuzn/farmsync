"""FarmSync 프로토콜 정의 (7계층 응용 계층).

이 파일이 OSI 7계층 중 **7 응용 계층**에 해당한다.
애플리케이션이 직접 정의하는 메시지 형식과 상태 전이 규약을 담는다.

계층 대응
    L7 응용       : 이 파일의 메시지 타입 (HELLO / SEND / SYNC / MSG / DELIVERED / READ / PRESENCE)
    L6 표현       : encode() / decode() — JSON 직렬화 + UTF-8, SYNC 묶음은 zlib 압축
    L5 세션       : session_id, last_seq(수신 오프셋) 로 대화 연속성 유지
    L4 전송       : WebSocket 위에서 동작하며 그 아래는 TCP (순서 보장·재전송)
    L3 네트워크   : 서버 IP까지의 라우팅 (코드 밖, OS가 담당)
    L2 데이터링크 : 이더넷/Wi-Fi 프레임, MAC 주소 (코드 밖)
    L1 물리       : 전기·전파 신호 (코드 밖)
"""

import json
import zlib
from typing import Any, Dict, List


class P:
    """메시지 타입 상수 (L7)."""

    # 클라이언트 -> 서버
    HELLO = "HELLO"          # 접속 + 내가 어디까지 받았는지 알림
    SEND = "SEND"            # 메시지 전송 (cid 로 멱등성 확보)
    READ = "READ"            # 읽음 표시
    PING = "PING"

    # 서버 -> 클라이언트
    WELCOME = "WELCOME"      # 세션 발급
    SYNC = "SYNC"            # 밀린 구간 일괄 전송
    MSG = "MSG"              # 새 메시지 (서버가 부여한 전역 순번 포함)
    DELIVERED = "DELIVERED"  # 서버 저장 완료 통보 (duplicate 플래그 포함)
    PRESENCE = "PRESENCE"    # 접속/접속 종료
    EVENT = "EVENT"          # 계층 로그용 서버 이벤트
    ERROR = "ERROR"
    PONG = "PONG"


# 계층 표기 (로그/UI 용도)
LAYER = {7: "응용", 6: "표현", 5: "세션", 4: "전송", 3: "네트워크", 2: "데이터링크", 1: "물리"}


def event(layer: int, text: str, **extra: Any) -> Dict[str, Any]:
    """L7 EVENT 메시지 생성 — 계층 로그 패널에 표시된다."""
    payload = {"type": P.EVENT, "layer": layer, "layer_name": LAYER.get(layer, "?"), "text": text}
    payload.update(extra)
    return payload


def msg(type_: str, **fields: Any) -> Dict[str, Any]:
    """L7 메시지 조립."""
    payload = {"type": type_}
    payload.update(fields)
    return payload


def encode(payload: Dict[str, Any], compress: bool = False) -> bytes:
    """L6 표현 계층: 파이썬 객체 -> JSON 문자열 -> UTF-8 바이트.

    compress=True 이면 zlib 으로 압축해 밀린 대화 묶음을 줄인다(표현 계층의 압축 역할).
    """
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if compress:
        return zlib.compress(body, 6)
    return body


def decode(raw: bytes) -> Dict[str, Any]:
    """L6 표현 계층: 수신 바이트 -> JSON -> 파이썬 객체."""
    # zlib 헤더(0x78)로 시작하면 압축된 묶음으로 판단한다.
    if raw[:1] == b"\x78":
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            pass
    return json.loads(raw.decode("utf-8"))


def encode_text(payload: Dict[str, Any], compress: bool = False) -> str:
    """텍스트 프레임으로 보낼 때 사용."""
    return encode(payload, compress=compress).decode("utf-8")


def validate_hello(payload: Dict[str, Any]) -> List[str]:
    """HELLO 필수 필드 검증 (L7 응용 계층의 입력 검증)."""
    errors = []
    for field in ("room", "user", "client_id"):
        if not isinstance(payload.get(field), str) or not payload.get(field):
            errors.append(f"{field} 누락")
    if not isinstance(payload.get("last_seq", 0), int):
        errors.append("last_seq 는 정수여야 함")
    return errors


def validate_send(payload: Dict[str, Any]) -> List[str]:
    errors = []
    if not isinstance(payload.get("cid"), str) or not payload.get("cid"):
        errors.append("cid 누락 (멱등성 키)")
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        errors.append("text 누락")
    return errors
