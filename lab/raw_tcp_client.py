# -*- coding: utf-8 -*-
"""
LayerTalk - 클라이언트
실행: python3 client.py <닉네임> [보낼메시지]
보낼 메시지를 인자로 주면 1회 전송 후 종료 (데모용), 없으면 대화형.
"""
import socket, threading, json, sys, time

HOST, PORT = "127.0.0.1", 5000
NICK = sys.argv[1] if len(sys.argv) > 1 else "guest"
FIRST = sys.argv[2] if len(sys.argv) > 2 else None

def L(layer, msg):
    print(f"[{NICK}] [{layer}] {msg}", flush=True)

def pack(obj):
    """[7 응용]->[6 표현]: 앱 프로토콜을 JSON 문자열로, UTF-8 바이트로"""
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    L("6 표현", f"JSON 직렬화 + UTF-8 인코딩 = {len(body)} bytes  (앱 프로토콜 상태 유지)")
    return len(body).to_bytes(4, "big") + body

def unpack(raw):
    return json.loads(raw.decode("utf-8"))

def reader(s):
    """수신 루프: [4 전송] 수신 -> [6 표현] 역직렬화 -> [7 응용] 화면 출력"""
    buf = b""
    while True:
        raw = s.recv(4096)
        if not raw: L("4 전송", "서버 연결 종료 (FIN 수신)"); break
        buf += raw
        while len(buf) >= 4:
            n = int.from_bytes(buf[:4], "big")
            if len(buf) < 4 + n: break
            msg = unpack(buf[4:4+n]); buf = buf[4+n:]
            if msg["type"] == "WELCOME":
                L("5 세션", f"서버가 세션 ID {msg['sid']} 발급 / 접속자 {msg['online']}")
            elif msg["type"] == "MSG":
                L("7 응용", f"표시: {msg['from']} > {msg['text']}")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
L("1 물리", "Wi-Fi/랜선 신호 <-> NIC(네트워크 카드) 준비")
L("2 링크", "이더넷/Wi-Fi 프레임으로 캡슐화 (MAC 주소는 OS가 처리)")
L("3 네트워크", f"목적지 IP = {HOST} (라우팅: 같은 PC이므로 loopback)")
L("4 전송", f"TCP {HOST}:{PORT} 로 connect() -> 3-way handshake")
s.connect((HOST, PORT))
L("5 세션", "연결 위에 세션 시작: LOGIN 전송")
s.sendall(pack({"type": "LOGIN", "nick": NICK}))

threading.Thread(target=reader, args=(s,), daemon=True).start()
time.sleep(0.3)

if FIRST:
    time.sleep(0.5)
    L("7 응용", f"사용자가 입력: '{FIRST}' -> 앱 프로토콜 MSG 생성")
    s.sendall(pack({"type": "MSG", "text": FIRST}))
    time.sleep(1.0)
    s.sendall(pack({"type": "QUIT"}))
    time.sleep(0.2)
else:
    try:
        while True:
            line = input()
            if line.strip() in ("/quit", "/q"): break
            L("7 응용", f"사용자가 입력: '{line}' -> 앱 프로토콜 MSG 생성")
            s.sendall(pack({"type": "MSG", "text": line}))
    except (EOFError, KeyboardInterrupt): pass
s.close()
