# -*- coding: utf-8 -*-
"""
LayerTalk - 중앙 서버
실행: python3 server.py
역할: ① 세션 관리  ② 메시지 릴레이(1:1 / 브로드캐스트)  ③ 파일 중계
"""
import socket, threading, json, time

HOST, PORT = "0.0.0.0", 5000
clients = {}          # nickname -> socket
lock = threading.Lock()

def log(layer, msg):
    print(f"  [{layer}] {msg}", flush=True)

def send(sock, obj):
    """[7 응용] 프로토콜 객체 -> [6 표현] JSON 직렬화 -> [4 전송] TCP 송신"""
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    log("6 표현", f"JSON 직렬화 + UTF-8 인코딩 -> {len(data)} bytes")
    sock.sendall(len(data).to_bytes(4, "big") + data)   # 4바이트 길이 헤더 = 메시지 프레이밍

def recv(sock):
    """[4 전송] TCP 수신 -> [6 표현] 역직렬화"""
    raw = b""
    while len(raw) < 4:
        chunk = sock.recv(4 - len(raw))
        if not chunk: return None
        raw += chunk
    n = int.from_bytes(raw, "big")
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk: return None
        buf += chunk
    return json.loads(buf.decode("utf-8"))

def broadcast(sender, text):
    payload = {"type": "MSG", "from": sender, "text": text, "ts": time.strftime("%H:%M:%S")}
    with lock:
        targets = [(n, s) for n, s in clients.items() if n != sender]
    for name, s in targets:
        log("7 응용", f"릴레이 -> {name} (중계만 하고 저장하지 않음)")
        try: send(s, payload)
        except Exception: pass
    return len(targets)

def handle(conn, addr):
    log("3 네트워크", f"새 연결: {addr[0]} -> 서버 {addr[1]} (TCP 3-way handshake 완료)")
    nick = None
    try:
        join = recv(conn)
        nick = join["nick"]
        with lock:
            clients[nick] = conn
        # [5 세션] 세션 ID 발급
        sid = f"SID-{abs(hash((nick, addr[1])))%100000:05d}"
        log("5 세션", f"{nick} 로그인 -> 세션 ID {sid} 발급, 세션 테이블 등록")
        send(conn, {"type": "WELCOME", "sid": sid, "online": list(clients)})
        broadcast(nick, f"{nick}님이 입장했습니다.")
        while True:
            msg = recv(conn)
            if msg is None: break
            if msg["type"] == "MSG":
                log("7 응용", f'수신 {{"type":"MSG","from":"{nick}","text":"{msg["text"]}"}}')
                n = broadcast(nick, msg["text"])
                log("4 전송", f"브로드캐스트 완료 -> {n}명에게 전달 (순서 보장)")
            elif msg["type"] == "QUIT":
                break
    except Exception as e:
        log("!", f"오류: {e}")
    finally:
        with lock: clients.pop(nick, None)
        conn.close()
        if nick:
            log("5 세션", f"{nick} 세션 종료 -> 세션 테이블에서 제거")
            broadcast(nick, f"{nick}님이 퇴장했습니다.")

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(5)
    log("4 전송", f"TCP {PORT} 포트 LISTEN 시작 (SOCK_STREAM = 순서보장·재전송)")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
