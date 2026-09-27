/*
전송 어댑터 — WebSocket 위에서 동작한다 (그 아래는 TCP).

역할
  - 연결 수립/종료를 sync-core 에 알린다
  - 끊기면 **지수 백오프**로 자동 재접속 (야외·산간 회선을 가정)
  - 데모용 '회선 끊기' 스위치를 제공한다 (자동 재접속까지 멈춘다)

L4 전송 계층 관점: WebSocket 은 TCP 연결 하나를 계속 쓴다.
TCP 스스로는 '연결이 끊긴 동안 앱이 보내려던 메시지'를 알 수 없다.
그래서 그 구간의 메시지는 sync-core 의 로컬 큐가 책임진다.
*/
(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    root.FarmSyncNet = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  function createTransport(opts) {
    var url = opts.url;
    var onOpen = opts.onOpen || function () {};
    var onMessage = opts.onMessage || function () {};
    var onClose = opts.onClose || function () {};
    var onLog = opts.onLog || function () {};
    var baseDelay = opts.baseDelay || 800;
    var maxDelay = opts.maxDelay || 12000;

    var ws = null;
    var manualOffline = false;
    var attempt = 0;
    var timer = null;
    var closedByUser = false;

    function clearTimer() {
      if (timer) { clearTimeout(timer); timer = null; }
    }

    function scheduleReconnect() {
      if (manualOffline || closedByUser) return;
      attempt++;
      var delay = Math.min(maxDelay, baseDelay * Math.pow(1.6, attempt - 1));
      onLog(4, "TCP 연결 끊김 · " + Math.round(delay / 1000) + "초 후 재접속 시도 #" + attempt);
      clearTimer();
      timer = setTimeout(open, delay);
    }

    function open() {
      if (manualOffline) return;
      if (ws && (ws.readyState === 0 || ws.readyState === 1)) return;
      clearTimer();
      try {
        ws = new WebSocket(url);
      } catch (e) {
        scheduleReconnect();
        return;
      }
      ws.onopen = function () {
        attempt = 0;
        onLog(4, "WebSocket 핸드셰이크 완료 (하부 TCP 3-way handshake 위에서 동작)");
        onLog(5, "세션 계층: 서버에 HELLO 전송 · 내 수신 오프셋 알림");
        onOpen();
      };
      ws.onmessage = function (ev) {
        var data;
        try { data = JSON.parse(ev.data); } catch (e) { return; }
        onMessage(data);
      };
      ws.onclose = function () {
        onClose();
        scheduleReconnect();
      };
      ws.onerror = function () { /* onclose 가 이어진다 */ };
    }

    return {
      open: open,
      isOpen: function () { return !!ws && ws.readyState === 1; },
      send: function (obj) {
        if (!ws || ws.readyState !== 1) return false;
        ws.send(JSON.stringify(obj));
        return true;
      },
      /** force=true 이면 자동 재접속까지 멈춘다 (수업 시연용 '회선 끊기'). */
      close: function (force) {
        manualOffline = !!force;
        clearTimer();
        if (ws && (ws.readyState === 0 || ws.readyState === 1)) {
          try { ws.close(); } catch (e) {}
        }
        onClose();
      },
      resume: function () {
        manualOffline = false;
        attempt = 0;
        open();
      },
      isManualOffline: function () { return manualOffline; },
      dispose: function () {
        closedByUser = true;
        manualOffline = true;
        clearTimer();
        if (ws) { try { ws.close(); } catch (e) {} }
      },
    };
  }

  /** 현재 페이지 주소에서 WebSocket URL 을 만든다 (배포 환경 자동 대응). */
  function wsUrl(path) {
    if (typeof window === "undefined") return "ws://localhost:8000/ws";
    var proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + window.location.host + (path || "/ws");
  }

  return { createTransport: createTransport, wsUrl: wsUrl };
});
