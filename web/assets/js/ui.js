/* 앱 화면 배선 (UI). 동기화 로직은 sync-core.js 에 있고, 여기서는 표시만 한다. */
(function () {
  "use strict";

  if (!window.FarmSyncCore || !window.FarmSyncStorage || !window.FarmSyncNet) {
    document.body.innerHTML =
      '<p style="padding:40px;font-family:sans-serif">스크립트를 불러오지 못했습니다. 서버를 통해 접속했는지 확인하세요.</p>';
    return;
  }

  var params = new URLSearchParams(location.search);
  var room = params.get("room") || "default";
  var user = params.get("user") || "농장주";

  var $ = function (id) { return document.getElementById(id); };

  // ------------------------------------------------------------- OSI 표
  var OSI = [
    { n: 7, nm: "응용", ds: "HELLO · SEND · SYNC_REQ / SYNC_RES · ACK · READ" },
    { n: 6, nm: "표현", ds: "JSON 직렬화 · UTF-8 · 묶음 압축" },
    { n: 5, nm: "세션", ds: "세션 ID · 수신 오프셋 · 재접속 복구" },
    { n: 4, nm: "전송", ds: "WebSocket over TCP · 순서 보장 · 재전송" },
    { n: 3, nm: "네트워크", ds: "서버 IP까지의 라우팅 · LTE ↔ Wi-Fi 전환" },
    { n: 2, nm: "데이터링크", ds: "무선 프레임 · MAC 주소" },
    { n: 1, nm: "물리", ds: "산간 약전계 신호 — 단절의 원인" },
  ];
  var LCOLOR = { 4: "#e23a63", 5: "#be3bd0", 6: "#7a4dff", 7: "#2f6bff" };
  var LBG = { 4: "rgba(226,58,99,.1)", 5: "rgba(190,59,208,.1)", 6: "rgba(122,77,255,.1)", 7: "rgba(47,107,255,.1)" };

  var osiTab = $("ositab");
  OSI.forEach(function (row) {
    var el = document.createElement("div");
    el.className = "osirow";
    el.dataset.n = row.n;
    el.innerHTML = '<span class="n">' + row.n + '</span><span class="nm">' + row.nm + '</span><span class="ds">' + row.ds + "</span>";
    osiTab.appendChild(el);
  });

  var osiTimer = null;
  function pulseOSI(layer) {
    var el = osiTab.querySelector('.osirow[data-n="' + layer + '"]');
    if (!el) return;
    el.style.setProperty("--rowc", LCOLOR[layer]);
    el.style.setProperty("--rowbg", LBG[layer]);
    el.classList.add("on");
    $("osiLive").textContent = layer + " " + OSI.filter(function (r) { return r.n === layer; })[0].nm + " 계층 처리 중";
    if (osiTimer) clearTimeout(osiTimer);
    osiTimer = setTimeout(function () {
      el.classList.remove("on");
      $("osiLive").textContent = "—";
    }, 2200);
  }

  // ------------------------------------------------------------- 계층 로그
  var logBox = $("log");
  function addLog(src, layer, text) {
    var d = document.createElement("div");
    d.innerHTML =
      '<span class="src">' + src + '</span>' +
      '<span class="ly" data-l="' + layer + '">L' + layer + " " + (window.__LAYER_NAME[layer] || "") + "</span>" +
      '<span class="tx"></span>';
    d.querySelector(".tx").textContent = text;
    logBox.appendChild(d);
    while (logBox.children.length > 220) logBox.removeChild(logBox.firstChild);
    logBox.scrollTop = logBox.scrollHeight;
  }
  window.__LAYER_NAME = { 1: "물리", 2: "데이터링크", 3: "네트워크", 4: "전송", 5: "세션", 6: "표현", 7: "응용" };

  // ------------------------------------------------------------- 전송 + 코어
  var transport = window.FarmSyncNet.createTransport({
    url: window.FarmSyncNet.wsUrl("/ws"),
    onLog: function (layer, text) { addLog("APP", layer, text); pulseOSI(layer); },
    onOpen: function () { core.onTransportOpen(); },
    onMessage: function (m) { core.handle(m); },
    onClose: function () { core.onTransportClose(); addLog("APP", 4, "전송 연결 종료 — 로컬 큐가 대화를 보존합니다"); },
  });

  var coreEvents = [];
  var wrappedRender = function (e) {
    render(e);
    switch (e.kind) {
      case "queued":
        addLog("APP", 7, "사용자 입력 '" + e.item.text.slice(0, 30) + "' → 앱 프로토콜 SEND 생성 (cid " + e.item.cid.slice(0, 8) + ")");
        pulseOSI(7);
        break;
      case "flush":
        addLog("APP", 4, "전송 계층으로 " + e.flushed + "건 송신");
        pulseOSI(4);
        break;
      case "welcome":
        addLog("APP", 5, "WELCOME 수신 — 세션 ID 발급 확인, 접속자 " + JSON.stringify(e.online));
        pulseOSI(5);
        break;
      case "sync":
        addLog("APP", 5, "재동기화: 수신 오프셋 " + e.fromSeq + " → " + e.toSeq + " · 밀린 " + e.added + "건 병합");
        addLog("APP", 6, "JSON 파싱 + UTF-8 디코딩 완료");
        pulseOSI(5);
        break;
      case "message":
        addLog("APP", 7, "새 메시지 seq " + e.message.seq + " 표시");
        pulseOSI(7);
        break;
      case "delivered":
        addLog("APP", 7, "DELIVERED — 서버가 seq " + e.seq + " 로 저장 완료");
        break;
      case "duplicate":
        addLog("APP", 7, "중복 차단 — cid 는 이미 seq " + e.seq + " 로 저장됨, 새로 저장하지 않음");
        break;
      case "layer":
        addLog("SRV", e.layer, e.text);
        pulseOSI(e.layer);
        break;
      case "presence":
        addLog("SRV", 5, e.user + (e.online ? " 접속" : " 접속 종료") + " · 현재 " + JSON.stringify(e.users));
        break;
      case "error":
        addLog("SRV", 7, "오류 " + e.code + " " + e.detail);
        break;
      default:
        break;
    }
  };

  var core = window.FarmSyncCore.createCore({
    storage: window.FarmSyncStorage.autoStorage(),
    transport: transport,
    room: room,
    user: user,
    onEvent: wrappedRender,
  });

  // ------------------------------------------------------------- 렌더
  var msgs = $("msgs");
  var stateText = { offline: "오프라인 · 큐 보존 중", connecting: "연결 중", syncing: "재동기화 중", online: "온라인" };

  function badgeFor(item) {
    if (item.duplicate) return ['dup', "중복 차단"];
    if (item.status === "sent") return ["sent", "✓ 서버 저장"];
    if (item.status === "inflight") return ["inflight", "↻ 전송 중"];
    return ["queued", "◷ 대기 중"];
  }

  function render(snap) {
    // 상태 배지
    $("stateBadge").dataset.state = snap.state;
    $("stateText").textContent = stateText[snap.state] || snap.state;
    $("roomLabel").textContent = "방 " + snap.room;
    $("kState").textContent = stateText[snap.state] || snap.state;
    $("kState").className = "v " + (snap.state === "online" ? "ok" : snap.state === "offline" ? "off" : "warn");
    $("kSession").textContent = snap.sessionId || "—";
    $("kUser").textContent = snap.user + (snap.clientId ? " · " + snap.clientId.slice(0, 8) : "");
    $("kOffset").textContent = "seq " + snap.lastSeq;
    $("kLatest").textContent = "seq " + snap.serverLatest;

    // 지표
    $("mDup").textContent = snap.counters.duplicatesIgnored;
    $("mPending").textContent = snap.pending;
    $("mSent").textContent = snap.counters.sent;
    $("mRecv").textContent = snap.counters.received;
    $("mResync").textContent = snap.counters.resyncs;
    $("mResyncMsg").textContent = snap.counters.resyncMessages;

    // 메시지 목록
    msgs.innerHTML = "";
    var day = document.createElement("div");
    day.className = "day";
    day.textContent = "서버 순번 순서로 정렬됨";
    msgs.appendChild(day);

    var bySeq = {};
    snap.inbox.forEach(function (m) { if (m.cid) bySeq[m.cid] = m; });

    // 서버 순번을 가진 것부터 (수신 확정)
    snap.inbox.forEach(function (m) {
      var wrap = document.createElement("div");
      wrap.className = "line" + (m.from === snap.user ? " me" : "");
      if (m.from !== snap.user) {
        var who = document.createElement("div");
        who.className = "who";
        who.textContent = m.from;
        wrap.appendChild(who);
      }
      var b = document.createElement("div");
      b.className = "bubble";
      b.textContent = m.text;
      wrap.appendChild(b);
      var meta = document.createElement("div");
      meta.className = "meta";
      meta.innerHTML = '<span class="badge seq">seq ' + m.seq + "</span>";
      if ((m.read_by || []).length) {
        meta.innerHTML += '<span class="badge read">✓✓ ' + m.read_by.join(", ") + " 읽음</span>";
      }
      wrap.appendChild(meta);
      msgs.appendChild(wrap);
    });

    // 아직 서버에 저장되지 않은 큐 항목
    snap.outbox.forEach(function (item) {
      if (item.status === "sent" && bySeq[item.cid]) return; // 이미 위에서 그림
      if (item.status === "sent" && !bySeq[item.cid]) {
        // 서버에서 온 브로드캐스트를 아직 못 받은 상태
      }
      var bd = badgeFor(item);
      var wrap = document.createElement("div");
      wrap.className = "line me";
      var b = document.createElement("div");
      b.className = "bubble";
      b.textContent = item.text;
      wrap.appendChild(b);
      var meta = document.createElement("div");
      meta.className = "meta";
      meta.innerHTML = '<span class="badge ' + bd[0] + '">' + bd[1] + "</span>" +
        (item.seq ? '<span class="badge seq">seq ' + item.seq + "</span>" : "");
      wrap.appendChild(meta);
      msgs.appendChild(wrap);
    });

    msgs.scrollTop = msgs.scrollHeight;

    // 큐 패널
    var q = $("queue");
    q.innerHTML = "";
    var pending = snap.outbox.filter(function (m) { return m.status !== "sent" || m.duplicate; });
    $("queueCount").textContent = pending.length + "건";
    if (!pending.length) {
      q.innerHTML = '<div class="empty">전송 대기 중인 메시지가 없습니다.</div>';
    } else {
      pending.forEach(function (m) {
        var bd = badgeFor(m);
        var d = document.createElement("div");
        d.className = "item";
        d.innerHTML = '<span class="badge ' + bd[0] + '">' + bd[1] + "</span>" +
          '<span class="txt"></span><span class="cid">' + m.cid.slice(0, 10) + "</span>";
        d.querySelector(".txt").textContent = m.text;
        q.appendChild(d);
      });
    }

    $("hint").textContent = snap.state === "offline"
      ? "지금은 끊긴 상태입니다 — 입력해도 사라지지 않습니다"
      : snap.pending > 0
        ? "전송 대기 " + snap.pending + "건 · 자동으로 다시 시도합니다"
        : "정상 연결 · 끊기 버튼으로 시연할 수 있습니다";
  }

  // ------------------------------------------------------------- 이벤트
  $("btnSend").addEventListener("click", function () {
    var v = $("input").value.trim();
    if (!v) return;
    core.submit(v);
    $("input").value = "";
    $("input").focus();
  });
  $("input").addEventListener("keydown", function (e) {
    if (e.key === "Enter") $("btnSend").click();
  });

  $("btnCut").addEventListener("click", function () {
    if (transport.isManualOffline()) {
      transport.resume();
      $("btnCut").classList.remove("on");
      $("btnCut").textContent = "회선 끊기";
      addLog("APP", 1, "회선 복구 시도 — 재접속");
    } else {
      core.forceOffline();
      $("btnCut").classList.add("on");
      $("btnCut").textContent = "회선 복구";
      addLog("APP", 1, "회선 강제 단절 — 메시지는 로컬 큐에 보존됩니다");
    }
  });

  $("btnRetry").addEventListener("click", function () {
    var n = core.retry();
    addLog("APP", 7, n ? "큐 재전송 " + n + "건 시도" : "재전송할 항목이 없습니다");
  });

  $("btnHistory").addEventListener("click", function () {
    fetch("/api/history?room=" + encodeURIComponent(room) + "&limit=100")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        addLog("SRV", 7, "서버 이력 " + data.messages.length + "건 조회 · 최신 seq " +
          (data.messages.length ? data.messages[data.messages.length - 1].seq : 0));
      })
      .catch(function () { addLog("SRV", 7, "이력 조회 실패"); });
  });

  $("btnReset").addEventListener("click", function () {
    core.reset().then(function () {
      msgs.innerHTML = "";
      logBox.innerHTML = "";
      addLog("APP", 7, "로컬 상태 초기화 — 수신 오프셋 0, 큐 비움");
    });
  });

  $("whoami").value = user;
  $("whoami").addEventListener("change", function () {
    var url = new URL(location.href);
    url.searchParams.set("user", this.value);
    url.searchParams.set("room", room);
    location.href = url.toString();
  });

  // ------------------------------------------------------------- 시작
  core.load().then(function () {
    render(core.snapshot());
    addLog("APP", 5, "로컬 상태 복원 — 수신 오프셋 seq " + core.snapshot().lastSeq);
    core.connect();
  });
})();
