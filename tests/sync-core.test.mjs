/*
동기화 코어 테스트 (Node 내장 테스트 러너).

    node --test tests/sync-core.test.mjs

검증 대상은 전송 계층이 절대 보장해주지 않는 부분이다.
  - 끊긴 동안 보낸 메시지가 로컬 큐에 남는가
  - 재접속 시 HELLO 에 수신 오프셋이 실려 나가는가
  - 밀린 구간만 병합하고, 중복 순번은 무시하는가
  - 배달 확인을 못 받은 메시지를 재전송하고, 서버가 중복으로 처리하는가
*/
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const Core = require("../web/assets/js/sync-core.js");
const Storage = require("../web/assets/js/storage.js");

/** 테스트용 전송 어댑터 — 실제로 보낸 대신 기록만 남긴다. */
function fakeTransport() {
  const sent = [];
  let open = false;
  return {
    sent,
    send(obj) {
      if (!open) throw new Error("전송 계층이 닫혀 있음");
      sent.push(obj);
    },
    open() {
      open = true;
      this.onOpen && this.onOpen();
    },
    close() {
      open = false;
    },
    isOpen: () => open,
  };
}

/** 실제 서버가 HELLO 에 답하는 WELCOME 을 흉내낸다 (보낼 것이 없을 때의 경로). */
function welcome(core, latest = 0) {
  core.handle({ type: "WELCOME", session_id: "SID-1", latest_seq: latest, online: [] });
}

function makeCore(overrides = {}) {
  const transport = fakeTransport();
  const events = [];
  const core = Core.createCore({
    storage: Storage.createMemoryStorage(),
    transport,
    room: "test-room",
    user: "농장주",
    clientId: "client-a",
    onEvent: (e) => events.push(e),
    ...overrides,
  });
  // 전송이 열리면 실제 소켓처럼 코어에 알린다.
  transport.onOpen = () => core.onTransportOpen();
  return { core, transport, events };
}

test("오프라인이어도 메시지는 로컬 큐에 남는다", () => {
  const { core, transport } = makeCore();
  const item = core.submit("회선 끊긴 상태에서 쓴 발주");
  assert.equal(item.status, "queued");
  assert.equal(transport.sent.length, 0, "전송 계층이 닫혀 있으면 보내지 않는다");
  const snap = core.snapshot();
  assert.equal(snap.pending, 1);
  assert.equal(snap.state, "offline");
});

test("재접속하면 HELLO 에 마지막 수신 오프셋이 실려 나간다", () => {
  const { core, transport } = makeCore();
  // 이전에 seq 7 까지 받은 상태를 만든다.
  core.load();
  core.handle({ type: "WELCOME", session_id: "SID-1", latest_seq: 7, online: [] });
  core.handle({ type: "MSG", seq: 7, cid: "c7", from: "기업", text: "m7", ts: 1 });
  assert.equal(core.snapshot().lastSeq, 7);

  core.connect(); // CONNECTING -> (전송 열림) -> HELLO
  const hello = transport.sent.find((m) => m.type === "HELLO");
  assert.ok(hello, "HELLO 를 보내야 한다");
  assert.equal(hello.last_seq, 7, "수신 오프셋이 실려야 한다");
  assert.equal(hello.client_id, "client-a");
});

test("SYNC 는 밀린 구간만 병합하고 중복 순번을 무시한다", () => {
  const { core } = makeCore();
  core.connect();
  const before = core.snapshot().lastSeq; // 0
  assert.equal(before, 0);

  core.handle({
    type: "SYNC",
    from_seq: 0,
    to_seq: 3,
    count: 3,
    messages: [
      { seq: 1, cid: "a", from: "농장주", text: "m1", ts: 1 },
      { seq: 2, cid: "b", from: "기업", text: "m2", ts: 2 },
      { seq: 3, cid: "c", from: "농장주", text: "m3", ts: 3 },
    ],
  });
  let snap = core.snapshot();
  assert.equal(snap.inbox.length, 3);
  assert.equal(snap.lastSeq, 3);
  assert.equal(snap.state, "online");

  // 같은 범위를 다시 받아도 중복이 쌓이지 않는다.
  core.handle({
    type: "SYNC", from_seq: 0, to_seq: 3, count: 3,
    messages: [
      { seq: 2, cid: "b", from: "기업", text: "m2", ts: 2 },
      { seq: 3, cid: "c", from: "농장주", text: "m3", ts: 3 },
    ],
  });
  snap = core.snapshot();
  assert.equal(snap.inbox.length, 3, "중복 순번은 병합되지 않는다");
  assert.equal(snap.counters.resyncs, 2);
});

test("수신 순서가 뒤바뀌어 도착해도 서버 순번으로 정렬한다", () => {
  const { core } = makeCore();
  core.handle({ type: "MSG", seq: 5, cid: "e", from: "기업", text: "다섯번째", ts: 5 });
  core.handle({ type: "MSG", seq: 3, cid: "c", from: "기업", text: "세번째", ts: 3 });
  core.handle({ type: "MSG", seq: 4, cid: "d", from: "기업", text: "네번째", ts: 4 });
  const seqs = core.snapshot().inbox.map((m) => m.seq);
  assert.deepEqual(seqs, [3, 4, 5], "들어온 순서가 아니라 순번 순서로 정렬되어야 한다");
  assert.equal(core.snapshot().counters.orderViolations, 0);
});

test("연결이 열리면 밀린 로컬 큐를 자동으로 내보낸다", () => {
  const { core, transport } = makeCore();
  core.submit("연결 전에 쓴 메시지 1");
  core.submit("연결 전에 쓴 메시지 2");
  assert.equal(transport.sent.length, 0);

  core.connect();
  welcome(core);
  const sends = transport.sent.filter((m) => m.type === "SEND");
  assert.equal(sends.length, 2, "큐에 있던 2건이 나가야 한다");
  assert.equal(new Set(sends.map((m) => m.cid)).size, 2, "cid 가 서로 달라야 한다");
});

test("배달 확인을 못 받은 메시지는 재접속 시 다시 보내고, 서버가 중복으로 처리한다", () => {
  const { core, transport } = makeCore();
  core.connect();
  welcome(core);
  const item = core.submit("확인을 못 받은 메시지");

  // 전송 계층이 끊긴다 (DELIVERED 를 받지 못한 상태)
  transport.close();
  core.onTransportClose();
  assert.equal(core.snapshot().state, "offline");
  const pending = core.snapshot().outbox.find((m) => m.cid === item.cid);
  assert.equal(pending.status, "inflight");

  // 재접속 → 큐로 되돌려 재전송
  core.connect();
  welcome(core);
  const resends = transport.sent.filter((m) => m.type === "SEND" && m.cid === item.cid);
  assert.equal(resends.length, 2, "같은 cid 로 두 번 나간다 (전송 계층은 이걸 막지 못한다)");

  // 서버가 먼저 저장해둔 경우 → duplicate=true 로 응답
  core.handle({ type: "DELIVERED", cid: item.cid, seq: 9, duplicate: true });
  const after = core.snapshot();
  assert.equal(after.outbox.find((m) => m.cid === item.cid).status, "sent");
  assert.equal(after.counters.duplicatesIgnored, 1, "중복은 저장되지 않은 것으로 집계된다");
  assert.equal(after.pending, 0);
  assert.equal(after.counters.sent, 0, "중복 건은 새로 저장된 건이 아니다");
});

test("서버 저장을 확인받으면 전송 완료로 표시된다", () => {
  const { core, transport } = makeCore();
  core.connect();
  const item = core.submit("정상 전송");
  core.handle({ type: "DELIVERED", cid: item.cid, seq: 1, duplicate: false });
  const snap = core.snapshot();
  assert.equal(snap.outbox.find((m) => m.cid === item.cid).status, "sent");
  assert.equal(snap.outbox.find((m) => m.cid === item.cid).seq, 1);
  assert.equal(snap.counters.sent, 1);
  assert.equal(snap.pending, 0);
});

test("읽음 표시가 수신 메시지에 반영된다", () => {
  const { core } = makeCore();
  core.handle({ type: "MSG", seq: 1, cid: "a", from: "기업", text: "확인 부탁", ts: 1 });
  assert.equal(core.snapshot().unread, 1);
  core.handle({ type: "READ", seq: 1, reader: "기업" });
  assert.equal(core.snapshot().unread, 0);
});

test("로컬 상태는 저장소에 남아 재시작 후 복원된다", async () => {
  const storage = Storage.createMemoryStorage();
  const t1 = fakeTransport();
  const core1 = Core.createCore({
    storage, transport: t1, room: "r", user: "농장주", clientId: "c1",
  });
  core1.submit("저장되어야 하는 메시지");
  core1.handle({ type: "MSG", seq: 4, cid: "x", from: "기업", text: "받은 메시지", ts: 1 });
  await core1.persist();

  const t2 = fakeTransport();
  const core2 = Core.createCore({
    storage, transport: t2, room: "r", user: "농장주", clientId: "c1",
  });
  await core2.load();
  const snap = core2.snapshot();
  assert.equal(snap.outbox.length, 1);
  assert.equal(snap.lastSeq, 4, "수신 오프셋도 복원되어야 한다");
  assert.equal(core2.snapshot().inbox.length, 1);
});

test("회선 끊기 스위치는 재접속을 멈추고, 해제하면 다시 붙는다", () => {
  const { core, transport } = makeCore();
  core.connect();
  welcome(core);
  core.forceOffline();
  assert.equal(core.snapshot().state, "offline");
  const countBefore = transport.sent.length;
  core.submit("끊긴 동안 쓴 메시지");
  assert.equal(transport.sent.length, countBefore, "끊긴 동안에는 전송되지 않는다");
  core.connect();
  welcome(core);
  assert.ok(transport.sent.some((m) => m.type === "SEND" && m.text === "끊긴 동안 쓴 메시지"));
});
