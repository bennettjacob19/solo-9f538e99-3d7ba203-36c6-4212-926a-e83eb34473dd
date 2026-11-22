/* Headless frontend tests for app/static/app.js using jsdom.
 *
 * Drives the page's real EventSource/fetch handlers with scripted server events
 * and asserts what a user would see: ordered line numbers, gap banners for
 * evicted ranges, no duplicate rows, and resume URLs built from the last shown
 * line.
 */
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "../..");
const HTML = fs.readFileSync(path.join(ROOT, "app/static/index.html"), "utf8");
const SCRIPT = fs.readFileSync(path.join(ROOT, "app/static/app.js"), "utf8");

/* ---------- harness ---------- */

class FakeEventSource {
  static instances = [];
  constructor(url) {
    this.url = url;
    this.listeners = Object.create(null);
    this.closed = false;
    this.onerror = null;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type, fn) {
    (this.listeners[type] ||= []).push(fn);
  }
  emit(type, data) {
    const ev = { data: data === undefined ? undefined : JSON.stringify(data) };
    for (const fn of this.listeners[type] || []) fn(ev);
  }
  fail() {
    if (this.onerror) this.onerror({});
  }
  close() {
    this.closed = true;
  }
  static latest() {
    return FakeEventSource.instances[FakeEventSource.instances.length - 1];
  }
  static reset() {
    FakeEventSource.instances = [];
  }
}

function statusPayload(overrides = {}) {
  return {
    status: "running",
    session_id: "epoch-a",
    run_id: 1,
    pid: 1234,
    command: ["python", "sample_build.py"],
    started_at: 1000,
    finished_at: null,
    exit_code: null,
    stdout_bytes: 10,
    stderr_bytes: 0,
    buffered: 12,
    capacity: 200,
    first_seq: 1,
    last_seq: 12,
    server_time: 1000,
    ...overrides,
  };
}

function makeLine(seq, text = `line ${seq}`, stream = "stdout") {
  return { seq, text, stream, ts: 1000 + seq };
}

async function boot() {
  const dom = new JSDOM(HTML, { runScripts: "outside-only", url: "http://localhost/" });
  const { window } = dom;
  window.EventSource = FakeEventSource;
  const fetched = [];
  window.fetch = async (url, opts = {}) => {
    fetched.push({ url: String(url), method: opts.method || "GET" });
    if (String(url).includes("/api/status")) {
      return { ok: true, json: async () => statusPayload() };
    }
    return { ok: true, json: async () => statusPayload() };
  };
  window.alert = () => {};
  window.eval(SCRIPT);
  await new Promise((r) => setTimeout(r, 0));
  return { dom, window, doc: window.document, fetched };
}

const rows = (doc) => [...doc.querySelectorAll(".row")];
const rowSeqs = (doc) => rows(doc).map((r) => Number(r.querySelector(".lineno").textContent));
const rowText = (doc, seq) =>
  rows(doc)
    .find((r) => Number(r.querySelector(".lineno").textContent) === seq)
    ?.querySelector(".text").textContent;
const gapBoxes = (doc) => [...doc.querySelectorAll(".gap-row")];
const gapRanges = (doc) =>
  gapBoxes(doc).map((g) => [Number(g.dataset.first), Number(g.dataset.last)]);
const connText = (doc) => doc.getElementById("conn-pill").textContent;

/* ---------- tests ---------- */

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("fresh page connects without a cursor and paints retained lines", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  assert.equal(es.url, "/api/logs/stream", "fresh page must not claim a cursor");
  assert.equal(connText(doc), "连接中…");

  es.emit("hello", { ...statusPayload(), cursor: 0 });
  assert.equal(es.listeners.hello.length, 1);
  assert.equal(connText(doc), "已连接");

  es.emit("lines", { lines: [makeLine(1), makeLine(2, "warn", "stderr"), makeLine(3, "—", "system")] });
  assert.deepEqual(rowSeqs(doc), [1, 2, 3]);
  assert.ok(rows(doc)[1].classList.contains("stderr"));
  assert.ok(rows(doc)[2].classList.contains("system"));
  assert.equal(gapBoxes(doc).length, 0);
  assert.match(doc.getElementById("cursor").textContent, /第 3 行/);
});

test("lines are rendered in order with no duplicates even if replayed", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload(), cursor: 0 });
  es.emit("lines", { lines: [makeLine(1), makeLine(2), makeLine(3)] });
  es.emit("hello", { ...statusPayload(), cursor: 0 }); // server re-greets after a resume race
  es.emit("lines", { lines: [makeLine(2), makeLine(3), makeLine(4)] });
  assert.deepEqual(rowSeqs(doc), [1, 2, 3, 4]);
});

test("gap event paints the missing range and continues from the oldest retained line", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload(), cursor: 0 });
  es.emit("lines", { lines: [makeLine(1), makeLine(2)] });
  // Viewer reconnects from line 2; server says 3..10 are gone.
  es.emit("gap", { first: 3, last: 10 });
  es.emit("lines", { lines: [makeLine(11), makeLine(12)] });

  assert.deepEqual(rowSeqs(doc), [1, 2, 11, 12]);
  assert.deepEqual(gapRanges(doc), [[3, 10]]);
  assert.match(gapBoxes(doc)[0].textContent, /第 3–10 行/);
  assert.match(gapBoxes(doc)[0].textContent, /共 8 行/);
  assert.match(doc.getElementById("gap-count").textContent, /缺失区间 1 段，共 8 行/);
});

test("duplicate gap notice for an already-covered range is ignored", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload(), cursor: 0 });
  es.emit("gap", { first: 1, last: 4 });
  es.emit("gap", { first: 1, last: 4 });
  assert.deepEqual(gapRanges(doc), [[1, 4]]);
});

test("a line-number jump with no gap event is still shown as an inferred gap", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload(), cursor: 0 });
  es.emit("lines", { lines: [makeLine(1)] });
  es.emit("lines", { lines: [makeLine(5), makeLine(6)] });
  assert.deepEqual(rowSeqs(doc), [1, 5, 6]);
  assert.deepEqual(gapRanges(doc), [[2, 4]]);
  assert.match(gapBoxes(doc)[0].textContent, /客户端未收到缺失通知/);
});

test("drop and reconnect resumes strictly after the last displayed line", async () => {
  FakeEventSource.reset();
  const { window, doc } = await boot();
  const first = FakeEventSource.latest();
  first.emit("hello", { ...statusPayload(), cursor: 0 });
  first.emit("lines", { lines: [makeLine(1), makeLine(2), makeLine(3)] });

  doc.getElementById("disconnect").click();
  assert.ok(first.closed, "simulated drop closes the stream");
  assert.equal(connText(doc), "已断开");

  doc.getElementById("reconnect").click();
  const second = FakeEventSource.latest();
  assert.notEqual(second, first);
  assert.equal(second.url, "/api/logs/stream?after=3&epoch=epoch-a");

  // Reconnect replay must continue, not repeat.
  second.emit("hello", { ...statusPayload(), cursor: 3 });
  second.emit("lines", { lines: [makeLine(4), makeLine(5)] });
  assert.deepEqual(rowSeqs(doc), [1, 2, 3, 4, 5]);
  assert.equal(gapBoxes(doc).length, 0);
});

test("a restarted server clears stale rows and starts over", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const first = FakeEventSource.latest();
  first.emit("hello", { ...statusPayload(), cursor: 0 });
  first.emit("lines", { lines: [makeLine(1), makeLine(2), makeLine(3)] });
  assert.deepEqual(rowSeqs(doc), [1, 2, 3]);

  doc.getElementById("disconnect").click();
  doc.getElementById("reconnect").click();
  const second = FakeEventSource.latest();
  assert.equal(second.url, "/api/logs/stream?after=3&epoch=epoch-a");

  // New session id: old line numbers are meaningless.
  second.emit("hello", { ...statusPayload({ session_id: "epoch-b", first_seq: 7 }), cursor: 6 });
  assert.deepEqual(rowSeqs(doc), [], "stale rows from the previous server are cleared");
  assert.match(doc.querySelector(".placeholder").textContent, /服务已重启/);
  second.emit("lines", { lines: [makeLine(7), makeLine(8)] });
  assert.deepEqual(rowSeqs(doc), [7, 8]);
  assert.equal(gapBoxes(doc).length, 0, "restart must not be reported as missing log lines");
});

test("transport error auto-resumes from the last shown line", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const first = FakeEventSource.latest();
  first.emit("hello", { ...statusPayload(), cursor: 0 });
  first.emit("lines", { lines: [makeLine(1), makeLine(2)] });
  first.fail();
  assert.equal(connText(doc), "断线，0.5s 后续读");

  await new Promise((r) => setTimeout(r, 620));
  const second = FakeEventSource.latest();
  assert.notEqual(second, first);
  assert.equal(second.url, "/api/logs/stream?after=2&epoch=epoch-a");
});

test("status pill and meta reflect the build lifecycle", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload({ status: "running" }) });
  assert.equal(doc.getElementById("status-pill").textContent, "running");
  assert.ok(doc.getElementById("start").disabled);
  assert.ok(!doc.getElementById("cancel").disabled);

  es.emit("status", statusPayload({ status: "succeeded", finished_at: 1002, exit_code: 0 }));
  assert.equal(doc.getElementById("status-pill").textContent, "succeeded");
  assert.ok(!doc.getElementById("start").disabled);
  assert.match(doc.getElementById("meta").textContent, /exit 0/);
});

test("DOM stays bounded for very long sessions", async () => {
  FakeEventSource.reset();
  const { doc } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload(), cursor: 0 });
  const batch = [];
  for (let i = 1; i <= 1300; i++) batch.push(makeLine(i));
  es.emit("lines", { lines: batch });
  assert.equal(rows(doc).length, 1200);
  assert.equal(rowSeqs(doc).at(-1), 1300);
  assert.equal(rowSeqs(doc)[0], 101);
});

test("start button posts and reconnects when the stream is down", async () => {
  FakeEventSource.reset();
  const { doc, fetched } = await boot();
  const es = FakeEventSource.latest();
  es.emit("hello", { ...statusPayload({ status: "idle", run_id: 0 }) });
  doc.getElementById("disconnect").click();
  doc.getElementById("start").click();
  await new Promise((r) => setTimeout(r, 20));
  assert.ok(fetched.some((f) => f.url === "/api/build/start" && f.method === "POST"));
  assert.equal(FakeEventSource.instances.length, 2);
});

(async () => {
  let failed = 0;
  for (const [name, fn] of tests) {
    try {
      await fn();
      console.log(`ok - ${name}`);
    } catch (err) {
      failed += 1;
      console.error(`FAIL - ${name}\n${err.stack}\n`);
    }
  }
  console.log(`\n${tests.length - failed}/${tests.length} frontend tests passed`);
  process.exit(failed ? 1 : 0);
})();
