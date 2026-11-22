/* Build log viewer: bounded DOM, resumable SSE, explicit gap rendering. */
(() => {
  "use strict";

  const MAX_DOM_ROWS = 1200;      // client-side display cap; server keeps 200
  const MAX_SEEN_GAPS = 50;

  const el = {
    log: document.getElementById("log"),
    start: document.getElementById("start"),
    cancel: document.getElementById("cancel"),
    disconnect: document.getElementById("disconnect"),
    reconnect: document.getElementById("reconnect"),
    follow: document.getElementById("follow"),
    statusPill: document.getElementById("status-pill"),
    connPill: document.getElementById("conn-pill"),
    runId: document.getElementById("run-id"),
    meta: document.getElementById("meta"),
    cursor: document.getElementById("cursor"),
    capacity: document.getElementById("capacity"),
    gapCount: document.getElementById("gap-count"),
    jumpBottom: document.getElementById("jump-bottom"),
  };

  const state = {
    cursor: 0,              // last line number handed to the DOM
    epoch: null,            // server session id the cursor belongs to
    es: null,
    connected: false,
    userClosed: false,
    retry: 0,
    retryTimer: null,
    runStatus: "idle",
    rows: 0,
    gaps: 0,
    missingTotal: 0,
    capacity: 200,
  };

  const nearBottom = () =>
    el.log.scrollHeight - el.log.scrollTop - el.log.clientHeight < 48;

  function scrollToBottom() {
    el.log.scrollTop = el.log.scrollHeight;
    el.jumpBottom.hidden = true;
  }

  function maybeFollow(wasNear) {
    if (el.follow.checked && wasNear) scrollToBottom();
    else if (!el.follow.checked || !wasNear) el.jumpBottom.hidden = false;
  }

  function clearPlaceholder() {
    const ph = el.log.querySelector(".placeholder");
    if (ph) ph.remove();
  }

  function trimDom() {
    while (state.rows > MAX_DOM_ROWS && el.log.firstElementChild) {
      const first = el.log.firstElementChild;
      if (first.classList.contains("row")) state.rows -= 1;
      first.remove();
    }
  }

  function renderLines(lines) {
    if (!lines || !lines.length) return;
    clearPlaceholder();
    const wasNear = nearBottom();
    const frag = document.createDocumentFragment();
    let appended = 0;
    for (const line of lines) {
      // The server streams strictly increasing sequence numbers, so anything
      // at or below the cursor is a replay of a line already on screen.
      if (line.seq <= state.cursor) continue;
      if (line.seq > state.cursor + 1) {
        // Server normally reports evictions via a "gap" event; this branch is
        // a safety net so the page never silently skips line numbers.
        addGap(frag, state.cursor + 1, line.seq - 1, true);
      }
      const row = document.createElement("div");
      row.className = "row " + (line.stream === "stderr" ? "stderr" : line.stream === "system" ? "system" : "stdout");
      const no = document.createElement("span");
      no.className = "lineno";
      no.textContent = line.seq;
      const text = document.createElement("span");
      text.className = "text";
      text.textContent = line.text === "" ? " " : line.text;
      row.append(no, text);
      frag.appendChild(row);
      state.cursor = line.seq;
      state.rows += 1;
      appended += 1;
    }
    if (appended) {
      el.log.appendChild(frag);
      trimDom();
      maybeFollow(wasNear);
    }
    updateCursorLabel();
  }

  function addGap(parent, first, last, implicit) {
    if (last < first) return;
    const count = last - first + 1;
    state.gaps += 1;
    state.missingTotal += count;
    const box = document.createElement("div");
    box.className = "gap-row";
    box.dataset.first = first;
    box.dataset.last = last;
    const label = document.createElement("span");
    const range = first === last ? `第 ${first} 行` : `第 ${first}–${last} 行`;
    label.textContent = `⚠ 日志缺失：${range}（共 ${count} 行）。服务端缓冲仅保留最近 ${state.capacity} 行，已从最早可用的行继续。`;
    if (implicit) label.textContent += "（客户端未收到缺失通知，按行号推断）";
    box.appendChild(label);
    if (state.gaps > MAX_SEEN_GAPS) {
      const old = el.log.querySelector(".gap-row");
      if (old) old.remove();
    }
    parent.appendChild(box);
    updateCursorLabel();
  }

  function renderGap(first, last) {
    if (!(last >= first)) return;
    if (last <= state.cursor) return;   // already acknowledged
    clearPlaceholder();
    const wasNear = nearBottom();
    const frag = document.createDocumentFragment();
    addGap(frag, first, last, false);
    el.log.appendChild(frag);
    state.cursor = Math.max(state.cursor, last);
    maybeFollow(wasNear);
    updateCursorLabel();
  }

  function updateCursorLabel() {
    el.cursor.textContent = `已显示至第 ${state.cursor} 行`;
    el.gapCount.textContent = state.gaps
      ? `缺失区间 ${state.gaps} 段，共 ${state.missingTotal} 行`
      : "";
  }

  function setStatus(status, payload) {
    state.runStatus = status || "idle";
    el.statusPill.className = "pill " + state.runStatus;
    el.statusPill.textContent = state.runStatus;
    const running = state.runStatus === "running";
    el.start.disabled = running;
    el.cancel.disabled = !running;
    if (payload) {
      if (payload.run_id) el.runId.textContent = `#${payload.run_id}`;
      const bits = [];
      if (payload.capacity) {
        state.capacity = payload.capacity;
        el.capacity.textContent = payload.capacity;
        bits.push(`缓冲 ${payload.buffered ?? "?"}/${payload.capacity}`);
      }
      if (payload.pid) bits.push(`pid ${payload.pid}`);
      if (payload.exit_code !== null && payload.exit_code !== undefined) bits.push(`exit ${payload.exit_code}`);
      if (payload.finished_at && payload.started_at) {
        bits.push(`用时 ${(payload.finished_at - payload.started_at).toFixed(1)}s`);
      }
      el.meta.textContent = bits.join(" · ");
    }
  }

  function setConn(kind, text) {
    el.connPill.className = "pill conn-" + kind;
    el.connPill.textContent = text;
  }

  function closeStream(notifyServer = false) {
    if (state.es) {
      state.es.close();
      state.es = null;
    }
    state.connected = false;
    if (state.retryTimer) {
      clearTimeout(state.retryTimer);
      state.retryTimer = null;
    }
    if (notifyServer) setConn("off", "已断开");
  }

  function resetView(serverCursor) {
    // Server restarted: old line numbers are meaningless, so clear the page
    // and adopt the server's starting point (oldest retained line).
    el.log.replaceChildren();
    state.rows = 0;
    state.gaps = 0;
    state.missingTotal = 0;
    state.cursor = typeof serverCursor === "number" ? serverCursor : 0;
    const ph = document.createElement("div");
    ph.className = "placeholder";
    ph.textContent = "服务已重启，正在重新加载保留的日志…";
    el.log.appendChild(ph);
    updateCursorLabel();
  }

  function connect() {
    closeStream();
    state.userClosed = false;
    // Always resume strictly after the last rendered line; the server replays
    // retained lines in order and flags any range that was evicted.  A fresh
    // page (cursor 0) sends no cursor, so the server starts at the oldest
    // retained line instead of claiming lines from a build we never saw.
    const params = [];
    if (state.cursor > 0) {
      params.push(`after=${encodeURIComponent(state.cursor)}`);
      if (state.epoch) params.push(`epoch=${encodeURIComponent(state.epoch)}`);
    }
    const url = params.length ? `/api/logs/stream?${params.join("&")}` : "/api/logs/stream";
    setConn("retry", "连接中…");
    const es = new EventSource(url);
    state.es = es;
    const startingEpoch = state.epoch;

    es.addEventListener("hello", (ev) => {
      state.connected = true;
      state.retry = 0;
      setConn("on", "已连接");
      const data = JSON.parse(ev.data);
      if (startingEpoch && data.session_id && data.session_id !== startingEpoch) {
        resetView(data.cursor);
      }
      if (data.session_id) state.epoch = data.session_id;
      if (typeof data.cursor === "number" && data.cursor > state.cursor) {
        // Server picked a later starting point (fresh page on retained logs);
        // adopt it so implicitly-skipped lines are not misreported as a gap.
        state.cursor = data.cursor;
        updateCursorLabel();
      }
      setStatus(data.status, data);
    });

    es.addEventListener("lines", (ev) => {
      const data = JSON.parse(ev.data);
      renderLines(data.lines);
    });

    es.addEventListener("gap", (ev) => {
      const data = JSON.parse(ev.data);
      renderGap(data.first, data.last);
    });

    es.addEventListener("status", (ev) => {
      const data = JSON.parse(ev.data);
      setStatus(data.status, data);
    });

    es.addEventListener("end", (ev) => {
      const data = JSON.parse(ev.data);
      setStatus(data.status || "idle", null);
      closeStream();
      setConn("off", "服务端已关闭");
    });

    es.onerror = () => {
      if (state.userClosed) return;
      closeStream();
      // Auto-resume from the last shown line with capped exponential backoff.
      state.retry += 1;
      const delay = Math.min(500 * 2 ** (state.retry - 1), 8000);
      setConn("retry", `断线，${(delay / 1000).toFixed(1)}s 后续读`);
      state.retryTimer = setTimeout(connect, delay);
    };
  }

  async function postJSON(url) {
    const res = await fetch(url, { method: "POST" });
    let body = null;
    try { body = await res.json(); } catch (_) { /* ignore */ }
    if (!res.ok) {
      const msg = body && body.detail ? (typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail)) : res.statusText;
      throw new Error(msg);
    }
    return body;
  }

  el.start.addEventListener("click", async () => {
    el.start.disabled = true;
    try {
      const body = await postJSON("/api/build/start");
      setStatus(body.status, body);
      if (!state.connected) connect();
    } catch (err) {
      alert("启动失败：" + err.message);
      el.start.disabled = false;
    }
  });

  el.cancel.addEventListener("click", async () => {
    el.cancel.disabled = true;
    try {
      const body = await postJSON("/api/build/cancel");
      setStatus(body.status, body);
    } catch (err) {
      alert("停止失败：" + err.message);
    }
  });

  el.disconnect.addEventListener("click", () => {
    state.userClosed = true;
    closeStream(true);
  });

  el.reconnect.addEventListener("click", () => {
    state.userClosed = false;
    state.retry = 0;
    connect();
  });

  el.follow.addEventListener("change", () => {
    if (el.follow.checked) scrollToBottom();
  });

  el.log.addEventListener("scroll", () => {
    if (nearBottom()) el.jumpBottom.hidden = true;
  });

  el.jumpBottom.addEventListener("click", () => {
    el.follow.checked = true;
    scrollToBottom();
  });

  window.addEventListener("beforeunload", () => closeStream());

  // Initial paint: connect and pick up whatever the server still retains.
  connect();

  // Periodically refresh status so a build started in another window shows up.
  setInterval(async () => {
    try {
      const res = await fetch("/api/status", { cache: "no-store" });
      if (res.ok) {
        const body = await res.json();
        setStatus(body.status, body);
      }
    } catch (_) { /* offline; SSE reconnect handles it */ }
  }, 4000);
})();
