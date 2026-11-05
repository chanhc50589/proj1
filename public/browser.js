/**
 * Browser logic for memdoor team-solve. Vanilla JS, no frameworks.
 * - One POST /api/run request streams the whole run via SSE (fetch + manual parser;
 *   EventSource cannot POST).
 * - Safe rendering: textContent only — model output is never treated as HTML.
 * - The operator password is sent per request and is never stored anywhere.
 */

const MODELS = [
  { id: "@cf/google/gemma-4-26b-a4b-it", label: "Gemma 4 26B A4B IT (default)" },
  { id: "@cf/qwen/qwen3.8-27b", label: "Qwen3.8 27B" },
  { id: "@cf/zai-org/glm-4.7-flash", label: "GLM 4.7 Flash" },
];

const ROLES = [
  { key: "planner", label: "Planner", color: "red" },
  { key: "ideator1", label: "First ideator", color: "orange" },
  { key: "ideator2", label: "Second ideator", color: "yellow" },
  { key: "solver", label: "Solver", color: "green" },
  { key: "manager", label: "Manager", color: "blue" },
  { key: "repair", label: "Repair worker", color: "purple" },
];

const STEPS = ["plan", "ideate", "solve", "test", "review", "repair", "verify"];
const MAX_BOX_CHARS = 20000; // DOM safety cap per role box

// ----- tiny DOM helpers (safe: textContent only) -----

/**
 * Look up an element by id. The HTML shell is owned entirely by this app, so
 * callers may use element properties directly; the return type is intentionally
 * loose to keep the single type-check program (DOM + Node + Workers) coherent.
 * @param {string} id
 * @returns {any}
 */
const $ = (id) => document.getElementById(id);

function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined && text !== null) node.textContent = String(text);
  if (className) node.className = className;
  return node;
}

function setBanner(message, kind) {
  const b = $("banner");
  b.textContent = message || "";
  b.className = kind || "";
}

// ----- SSE parsing (keep in sync with pushSseLines in index.js) -----

function pushSseLines(state, chunk) {
  state.buf = (state.buf || "") + chunk;
  const events = [];
  let idx;
  while ((idx = state.buf.indexOf("\n\n")) !== -1) {
    const frame = state.buf.slice(0, idx);
    state.buf = state.buf.slice(idx + 2);
    let event = "message";
    const dataLines = [];
    for (const rawLine of frame.split("\n")) {
      const line = rawLine.replace(/\r$/, "");
      if (line.startsWith(":")) continue; // heartbeat comment
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
    }
    if (dataLines.length === 0) continue;
    const dataStr = dataLines.join("\n");
    let data;
    try { data = JSON.parse(dataStr); } catch { data = { raw: dataStr }; }
    events.push({ event, data });
  }
  return events;
}

// ----- UI construction -----

const roleBoxes = new Map(); // roleKey -> { box, body, tag }

function buildModelSelects() {
  const container = $("model-selects");
  for (const role of ROLES) {
    const label = el("label", `${role.label} model`);
    label.setAttribute("for", `model-${role.key}`);
    const select = document.createElement("select");
    select.id = `model-${role.key}`;
    for (const m of MODELS) {
      const opt = document.createElement("option");
      opt.value = m.id;
      opt.textContent = m.label;
      if (m.id === MODELS[0].id) opt.selected = true;
      select.appendChild(opt);
    }
    container.appendChild(label);
    container.appendChild(select);
  }
}

function buildSteps() {
  const ol = $("steps");
  for (const step of STEPS) {
    const li = el("li", step, "pending");
    li.dataset.step = step;
    ol.appendChild(li);
  }
}

function buildRoleBoxes() {
  const stream = $("stream");
  for (const role of ROLES) {
    const box = el("div", null, `role r-${role.key}`);
    const header = el("header");
    header.appendChild(el("span", role.label));
    const tag = el("span", "idle", "status-tag");
    header.appendChild(tag);
    const body = el("pre", "");
    box.appendChild(header);
    box.appendChild(body);
    stream.appendChild(box);
    roleBoxes.set(role.key, { box, body, tag });
  }
}

function setStep(stepName) {
  if (!STEPS.includes(stepName)) return; // e.g. the initial "queued" status
  for (const li of $("steps").children) {
    li.className = li.dataset.step === stepName ? "active" : "pending";
  }
}

function markAllStepsDone() {
  for (const li of $("steps").children) li.className = "done";
}

function appendDelta(roleKey, text) {
  const entry = roleBoxes.get(roleKey);
  if (!entry) return;
  entry.body.textContent += text;
  if (entry.body.textContent.length > MAX_BOX_CHARS) {
    entry.body.textContent = "\u2026[earlier output trimmed]\n" + entry.body.textContent.slice(-MAX_BOX_CHARS);
  }
}

function setRoleTag(roleKey, text) {
  const entry = roleBoxes.get(roleKey);
  if (entry) entry.tag.textContent = text;
}

function showGate(phase, status, output) {
  const g = $("gate-output");
  g.hidden = false;
  g.textContent += `[${phase}] status=${status}\n${output}\n\n`;
}

function resetRunView() {
  for (const [, entry] of roleBoxes) { entry.body.textContent = ""; entry.tag.textContent = "idle"; }
  $("ledger-view").textContent = "(empty)";
  $("gate-output").hidden = true;
  $("gate-output").textContent = "";
  setStep("plan");
}

// ----- run submission (one POST, SSE stream) -----

let activeController = null;

async function submitRun() {
  const task = $("task").value.trim();
  const password = $("password").value;
  if (!task) { setBanner("Enter a task first.", "error"); return; }
  if (task.length > 4000) { setBanner("Task exceeds 4000 characters.", "error"); return; }
  if (!password) { setBanner("Enter the operator password.", "error"); return; }

  const models = {};
  for (const role of ROLES) models[role.key] = $(`model-${role.key}`).value;

  setBanner("Running\u2026", "info");
  resetRunView();
  $("submit").disabled = true;
  $("cancel").disabled = false;
  activeController = new AbortController();

  try {
    const resp = await fetch("/api/run", {
      method: "POST",
      headers: { "content-type": "application/json", "x-operator-password": password },
      body: JSON.stringify({ task, models }),
      signal: activeController.signal,
    });

    if (!resp.ok) {
      let detail = `HTTP ${resp.status}`;
      try {
        const err = await resp.json();
        if (err && err.error === "run_in_progress") detail = "A run is already in progress. Wait for it to finish, then retry (requests are not queued).";
        else if (err && err.error === "unauthorized") detail = "Incorrect operator password.";
        else if (err && err.error === "not_configured") detail = err.message || "Server is not configured.";
        else if (err && err.message) detail = err.message;
      } catch { /* keep HTTP status detail */ }
      setBanner(detail, "error");
      return;
    }

    const reader = resp.body.getReader();
    const dec = new TextDecoder();
    const state = { buf: "" };
    let sawDone = false;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const ev of pushSseLines(state, dec.decode(value, { stream: true }))) {
        handleEvent(ev, () => { sawDone = true; });
      }
    }
    if (!sawDone) {
      setBanner("Connection closed before the run reported a final status. Check history after reload.", "error");
    }
  } catch (e) {
    if (e && e.name === "AbortError") {
      setBanner("Cancelled. The server marks the run as interrupted; interrupted generations cannot be resumed.", "error");
    } else {
      setBanner("Connection failed. Check the deployment and try again later.", "error");
    }
  } finally {
    $("submit").disabled = false;
    $("cancel").disabled = true;
    activeController = null;
  }
}

function handleEvent(ev, markDone) {
  const d = ev.data || {};
  switch (ev.event) {
    case "status":
      if (typeof d.stepName === "string") setStep(d.stepName);
      break;
    case "role":
      setRoleTag(d.role, `running \u00b7 ${d.model}`);
      break;
    case "delta":
      appendDelta(d.role, d.text || "");
      break;
    case "artifact":
      setRoleTag(d.role, "done");
      break;
    case "ledger":
      $("ledger-view").textContent = d.content || "";
      break;
    case "test":
      showGate(d.phase, d.status, d.output || "");
      break;
    case "error":
      setBanner(`Run stopped: ${d.code}. ${d.message || ""}`, "error");
      break;
    case "done":
      markDone();
      markAllStepsDone();
      setBanner(
        d.status === "completed"
          ? `Run completed. Outcome: ${d.outcome}.`
          : d.status === "interrupted"
            ? "Run interrupted (cancelled or disconnected). It cannot be resumed; start a new run."
            : "Run failed.",
        d.status === "completed" && d.outcome === "verification_passed" ? "info" : "error",
      );
      break;
    default:
      break; // unknown event types are ignored safely
  }
}

// ----- history (password per request; never stored) -----

async function loadHistory() {
  const password = $("history-password").value;
  if (!password) { setBanner("Enter the operator password to load history.", "error"); return; }
  const list = $("history-list");
  list.textContent = "";
  try {
    const resp = await fetch("/api/runs", { headers: { "x-operator-password": password } });
    if (resp.status === 401) { setBanner("Incorrect operator password.", "error"); return; }
    if (resp.status === 503) { setBanner("Server is not configured (missing OPERATOR_PASSWORD).", "error"); return; }
    if (!resp.ok) { setBanner(`History unavailable (HTTP ${resp.status}).`, "error"); return; }
    const data = await resp.json();
    for (const run of data.runs || []) {
      const li = el("li");
      const badge = el("span", run.status, "badge");
      li.appendChild(badge);
      li.appendChild(el("span", ` ${new Date(run.createdAt).toLocaleString()} \u2014 ${run.taskPreview}`));
      li.appendChild(el("span", run.outcome ? ` \u00b7 ${run.outcome}` : ""));
      li.addEventListener("click", () => showRunDetail(run.id, password));
      list.appendChild(li);
    }
    if (!data.runs || data.runs.length === 0) list.appendChild(el("li", "No runs retained yet."));
    setBanner("History loaded.", "info");
  } catch {
    setBanner("History fetch failed.", "error");
  }
}

async function showRunDetail(id, password) {
  const detail = $("history-detail");
  detail.textContent = "";
  try {
    const resp = await fetch(`/api/runs/get?id=${encodeURIComponent(id)}`, {
      headers: { "x-operator-password": password },
    });
    if (!resp.ok) { detail.appendChild(el("p", `Detail unavailable (HTTP ${resp.status}).`)); return; }
    const data = await resp.json();
    const title = el("h3", `Run ${data.run.id} \u00b7 ${data.run.status}${data.run.outcome ? ` \u00b7 ${data.run.outcome}` : ""}`);
    detail.appendChild(title);
    detail.appendChild(el("p", `Task: ${data.run.taskPreview}`));
    const ledgerTitle = el("h4", "Ledger");
    detail.appendChild(ledgerTitle);
    const ledgerPre = el("pre", data.ledger || "(no ledger retained)");
    detail.appendChild(ledgerPre);
    const evTitle = el("h4", "Events");
    detail.appendChild(evTitle);
    for (const ev of data.events || []) {
      const p = el("p");
      p.appendChild(el("strong", `${ev.ts} \u00b7 ${ev.type}`));
      const text = ev.type === "artifact" || ev.type === "test"
        ? JSON.stringify(ev.payload, null, 1)
        : JSON.stringify(ev.payload);
      p.appendChild(document.createTextNode(" " + text));
      detail.appendChild(p);
    }
  } catch {
    detail.appendChild(el("p", "Detail fetch failed."));
  }
}

// ----- wiring -----

function init() {
  buildModelSelects();
  buildSteps();
  buildRoleBoxes();
  $("task").addEventListener("input", () => {
    $("task-counter").textContent = `${$("task").value.length} / 4000`;
  });
  $("submit").addEventListener("click", submitRun);
  $("cancel").addEventListener("click", () => { if (activeController) activeController.abort(); });
  $("load-history").addEventListener("click", loadHistory);
}

init();