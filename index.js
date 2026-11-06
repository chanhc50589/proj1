/// <reference types="@cloudflare/workers-types" />
/**
 * memdoor team-solve — Cloudflare Worker + Durable Object (SQLite), vanilla JS.
 *
 * One module contains: pure helpers (unit-tested), the HTML/CSS shell, the Worker
 * router, and the RunCoordinator Durable Object that owns run state, the workflow
 * engine, Workers AI streaming, and event/ledger persistence.
 *
 * Source workflow preserved (fresh contexts, artifact/ledger handoffs, exact role
 * responsibilities, one conditional repair round, mandatory final verify):
 *   plan ─┬─ ideate-a ─┬─ solve ── test ── review ── repair ── verify
 *         └─ ideate-b ─┘
 * Sandbox adaptation: the strict test/verify commands become a deterministic
 * strict-output-contract gate run by the engine (no shell, no tools, no filesystem).
 */

// ---------------------------------------------------------------------------
// Constants and caps
// ---------------------------------------------------------------------------

export const MODEL_ALLOWLIST = [
  "@cf/google/gemma-4-26b-a4b-it", // default
  "@cf/qwen/qwen3.8-27b",
  "@cf/zai-org/glm-4.7-flash",
];

export const DEFAULT_MODEL = MODEL_ALLOWLIST[0];

export const ROLES = [
  { key: "planner",  label: "Planner",        color: "red",    step: "plan"   },
  { key: "ideator1", label: "First ideator",  color: "orange", step: "ideate" },
  { key: "ideator2", label: "Second ideator", color: "yellow", step: "ideate" },
  { key: "solver",   label: "Solver",         color: "green",  step: "solve"  },
  { key: "manager",  label: "Manager",        color: "blue",   step: "review" },
  { key: "repair",   label: "Repair worker",  color: "purple", step: "repair" },
];

export const PROPOSAL_A_MARKER = "<!-- proposal-a -->";
export const PROPOSAL_B_MARKER = "<!-- proposal-b -->";

export const CAPS = {
  MAX_TASK_CHARS: 4000,
  MAX_BODY_BYTES: 32768,
  MAX_OUTPUT_TOKENS: 2048,          // per model call
  MAX_MODEL_CALLS: 8,               // engine uses 6
  MAX_STEPS: 8,                     // engine uses 7 stages
  MAX_ROLE_OUTPUT_CHARS: 12000,     // belt-and-braces behind max_tokens
  MAX_SOLUTION_CHARS: 6000,         // strict gate limit on the enclosed solution
  MAX_EVENTS_PER_RUN: 300,
  MAX_EVENT_PAYLOAD_CHARS: 8000,
  MAX_ARTIFACT_STORED_CHARS: 8000,
  MAX_ARTIFACT_EMITTED_CHARS: 16000,
  MAX_PROPOSAL_CHARS: 4000,         // inserted into the ledger
  MAX_SECTION_APPEND_CHARS: 2000,   // solver decision / review / repair sections
  MAX_LEDGER_CHARS: 48000,
  MAX_LEDGER_EMITTED_CHARS: 16384,
  RUN_TTL_MS: 24 * 60 * 60 * 1000,  // 24 hours
  MAX_RUNS: 20,
  STALE_RUN_MS: 10 * 60 * 1000,     // active run with no updates for 10 min => interrupted
  HEARTBEAT_MS: 15000,
  MIN_LEDGER_LINES: 50,
};

export const SOLUTION_START = "<<<SOLUTION-START>>>";
export const SOLUTION_END = "<<<SOLUTION-END>>>";

export class LedgerError extends Error {}
export class WorkflowAbort extends Error {}
export class WorkflowFailure extends Error {
  constructor(code) { super(`workflow failed: ${code}`); this.code = code; }
}

// ---------------------------------------------------------------------------
// Logging — structured, low-sensitivity. NEVER log prompts, model output,
// passwords, authorization headers, or secret values.
// ---------------------------------------------------------------------------

function log(event, fields = {}) {
  try { console.log(JSON.stringify({ ts: new Date().toISOString(), event, ...fields })); }
  catch { /* logging must never break the request */ }
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested)
// ---------------------------------------------------------------------------

export function countOccurrences(haystack, needle) {
  let n = 0, i = 0;
  if (typeof haystack !== "string" || !needle) return n;
  while ((i = haystack.indexOf(needle, i)) !== -1) { n++; i += needle.length; }
  return n;
}

export function truncateUtf8(str, maxChars) {
  if (typeof str !== "string") return "";
  if (str.length <= maxChars) return str;
  let cut = str.slice(0, maxChars);
  const last = cut.codePointAt(cut.length - 1);
  if (last >= 0xd800 && last <= 0xdbff) cut = cut.slice(0, -1); // avoid splitting a pair
  return cut + "\u2026[truncated]";
}

export function sseFrame(event, data) {
  if (!/^[a-z][a-z0-9_-]*$/.test(event)) throw new Error("bad SSE event name");
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

/** Incremental SSE parser (reference implementation; browser.js keeps an identical copy). */
export function pushSseLines(state, chunk) {
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
      if (line.startsWith(":")) continue; // comment / heartbeat
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

/** Hash-then-compare: constant-time on fixed-size digests; no plaintext comparison. */
export async function safeEqualPassword(provided, expected) {
  if (typeof provided !== "string" || typeof expected !== "string") return false;
  if (provided.length === 0 || provided.length > 1024) return false;
  const enc = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode(provided)),
    crypto.subtle.digest("SHA-256", enc.encode(expected)),
  ]);
  const va = new Uint8Array(a), vb = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < va.length; i++) diff |= va[i] ^ vb[i];
  return diff === 0;
}

export function validateSubmission(body) {
  const fail = (error) => ({ ok: false, error });
  if (!body || typeof body !== "object" || Array.isArray(body)) return fail("body must be a JSON object");
  const task = typeof body.task === "string" ? body.task.trim() : null;
  if (!task) return fail("task is required");
  if (task.length > CAPS.MAX_TASK_CHARS) return fail(`task exceeds ${CAPS.MAX_TASK_CHARS} characters`);
  const m = body.models;
  if (!m || typeof m !== "object" || Array.isArray(m)) {
    return fail("models object with one selection per role is required");
  }
  const models = {};
  for (const r of ROLES) {
    const v = m[r.key];
    if (typeof v !== "string" || !MODEL_ALLOWLIST.includes(v)) {
      return fail(`invalid model for role "${r.key}"`);
    }
    models[r.key] = v;
  }
  for (const k of Object.keys(m)) {
    if (!ROLES.some((r) => r.key === k)) return fail(`unknown role "${k}"`);
  }
  return { ok: true, value: { task, models } };
}

/** Map any thrown error to a SAFE user-facing code + message. No raw provider text. */
export function mapAiError(err) {
  const msg = String((err && err.message) || err || "");
  if (err instanceof WorkflowAbort) {
    return { code: "interrupted", message: "The run was cancelled or disconnected." };
  }
  if (err instanceof WorkflowFailure) {
    const m = mapAiError({ message: err.code });
    return m.code === "internal_error" ? { code: err.code, message: m.message } : m;
  }
  if (/too many requests|rate.?limit|\b429\b|quota/i.test(msg)) {
    return { code: "ai_quota_or_rate_limit", message: "Workers AI quota or rate limit reached. The run stopped. Please retry later." };
  }
  if (/capacity|overloaded|no more capacity|\b503\b|\b529\b/i.test(msg)) {
    return { code: "ai_capacity", message: "Workers AI is temporarily out of capacity. The run stopped. Please retry later." };
  }
  if (/not found|unknown model|invalid model|does not exist/i.test(msg)) {
    return { code: "model_unavailable", message: "The selected model is currently unavailable. The run stopped. Please retry later or pick another model." };
  }
  if (/context (length|window)|too many tokens|maximum context/i.test(msg)) {
    return { code: "input_too_long", message: "Model input exceeded the context window. The run stopped." };
  }
  if (/durable object|sqlite|storage|too large|exceeded/i.test(msg)) {
    return { code: "storage_limit", message: "Run storage limit reached. The run stopped safely; retained history stays within limits." };
  }
  return { code: "internal_error", message: "The run failed unexpectedly and was stopped safely." };
}

// ---------------------------------------------------------------------------
// Shared ledger operations (the source workflow's on-disk notes file)
// ---------------------------------------------------------------------------

function escapeRegExp(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

function hasHeading(text, heading) {
  return new RegExp(`^#{1,3}\\s*${escapeRegExp(heading)}\\s*$`, "im").test(text);
}

/** Insert `text` on its own lines immediately before the unique marker line. */
export function insertBeforeMarker(ledger, marker, text) {
  const count = countOccurrences(ledger, marker);
  if (count === 0) throw new LedgerError(`marker ${marker} not found`);
  if (count > 1) throw new LedgerError(`marker ${marker} occurs ${count} times`);
  const at = ledger.indexOf(marker);
  const lineStart = ledger.lastIndexOf("\n", at) + 1;
  return ledger.slice(0, lineStart) + String(text).replace(/\s+$/, "") + "\n\n" + ledger.slice(lineStart);
}

/** Append `text` under a section heading, before the next heading (or at the end). */
export function appendUnderHeading(ledger, heading, text) {
  const m = new RegExp(`^#{1,3}\\s*${escapeRegExp(heading)}\\s*$`, "im").exec(ledger);
  const body = String(text).trimEnd();
  if (!m) return ledger.replace(/\s*$/, "") + "\n\n" + body + "\n";
  const afterHeading = m.index + m[0].length;
  const next = /\n#{1,3}\s/.exec(ledger.slice(afterHeading));
  const insertAt = next ? afterHeading + next.index + 1 : ledger.length;
  return ledger.slice(0, insertAt).replace(/\s*$/, "") + "\n\n" + body + "\n" + ledger.slice(insertAt);
}

/**
 * Deterministic normalization of the planner-produced ledger (the plan prompt asks
 * the planner to do this; the engine verifies and repairs deterministically):
 * title with run id, required headings, both markers exactly once, >= 50 lines.
 */
export function normalizeLedger(text, runId) {
  let t = typeof text === "string" ? text : "";
  const idTag = `RUN-${String(runId).slice(0, 8)}`;
  if (!t.includes(idTag)) t = `# Team-solve run ${idTag}\n\n` + t;
  for (const h of ["Plan", "Proposal A", "Proposal B", "Solver decision", "Review", "Repair"]) {
    if (!hasHeading(t, h)) t += `\n\n## ${h}\n`;
  }
  for (const [heading, marker] of [["Proposal A", PROPOSAL_A_MARKER], ["Proposal B", PROPOSAL_B_MARKER]]) {
    let count = countOccurrences(t, marker);
    while (count > 1) { // keep first occurrence, strip extras (deterministic)
      const first = t.indexOf(marker);
      const second = t.indexOf(marker, first + marker.length);
      t = t.slice(0, second) + t.slice(second + marker.length);
      count = countOccurrences(t, marker);
    }
    if (count === 0) {
      const m = new RegExp(`^#{1,3}\\s*${escapeRegExp(heading)}\\s*$`, "im").exec(t);
      const at = m ? m.index + m[0].length : t.length;
      t = t.slice(0, at) + `\n\n${marker}\n` + t.slice(at);
    }
  }
  const lines = t.split("\n");
  while (lines.length < CAPS.MIN_LEDGER_LINES) lines.push("");
  return truncateUtf8(lines.join("\n"), CAPS.MAX_LEDGER_CHARS);
}

// ---------------------------------------------------------------------------
// Strict gate — sandbox equivalent of the source workflow's strict test command.
// Deterministic, model-free, no shell. It can fail, which is what drives
// review -> exactly one repair round -> mandatory final verify.
// ---------------------------------------------------------------------------

export function runTestGate(roleText) {
  const text = typeof roleText === "string" ? roleText : "";
  const violations = [];
  const starts = countOccurrences(text, SOLUTION_START);
  const ends = countOccurrences(text, SOLUTION_END);
  if (starts !== 1) violations.push(`solution start marker must occur exactly once (found ${starts})`);
  if (ends !== 1) violations.push(`solution end marker must occur exactly once (found ${ends})`);
  let inner = "";
  if (starts === 1 && ends === 1) {
    const s = text.indexOf(SOLUTION_START) + SOLUTION_START.length;
    const e = text.indexOf(SOLUTION_END);
    if (e < s) violations.push("solution end marker appears before start marker");
    else {
      inner = text.slice(s, e).trim();
      if (!inner) violations.push("solution is empty");
      else if (inner.length > CAPS.MAX_SOLUTION_CHARS) {
        violations.push(`solution exceeds ${CAPS.MAX_SOLUTION_CHARS} characters (${inner.length})`);
      }
    }
  }
  if (text.length > CAPS.MAX_ROLE_OUTPUT_CHARS) {
    violations.push(`role output exceeds ${CAPS.MAX_ROLE_OUTPUT_CHARS} characters`);
  }
  const output = violations.length
    ? "TEST FAILED\n- " + violations.join("\n- ")
    : "TEST PASSED\n- strict output contract satisfied: envelope exactly once, non-empty solution, size within limits";
  return { status: violations.length ? 1 : 0, output, solution: inner };
}

// ---------------------------------------------------------------------------
// Prompts — adapted from the memdoor team-solve source. The source text is
// preserved; sandbox adaptations (no filesystem/shell) are marked with comments.
// ---------------------------------------------------------------------------

const SYSTEM_ADAPTER = [
  "You are one role inside an automated, sandboxed workflow run.",
  "You have no filesystem, no shell, no tools, and no network access.",
  "The only shared state is the run ledger, provided to you in the task message.",
  "Communicate only through your reply. Never mention or simulate tool use.",
  "Reply in plain text.",
].join(" ");

export function buildPlannerPrompt(task, runId) {
  // Source prompt preserved; {{.partition}} -> run id; "verify files exist" -> engine verifies.
  return `You are the planner in a ledger-based team working with fresh contexts. Read the task and create a plan that includes:

- A clear decomposition of the work and its dependencies.
- The requirements for a correct, complete, and useful result.
- Two or three viable approaches, with their trade-offs.
- Specific instructions for independent ideators, the solver, and the reviewer.

Do not change the task or its requirements to make it easier.

Initialize the notes for THIS run. Your entire reply IS the initial run ledger. Give it a title, include the run identifier RUN-${String(runId).slice(0, 8)}, and create separate headings for Plan, Proposal A, Proposal B, Solver decision, Review, and Repair. Put the unique literal marker ${PROPOSAL_A_MARKER} under Proposal A and ${PROPOSAL_B_MARKER} under Proposal B, each on its own line.
Leave enough blank lines so the ledger is at least 50 lines long. Parallel workers will add notes at distinct markers using small patches; do not overwrite the ledger.
The workflow engine verifies that the ledger contains both markers, each exactly once, and that it is at least 50 lines long. Communicate only through this ledger. Do not rely on prior conversation and do not delegate to other agents.

TASK:
${task}`;
}

export function buildIdeatorAPrompt(task, ledger) {
  // Source prompt preserved; "insert before marker / verify on disk" is performed by the engine.
  return `You are independent ideator A in a fresh context. Read the task, plan, and current notes below. Develop one concrete proposal: the most direct approach you can justify as meeting the stated constraints. Explain the core idea, assumptions, reasoning, trade-offs, and relevant edge cases; include implementation details only if the task calls for them.

Reply with ONLY the text of your proposal for the Proposal A section. The workflow engine will insert it immediately before the unique ${PROPOSAL_A_MARKER} marker, keeping the marker and every other line intact; another ideator is writing concurrently to a distinct section. Never rewrite the notes or modify the solution; the engine enforces this by construction. Do not spawn other agents or rely on chat context.

TASK:
${task}

RUN LEDGER:
${ledger}`;
}

export function buildIdeatorBPrompt(task, ledger) {
  return `You are independent ideator B working in a fresh context. Read the task, plan, and current notes below. Develop one concrete alternative proposal using a different approach or perspective from the most obvious one, and compare its tradeoffs with that approach. Explain the proposal clearly, including how it works, why it is suitable, and any relevant limitations or risks.

Reply with ONLY the text of your proposal for the Proposal B section. The workflow engine will insert it immediately before the unique ${PROPOSAL_B_MARKER} marker, keeping the marker and all other lines intact; another ideator is writing concurrently to a separate section. Do not rewrite the notes or modify the solution. Do not spawn other agents or rely on chat context.

TASK:
${task}

RUN LEDGER:
${ledger}`;
}

export function buildSolverPrompt(task, ledger) {
  // Source prompt preserved; the strict envelope IS the sandbox's strict command contract.
  return `You are the solver in a fresh context. Read the task, plan, and notes below, including both proposals. Select and justify an approach for the Solver decision section of the notes; begin your reply with that justification. Briefly explain why it meets the task's requirements and how you will carry it out.

Complete the task as specified, respecting its constraints and required format. Check the result for correctness and completeness before submitting. Do not change the task or its requirements. The downstream process handles evaluation and any repair round. Do not spawn agents or use chat context.

STRICT OUTPUT CONTRACT (checked mechanically right after your reply):
- End your reply with the solution enclosed exactly once between the literal lines
${SOLUTION_START}
and
${SOLUTION_END}
- The enclosed solution must be non-empty and at most ${CAPS.MAX_SOLUTION_CHARS} characters.
A missing, duplicated, empty, or oversized envelope is a failed test.

TASK:
${task}

RUN LEDGER:
${ledger}`;
}

export function buildManagerPrompt(task, ledger, solutionText, gate) {
  // Source prompt preserved; "captured results" are the gate status/output and the solution text.
  return `You are the manager/reviewer in a fresh context. Read the task, notes, solution, test status, and test output below. Inspect the captured results directly; do not rely on a previous agent's claims. Write a review describing the outcome; the engine will append it to the Review section of the notes.

If the test status is nonzero, analyze the failing cases and write specific, actionable repair instructions for the next worker in a fresh context. Do not make the repairs yourself.

If the test status is zero, record that no repair is needed. Do not alter test data or bypass failed comparisons.

There is exactly one conditional repair round, followed by a mandatory strict test command. Do not claim overall completion until that final command passes.

TASK:
${task}

RUN LEDGER:
${truncateUtf8(ledger, CAPS.MAX_LEDGER_CHARS)}

SOLUTION:
${truncateUtf8(solutionText, CAPS.MAX_ROLE_OUTPUT_CHARS)}

TEST STATUS: ${gate.status}
TEST OUTPUT:
${truncateUtf8(gate.output, 2000)}`;
}

export function buildRepairPrompt(task, ledger, solutionText, gate, reviewText, testPassed) {
  // Source prompt preserved; "run the solution on a failing sample" is impossible in the
  // sandbox, so the worker reasons over the failing sample described by the gate/review
  // and the mandatory final gate runs mechanically afterwards.
  const statusLine = testPassed
    ? "TEST STATUS: 0 (passed)"
    : `TEST STATUS: ${gate.status}`;
  return `You are the sole repair worker in a fresh context. Read the test status, review, and notes below. If the initial test status is zero, leave the solution unchanged and reply with exactly one line: "No repair was needed."

Otherwise, read the task, plan, solution, test output, and relevant samples below. Correct the solution in accordance with the manager's instructions. After the corrected solution, append a concise explanation of the repair. Do not begin another repair round and do not alter any test inputs or expected outputs.

The next strict command alone determines workflow success. Therefore, if and only if you changed the solution, re-emit the corrected solution inside the strict envelope exactly once:
${SOLUTION_START}
...corrected solution (non-empty, at most ${CAPS.MAX_SOLUTION_CHARS} characters)...
${SOLUTION_END}
Do not spawn agents or rely on chat history.

TASK:
${task}

RUN LEDGER:
${truncateUtf8(ledger, CAPS.MAX_LEDGER_CHARS)}

SOLUTION:
${truncateUtf8(solutionText, CAPS.MAX_ROLE_OUTPUT_CHARS)}

${statusLine}
TEST OUTPUT:
${truncateUtf8(gate.output, 2000)}

MANAGER REVIEW:
${truncateUtf8(reviewText, CAPS.MAX_SECTION_APPEND_CHARS)}`;
}

// ---------------------------------------------------------------------------
// Workers AI streaming (server-side only). Emits {"response": "..."} deltas;
// reasoning fields (reasoning_content / thinking_blocks) are intentionally ignored.
// ---------------------------------------------------------------------------

async function streamAiToText(ai, model, messages, maxTokens, onDelta, signal) {
  const resp = await ai.run(model, { messages, stream: true, max_tokens: maxTokens });
  if (!resp || typeof resp.getReader !== "function") {
    throw new Error("model returned no streaming response");
  }
  const reader = resp.getReader();
  const dec = new TextDecoder();
  let buf = "", full = "";
  try {
    for (;;) {
      if (signal && signal.aborted) { try { await reader.cancel(); } catch { /* ignore */ } throw new WorkflowAbort(); }
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf("\n")) !== -1) {
        const line = buf.slice(0, nl).replace(/\r$/, "");
        buf = buf.slice(nl + 1);
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (!data || data === "[DONE]") continue;
        let obj;
        try { obj = JSON.parse(data); } catch { continue; }
        if (typeof obj.response === "string" && obj.response.length > 0) {
          full += obj.response;
          if (onDelta) onDelta(obj.response);
        }
        // reasoning_content / thinking_blocks: skipped, never surfaced or stored
      }
    }
  } finally {
    try { reader.releaseLock(); } catch { /* ignore */ }
  }
  return full;
}

// ---------------------------------------------------------------------------
// Workflow engine — one instance per run, executes inside the Durable Object.
// Fresh context per role: each model call receives ONLY system adapter + its own
// prompt (task + current ledger snapshot). No chat history is carried over.
// ---------------------------------------------------------------------------

class WorkflowEngine {
  constructor(deps) {
    this.ai = deps.ai;
    this.runId = deps.runId;
    this.task = deps.task;
    this.models = deps.models;
    this.out = deps.out;               // { send(event, data) }
    this.signal = deps.signal;         // client-facing signal (request-driven)
    this.persistEvent = deps.persistEvent;
    this.setLedger = deps.setLedger;
    this.getLedger = deps.getLedger;
    this.finishRun = deps.finishRun;
    this.modelCalls = 0;
    // Internal controller: aborted by client disconnect AND by any role failure,
    // so sibling parallel streams stop promptly and no further roles start.
    this.internal = new AbortController();
    if (deps.signal) deps.signal.addEventListener("abort", () => this.internal.abort(), { once: true });
  }

  isAborted() {
    return Boolean(this.signal && this.signal.aborted) || this.internal.signal.aborted;
  }

  emit(event, data) { this.out.send(event, data); }
  emitStatus(step, stepName) {
    this.emit("status", { runId: this.runId, step, stepName, maxSteps: CAPS.MAX_STEPS });
  }
  emitLedger(content) {
    this.emit("ledger", { content: truncateUtf8(content, CAPS.MAX_LEDGER_EMITTED_CHARS) });
  }
  emitArtifact(roleKey, text) {
    const role = ROLES.find((r) => r.key === roleKey) || { key: roleKey, label: roleKey, color: "gray" };
    this.emit("artifact", { role: roleKey, label: role.label, color: role.color, content: truncateUtf8(text, CAPS.MAX_ARTIFACT_EMITTED_CHARS) });
  }
  emitTest(phase, gate) {
    this.emit("test", { phase, status: gate.status, output: truncateUtf8(gate.output, 2000) });
  }

  async callRole(roleKey, prompt) {
    if (this.modelCalls >= CAPS.MAX_MODEL_CALLS) throw new WorkflowFailure("model_call_limit");
    if (this.isAborted()) throw new WorkflowAbort();
    this.modelCalls++;
    const role = ROLES.find((r) => r.key === roleKey) || { key: roleKey, label: roleKey, color: "gray" };
    const model = this.models[roleKey];
    this.emit("role", { role: roleKey, label: role.label, color: role.color, model });
    this.persistEvent("role", { role: roleKey, model });
    const t0 = Date.now();
    let text;
    try {
      text = await streamAiToText(
        this.ai, model,
        [{ role: "system", content: SYSTEM_ADAPTER }, { role: "user", content: prompt }],
        CAPS.MAX_OUTPUT_TOKENS,
        (d) => this.emit("delta", { role: roleKey, text: d }),
        this.internal.signal,
      );
    } catch (e) {
      if (e instanceof WorkflowAbort || this.isAborted()) throw new WorkflowAbort();
      const mapped = mapAiError(e);
      this.emit("error", { code: mapped.code, message: mapped.message, role: roleKey });
      this.persistEvent("error", { code: mapped.code, role: roleKey });
      log("role_failed", { runId: this.runId, role: roleKey, code: mapped.code, ms: Date.now() - t0 });
      this.internal.abort(); // stop any sibling parallel stream immediately
      throw new WorkflowFailure(mapped.code); // stop the run; NEVER retry quota/capacity errors
    }
    log("role_completed", { runId: this.runId, role: roleKey, model, ms: Date.now() - t0, chars: text.length });
    this.persistEvent("artifact", { role: roleKey, content: truncateUtf8(text, CAPS.MAX_ARTIFACT_STORED_CHARS) });
    this.emitArtifact(roleKey, text);
    return { text };
  }

  /** Marker-based insertion with bounded re-read/retry (source conflict semantics). */
  insertWithRetry(ledger, marker, text, heading, maxChars) {
    const body = truncateUtf8(text, maxChars);
    let current = ledger;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        return insertBeforeMarker(current, marker, body);
      } catch (e) {
        if (!(e instanceof LedgerError)) throw e;
        const stored = this.getLedger(); // re-read the shared ledger, then retry minimally
        current = stored !== null ? stored : current;
      }
    }
    // Deterministic fallback after bounded retries; visible in the ledger.
    return appendUnderHeading(current, heading, body + "\n\n[engine: marker insertion retried and failed; appended here]");
  }

  async run() {
    this.emitStatus(1, "plan");

    const plan = await this.callRole("planner", buildPlannerPrompt(this.task, this.runId));
    let ledger = normalizeLedger(plan.text, this.runId);
    this.setLedger(ledger);
    this.emitLedger(ledger);

    this.emitStatus(2, "ideate");
    // The two ideators are separate role instances, run in parallel; each writes only
    // to its own marker section. allSettled ensures the sibling stream is stopped and
    // settled before failure handling, with no unhandled rejections.
    const settled = await Promise.allSettled([
      this.callRole("ideator1", buildIdeatorAPrompt(this.task, ledger)),
      this.callRole("ideator2", buildIdeatorBPrompt(this.task, ledger)),
    ]);
    const reasons = [];
    for (const r of settled) {
      if (r.status === "rejected") reasons.push(r.reason);
    }
    if (reasons.length > 0) {
      const failure = reasons.find((reason) => !(reason instanceof WorkflowAbort));
      throw failure || reasons[0]; // prefer the real failure over the abort it caused
    }
    const a = /** @type {{ text: string }} */ (settled[0].value);
    const b = /** @type {{ text: string }} */ (settled[1].value);

    ledger = this.insertWithRetry(ledger, PROPOSAL_A_MARKER, a.text, "Proposal A", CAPS.MAX_PROPOSAL_CHARS);
    this.setLedger(ledger); this.emitLedger(ledger);
    ledger = this.insertWithRetry(ledger, PROPOSAL_B_MARKER, b.text, "Proposal B", CAPS.MAX_PROPOSAL_CHARS);
    this.setLedger(ledger); this.emitLedger(ledger);

    this.emitStatus(3, "solve");
    const solution = await this.callRole("solver", buildSolverPrompt(this.task, ledger));
    ledger = appendUnderHeading(ledger, "Solver decision", truncateUtf8(solution.text, CAPS.MAX_SECTION_APPEND_CHARS));
    this.setLedger(ledger); this.emitLedger(ledger);

    this.emitStatus(4, "test");
    const gate = runTestGate(solution.text);
    this.persistEvent("test", { phase: "initial", status: gate.status, output: truncateUtf8(gate.output, 2000) });
    this.emitTest("test", gate);

    this.emitStatus(5, "review");
    const review = await this.callRole("manager", buildManagerPrompt(this.task, ledger, solution.text, gate));
    ledger = appendUnderHeading(ledger, "Review", truncateUtf8(review.text, CAPS.MAX_SECTION_APPEND_CHARS));
    this.setLedger(ledger); this.emitLedger(ledger);

    this.emitStatus(6, "repair");
    // The repair role always runs (source behavior); it reports a no-op when the test passed.
    const repair = await this.callRole(
      "repair",
      buildRepairPrompt(this.task, ledger, solution.text, gate, review.text, gate.status === 0),
    );
    ledger = appendUnderHeading(ledger, "Repair", truncateUtf8(repair.text, CAPS.MAX_SECTION_APPEND_CHARS));
    this.setLedger(ledger); this.emitLedger(ledger);
    // If the initial test passed, the solution stands unchanged; only a failed test
    // lets the repair output replace it for the mandatory final verify.
    const finalText = gate.status !== 0 ? repair.text : solution.text;

    this.emitStatus(7, "verify");
    const finalGate = runTestGate(finalText);
    this.persistEvent("test", { phase: "final", status: finalGate.status, output: truncateUtf8(finalGate.output, 2000) });
    this.emitTest("verify", finalGate);

    const outcome = finalGate.status === 0 ? "verification_passed" : "verification_not_passed";
    this.finishRun("completed", outcome, null);
    log("run_finished", { runId: this.runId, status: "completed", outcome });
    this.emit("done", { runId: this.runId, status: "completed", outcome });
  }
}

// ---------------------------------------------------------------------------
// HTML + CSS shell (served by this Worker; no separate frontend)
// ---------------------------------------------------------------------------

const HTML_SHELL = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>memdoor team-solve</title>
<style>
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, sans-serif; margin: 0 auto; padding: 1rem; max-width: 60rem; }
h1 { font-size: 1.4rem; margin: 0; } h2 { font-size: 1.05rem; margin: 0 0 .5rem; }
header p { margin: .25rem 0 0; opacity: .75; }
section { margin: 1rem 0; padding: .75rem; border: 1px solid #8884; border-radius: 8px; }
label { display: block; margin: .5rem 0 .15rem; font-weight: 600; }
textarea { width: 100%; font: inherit; }
input, select, button { font: inherit; }
.row { display: flex; gap: .5rem; align-items: center; margin: .5rem 0; flex-wrap: wrap; }
#banner.error { color: #c62828; font-weight: 600; }
#banner.info { opacity: .8; }
#task-counter { opacity: .7; }
.role { border: 1px solid; border-radius: 8px; margin: .5rem 0; overflow: hidden; }
.role > header { display: flex; gap: .5rem; align-items: baseline; padding: .35rem .6rem; font-weight: 600; }
.role > header .status-tag { font-weight: 400; font-size: .8rem; opacity: .8; }
.role pre { margin: 0; padding: .5rem .6rem; white-space: pre-wrap; word-break: break-word; max-height: 18rem; overflow: auto; }
.r-planner  { border-color: #d32f2f; } .r-planner  > header { background: rgba(211,47,47,.15); }
.r-ideator1 { border-color: #ef6c00; } .r-ideator1 > header { background: rgba(239,108,0,.15); }
.r-ideator2 { border-color: #f9a825; } .r-ideator2 > header { background: rgba(249,168,37,.2); }
.r-solver   { border-color: #2e7d32; } .r-solver   > header { background: rgba(46,125,50,.15); }
.r-manager  { border-color: #1565c0; } .r-manager  > header { background: rgba(21,101,192,.15); }
.r-repair   { border-color: #6a1b9a; } .r-repair   > header { background: rgba(106,27,154,.15); }
#steps { margin: 0; padding-left: 1.2rem; }
#steps li.pending { opacity: .45; }
#steps li.active { font-weight: 700; }
#gate-output { white-space: pre-wrap; font-family: ui-monospace, monospace; background: rgba(127,127,127,.1); padding: .5rem; border-radius: 6px; margin-top: .5rem; }
#ledger-view { white-space: pre-wrap; word-break: break-word; max-height: 20rem; overflow: auto; background: rgba(127,127,127,.1); padding: .5rem; border-radius: 6px; }
#history-list li { cursor: pointer; margin: .25rem 0; }
.badge { padding: 0 .35rem; border-radius: 4px; font-size: .75rem; border: 1px solid; }
</style>
</head>
<body>
<header>
  <h1>memdoor team-solve</h1>
  <p>plan &rarr; ideate A &#8741; B &rarr; solve &rarr; test &rarr; review &rarr; repair &rarr; verify</p>
</header>
<main>
  <section id="panel-new">
    <h2>New run</h2>
    <div id="model-selects"></div>
    <label for="task">Task</label>
    <textarea id="task" rows="5" maxlength="4000" placeholder="Describe the task for the team..."></textarea>
    <div class="row"><span id="task-counter">0 / 4000</span></div>
    <label for="password">Operator password</label>
    <input id="password" type="password" autocomplete="off" placeholder="required for every run">
    <div class="row">
      <button id="submit" type="button">Run workflow</button>
      <button id="cancel" type="button" disabled>Cancel</button>
      <span id="banner" role="alert"></span>
    </div>
  </section>
  <section id="panel-progress">
    <h2>Progress</h2>
    <ol id="steps"></ol>
    <div id="gate-output" hidden></div>
  </section>
  <section id="panel-stream">
    <h2>Roles</h2>
    <div id="stream"></div>
  </section>
  <section id="panel-ledger">
    <h2>Run ledger</h2>
    <pre id="ledger-view">(empty)</pre>
  </section>
  <section id="panel-history">
    <h2>Run history</h2>
    <p>History is protected by the operator password, sent per request and never stored by this page.</p>
    <div class="row">
      <input id="history-password" type="password" autocomplete="off" placeholder="operator password">
      <button id="load-history" type="button">Load history</button>
    </div>
    <ul id="history-list"></ul>
    <div id="history-detail"></div>
  </section>
</main>
<script type="module" src="/browser.js"></script>
</body>
</html>`;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

function json(status, obj) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

function applySecurityHeaders(resp) {
  const h = resp.headers;
  h.set("content-security-policy",
    "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return resp;
}

function requireSameOrigin(request) {
  const origin = request.headers.get("origin");
  if (!origin) return null; // non-browser clients still need the password
  try {
    if (new URL(origin).origin !== new URL(request.url).origin) return json(403, { error: "forbidden_origin" });
  } catch {
    return json(403, { error: "forbidden_origin" });
  }
  return null;
}

async function requireOperator(request, env) {
  const pwd = env.OPERATOR_PASSWORD;
  if (typeof pwd !== "string" || pwd.length === 0) {
    // Fail clearly and safely; never reveal whether the secret exists elsewhere.
    return json(503, { error: "not_configured", message: "Server is not configured: the OPERATOR_PASSWORD secret is missing. Set it and retry." });
  }
  const provided = request.headers.get("x-operator-password") || "";
  if (!(await safeEqualPassword(provided, pwd))) return json(401, { error: "unauthorized" });
  return null;
}

async function readJsonCapped(request, cap) {
  if (!request.body) return { ok: false };
  const reader = request.body.getReader();
  const dec = new TextDecoder();
  let total = 0, text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > cap) { try { await reader.cancel(); } catch { /* ignore */ } return { tooLarge: true }; }
    text += dec.decode(value, { stream: true });
  }
  text += dec.decode();
  try { return { ok: true, body: JSON.parse(text) }; } catch { return { parseError: true }; }
}

function health(env) {
  return {
    ok: true,
    service: "memdoor-team-solve",
    configured: typeof env.OPERATOR_PASSWORD === "string" && env.OPERATOR_PASSWORD.length > 0,
    aiBinding: Boolean(env.AI),
    doBinding: Boolean(env.RUN_COORDINATOR),
    time: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Worker routes
// ---------------------------------------------------------------------------

async function handleRunSubmission(request, env) {
  const originFail = requireSameOrigin(request);
  if (originFail) return originFail;
  const authFail = await requireOperator(request, env);
  if (authFail) return authFail;

  const ct = (request.headers.get("content-type") || "").toLowerCase();
  if (!ct.includes("application/json")) return json(415, { error: "unsupported_media_type" });

  const parsed = await readJsonCapped(request, CAPS.MAX_BODY_BYTES);
  if (parsed.tooLarge) return json(413, { error: "payload_too_large" });
  if (!parsed.ok) return json(400, { error: "invalid_json" });

  const v = validateSubmission(parsed.body);
  if (!v.ok) return json(400, { error: "invalid_input", message: v.error });
  if (!env.RUN_COORDINATOR) return json(500, { error: "internal_error" });

  const stub = env.RUN_COORDINATOR.get(env.RUN_COORDINATOR.idFromName("singleton"));
  try {
    // The DO response IS the SSE stream for the whole run (single request, single stream).
    return await doRequest(env, "/start", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(v.value),
    });
  } catch (e) {
    log("do_start_failed", { code: mapAiError(e).code });
    return json(500, { error: "internal_error" });
  }
}

const DO_ORIGIN = "https://run-coordinator";
async function doRequest(env, path, init) {
  const stub = env.RUN_COORDINATOR.get(env.RUN_COORDINATOR.idFromName("singleton"));
  return stub.fetch(new URL(path, DO_ORIGIN).toString(), init);
}

async function handleListRuns(request, env) {
  const originFail = requireSameOrigin(request);
  if (originFail) return originFail;
  const authFail = await requireOperator(request, env);
  if (authFail) return authFail;
  try {
    return await doRequest(env, "/runs", { method: "GET" });
  } catch (e) {
    log("do_history_failed", { code: mapAiError(e).code });
    return json(500, { error: "internal_error" });
  }
}

async function handleGetRun(request, env, url) {
  const originFail = requireSameOrigin(request);
  if (originFail) return originFail;
  const authFail = await requireOperator(request, env);
  if (authFail) return authFail;
  const id = url.searchParams.get("id") || "";
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)) {
    return json(400, { error: "invalid_input" });
  }
  try {
    return await doRequest(env, `/runs/get?id=${encodeURIComponent(id)}`, { method: "GET" });
  } catch (e) {
    log("do_history_failed", { code: mapAiError(e).code });
    return json(500, { error: "internal_error" });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
        return applySecurityHeaders(new Response(HTML_SHELL, {
          headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
        }));
      }
      if (request.method === "GET" && url.pathname === "/browser.js") {
        const asset = await env.ASSETS.fetch(request);
        return applySecurityHeaders(new Response(asset.body, asset));
      }
      if (request.method === "GET" && url.pathname === "/favicon.ico") {
        return new Response(null, { status: 204 });
      }
      if (request.method === "GET" && url.pathname === "/health") {
        // Operational check only: validates bindings/secret presence. No model call, no AI quota.
        return applySecurityHeaders(json(200, health(env)));
      }
      if (request.method === "POST" && url.pathname === "/api/run") {
        return applySecurityHeaders(await handleRunSubmission(request, env));
      }
      if (request.method === "GET" && url.pathname === "/api/runs") {
        return applySecurityHeaders(await handleListRuns(request, env));
      }
      if (request.method === "GET" && url.pathname === "/api/runs/get") {
        return applySecurityHeaders(await handleGetRun(request, env, url));
      }
      return applySecurityHeaders(json(404, { error: "not_found" }));
    } catch (e) {
      log("worker_error", { path: url.pathname, code: mapAiError(e).code });
      return applySecurityHeaders(json(500, { error: "internal_error" }));
    }
  },
};

// ---------------------------------------------------------------------------
// RunCoordinator Durable Object (SQLite-backed)
// ---------------------------------------------------------------------------

const SCHEMA_VERSION = 1;

export class RunCoordinator {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.schemaError = null;
    this.ready = this.#initSchema();
  }

  async #initSchema() {
    // DDL v1 — idempotent. Future changes: append additive statements + bump SCHEMA_VERSION,
    // guarded by the meta table so every restart converges to the same shape.
    try {
      this.state.storage.sql.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)`);
      this.state.storage.sql.exec(`CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        task TEXT NOT NULL,
        models_json TEXT NOT NULL,
        outcome TEXT,
        error_code TEXT
      )`);
      this.state.storage.sql.exec(`CREATE TABLE IF NOT EXISTS events (
        run_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (run_id, seq)
      )`);
      this.state.storage.sql.exec(`CREATE TABLE IF NOT EXISTS ledger (
        run_id TEXT PRIMARY KEY,
        content TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )`);
      const row = [...this.state.storage.sql.exec(`SELECT v FROM meta WHERE k = 'schema_version'`)][0];
      if (!row) {
        this.state.storage.sql.exec(`INSERT INTO meta (k, v) VALUES (?, ?)`, "schema_version", String(SCHEMA_VERSION));
      }
    } catch (e) {
      this.schemaError = mapAiError(e).code;
      log("do_schema_error", { code: this.schemaError });
    }
  }

  async fetch(request) {
    await this.ready;
    if (this.schemaError) return json(503, { error: "storage_unavailable" });
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/start") return await this.#start(request);
      if (request.method === "GET" && url.pathname === "/runs") return await this.#listRuns();
      if (request.method === "GET" && url.pathname === "/runs/get") return await this.#getRun(url);
      return json(404, { error: "not_found" });
    } catch (e) {
      log("do_error", { path: url.pathname, code: mapAiError(e).code });
      return json(500, { error: "internal_error" });
    }
  }

  // ----- persistence helpers -----

  #addEvent(runId, type, payload) {
    try {
      const c = [...this.state.storage.sql.exec(`SELECT COUNT(*) AS n FROM events WHERE run_id = ?`, runId)][0];
      const seq = Number(c.n) + 1;
      if (seq > CAPS.MAX_EVENTS_PER_RUN) return; // bounded event history
      this.state.storage.sql.exec(
        `INSERT OR REPLACE INTO events (run_id, seq, ts, type, payload) VALUES (?, ?, ?, ?, ?)`,
        runId, seq, Date.now(), type, truncateUtf8(JSON.stringify(payload), CAPS.MAX_EVENT_PAYLOAD_CHARS),
      );
      this.state.storage.sql.exec(`UPDATE runs SET updated_at = ? WHERE id = ?`, Date.now(), runId);
    } catch (e) {
      log("do_event_failed", { runId, type, code: mapAiError(e).code });
    }
  }

  #setLedger(runId, content) {
    try {
      this.state.storage.sql.exec(
        `INSERT OR REPLACE INTO ledger (run_id, content, updated_at) VALUES (?, ?, ?)`,
        runId, truncateUtf8(content, CAPS.MAX_LEDGER_CHARS), Date.now(),
      );
    } catch (e) {
      log("do_ledger_failed", { runId, code: mapAiError(e).code });
    }
  }

  #getLedger(runId) {
    const row = [...this.state.storage.sql.exec(`SELECT content FROM ledger WHERE run_id = ?`, runId)][0];
    return row ? row.content : null;
  }

  #getActiveRun() {
    const row = [...this.state.storage.sql.exec(`SELECT id FROM runs WHERE status = 'active' LIMIT 1`)][0];
    return row ? row.id : null;
  }

  #finishRun(runId, status, outcome, errorCode) {
    try {
      this.state.storage.sql.exec(
        `UPDATE runs SET status = ?, outcome = ?, error_code = ?, updated_at = ? WHERE id = ?`,
        status, outcome, errorCode, Date.now(), runId,
      );
      this.#addEvent(runId, "done", { status, outcome: outcome || null, errorCode: errorCode || null });
    } catch (e) {
      log("do_finish_failed", { runId, code: mapAiError(e).code });
    }
  }

  /** Runs with status 'active' but no updates for STALE_RUN_MS are marked interrupted. */
  async #reapStale() {
    const cutoff = Date.now() - CAPS.STALE_RUN_MS;
    const stale = [...this.state.storage.sql.exec(
      `SELECT id FROM runs WHERE status = 'active' AND updated_at < ?`, cutoff)];
    for (const { id } of stale) {
      this.#finishRun(id, "interrupted", null, "stale");
      log("run_reaped_stale", { runId: id });
    }
  }

  /**
   * Exact retention behavior:
   *  - delete non-active runs older than 24h (RUN_TTL_MS);
   *  - keep only the 20 most recent runs; delete the rest;
   *  - delete events and ledger of every deleted run;
   *  - trim each kept run's events to the newest 300 (MAX_EVENTS_PER_RUN).
   * Runs are never removed while 'active' (stale ones were reaped first).
   */
  async #prune() {
    try {
      this.state.storage.sql.exec(
        `DELETE FROM runs WHERE status != 'active' AND created_at < ?`, Date.now() - CAPS.RUN_TTL_MS);
      const keepers = [...this.state.storage.sql.exec(
        `SELECT id FROM runs ORDER BY created_at DESC LIMIT ${CAPS.MAX_RUNS}`)].map((r) => r.id);
      const all = [...this.state.storage.sql.exec(`SELECT id FROM runs`)].map((r) => r.id);
      for (const id of all) {
        if (!keepers.includes(id)) {
          this.state.storage.sql.exec(`DELETE FROM events WHERE run_id = ?`, id);
          this.state.storage.sql.exec(`DELETE FROM ledger WHERE run_id = ?`, id);
          this.state.storage.sql.exec(`DELETE FROM runs WHERE id = ?`, id);
        } else {
          this.state.storage.sql.exec(
            `DELETE FROM events WHERE run_id = ? AND seq <= (SELECT MAX(seq) - ${CAPS.MAX_EVENTS_PER_RUN} FROM events WHERE run_id = ?)`,
            id, id);
        }
      }
    } catch (e) {
      log("do_prune_failed", { code: mapAiError(e).code }); // retention is best-effort; run continues
    }
  }

  // ----- run start (single active run; reserve is atomic) -----

  async #start(request) {
    if (!this.env.AI) {
      return json(503, { error: "not_configured", message: "Server is not configured: the AI binding is unavailable." });
    }
    const body = await request.json().catch(() => null);
    const v = body && validateSubmission(body);
    if (!v || !v.ok) return json(400, { error: "invalid_input" });

    let reserved = null;
    // Atomic reap + prune + check-and-reserve so two simultaneous submissions
    // cannot both start, and cleanup cannot interleave with reservation.
    await this.state.blockConcurrencyWhile(async () => {
      await this.#reapStale();
      await this.#prune();
      const active = this.#getActiveRun();
      if (active) { reserved = { conflict: active }; return; }
      const id = crypto.randomUUID();
      this.state.storage.sql.exec(
        `INSERT INTO runs (id, created_at, updated_at, status, task, models_json) VALUES (?, ?, ?, 'active', ?, ?)`,
        id, Date.now(), Date.now(), v.value.task, JSON.stringify(v.value.models),
      );
      reserved = { id };
    });
    if (reserved.conflict) {
      return json(409, { error: "run_in_progress", runId: reserved.conflict });
    }
    const runId = reserved.id;
    log("run_started", { runId, models: v.value.models });

    const ts = new TransformStream();
    const writer = ts.writable.getWriter();
    const enc = new TextEncoder();
    const ac = new AbortController();
    if (request.signal) request.signal.addEventListener("abort", () => ac.abort(), { once: true });
    let closed = false;
    const onWriteFailure = () => {
      closed = true;
      try { ac.abort(); } catch { /* ignore */ } // stop the engine even if the signal never fires
    };
    const send = (event, data) => {
      if (closed) return;
      try { writer.write(enc.encode(sseFrame(event, data))).catch(onWriteFailure); }
      catch { onWriteFailure(); }
    };
    const sendRaw = (s) => {
      if (closed) return;
      try { writer.write(enc.encode(s)).catch(onWriteFailure); }
      catch { onWriteFailure(); }
    };
    const close = () => { closed = true; try { writer.close(); } catch { /* already closed */ } };

    const persistEvent = (type, payload) => this.#addEvent(runId, type, payload);
    const deps = {
      ai: this.env.AI,
      runId,
      task: v.value.task,
      models: v.value.models,
      out: { send },
      signal: ac.signal,
      persistEvent,
      setLedger: (c) => this.#setLedger(runId, c),
      getLedger: () => this.#getLedger(runId),
      finishRun: (status, outcome, errorCode) => this.#finishRun(runId, status, outcome, errorCode),
    };

    const done = (async () => {
      const hb = setInterval(() => sendRaw(": ping\n\n"), CAPS.HEARTBEAT_MS);
      try {
        send("status", { runId, step: 0, stepName: "queued", maxSteps: CAPS.MAX_STEPS });
        await new WorkflowEngine(deps).run();
      } catch (e) {
        if (e instanceof WorkflowAbort || ac.signal.aborted) {
          deps.finishRun("interrupted", null, "client_disconnect_or_cancel");
          send("done", { runId, status: "interrupted", outcome: null });
          log("run_finished", { runId, status: "interrupted" });
        } else {
          const mapped = mapAiError(e);
          deps.finishRun("failed", null, mapped.code);
          if (!(e instanceof WorkflowFailure)) send("error", { code: mapped.code, message: mapped.message });
          send("done", { runId, status: "failed", outcome: null });
          log("run_finished", { runId, status: "failed", code: mapped.code });
        }
      } finally {
        clearInterval(hb);
        close();
      }
    })();
    if (typeof this.state.waitUntil === "function") this.state.waitUntil(done);

    return new Response(ts.readable, {
      headers: {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-store, no-transform",
        "x-accel-buffering": "no",
      },
    });
  }

  // ----- history -----

  #runSummary(r) {
    return {
      id: r.id,
      createdAt: new Date(r.created_at).toISOString(),
      updatedAt: new Date(r.updated_at).toISOString(),
      status: r.status,
      outcome: r.outcome || null,
      errorCode: r.error_code || null,
      taskPreview: truncateUtf8(r.task, 200),
      models: safeParseModels(r.models_json),
    };
  }

  async #listRuns() {
    await this.#reapStale();
    const rows = [...this.state.storage.sql.exec(
      `SELECT * FROM runs ORDER BY created_at DESC LIMIT ${CAPS.MAX_RUNS}`)];
    return json(200, { runs: rows.map((r) => this.#runSummary(r)) });
  }

  async #getRun(url) {
    const id = url.searchParams.get("id") || "";
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)) {
      return json(400, { error: "invalid_input" });
    }
    await this.#reapStale();
    const run = [...this.state.storage.sql.exec(`SELECT * FROM runs WHERE id = ?`, id)][0];
    if (!run) return json(404, { error: "not_found" });
    const events = [...this.state.storage.sql.exec(
      `SELECT ts, type, payload FROM events WHERE run_id = ? ORDER BY seq ASC`, id)]
      .map((e) => ({ ts: new Date(e.ts).toISOString(), type: e.type, payload: safeParseJson(e.payload) }));
    const ledger = this.#getLedger(id);
    return json(200, { run: this.#runSummary(run), events, ledger: ledger || null });
  }
}

function safeParseJson(s) { try { return JSON.parse(s); } catch { return null; } }
function safeParseModels(s) {
  const m = safeParseJson(s);
  if (!m || typeof m !== "object") return {};
  const out = {};
  for (const r of ROLES) if (typeof m[r.key] === "string") out[r.key] = m[r.key];
  return out;
}
