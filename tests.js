/**
 * Unit tests for pure helpers (no Cloudflare credentials, no live AI inference).
 * Run: npm test   (Node >= 20, built-in test runner)
 *
 * Candidly NOT covered here (requires a real runtime):
 *  - actual Workers AI streaming behavior through the `AI` binding,
 *  - Durable Object SQLite storage semantics, single-run lock under real concurrency,
 *  - SSE over a real network connection, asset serving, secret presence.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  MODEL_ALLOWLIST,
  DEFAULT_MODEL,
  ROLES,
  CAPS,
  PROPOSAL_A_MARKER,
  PROPOSAL_B_MARKER,
  SOLUTION_START,
  SOLUTION_END,
  validateSubmission,
  sseFrame,
  pushSseLines,
  safeEqualPassword,
  mapAiError,
  WorkflowAbort,
  countOccurrences,
  truncateUtf8,
  insertBeforeMarker,
  appendUnderHeading,
  normalizeLedger,
  LedgerError,
  runTestGate,
} from "./index.js";

const VALID_MODELS = () => {
  const m = {};
  for (const r of ROLES) m[r.key] = DEFAULT_MODEL;
  return m;
};

test("model allowlist contains exactly the three documented models", () => {
  assert.deepEqual(MODEL_ALLOWLIST, [
    "@cf/google/gemma-4-26b-a4b-it",
    "@cf/qwen/qwen3.8-27b",
    "@cf/zai-org/glm-4.7-flash",
  ]);
});

test("validateSubmission accepts a valid submission and normalizes task", () => {
  const v = validateSubmission({ task: "  build a thing  ", models: VALID_MODELS() });
  assert.equal(v.ok, true);
  assert.equal(v.value.task, "build a thing");
  assert.equal(Object.keys(v.value.models).length, ROLES.length);
});

test("validateSubmission rejects invalid inputs", () => {
  assert.equal(validateSubmission(null).ok, false);
  assert.equal(validateSubmission("x").ok, false);
  assert.equal(validateSubmission({ models: VALID_MODELS() }).ok, false); // no task
  assert.equal(validateSubmission({ task: "   ", models: VALID_MODELS() }).ok, false);
  assert.equal(validateSubmission({ task: "x".repeat(CAPS.MAX_TASK_CHARS + 1), models: VALID_MODELS() }).ok, false);
  assert.equal(validateSubmission({ task: "x" }).ok, false); // no models
  const bad = VALID_MODELS(); bad.solver = "@cf/not-a-model";
  assert.equal(validateSubmission({ task: "x", models: bad }).ok, false);
  const extra = VALID_MODELS(); extra.hacker = DEFAULT_MODEL;
  assert.equal(validateSubmission({ task: "x", models: extra }).ok, false);
  const missing = { ...VALID_MODELS() }; delete missing.manager;
  assert.equal(validateSubmission({ task: "x", models: missing }).ok, false);
});

test("sseFrame formats correctly", () => {
  assert.equal(sseFrame("status", { a: 1 }), 'event: status\ndata: {"a":1}\n\n');
  assert.throws(() => sseFrame("bad event!", {}));
});

test("pushSseLines parses frames, heartbeats, partial chunks, and bad JSON", () => {
  const state = { buf: "" };
  const one = pushSseLines(state, ': ping\n\nevent: status\ndata: {"step":1}\n\nevent: delta\ndata: {"role":"planner","text":"hi"}\n\n');
  assert.equal(one.length, 2);
  assert.equal(one[0].event, "status");
  assert.equal(one[0].data.step, 1);
  assert.equal(one[1].event, "delta");
  assert.equal(one[1].data.text, "hi");
  const partial = pushSseLines(state, 'event: done\ndata: {"runI');
  assert.equal(partial.length, 0);
  const rest = pushSseLines(state, 'd":"r1"}\n\nevent: x\ndata: not-json\n\n');
  assert.equal(rest.length, 2);
  assert.equal(rest[0].event, "done");
  assert.equal(rest[0].data.runId, "r1");
  assert.equal(rest[1].event, "x");
  assert.equal(rest[1].data.raw, "not-json");
});

test("safeEqualPassword compares in constant time on digests", async () => {
  assert.equal(await safeEqualPassword("secret", "secret"), true);
  assert.equal(await safeEqualPassword("secret", "secreT"), false);
  assert.equal(await safeEqualPassword("secret", "secret-but-longer"), false);
  assert.equal(await safeEqualPassword("", "secret"), false);
  assert.equal(await safeEqualPassword(null, "secret"), false);
});

test("mapAiError maps classes to safe codes and never leaks details", () => {
  const cases = [
    ["Too Many Requests: quota exceeded for account", "ai_quota_or_rate_limit"],
    ["no more capacity available right now", "ai_capacity"],
    ["model @cf/x/y not found", "model_unavailable"],
    ["prompt exceeded the context window", "input_too_long"],
    ["Durable Object storage limit exceeded", "storage_limit"],
    ["something utterly unexpected", "internal_error"],
  ];
  for (const [message, code] of cases) {
    const mapped = mapAiError(new Error(message));
    assert.equal(mapped.code, code);
    assert.ok(!mapped.message.includes(message));
  }
  assert.equal(mapAiError(new WorkflowAbort()).code, "interrupted");
});

test("insertBeforeMarker inserts before the unique marker and keeps it", () => {
  const ledger = `## Proposal A\n\n${PROPOSAL_A_MARKER}\n\n## Proposal B\n\n${PROPOSAL_B_MARKER}\n`;
  const next = insertBeforeMarker(ledger, PROPOSAL_A_MARKER, "Idea A text");
  assert.ok(next.indexOf("Idea A text") < next.indexOf(PROPOSAL_A_MARKER));
  assert.equal(countOccurrences(next, PROPOSAL_A_MARKER), 1);
  assert.ok(next.includes("## Proposal B"));
  assert.throws(() => insertBeforeMarker("no marker", PROPOSAL_A_MARKER, "x"), LedgerError);
  assert.throws(() => insertBeforeMarker(`${PROPOSAL_A_MARKER}\n${PROPOSAL_A_MARKER}`, PROPOSAL_A_MARKER, "x"), LedgerError);
});

test("appendUnderHeading appends before the next heading or at the end", () => {
  const ledger = "## Solver decision\n\n## Review\n";
  const next = appendUnderHeading(ledger, "Solver decision", "decided X");
  assert.ok(next.indexOf("decided X") > next.indexOf("## Solver decision"));
  assert.ok(next.indexOf("decided X") < next.indexOf("## Review"));
  const tail = appendUnderHeading("no headings", "Review", "r");
  assert.ok(tail.endsWith("r\n"));
});

test("normalizeLedger guarantees title, headings, unique markers, and 50 lines", () => {
  const runId = "0123abcd-1234-5678-9abc-def012345678";
  const out = normalizeLedger("just some plan text", runId);
  assert.ok(out.includes("RUN-0123abcd"));
  for (const h of ["Plan", "Proposal A", "Proposal B", "Solver decision", "Review", "Repair"]) {
    assert.ok(new RegExp(`^#{1,3}\\s*${h}\\s*$`, "im").test(out));
  }
  assert.equal(countOccurrences(out, PROPOSAL_A_MARKER), 1);
  assert.equal(countOccurrences(out, PROPOSAL_B_MARKER), 1);
  assert.ok(out.split("\n").length >= 50);
  const dedup = normalizeLedger(`# t RUN-0123abcd\n${PROPOSAL_A_MARKER}\n${PROPOSAL_A_MARKER}\n`, runId);
  assert.equal(countOccurrences(dedup, PROPOSAL_A_MARKER), 1);
});

test("runTestGate enforces the strict output contract", () => {
  const good = runTestGate(`decision text\n${SOLUTION_START}\nthe solution\n${SOLUTION_END}`);
  assert.equal(good.status, 0);
  assert.equal(good.solution, "the solution");
  assert.equal(runTestGate("no envelope").status, 1);
  assert.equal(runTestGate(`${SOLUTION_START}\n${SOLUTION_START}\n${SOLUTION_END}`).status, 1);
  assert.equal(runTestGate(`${SOLUTION_START}\n   \n${SOLUTION_END}`).status, 1);
  assert.equal(runTestGate(`${SOLUTION_END}\n${SOLUTION_START}`).status, 1);
  const big = runTestGate(`${SOLUTION_START}\n${"x".repeat(CAPS.MAX_SOLUTION_CHARS + 1)}\n${SOLUTION_END}`);
  assert.equal(big.status, 1);
  assert.ok(big.output.startsWith("TEST FAILED"));
  assert.ok(runTestGate(`${SOLUTION_START}\nok\n${SOLUTION_END}`).output.startsWith("TEST PASSED"));
});

test("truncateUtf8 truncates without splitting surrogate pairs", () => {
  assert.equal(truncateUtf8("abcdef", 3), "abc\u2026[truncated]");
  assert.equal(truncateUtf8("ab", 3), "ab");
  const emoji = "\u{1F600}\u{1F601}"; // two 4-byte characters
  const cut = truncateUtf8(emoji, 1);
  const stripped = cut.replace("\u2026[truncated]", "");
  assert.ok(!/[\uD800-\uDBFF]$/.test(stripped)); // no lone high surrogate at the cut
  assert.equal(truncateUtf8(null, 5), "");
});