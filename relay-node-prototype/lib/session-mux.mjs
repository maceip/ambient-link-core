// SessionMux — canonical per-session state machine for the relay.
//
// Receives normalized events from N parallel producers (hooks, jsonl tailer,
// process watcher), dedupes them per session-id × event-type within a short
// window, applies a pure state-transition function, and emits broadcast events
// only when the canonical state changes.
//
// Invariants:
//   1. A session's state is a deterministic function of the ordered, deduped
//      event stream it has received. Same events in same order → same state.
//   2. No event mutates state without being dedup-checked first. Producers can
//      double-fire the same logical event freely; we collapse it here.
//   3. The mux never assumes any producer is authoritative. Hooks may fail to
//      install, JSONL may be rotated mid-session, processes may be missed.
//      Each source contributes evidence; the state machine treats them
//      symmetrically.
//   4. Memory is bounded — see _gcRecent() and the LRU-ish session reaper.
//   5. Bad input is logged and dropped, never throws.
//
// Coordinate with phone-shared/PROTOCOL.md when changing emitted event shapes.

import { createHash } from 'node:crypto';

/** @typedef {'session_start'|'user_prompt'|'assistant_message'|'tool_use'|'permission_prompt'|'stop'|'session_end'} EventType */
/** @typedef {'STARTING'|'BUSY'|'IDLE'|'AWAITING_PERMISSION'|'DEAD'} SessionState */
/** @typedef {'hooks'|'jsonl'|'proc'} ProducerName */

/**
 * @typedef NormalizedEvent
 * @property {string}        session_id
 * @property {string}        agent
 * @property {string}        cwd
 * @property {EventType}     event_type
 * @property {object|string} [payload]
 * @property {ProducerName}  source
 * @property {number}        [observed_at]
 */

export const EVENT_TYPES = Object.freeze({
  SESSION_START:     'session_start',
  USER_PROMPT:       'user_prompt',
  ASSISTANT_MESSAGE: 'assistant_message',
  TOOL_USE:          'tool_use',
  PERMISSION_PROMPT: 'permission_prompt',
  STOP:              'stop',
  SESSION_END:       'session_end',
});

export const STATES = Object.freeze({
  STARTING:            'STARTING',
  BUSY:                'BUSY',
  IDLE:                'IDLE',
  AWAITING_PERMISSION: 'AWAITING_PERMISSION',
  DEAD:                'DEAD',
});

// ── tunables ────────────────────────────────────────────────────────────
const DEFAULTS = Object.freeze({
  // Window during which the same (session_id, event_type) from any source is
  // treated as a single logical event. Sized for worst-case producer skew:
  // hooks fire ≤50ms, JSONL tailer ≤500ms, proc watcher ≤2s.
  dedupWindowMs: 2500,
  // Debounce that flips a BUSY session to IDLE when no further events arrive.
  // Lower → twitchier peek cards; higher → laggy peeks. 2s matches the tmux
  // relay default and aligns with phone-shared/PROTOCOL.md.
  idleMs: 2000,
  // Per-session caps to bound memory regardless of how chatty a producer is.
  maxAssistantSnippet:  4096,
  maxPermissionSnippet: 1024,
  // Cap total sessions retained (oldest dead sessions reaped first).
  maxSessions: 256,
  // GC sweep cadence for the dedup memory.
  gcEveryMs: 60_000,
});

// ── public API ──────────────────────────────────────────────────────────

/**
 * Stable, human-readable thread id derived from agent + cwd. Two sequential
 * `claude` invocations in the same project collapse into the same thread.
 *
 * @param {string} agent
 * @param {string} cwd
 * @returns {string}
 */
export function threadIdFor(agent, cwd) {
  const a = String(agent || 'unknown');
  const c = String(cwd   || '');
  const h = createHash('sha256').update(`${a}::${c}`).digest('hex');
  return `${a}-${h.slice(0, 10)}`;
}

/** @typedef {{ logger?: { info?: Function, warn?: Function, error?: Function }, idleMs?: number, dedupWindowMs?: number, maxSessions?: number, gcEveryMs?: number }} MuxOpts */

export class SessionMux {
  /**
   * @param {(evt: object) => void} emit  fired on every broadcast-worthy state change
   * @param {MuxOpts} [opts]
   */
  constructor(emit, opts = {}) {
    if (typeof emit !== 'function') throw new TypeError('SessionMux requires an emit fn');
    this._emit = emit;
    this._opt  = { ...DEFAULTS, ...opts };
    this._log  = opts.logger ?? console;

    /** @type {Map<string, Session>} */          this._sessions    = new Map();
    /** @type {Map<string, Set<string>>} */      this._threadIndex = new Map();   // thread_id → Set<session_id>
    /** @type {Map<string, number>} */           this._recent      = new Map();   // dedup key → ts(ms)

    this._gcTimer = setInterval(() => this._gc(), this._opt.gcEveryMs);
    if (this._gcTimer.unref) this._gcTimer.unref();
  }

  close() {
    clearInterval(this._gcTimer);
    for (const s of this._sessions.values()) s.clearIdleTimer();
  }

  /**
   * Single entry point for all producers. Validates, dedupes, applies state
   * transition, emits on change. Never throws.
   *
   * @param {NormalizedEvent} ev
   */
  ingest(ev) {
    const v = validate(ev, this._log);
    if (!v) return;

    if (this._isDuplicate(v)) return;

    let s = this._sessions.get(v.session_id);
    const wasNew = !s;
    if (!s) {
      s = this._createSession(v);
      if (this._sessions.size > this._opt.maxSessions) this._reapOldestDead();
    }

    const before = s.state;
    s.absorb(v, this._opt);
    const after  = s.state;

    if (wasNew) {
      this._emit({
        type: 'thread_started',
        thread: s.threadId, label: s.label, agent: s.agent, cwd: s.cwd,
        at: v.observed_at,
      });
    }
    if (after !== before) {
      this._emitTransition(s, after, v.observed_at);
    } else if (after === STATES.BUSY) {
      // Still busy, but a fresh event arrived — arm/reset the idle debounce.
      s.armIdleTimer(this._opt.idleMs, () => this._inferredIdle(s));
    }
  }

  /**
   * External signal that a session is gone (proc-watcher confirms PID dead,
   * etc.). Idempotent.
   * @param {string} session_id
   */
  markDead(session_id) {
    const s = this._sessions.get(session_id);
    if (!s || s.state === STATES.DEAD) return;
    s.clearIdleTimer();
    s.state = STATES.DEAD;
    this._emit({ type: 'thread_ended', thread: s.threadId, session_id, at: Date.now() });
  }

  /** Read-only snapshot for diagnostics / hello-message payloads. */
  snapshot() {
    return [...this._sessions.values()].map(s => ({
      session_id: s.id,
      thread_id:  s.threadId,
      agent:      s.agent,
      cwd:        s.cwd,
      state:      s.state,
      last_event_at: s.lastEventAt,
      last_source:   s.lastSource,
    }));
  }

  // ── internals ─────────────────────────────────────────────────────────

  _isDuplicate(v) {
    // Streamed assistant chunks each carry distinct content; collapsing them
    // would lose the latest snippet. Only dedupe state-affecting transitions.
    if (v.event_type === EVENT_TYPES.ASSISTANT_MESSAGE ||
        v.event_type === EVENT_TYPES.USER_PROMPT) return false;
    const k = `${v.session_id}|${v.event_type}`;
    const last = this._recent.get(k);
    const now  = Date.now();
    if (last && (now - last) < this._opt.dedupWindowMs) return true;
    this._recent.set(k, now);
    return false;
  }

  _createSession(v) {
    const s = new Session(v.session_id, v.agent, v.cwd);
    this._sessions.set(s.id, s);
    let set = this._threadIndex.get(s.threadId);
    if (!set) { set = new Set(); this._threadIndex.set(s.threadId, set); }
    set.add(s.id);
    return s;
  }

  _emitTransition(s, state, at) {
    switch (state) {
      case STATES.BUSY:
        this._emit({ type: 'thread_busy', thread: s.threadId, session_id: s.id, at });
        break;
      case STATES.AWAITING_PERMISSION:
        this._emit({
          type: 'thread_idle', thread: s.threadId, session_id: s.id,
          awaiting: 'permission',
          permissionPrompt: s.lastPermissionPrompt,
          lastAssistant:    s.lastAssistant,
          at,
        });
        break;
      case STATES.IDLE:
        this._emit({
          type: 'thread_idle', thread: s.threadId, session_id: s.id,
          awaiting: 'reply',
          lastAssistant: s.lastAssistant,
          at,
        });
        break;
      case STATES.DEAD:
        this._emit({ type: 'thread_ended', thread: s.threadId, session_id: s.id, at });
        break;
      // STARTING never gets emitted as a transition — the thread_started covers it.
    }
  }

  _inferredIdle(s) {
    if (s.state !== STATES.BUSY) return;
    s.state = STATES.IDLE;
    this._emit({
      type: 'thread_idle', thread: s.threadId, session_id: s.id,
      awaiting: 'reply', lastAssistant: s.lastAssistant,
      inferred: true, at: Date.now(),
    });
  }

  _reapOldestDead() {
    let oldest = null;
    for (const s of this._sessions.values()) {
      if (s.state !== STATES.DEAD) continue;
      if (!oldest || s.lastEventAt < oldest.lastEventAt) oldest = s;
    }
    if (!oldest) return;
    this._sessions.delete(oldest.id);
    const tset = this._threadIndex.get(oldest.threadId);
    if (tset) { tset.delete(oldest.id); if (tset.size === 0) this._threadIndex.delete(oldest.threadId); }
  }

  _gc() {
    const cutoff = Date.now() - this._opt.dedupWindowMs * 4;
    for (const [k, ts] of this._recent) if (ts < cutoff) this._recent.delete(k);
  }
}

// ── internal: Session aggregate ─────────────────────────────────────────

class Session {
  constructor(id, agent, cwd) {
    this.id          = String(id);
    this.agent       = String(agent || 'unknown');
    this.cwd         = String(cwd   || '');
    this.threadId    = threadIdFor(this.agent, this.cwd);
    this.label       = `${this.agent}: ${shortCwd(this.cwd)}`;
    /** @type {SessionState} */ this.state = STATES.STARTING;
    this.lastEventAt = Date.now();
    this.lastSource  = '';
    this.lastAssistant       = '';
    this.lastPermissionPrompt= '';
    this._idleTimer = null;
  }

  /** Apply a validated event to this session. Mutates state + snippet fields. */
  absorb(ev, opt) {
    this.lastEventAt = ev.observed_at;
    this.lastSource  = ev.source;
    const T = EVENT_TYPES;

    switch (ev.event_type) {
      case T.SESSION_START:
        if (this.state === STATES.STARTING || this.state === STATES.DEAD) {
          this.state = STATES.IDLE;
        }
        return;

      case T.ASSISTANT_MESSAGE: {
        const txt = extractText(ev.payload);
        if (txt) this.lastAssistant = clip(txt, opt.maxAssistantSnippet);
        this.state = STATES.BUSY;
        return;
      }

      case T.USER_PROMPT:
      case T.TOOL_USE:
        this.state = STATES.BUSY;
        return;

      case T.PERMISSION_PROMPT: {
        const txt = extractText(ev.payload);
        if (txt) this.lastPermissionPrompt = clip(txt, opt.maxPermissionSnippet);
        this.state = STATES.AWAITING_PERMISSION;
        return;
      }

      case T.STOP:
        this.state = STATES.IDLE;
        return;

      case T.SESSION_END:
        this.state = STATES.DEAD;
        return;
    }
  }

  armIdleTimer(ms, onFire) {
    this.clearIdleTimer();
    this._idleTimer = setTimeout(onFire, ms);
    if (this._idleTimer.unref) this._idleTimer.unref();
  }
  clearIdleTimer() {
    if (this._idleTimer) { clearTimeout(this._idleTimer); this._idleTimer = null; }
  }
}

// ── pure helpers ────────────────────────────────────────────────────────

/** @returns {NormalizedEvent|null} */
function validate(ev, log) {
  if (!ev || typeof ev !== 'object') { log.warn?.('[mux] dropping non-object event'); return null; }
  const t = ev.event_type;
  if (!Object.values(EVENT_TYPES).includes(t)) {
    log.warn?.(`[mux] dropping unknown event_type=${t}`);
    return null;
  }
  if (typeof ev.session_id !== 'string' || ev.session_id.length === 0) {
    log.warn?.('[mux] dropping event without session_id');
    return null;
  }
  return {
    session_id:  ev.session_id,
    agent:       String(ev.agent || 'unknown'),
    cwd:         String(ev.cwd   || ''),
    event_type:  t,
    payload:     ev.payload,
    source:      ev.source ?? 'unknown',
    observed_at: typeof ev.observed_at === 'number' ? ev.observed_at : Date.now(),
  };
}

/** Coerce common payload shapes into a single text string. */
function extractText(p) {
  if (p == null) return '';
  if (typeof p === 'string') return p;
  if (typeof p.text    === 'string') return p.text;
  if (typeof p.content === 'string') return p.content;
  if (typeof p.message === 'string') return p.message;
  if (p.message && typeof p.message === 'object') return extractText(p.message);
  if (Array.isArray(p.content)) {
    return p.content
      .filter(c => c && c.type === 'text' && typeof c.text === 'string')
      .map(c => c.text)
      .join('\n');
  }
  return '';
}

function clip(s, n) { return s.length > n ? s.slice(0, n) : s; }

function shortCwd(p) {
  if (!p) return '';
  const home = process.env.HOME || '';
  return home && p.startsWith(home) ? '~' + p.slice(home.length) : p;
}
