# Ambient Link core — hard rules

This repo builds the **laptop-side relay** (`host/`): it observes real
Claude/Codex/Cursor sessions on the machine it runs on, serves the glasses web
app, and delivers replies into those same sessions.

Binding constraints (full version + post-mortem:
`~/ambient-link-meta/CLAUDE.md` and `~/ambient-link-meta/RESTART-DECISION.md`):

1. The full relay (tailers, proc watcher, reaper, store, delivery) runs on the
   **laptop only**.
2. The cloud box runs this binary ONLY with `AMBIENT_LINK_ROLE=proxy`: a
   stateless forwarder — no tailers, no local ingestion, no durable store, no
   session invention. Acceptance test: laptop disconnected ⇒ hosted web app
   shows zero sessions. Never re-enable local producers in the proxy role.
3. Nothing on the cloud may spawn or manage agents.
4. Before implementing any component, state which machine it runs on.
5. No fake/demo sessions on any product-facing route.
