# relay-node-prototype (legacy)

> **Status:** preserved for reference. The production host daemon is in
> [`../host/`](../host) (Go). This Node prototype was the first end-to-end
> implementation of the protocol; keep it for protocol-evolution sanity
> checks but don't ship it.

One Node process. Mirrors tmux sessions, detects when each agent goes idle,
broadcasts to WS subscribers, and (when an idle event fires) emits a Web Push
to every PWA that has subscribed.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| WS  | `/face-chat/ws`             | Multi-thread chat stream + input |
| GET | `/face-chat/push/vapid`     | Returns `{publicKey}` for the PWA to subscribe |
| POST| `/face-chat/push/subscribe` | PWA POSTs its `PushSubscription` JSON |
| POST| `/face-chat/push/test`      | Force a test push to every subscriber |
| GET | `/face-chat/push/status`    | `{vapid, subscriptions, threads}` |

## First-time setup

```sh
cd relay
npm install
node -e "console.log(JSON.stringify(require('web-push').generateVAPIDKeys(), null, 2))" \
  | tee etc/vapid.json     # add { "subject": "mailto:you@host" } to it
mkdir -p etc
cp etc/threads.json.example etc/threads.json   # if you change defaults
node server.mjs
```

By default it listens on `127.0.0.1:5180`. Override with `FC_PORT`, `FC_HOST`.

## Caddy reverse-proxy (same host as your static `web/`)

```
example.com {
    root * /var/www/face-chat-final/web
    file_server

    @push path /face-chat/push/*
    handle @push {
        reverse_proxy 127.0.0.1:5180
    }
    handle /face-chat/ws {
        reverse_proxy 127.0.0.1:5180
    }
}
```

## Running under systemd

```ini
# /etc/systemd/system/face-chat-relay.service
[Unit]
Description=face-chat relay
After=network.target

[Service]
ExecStart=/usr/bin/node /opt/face-chat-final/relay/server.mjs
Environment=FC_PORT=5180 FC_HOST=127.0.0.1
WorkingDirectory=/opt/face-chat-final/relay
Restart=on-failure
User=face-chat

[Install]
WantedBy=multi-user.target
```

## Hot-reload threads

`kill -HUP $(pgrep -f relay/server.mjs)` to re-read `etc/threads.json`
without losing state for the threads that haven't changed.

## Idle detection — heuristics worth knowing

- Default `FC_IDLE_MS=2000`. If you set this too low, partial output from
  Claude streaming token-by-token may falsely trigger idle events.
- The "last assistant snippet" sent in push notifications is heuristically
  extracted from `tmux capture-pane` output: strip ANSI, drop box-drawing
  characters, drop bare prompt lines (`> `, `❯ `, `$`, `#`), keep last
  ~12 non-trivial lines. Agent-specific extractors can be added per
  `agent` type if the generic version misreads any particular CLI.
