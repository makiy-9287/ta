# Deploying on a VPS

A 1 vCPU / 1 GB box is plenty. The engine is a single Python process with three
dependencies and no database server.

## 1. Install

```bash
sudo adduser --system --group --home /opt/sniper_flow sniper
sudo -u sniper -H bash

cd /opt/sniper_flow
# copy the project here (scp, git clone, unzip - whatever you prefer)

python3 -m venv venv
./venv/bin/pip install -r requirements.txt

cp .env.example .env
nano .env            # TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
```

## 2. Verify before going live

```bash
./venv/bin/python main.py --check      # config validation
./venv/bin/python main.py --selftest   # offline logic tests
```

Then run it in the foreground once and watch the first watchlist build and zone
scan complete (a couple of minutes for ~200 symbols):

```bash
./venv/bin/python main.py
```

You should get a startup message in Telegram. Send `/status` to confirm the
command channel works, then Ctrl-C.

## 3. Run it as a service

```bash
exit                                   # back to your sudo user
sudo cp /opt/sniper_flow/deploy/sniper-flow.service /etc/systemd/system/
sudo nano /etc/systemd/system/sniper-flow.service   # check User/paths
sudo systemctl daemon-reload
sudo systemctl enable --now sniper-flow
```

Useful commands:

```bash
systemctl status sniper-flow
journalctl -u sniper-flow -f            # live logs
journalctl -u sniper-flow --since "1 hour ago"
sudo systemctl restart sniper-flow
```

The service restarts automatically on crash and shuts down cleanly on SIGTERM
(streams closed, database flushed, counters persisted). Open setups survive a
restart — the monitor reloads them from SQLite and keeps tracking.

## 4. Without systemd

```bash
# tmux / screen
tmux new -s sniper
cd /opt/sniper_flow && ./venv/bin/python main.py
# detach with Ctrl-B then D

# or nohup
nohup ./venv/bin/python main.py >> data/stdout.log 2>&1 &
```

## 5. Housekeeping

- Logs rotate automatically (`data/sniper.log`, 8 MB × 3 backups).
- The database prunes closed setups older than 90 days.
- Back up `data/sniper.db` if you care about the performance history:
  `sqlite3 data/sniper.db ".backup '/root/sniper-$(date +%F).db'"`

## 6. If something looks wrong

| Symptom | Check |
|---|---|
| No signals for days | `/stats` — the rejection reasons tell you which gate is blocking |
| No zones at all | `/watchlist` populated? volume threshold too high? |
| `rate limited` in logs | lower `WEIGHT_BUDGET_PER_MIN`; a shared/NAT IP counts against you |
| Telegram silent | `journalctl` for `telegram` errors; confirm chat id (negative for groups) |
| Memory climbing | `/health` shows RSS; report it with `MAX_ARMED_SYMBOLS` and uptime |
| `mark price stream unhealthy` / `no data since connect` | the host is not receiving websocket data — `/health` will show `Price source: rest`. The engine keeps working on REST polling; if you want full-resolution flow, move to a host that can reach `fstream.binance.com` |
| Commands silent but logs healthy | fixed in 1.0.1 — a latched weight limiter used to block the handler. Confirm with `/health`: `waits` should not climb |

Region note: Binance blocks some IP ranges. If REST calls fail immediately on a
fresh VPS, the host's region is the first thing to check.
