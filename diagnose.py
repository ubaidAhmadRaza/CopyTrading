"""
diagnose.py
Run this while the slave is stopped (or alongside it) to inspect the queue
and show exactly why trades are not executing.

    python diagnose.py --db trade_copier.db
"""
from __future__ import annotations
import argparse
import json
import sqlite3
from datetime import datetime, timezone

def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="trade_copier.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    print("\n" + "="*60)
    print("  QUEUE DEPTH (slave_events by status)")
    print("="*60)
    for row in conn.execute("SELECT status, COUNT(*) n FROM slave_events GROUP BY status"):
        print(f"  {row['status']:15s} : {row['n']}")

    print("\n" + "="*60)
    print("  SLAVE CURSOR (last materialised event_queue id)")
    print("="*60)
    for row in conn.execute("SELECT * FROM slave_cursor"):
        print(f"  slave_login={row['slave_login']}  last_id={row['last_id']}")

    print("\n" + "="*60)
    print("  PENDING / RETRY events (top 10, with payload preview)")
    print("="*60)
    now = utcnow()
    rows = conn.execute("""
        SELECT se.event_id, se.slave_login, se.status, se.attempts,
               se.next_attempt_at, se.error, se.claimed_at,
               eq.event_type, eq.payload
        FROM slave_events se
        JOIN event_queue eq ON eq.event_id = se.event_id
        WHERE se.status IN ('PENDING','RETRY','PROCESSING')
        ORDER BY eq.id ASC
        LIMIT 10
    """).fetchall()

    if not rows:
        print("  (none — queue is empty or all FAILED/COMPLETED)")
    for r in rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = {}
        due = r["next_attempt_at"] or "now"
        overdue = due <= now if r["next_attempt_at"] else True
        print(f"\n  event_id   : {r['event_id']}")
        print(f"  slave      : {r['slave_login']}")
        print(f"  status     : {r['status']}  attempts={r['attempts']}")
        print(f"  next_try   : {due}  {'<< OVERDUE' if overdue else '(future)'}")
        print(f"  claimed_at : {r['claimed_at']}")
        print(f"  error      : {r['error']}")
        print(f"  event_type : {r['event_type']}")
        print(f"  symbol     : {p.get('symbol')}  volume={p.get('volume')}  "
              f"price={p.get('open_price') or p.get('price')}")

    print("\n" + "="*60)
    print("  FAILED events (top 5)")
    print("="*60)
    rows = conn.execute("""
        SELECT se.event_id, se.slave_login, se.attempts, se.error, se.retcode,
               eq.event_type, eq.payload
        FROM slave_events se
        JOIN event_queue eq ON eq.event_id = se.event_id
        WHERE se.status = 'FAILED'
        ORDER BY se.updated_at DESC
        LIMIT 5
    """).fetchall()
    if not rows:
        print("  (none)")
    for r in rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = {}
        print(f"\n  event_id   : {r['event_id']}")
        print(f"  slave      : {r['slave_login']}")
        print(f"  attempts   : {r['attempts']}")
        print(f"  retcode    : {r['retcode']}")
        print(f"  error      : {r['error']}")
        print(f"  event_type : {r['event_type']}")
        print(f"  symbol     : {p.get('symbol')}")

    print("\n" + "="*60)
    print("  RECENT EXECUTION LOGS (last 10)")
    print("="*60)
    rows = conn.execute("""
        SELECT * FROM execution_logs ORDER BY id DESC LIMIT 10
    """).fetchall()
    if not rows:
        print("  (none — executor has never run successfully)")
    for r in rows:
        print(f"  [{r['timestamp'][:19]}] {r['action']:20s} {r['status']:10s} "
              f"rc={r['retcode']} err={r['error']}")

    print("\n" + "="*60)
    print("  MASTER STATE (positions count)")
    print("="*60)
    row = conn.execute(
        "SELECT value, updated_at FROM master_state WHERE key='positions'"
    ).fetchone()
    if row:
        try:
            positions = json.loads(row["value"])
            print(f"  {len(positions)} positions  (updated {row['updated_at']})")
            for p in positions[:5]:
                print(f"    ticket={p.get('ticket')}  symbol={p.get('symbol')}  "
                      f"volume={p.get('volume')}")
        except Exception as e:
            print(f"  parse error: {e}")
    else:
        print("  (no master state — master process has not written yet)")

    print("\n" + "="*60)
    print("  HEARTBEATS")
    print("="*60)
    for row in conn.execute("SELECT * FROM heartbeats ORDER BY component"):
        print(f"  {row['component']:30s} {row['status']:15s} {row['detail']}  "
              f"@ {row['updated_at']}")

    print("\n" + "="*60)
    print("  TICKET MAPPINGS (open)")
    print("="*60)
    rows = conn.execute("""
        SELECT * FROM ticket_mapping WHERE closed=0 LIMIT 20
    """).fetchall()
    if not rows:
        print("  (none — no trades have been copied yet)")
    for r in rows:
        print(f"  master={r['master_ticket']}  slave={r['slave_ticket']}  "
              f"login={r['slave_login']}  symbol={r['symbol']}")

    conn.close()
    print()

if __name__ == "__main__":
    main()