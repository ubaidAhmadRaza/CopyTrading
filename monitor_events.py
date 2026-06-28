"""Monitor database for new events in real-time"""
import sqlite3
import time
import os

db_path = "trade_copier.db"

if not os.path.exists(db_path):
    print(f"ERROR: Database '{db_path}' not found!")
    exit(1)

print("Monitoring for new events... (Ctrl+C to stop)")
print("=" * 60)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

last_id = 0

try:
    while True:
        # Check event_queue
        events = conn.execute(
            "SELECT * FROM event_queue WHERE id > ? ORDER BY id", 
            (last_id,)
        ).fetchall()
        
        for e in events:
            print(f"\n[{e['created_at']}] NEW EVENT:")
            print(f"  ID: {e['id']}")
            print(f"  Type: {e['event_type']}")
            print(f"  Status: {e['status']}")
            print(f"  Payload: {e['payload'][:100]}...")
            last_id = e['id']
        
        # Check execution_logs
        logs = conn.execute(
            "SELECT * FROM execution_logs ORDER BY id DESC LIMIT 1"
        ).fetchall()
        
        if logs:
            log = logs[0]
            print(f"\n[EXECUTION] {log['timestamp']}: {log['action']} {log['symbol']} = {log['status']}")
        
        # Check slave_events
        slave_evts = conn.execute(
            "SELECT * FROM slave_events ORDER BY id DESC LIMIT 1"
        ).fetchall()
        
        if slave_evts:
            se = slave_evts[0]
            print(f"[SLAVE EVENT] {se['event_id']}: {se['status']} (attempts: {se['attempts']})")
        
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nMonitoring stopped")
    conn.close()