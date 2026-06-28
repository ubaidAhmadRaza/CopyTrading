"""Quick diagnostic - check database schema and contents"""
import sqlite3
import os

db_path = "trade_copier.db"

if not os.path.exists(db_path):
    print(f"ERROR: Database file '{db_path}' not found!")
    print("Current directory:", os.getcwd())
    print("Files in directory:", os.listdir('.'))
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("\n=== ALL TABLES ===")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for t in tables:
    print(f"Table: {t['name']}")
    # Show schema for each table
    schema = conn.execute(f"PRAGMA table_info({t['name']})").fetchall()
    print("  Columns:", [f"{s['name']} ({s['type']})" for s in schema])
    
    # Show row count
    count = conn.execute(f"SELECT COUNT(*) as cnt FROM {t['name']}").fetchone()
    print(f"  Rows: {count['cnt']}\n")

print("\n=== EVENT QUEUE CONTENTS ===")
queue = conn.execute("SELECT * FROM event_queue ORDER BY id LIMIT 5").fetchall()
if queue:
    for q in queue:
        print(dict(q))
else:
    print("(empty)")

print("\n=== ACCOUNTS ===")
accounts = conn.execute("SELECT * FROM accounts").fetchall()
for a in accounts:
    print(dict(a))

print("\n=== HEARTBEATS ===")
hb = conn.execute("SELECT * FROM heartbeats").fetchall()
for h in hb:
    print(dict(h))

print("\n=== STATS ===")
try:
    stats = conn.execute("SELECT * FROM stats").fetchone()
    if stats:
        print(dict(stats))
except:
    print("No stats table")

print("\n=== LOOKING FOR EXECUTION-LIKE TABLES ===")
# Try common variations
for table_name in ['execution_log', 'executions', 'trade_log', 'trades', 'orders']:
    try:
        data = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
        print(f"\n{table_name}:")
        for d in data:
            print(dict(d))
    except:
        pass

conn.close()