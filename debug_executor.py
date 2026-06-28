"""Quick diagnostic for trade execution issues"""
import sqlite3
import json

db_path = "trade_copier.db"  # Adjust if different

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("\n=== QUEUE STATUS ===")
queue = conn.execute("SELECT event_type, COUNT(*) as cnt FROM event_queue GROUP BY event_type").fetchall()
for q in queue:
    print(f"{q['event_type']}: {q['cnt']}")

print("\n=== RECENT EXECUTIONS ===")
recent = conn.execute("""
    SELECT timestamp, slave_login, master_ticket, symbol, action, status, retcode, error 
    FROM execution_log 
    ORDER BY timestamp DESC 
    LIMIT 10
""").fetchall()
for r in recent:
    print(f"{r['timestamp'][:19]} | Ticket:{r['master_ticket']} | {r['symbol']} | {r['action']} | {r['status']} | RC:{r['retcode']} | {r['error']}")

print("\n=== SLAVE ACCOUNT ===")
slaves = conn.execute("SELECT * FROM accounts WHERE role='slave'").fetchall()
for s in slaves:
    print(f"Login: {s['login']}, Balance: {s['balance']}, Equity: {s.get('equity', 'N/A')}")

print("\n=== SYMBOL MAPPING ===")
config = conn.execute("SELECT value FROM config WHERE key='symbol_mapping'").fetchone()
if config:
    print(json.loads(config['value']))
else:
    print("No symbol mapping found")

conn.close()