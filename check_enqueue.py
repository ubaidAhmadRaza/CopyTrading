# check_enqueue.py
import sqlite3

conn = sqlite3.connect("trade_copier.db")
conn.row_factory = sqlite3.Row

# Check if there are ANY events at all
print("Event queue count:", conn.execute("SELECT COUNT(*) FROM event_queue").fetchone()[0])
print("Slave events count:", conn.execute("SELECT COUNT(*) FROM slave_events").fetchone()[0])
print("Execution logs count:", conn.execute("SELECT COUNT(*) FROM execution_logs").fetchone()[0])

# Check the latest master state
print("\nMaster state:")
for row in conn.execute("SELECT * FROM master_state"):
    val = row['value'][:200] if row['value'] else 'NULL'
    print(f"  {row['key']}: {val}")

# Force a manual insert to test if writes work
try:
    conn.execute("INSERT INTO event_queue (event_id, event_type, payload, created_at) VALUES ('test123', 'TEST', '{}', datetime('now'))")
    conn.commit()
    print("\n✓ Test insert worked")
    # Clean up
    conn.execute("DELETE FROM event_queue WHERE event_id='test123'")
    conn.commit()
except Exception as e:
    print(f"\n✗ Test insert failed: {e}")

conn.close()