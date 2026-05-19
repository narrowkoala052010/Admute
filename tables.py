"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Database inspection utility — pretty-prints all tables in admute.db.
Run directly: python tables.py
"""

import sqlite3
from tabulate import tabulate

DB_PATH = "admute.db"

def inspect_database():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]

    print(f"\n{'═'*60}")
    print(f"   AdMute Sentinel: Master Database Inspection")
    print(f"{'═'*60}\n")

    for table in tables:
        print(f"📁 TABLE: {table.upper()}")
        
        # Determine how many rows to show
        # We don't want to print 100,000 hashes!
        limit = 5 if table == "hashes" else 20
        
        try:
            cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")
            rows = cursor.fetchall()
            
            if not rows:
                print("   [Empty Table]\n")
                continue

            # Extract headers from the row keys
            headers = rows[0].keys()
            
            # Format the data for tabulate
            data = [list(row) for row in rows]
            
            print(tabulate(data, headers=headers, tablefmt="grid"))
            
            # Show total count for context
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            if total > limit:
                print(f"   ... and {total - limit} more rows.")
            
            print("\n")
            
        except Exception as e:
            print(f"   ❌ Error reading table: {e}\n")

    conn.close()

if __name__ == "__main__":
    import os
    if os.path.exists(DB_PATH):
        inspect_database()
    else:
        print(f"Error: {DB_PATH} not found.")