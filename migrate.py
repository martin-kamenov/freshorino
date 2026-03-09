"""
migrate.py — Freshorino Migration Script
Migrates existing store.db to the new enhanced schema.
Usage: python migrate.py [path/to/old/store.db]
"""
import sqlite3, sys, os, shutil
from datetime import datetime

OLD_DB = sys.argv[1] if len(sys.argv)>1 else "store.db"
NEW_DB = "store.db"
EUR_RATE = 1.95583

def run():
    if not os.path.exists(OLD_DB):
        print(f"❌ {OLD_DB} not found.")
        sys.exit(1)

    backup = OLD_DB+".backup_"+datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy(OLD_DB, backup)
    print(f"✅ Backup: {backup}")

    db = sqlite3.connect(NEW_DB)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    c = db.cursor()

    # Run init from app.py
    import app as freshorino
    with freshorino.app.app_context():
        freshorino.init_db()

    print("✅ Schema initialised.")

    # Count
    prods  = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    sales  = db.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    items  = db.execute("SELECT COUNT(*) FROM sale_items").fetchone()[0]
    waste  = db.execute("SELECT COUNT(*) FROM waste").fetchone()[0]
    print(f"📊 Current data: {prods} products, {sales} sales, {items} sale_items, {waste} waste")
    db.close()
    print("✅ Migration complete!")

if __name__ == "__main__":
    run()
