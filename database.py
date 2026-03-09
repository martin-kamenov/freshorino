"""
database.py — Freshorino Web POS
SQLite database layer with full schema and query functions.
"""
import sqlite3
import hashlib
import os
from datetime import datetime, date, timedelta

DB_PATH = os.environ.get("DB_PATH", "store.db")
EUR_RATE = 1.95583

def bgn_to_eur(bgn):
    try: return float(bgn) / EUR_RATE
    except: return 0.0

def eur_to_bgn(eur):
    try: return float(eur) * EUR_RATE
    except: return 0.0

def normalize_text(s):
    return (s or "").strip().casefold()

def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

def verify_password(p, h):
    return hash_password(p) == h

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 3000")
    return conn

def _safe_alter(cursor, stmts):
    for stmt in stmts:
        try: cursor.execute(stmt)
        except sqlite3.OperationalError: pass

def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'staff', full_name TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE, color TEXT NOT NULL DEFAULT '#4A7C1F')""")
    c.execute("""CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, name_norm TEXT,
        barcode TEXT UNIQUE, category_id INTEGER REFERENCES categories(id),
        price REAL NOT NULL, purchase_price REAL NOT NULL DEFAULT 0,
        quantity REAL NOT NULL DEFAULT 0, unit_type TEXT NOT NULL DEFAULT 'piece',
        expiry_date TEXT, is_quick INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1)""")
    c.execute("""CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL,
        user_id INTEGER REFERENCES users(id), total REAL NOT NULL,
        discount_percent REAL NOT NULL DEFAULT 0, discount_amount REAL NOT NULL DEFAULT 0,
        amount_received REAL, change_given REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS sale_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, sale_id INTEGER NOT NULL REFERENCES sales(id),
        product_id INTEGER NOT NULL REFERENCES products(id),
        product_name TEXT NOT NULL DEFAULT '', quantity REAL NOT NULL,
        unit_type TEXT NOT NULL, unit_price REAL NOT NULL,
        cost_price REAL NOT NULL DEFAULT 0, line_total REAL NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS waste (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL,
        product_id INTEGER NOT NULL REFERENCES products(id),
        quantity REAL NOT NULL, unit_type TEXT NOT NULL, reason TEXT)""")
    _safe_alter(c, [
        "ALTER TABLE products ADD COLUMN unit_type TEXT NOT NULL DEFAULT 'piece'",
        "ALTER TABLE products ADD COLUMN purchase_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE products ADD COLUMN expiry_date TEXT",
        "ALTER TABLE products ADD COLUMN name_norm TEXT",
        "ALTER TABLE products ADD COLUMN category_id INTEGER",
        "ALTER TABLE products ADD COLUMN is_quick INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE products ADD COLUMN active INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE sale_items ADD COLUMN cost_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sale_items ADD COLUMN product_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE sales ADD COLUMN discount_percent REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sales ADD COLUMN discount_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sales ADD COLUMN user_id INTEGER",
        "ALTER TABLE sales ADD COLUMN amount_received REAL",
        "ALTER TABLE sales ADD COLUMN change_given REAL",
    ])
    c.execute("SELECT id, name FROM products WHERE name_norm IS NULL OR name_norm = ''")
    for pid, name in c.fetchall():
        c.execute("UPDATE products SET name_norm=? WHERE id=?", (normalize_text(name), pid))
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_norm ON products(name_norm)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)")
    c.execute("SELECT value FROM settings WHERE key='currency_migrated_to_eur'")
    if not c.fetchone():
        c.execute("UPDATE products SET price=price/?, purchase_price=purchase_price/?", (EUR_RATE,EUR_RATE))
        c.execute("UPDATE sales SET total=total/?, discount_amount=discount_amount/?", (EUR_RATE,EUR_RATE))
        c.execute("UPDATE sale_items SET unit_price=unit_price/?, cost_price=cost_price/?, line_total=line_total/?", (EUR_RATE,EUR_RATE,EUR_RATE))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('currency_migrated_to_eur','1')")
    c.execute("SELECT id FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (username, password_hash, role, full_name) VALUES ('admin', ?, 'admin', 'Администратор')", (hash_password("admin123"),))
    for name, color in [("Плодове","#E8430A"),("Зеленчуци","#2D6A1F"),("Млечни","#4A90D9"),("Месо","#C0392B"),("Хляб","#C9A84C"),("Напитки","#8E44AD"),("Друго","#7F8C8D")]:
        c.execute("INSERT OR IGNORE INTO categories(name,color) VALUES(?,?)", (name,color))
    conn.commit()
    conn.close()

def authenticate_user(username, password):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, username, role, full_name, password_hash FROM users WHERE username=?", (username,))
    row = c.fetchone()
    conn.close()
    if row and verify_password(password, row["password_hash"]):
        return dict(row)
    return None

def get_user_by_id(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, username, role, full_name FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def list_users():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, username, role, full_name, created_at FROM users ORDER BY role, username")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def create_user(username, password, role, full_name):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password_hash, role, full_name) VALUES (?,?,?,?)",
                  (username, hash_password(password), role, full_name))
        conn.commit()
        return True, None
    except sqlite3.IntegrityError:
        return False, "Потребителското име вече е заето."
    finally: conn.close()

def delete_user(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id=? AND username != 'admin'", (user_id,))
    conn.commit()
    conn.close()

def list_categories():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, color FROM categories ORDER BY name")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def save_category(name, color, cat_id=None):
    conn = get_connection()
    c = conn.cursor()
    try:
        if cat_id:
            c.execute("UPDATE categories SET name=?, color=? WHERE id=?", (name,color,cat_id))
        else:
            c.execute("INSERT INTO categories(name,color) VALUES(?,?)", (name,color))
        conn.commit()
        return True, None
    except sqlite3.IntegrityError:
        return False, "Категорията вече съществува."
    finally: conn.close()

def fetch_products(query="", category_id=None, limit=2000):
    conn = get_connection()
    c = conn.cursor()
    sql = """SELECT p.id, p.name, p.barcode, p.price, p.purchase_price,
               p.quantity, p.unit_type, p.expiry_date, p.is_quick, p.active,
               p.category_id, cat.name as category_name, cat.color as category_color
        FROM products p LEFT JOIN categories cat ON cat.id = p.category_id
        WHERE p.active = 1"""
    params = []
    if query:
        q = normalize_text(query)
        sql += " AND (p.name_norm LIKE ? OR p.barcode LIKE ?)"
        params += [f"%{q}%", f"%{query}%"]
    if category_id:
        sql += " AND p.category_id = ?"
        params.append(category_id)
    sql += " ORDER BY p.name LIMIT ?"
    params.append(limit)
    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def get_product(product_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT p.*, cat.name as category_name FROM products p LEFT JOIN categories cat ON cat.id=p.category_id WHERE p.id=?", (product_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def find_by_barcode(barcode):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE barcode=? AND active=1", (barcode,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def save_product(name, barcode, sell_price_eur, purchase_price_eur, quantity, unit_type, expiry_date, category_id, is_quick=False, product_id=None):
    conn = get_connection()
    c = conn.cursor()
    name_norm = normalize_text(name)
    try:
        if product_id is None:
            c.execute("INSERT INTO products (name,name_norm,barcode,price,purchase_price,quantity,unit_type,expiry_date,category_id,is_quick) VALUES (?,?,?,?,?,?,?,?,?,?)",
                      (name,name_norm,barcode or None,sell_price_eur,purchase_price_eur,quantity,unit_type,expiry_date or None,category_id or None,int(is_quick)))
        else:
            c.execute("UPDATE products SET name=?,name_norm=?,barcode=?,price=?,purchase_price=?,quantity=?,unit_type=?,expiry_date=?,category_id=?,is_quick=? WHERE id=?",
                      (name,name_norm,barcode or None,sell_price_eur,purchase_price_eur,quantity,unit_type,expiry_date or None,category_id or None,int(is_quick),product_id))
        conn.commit()
        return True, None
    except sqlite3.IntegrityError:
        return False, "Вече има продукт с този баркод."
    finally: conn.close()

def delete_product(product_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE products SET active=0 WHERE id=?", (product_id,))
    conn.commit()
    conn.close()

def restock_product(product_id, add_qty):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE products SET quantity=quantity+? WHERE id=?", (add_qty,product_id))
    conn.commit()
    conn.close()

def fetch_quick_products():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, price, unit_type, quantity FROM products WHERE is_quick=1 AND active=1 ORDER BY name")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def fetch_expiring_products(days_ahead=3):
    start = date.today().isoformat()
    end = (date.today() + timedelta(days=days_ahead)).isoformat()
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, barcode, quantity, unit_type, expiry_date FROM products WHERE expiry_date IS NOT NULL AND TRIM(expiry_date)<>'' AND date(expiry_date) BETWEEN date(?) AND date(?) AND active=1 ORDER BY date(expiry_date), name", (start,end))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def fetch_low_stock(threshold=5):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, quantity, unit_type FROM products WHERE quantity<=? AND active=1 ORDER BY quantity ASC LIMIT 20", (threshold,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def record_waste(product_id, qty, reason=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT quantity, unit_type FROM products WHERE id=?", (product_id,))
    row = c.fetchone()
    if not row: conn.close(); return False, "Продуктът не е намерен."
    if qty > row["quantity"]: conn.close(); return False, f"Недостатъчна наличност: {row['quantity']:.3f}"
    c.execute("UPDATE products SET quantity=quantity-? WHERE id=?", (qty,product_id))
    c.execute("INSERT INTO waste (date,product_id,quantity,unit_type,reason) VALUES (?,?,?,?,?)",
              (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),product_id,qty,row["unit_type"],reason or None))
    conn.commit()
    conn.close()
    return True, None

def fetch_waste(date_from=None, date_to=None, limit=200):
    conn = get_connection()
    c = conn.cursor()
    sql = "SELECT w.id, w.date, p.name as product_name, w.quantity, w.unit_type, w.reason, p.purchase_price FROM waste w JOIN products p ON p.id=w.product_id WHERE 1=1"
    params = []
    if date_from: sql += " AND w.date >= ?"; params.append(date_from)
    if date_to: sql += " AND w.date <= ?"; params.append(date_to + " 23:59:59")
    sql += " ORDER BY w.date DESC LIMIT ?"; params.append(limit)
    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def record_sale(items, discount_percent, amount_received=None, user_id=None):
    discount_percent = max(0.0, min(100.0, discount_percent or 0.0))
    pre_total = sum(it["line_total"] for it in items)
    discount_amount = pre_total * (discount_percent / 100.0)
    final_total = pre_total - discount_amount
    factor = 1.0 - (discount_percent / 100.0)
    change_given = (amount_received - final_total) if amount_received is not None else None
    conn = get_connection()
    c = conn.cursor()
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO sales (date,user_id,total,discount_percent,discount_amount,amount_received,change_given) VALUES (?,?,?,?,?,?,?)",
              (date_str,user_id,final_total,discount_percent,discount_amount,amount_received,change_given))
    sale_id = c.lastrowid
    for it in items:
        dl = it["line_total"] * factor
        c.execute("INSERT INTO sale_items (sale_id,product_id,product_name,quantity,unit_type,unit_price,cost_price,line_total) VALUES (?,?,?,?,?,?,?,?)",
                  (sale_id,it["product_id"],it["name"],it["quantity"],it["unit_type"],it["unit_price"],it["cost_price"],dl))
        c.execute("UPDATE products SET quantity=quantity-? WHERE id=?", (it["quantity"],it["product_id"]))
    conn.commit()
    conn.close()
    return sale_id, final_total

def fetch_sales(date_from=None, date_to=None, limit=500):
    conn = get_connection()
    c = conn.cursor()
    sql = """SELECT s.id, s.date, s.total, s.discount_percent, s.discount_amount,
               s.amount_received, s.change_given, u.full_name as cashier,
               COALESCE(SUM(si.line_total - si.cost_price*si.quantity),0) as profit
        FROM sales s LEFT JOIN users u ON u.id=s.user_id
        LEFT JOIN sale_items si ON si.sale_id=s.id WHERE 1=1"""
    params = []
    if date_from: sql += " AND s.date >= ?"; params.append(date_from)
    if date_to: sql += " AND s.date <= ?"; params.append(date_to + " 23:59:59")
    sql += " GROUP BY s.id ORDER BY s.date DESC LIMIT ?"; params.append(limit)
    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def get_sale_detail(sale_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT s.id, s.date, s.total, s.discount_percent, s.discount_amount, s.amount_received, s.change_given, u.full_name as cashier FROM sales s LEFT JOIN users u ON u.id=s.user_id WHERE s.id=?", (sale_id,))
    sale = c.fetchone()
    if not sale: conn.close(); return None, []
    c.execute("SELECT si.product_name, si.quantity, si.unit_type, si.unit_price, si.cost_price, si.line_total FROM sale_items si WHERE si.sale_id=?", (sale_id,))
    items = [dict(r) for r in c.fetchall()]
    conn.close()
    return dict(sale), items

def get_dashboard_stats():
    conn = get_connection()
    c = conn.cursor()
    today = date.today().isoformat()
    month_ago = (date.today() - timedelta(days=29)).isoformat()
    def _q(sql, p=()):
        c.execute(sql, p)
        return c.fetchone()
    t = _q("SELECT COALESCE(SUM(s.total),0) as revenue, COUNT(s.id) as tx_count, COALESCE(SUM(si.line_total - si.cost_price*si.quantity),0) as profit FROM sales s LEFT JOIN sale_items si ON si.sale_id=s.id WHERE s.date LIKE ?", (today+"%",))
    m = _q("SELECT COALESCE(SUM(s.total),0) as revenue, COUNT(DISTINCT s.id) as tx_count, COALESCE(SUM(si.line_total - si.cost_price*si.quantity),0) as profit FROM sales s LEFT JOIN sale_items si ON si.sale_id=s.id WHERE s.date >= ?", (month_ago,))
    pc = _q("SELECT COUNT(*) FROM products WHERE active=1")[0]
    lsc = _q("SELECT COUNT(*) FROM products WHERE quantity<=5 AND active=1")[0]
    ec = _q("SELECT COUNT(*) FROM products WHERE expiry_date IS NOT NULL AND TRIM(expiry_date)<>'' AND date(expiry_date) BETWEEN date('now') AND date('now','+3 days') AND active=1")[0]
    c.execute("SELECT DATE(s.date) as day, COALESCE(SUM(s.total),0) as revenue, COALESCE(SUM(si.line_total - si.cost_price*si.quantity),0) as profit FROM sales s LEFT JOIN sale_items si ON si.sale_id=s.id WHERE s.date >= ? GROUP BY day ORDER BY day", (month_ago,))
    chart_data = [dict(r) for r in c.fetchall()]
    c.execute("SELECT si.product_name, SUM(si.quantity) as total_qty, SUM(si.line_total) as total_revenue FROM sale_items si JOIN sales s ON s.id=si.sale_id WHERE s.date >= ? GROUP BY si.product_name ORDER BY total_revenue DESC LIMIT 5", (month_ago,))
    top_sellers = [dict(r) for r in c.fetchall()]
    c.execute("SELECT p.name, COALESCE(SUM(si.quantity),0) as sold_qty, p.quantity as stock FROM products p LEFT JOIN sale_items si ON si.product_id=p.id LEFT JOIN sales s ON s.id=si.sale_id AND s.date >= ? WHERE p.active=1 GROUP BY p.id ORDER BY sold_qty ASC, p.quantity DESC LIMIT 5", (month_ago,))
    slow_movers = [dict(r) for r in c.fetchall()]
    conn.close()
    return {
        "today_revenue": t["revenue"], "today_profit": t["profit"], "today_tx": t["tx_count"],
        "month_revenue": m["revenue"], "month_profit": m["profit"], "month_tx": m["tx_count"],
        "product_count": pc, "low_stock_count": lsc, "expiring_count": ec,
        "chart_data": chart_data, "top_sellers": top_sellers, "slow_movers": slow_movers,
    }

EUR_RATE = EUR_RATE
