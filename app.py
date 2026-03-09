"""Freshorino — Neo-Grocer POS & Store Management | Flask + SQLite"""
import sqlite3, json, hashlib, os
from datetime import datetime, date, timedelta
from functools import wraps
from flask import (Flask, render_template, request, jsonify,
                   redirect, url_for, session, g)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "freshorino-secret-2025")
DB_PATH  = os.path.join(os.path.dirname(__file__), "store.db")
EUR_RATE = 1.95583

# ── DB helpers ──────────────────────────────────────────────
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA busy_timeout=3000")
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db

@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop("db", None)
    if db: db.close()

def qry(sql, p=(), one=False):
    c = get_db().execute(sql, p)
    return c.fetchone() if one else c.fetchall()

def exe(sql, p=()):
    db = get_db()
    c = db.execute(sql, p)
    db.commit()
    return c

def eur_bgn(e):
    try: return round(float(e)*EUR_RATE,2)
    except: return 0.0

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    c = db.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'staff', full_name TEXT,
            created_at TEXT NOT NULL DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE, color TEXT NOT NULL DEFAULT '#7CB518',
            icon TEXT DEFAULT '🥦');
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, name_norm TEXT, barcode TEXT UNIQUE,
            category_id INTEGER REFERENCES categories(id),
            price REAL NOT NULL, purchase_price REAL NOT NULL DEFAULT 0,
            quantity REAL NOT NULL DEFAULT 0,
            unit_type TEXT NOT NULL DEFAULT 'piece', expiry_date TEXT,
            created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS sales(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL DEFAULT(datetime('now')),
            total REAL NOT NULL, discount_percent REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            user_id INTEGER REFERENCES users(id), notes TEXT);
        CREATE TABLE IF NOT EXISTS sale_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id INTEGER NOT NULL REFERENCES sales(id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            product_name TEXT, quantity REAL NOT NULL, unit_type TEXT NOT NULL,
            unit_price REAL NOT NULL, cost_price REAL NOT NULL DEFAULT 0,
            line_total REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS waste(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL DEFAULT(datetime('now')),
            product_id INTEGER NOT NULL REFERENCES products(id),
            quantity REAL NOT NULL, unit_type TEXT NOT NULL,
            reason TEXT, user_id INTEGER REFERENCES users(id));
        CREATE INDEX IF NOT EXISTS idx_pnorm  ON products(name_norm);
        CREATE INDEX IF NOT EXISTS idx_sidate ON sales(date);
    """)
    alters=[
        "ALTER TABLE products   ADD COLUMN name_norm TEXT",
        "ALTER TABLE products   ADD COLUMN purchase_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE products   ADD COLUMN unit_type TEXT NOT NULL DEFAULT 'piece'",
        "ALTER TABLE products   ADD COLUMN expiry_date TEXT",
        "ALTER TABLE products   ADD COLUMN category_id INTEGER",
        "ALTER TABLE products   ADD COLUMN created_at TEXT",
        "ALTER TABLE sales      ADD COLUMN discount_percent REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sales      ADD COLUMN discount_amount  REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sales      ADD COLUMN user_id INTEGER",
        "ALTER TABLE sales      ADD COLUMN notes TEXT",
        "ALTER TABLE sale_items ADD COLUMN cost_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE sale_items ADD COLUMN product_name TEXT",
        "ALTER TABLE waste      ADD COLUMN user_id INTEGER",
    ]
    for s in alters:
        try: c.execute(s)
        except: pass
    rows=c.execute("SELECT id,name FROM products WHERE name_norm IS NULL").fetchall()
    for pid,name in rows:
        c.execute("UPDATE products SET name_norm=? WHERE id=?",(name.strip().casefold(),pid))
    c.execute("""UPDATE sale_items SET product_name=(
        SELECT name FROM products WHERE products.id=sale_items.product_id)
        WHERE product_name IS NULL""")
    # Check BOTH possible key names (old DBs used currency_migrated_to_eur)
    already_migrated = (
        c.execute("SELECT value FROM settings WHERE key='eur_migrated'").fetchone() or
        c.execute("SELECT value FROM settings WHERE key='currency_migrated_to_eur'").fetchone()
    )
    if not already_migrated:
        r=EUR_RATE
        c.execute("UPDATE products SET price=price/?,purchase_price=purchase_price/?",(r,r))
        c.execute("UPDATE sales SET total=total/?,discount_amount=discount_amount/?",(r,r))
        c.execute("UPDATE sale_items SET unit_price=unit_price/?,cost_price=cost_price/?,line_total=line_total/?",(r,r,r))
    c.execute("INSERT OR REPLACE INTO settings VALUES('eur_migrated','1')")
    c.execute("INSERT OR REPLACE INTO settings VALUES('currency_migrated_to_eur','1')")
    if not c.execute("SELECT 1 FROM users LIMIT 1").fetchone():
        c.execute("INSERT INTO users(username,password_hash,role,full_name) VALUES(?,?,?,?)",
                  ("admin",hash_pw("admin123"),"admin","Administrator"))
    cats = [("Плодове", "#E8502A", "🍎"), ("Зеленчуци", "#7CB518", "🥦"), ("Млечни", "#F5C842", "🧀"),
            ("Месо", "#C0392B", "🥩"), ("Хляб", "#D4A04A", "🍞"), ("Напитки", "#2980B9", "🥤"),
            ("Консерви", "#7F8C8D", "🥫"), ("Разни", "#8E44AD", "📦"),
            ("Захарни изделия", "#E91E8C", "🍰"), ("Ядки", "#8B6914", "🥜"),
            ("Солени изделия", "#FF6B35", "🧂"), ("Зърнени култури", "#C8A951", "🌾"),
            ("Сандвичи", "#F5A623", "🥪")]
    for n,col,ic in cats:
        c.execute("INSERT OR IGNORE INTO categories(name,color,icon) VALUES(?,?,?)",(n,col,ic))
    db.commit(); db.close()
    print("✅ DB ready")

# ── Auth ────────────────────────────────────────────────────
def login_req(f):
    @wraps(f)
    def d(*a,**k):
        if "uid" not in session: return redirect(url_for("login",next=request.path))
        return f(*a,**k)
    return d

def cur_user():
    return {"id":session.get("uid"),"username":session.get("username"),
            "role":session.get("role"),"full_name":session.get("full_name")}

# ── Pages ───────────────────────────────────────────────────
@app.route("/")
@login_req
def index(): return redirect(url_for("dashboard"))

@app.route("/login",methods=["GET","POST"])
def login():
    err=None
    if request.method=="POST":
        d=request.get_json(silent=True) or request.form
        u=(d.get("username") or "").strip(); pw=d.get("password") or ""
        row=qry("SELECT * FROM users WHERE username=?",(u,),one=True)
        if row and row["password_hash"]==hash_pw(pw):
            session.clear()
            session.update({"uid":row["id"],"username":row["username"],
                            "role":row["role"],"full_name":row["full_name"] or row["username"]})
            if request.is_json: return jsonify({"ok":True,"redirect":url_for("dashboard")})
            return redirect(request.args.get("next") or url_for("dashboard"))
        err="Невалидни данни за вход."
        if request.is_json: return jsonify({"ok":False,"error":err}),401
    return render_template("login.html",error=err)

@app.route("/logout")
def logout(): session.clear(); return redirect(url_for("login"))

@app.route("/dashboard")
@login_req
def dashboard(): return render_template("dashboard.html",user=cur_user())

@app.route("/pos")
@login_req
def pos():
    cats=qry("SELECT * FROM categories ORDER BY name")
    return render_template("pos.html",user=cur_user(),categories=[dict(c) for c in cats])

@app.route("/products")
@login_req
def products():
    cats=qry("SELECT * FROM categories ORDER BY name")
    return render_template("products.html",user=cur_user(),categories=[dict(c) for c in cats])

@app.route("/reports")
@login_req
def reports(): return render_template("reports.html",user=cur_user())

@app.route("/inventory")
@login_req
def inventory(): return render_template("inventory.html",user=cur_user())

@app.route("/waste")
@login_req
def waste_page(): return render_template("waste.html",user=cur_user())

@app.route("/settings")
@login_req
def settings():
    if session.get("role")!="admin": return redirect(url_for("dashboard"))
    users=qry("SELECT id,username,full_name,role,created_at FROM users ORDER BY id")
    return render_template("settings.html",user=cur_user(),users=[dict(u) for u in users])

# ── API: Products ────────────────────────────────────────────
def prod_dict(p):
    price=p["price"] or 0; pp=p["purchase_price"] or 0
    return {"id":p["id"],"name":p["name"],"barcode":p["barcode"],
            "category_id":p["category_id"],
            "category_name":p["category_name"] if "category_name" in p.keys() else None,
            "category_color":p["category_color"] if "category_color" in p.keys() else None,
            "price":round(price,4),"price_bgn":eur_bgn(price),
            "purchase_price":round(pp,4),"purchase_price_bgn":eur_bgn(pp),
            "quantity":p["quantity"],"unit_type":p["unit_type"],"expiry_date":p["expiry_date"],
            "margin_pct":round((price-pp)/price*100 if price>0 else 0,1)}

@app.route("/api/products")
@login_req
def api_products():
    q=request.args.get("q","").strip().casefold()
    cat=request.args.get("category","")
    lim=min(int(request.args.get("limit",1000)),2000)
    sort_by=request.args.get("sort","name")
    sort_dir=request.args.get("dir","asc").upper()
    if sort_dir not in ("ASC","DESC"): sort_dir="ASC"
    sort_map={"name":"p.name","price":"p.price","purchase_price":"p.purchase_price",
              "quantity":"p.quantity","expiry_date":"p.expiry_date",
              "margin":"(p.price-p.purchase_price)/NULLIF(p.price,0)",
              "markup":"(p.price-p.purchase_price)/NULLIF(p.purchase_price,0)",
              "category":"c.name"}
    order_sql=sort_map.get(sort_by,"p.name")
    where,params=["1=1"],[]
    if q:
        where.append("(p.name_norm LIKE ? OR COALESCE(p.barcode,'') LIKE ?)")
        params+=[f"%{q}%",f"%{q}%"]
    if cat: where.append("p.category_id=?"); params.append(int(cat))
    rows=qry(f"""SELECT p.*,c.name AS category_name,c.color AS category_color
        FROM products p LEFT JOIN categories c ON c.id=p.category_id
        WHERE {' AND '.join(where)} ORDER BY {order_sql} {sort_dir} LIMIT ?""",params+[lim])
    return jsonify([prod_dict(r) for r in rows])

@app.route("/api/products/<int:pid>")
@login_req
def api_product_get(pid):
    r=qry("SELECT p.*,c.name AS category_name,c.color AS category_color FROM products p LEFT JOIN categories c ON c.id=p.category_id WHERE p.id=?",(pid,),one=True)
    return jsonify(prod_dict(r)) if r else (jsonify({"error":"Not found"}),404)

@app.route("/api/products",methods=["POST"])
@login_req
def api_product_create():
    d=request.get_json(); name=(d.get("name") or "").strip()
    if not name: return jsonify({"error":"Името е задължително."}),400
    try:
        c=exe("INSERT INTO products(name,name_norm,barcode,category_id,price,purchase_price,quantity,unit_type,expiry_date) VALUES(?,?,?,?,?,?,?,?,?)",
              (name,name.casefold(),d.get("barcode") or None,d.get("category_id") or None,
               float(d.get("price") or 0),float(d.get("purchase_price") or 0),
               float(d.get("quantity") or 0),d.get("unit_type") or "piece",d.get("expiry_date") or None))
        return jsonify({"ok":True,"id":c.lastrowid})
    except sqlite3.IntegrityError: return jsonify({"error":"Баркодът вече съществува."}),409

@app.route("/api/products/<int:pid>",methods=["PUT"])
@login_req
def api_product_update(pid):
    d=request.get_json(); name=(d.get("name") or "").strip()
    if not name: return jsonify({"error":"Името е задължително."}),400
    try:
        exe("UPDATE products SET name=?,name_norm=?,barcode=?,category_id=?,price=?,purchase_price=?,quantity=?,unit_type=?,expiry_date=? WHERE id=?",
            (name,name.casefold(),d.get("barcode") or None,d.get("category_id") or None,
             float(d.get("price") or 0),float(d.get("purchase_price") or 0),
             float(d.get("quantity") or 0),d.get("unit_type") or "piece",d.get("expiry_date") or None,pid))
        return jsonify({"ok":True})
    except sqlite3.IntegrityError: return jsonify({"error":"Баркодът вече съществува."}),409

@app.route("/api/products/<int:pid>",methods=["DELETE"])
@login_req
def api_product_delete(pid):
    exe("DELETE FROM products WHERE id=?",(pid,)); return jsonify({"ok":True})

@app.route("/api/products/<int:pid>/restock",methods=["POST"])
@login_req
def api_restock(pid):
    d=request.get_json(); qty=float(d.get("quantity") or 0)
    if qty<=0: return jsonify({"error":"qty>0"}),400
    exe("UPDATE products SET quantity=quantity+? WHERE id=?",(qty,pid))
    r=qry("SELECT quantity FROM products WHERE id=?",(pid,),one=True)
    return jsonify({"ok":True,"new_quantity":r["quantity"]})

@app.route("/api/categories")
@login_req
def api_categories():
    rows=qry("SELECT * FROM categories ORDER BY name")
    return jsonify([dict(r) for r in rows])

# ── API: Sales ───────────────────────────────────────────────
@app.route("/api/sales",methods=["POST"])
@login_req
def api_sale_create():
    d=request.get_json()
    items=d.get("items",[])
    disc=max(0.0,min(100.0,float(d.get("discount_percent") or 0)))
    recv=float(d.get("received_eur") or 0)
    uid=session.get("uid")
    if not items: return jsonify({"error":"Не са добавени артикули."}),400
    pre=sum(float(i["line_total"]) for i in items)
    disc_amt=pre*(disc/100); final=pre-disc_amt; factor=1.0-(disc/100)
    change=max(0.0,recv-final) if recv else 0.0
    db=get_db(); ds=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c=db.execute("INSERT INTO sales(date,total,discount_percent,discount_amount,user_id) VALUES(?,?,?,?,?)",
                 (ds,final,disc,disc_amt,uid))
    sid=c.lastrowid
    for it in items:
        p=db.execute("SELECT quantity,unit_type,name FROM products WHERE id=?",(it["product_id"],)).fetchone()
        if not p: db.rollback(); return jsonify({"error":"Продуктът не е намерен."}),404
        if float(it["quantity"])>p["quantity"]: db.rollback(); return jsonify({"error":f"Недостатъчна наличност: {p['name']}"}),409
        db.execute("INSERT INTO sale_items(sale_id,product_id,product_name,quantity,unit_type,unit_price,cost_price,line_total) VALUES(?,?,?,?,?,?,?,?)",
                   (sid,it["product_id"],p["name"],float(it["quantity"]),it["unit_type"],
                    float(it["unit_price"]),float(it.get("cost_price") or 0),float(it["line_total"])*factor))
        db.execute("UPDATE products SET quantity=quantity-? WHERE id=?",(float(it["quantity"]),it["product_id"]))
    db.commit()
    return jsonify({"ok":True,"sale_id":sid,"total":round(final,2),"total_bgn":eur_bgn(final),
                    "change":round(change,2),"change_bgn":eur_bgn(change)})

@app.route("/api/sales")
@login_req
def api_sales_list():
    df=request.args.get("from",""); dt=request.args.get("to","")
    page=max(1,int(request.args.get("page",1))); per=2000  # load all sales
    where,params=["1=1"],[]
    if df: where.append("s.date>=?"); params.append(df+" 00:00:00")
    if dt: where.append("s.date<=?"); params.append(dt+" 23:59:59")
    rows=qry(f"""SELECT s.id,s.date,s.total,s.discount_percent,s.discount_amount,u.username,
               COALESCE(SUM(si.line_total-si.cost_price*si.quantity),0) AS profit
        FROM sales s LEFT JOIN users u ON u.id=s.user_id
        LEFT JOIN sale_items si ON si.sale_id=s.id
        WHERE {' AND '.join(where)} GROUP BY s.id ORDER BY s.date DESC LIMIT ? OFFSET ?""",
        params+[per,(page-1)*per])
    return jsonify([{"id":r["id"],"date":r["date"],"total":round(r["total"],2),
                     "total_bgn":eur_bgn(r["total"]),"discount_percent":r["discount_percent"],
                     "discount_amount":round(r["discount_amount"],2),
                     "profit":round(r["profit"],2),"profit_bgn":eur_bgn(r["profit"]),
                     "username":r["username"]} for r in rows])

@app.route("/api/sales/<int:sid>/items")
@login_req
def api_sale_items(sid):
    rows=qry("SELECT * FROM sale_items WHERE sale_id=?",(sid,))
    return jsonify([{"product_id":r["product_id"],"product_name":r["product_name"],
                     "quantity":r["quantity"],"unit_type":r["unit_type"],
                     "unit_price":round(r["unit_price"],4),"unit_price_bgn":eur_bgn(r["unit_price"]),
                     "line_total":round(r["line_total"],2),"line_total_bgn":eur_bgn(r["line_total"]),
                     "profit":round(r["line_total"]-r["cost_price"]*r["quantity"],2)} for r in rows])

# ── API: Waste ───────────────────────────────────────────────
@app.route("/api/waste",methods=["POST"])
@login_req
def api_waste_create():
    d=request.get_json(); pid=int(d.get("product_id") or 0)
    qty=float(d.get("quantity") or 0)
    if qty<=0: return jsonify({"error":"qty>0"}),400
    p=qry("SELECT * FROM products WHERE id=?",(pid,),one=True)
    if not p: return jsonify({"error":"Не е намерен."}),404
    if qty>p["quantity"]: return jsonify({"error":f"Наличност: {p['quantity']:.3f}"}),409
    exe("UPDATE products SET quantity=quantity-? WHERE id=?",(qty,pid))
    exe("INSERT INTO waste(product_id,quantity,unit_type,reason,user_id) VALUES(?,?,?,?,?)",
        (pid,qty,p["unit_type"],(d.get("reason") or "").strip() or None,session.get("uid")))
    return jsonify({"ok":True})

@app.route("/api/waste")
@login_req
def api_waste_list():
    rows=qry("SELECT w.*,p.name AS pname FROM waste w LEFT JOIN products p ON p.id=w.product_id ORDER BY w.date DESC LIMIT 200")
    return jsonify([{"id":r["id"],"date":r["date"],"product_name":r["pname"],
                     "quantity":r["quantity"],"unit_type":r["unit_type"],"reason":r["reason"]} for r in rows])

# ── API: Dashboard ────────────────────────────────────────────
@app.route("/api/dashboard/stats")
@login_req
def api_stats():
    def ps(fr, to=None):
        to_cond = f"AND s.date <= '{to} 23:59:59'" if to else ""
        r = qry(f"""SELECT
            (SELECT COALESCE(SUM(total),0) FROM sales
             WHERE date >= '{fr} 00:00:00' {to_cond}) AS rev,
            (SELECT COALESCE(SUM(si.line_total - si.cost_price*si.quantity),0)
             FROM sale_items si JOIN sales s ON s.id=si.sale_id
             WHERE s.date >= '{fr} 00:00:00' {to_cond}) AS profit,
            (SELECT COUNT(*) FROM sales
             WHERE date >= '{fr} 00:00:00' {to_cond}) AS tx""", one=True)
        rev = r["rev"] or 0; prof = r["profit"] or 0
        return {"revenue":round(rev,2),"profit":round(prof,2),"tx_count":r["tx"] or 0,
                "revenue_bgn":eur_bgn(rev),"profit_bgn":eur_bgn(prof)}
    today = date.today().isoformat()
    inv=qry("SELECT COALESCE(SUM(price*quantity),0) AS sv,COALESCE(SUM(purchase_price*quantity),0) AS cv,COUNT(*) AS pc,COALESCE(SUM(CASE WHEN quantity<=0 THEN 1 ELSE 0 END),0) AS oos FROM products",one=True)
    exp=qry("SELECT COUNT(*) AS c FROM products WHERE expiry_date IS NOT NULL AND TRIM(expiry_date)!='' AND date(expiry_date) BETWEEN date('now') AND date('now','+3 days')",one=True)["c"]
    return jsonify({"today":ps(today),
                    "week":ps((date.today()-timedelta(days=7)).isoformat()),
                    "month":ps((date.today()-timedelta(days=30)).isoformat()),
                    "inventory":{"sell_value":round(inv["sv"],2),"cost_value":round(inv["cv"],2),
                                 "product_count":inv["pc"],"out_of_stock":inv["oos"],"expiring_soon":exp}})

@app.route("/api/dashboard/charts")
@login_req
def api_charts():
    days=int(request.args.get("days",30))
    date_from=request.args.get("from","")
    date_to=request.args.get("to","")
    if date_from and date_to:
        date_cond=f"s.date >= '{date_from} 00:00:00' AND s.date <= '{date_to} 23:59:59'"
        si_cond=f"s.date >= '{date_from} 00:00:00' AND s.date <= '{date_to} 23:59:59'"
    else:
        date_cond=f"s.date>=date('now','-{days} days')"
        si_cond=f"s.date>=date('now','-{days} days')"
    # Revenue: SUM(sales.total) per day — no JOIN to avoid multiplication
    # date_cond_plain = without 's.' alias (for direct sales table query)
    date_cond_plain = date_cond.replace('s.date','date')
    daily_rev=qry(f"""SELECT date(date) AS day, COALESCE(SUM(total),0) AS rev,
        COUNT(*) AS tx_count FROM sales WHERE {date_cond_plain} GROUP BY day ORDER BY day""")
    daily_profit=qry(f"""SELECT date(s.date) AS day,
        COALESCE(SUM(si.line_total-si.cost_price*si.quantity),0) AS profit
        FROM sale_items si JOIN sales s ON s.id=si.sale_id
        WHERE {si_cond} GROUP BY day ORDER BY day""")
    profit_by_day={r["day"]:r["profit"] for r in daily_profit}
    daily=[{"day":r["day"],"revenue":round(r["rev"],2),"revenue_bgn":eur_bgn(r["rev"]),
            "profit":round(profit_by_day.get(r["day"],0),2),
            "tx_count":r["tx_count"]} for r in daily_rev]
    best=qry(f"""SELECT si.product_name AS name,SUM(si.quantity) AS tq,SUM(si.line_total) AS tr
        FROM sale_items si JOIN sales s ON s.id=si.sale_id
        WHERE {si_cond} GROUP BY si.product_id ORDER BY tq DESC LIMIT 5""")
    slow=qry(f"""SELECT p.name,p.quantity,COALESCE(SUM(si.quantity),0) AS s30
        FROM products p LEFT JOIN sale_items si ON si.product_id=p.id
            AND si.sale_id IN(SELECT id FROM sales WHERE {date_cond_plain})
        WHERE p.quantity>0 GROUP BY p.id ORDER BY s30 ASC,p.quantity DESC LIMIT 5""")
    catrev=qry(f"""SELECT COALESCE(c.name,'Без категория') AS cat,COALESCE(c.color,'#888') AS col,SUM(si.line_total) AS rev
        FROM sale_items si JOIN products p ON p.id=si.product_id
        LEFT JOIN categories c ON c.id=p.category_id
        JOIN sales s ON s.id=si.sale_id
        WHERE {si_cond} GROUP BY p.category_id ORDER BY rev DESC""")
    return jsonify({
        "daily":daily,
        "best_sellers":[{"name":r["name"],"qty":round(r["tq"],2),"revenue":round(r["tr"],2)} for r in best],
        "slow_movers":[{"name":r["name"],"stock":round(r["quantity"],2),"sold_30d":round(r["s30"],2)} for r in slow],
        "category_revenue":[{"category":r["cat"],"color":r["col"],"revenue":round(r["rev"],2)} for r in catrev],
    })

@app.route("/api/dashboard/hourly")
@login_req
def api_hourly():
    date_from=request.args.get("from","")
    date_to=request.args.get("to","")
    days=int(request.args.get("days",30))
    if date_from and date_to:
        cond=f"date >= '{date_from} 00:00:00' AND date <= '{date_to} 23:59:59'"
    else:
        cond=f"date>=date('now','-{days} days')"
    rows=qry(f"""SELECT CAST(strftime('%H',date) AS INTEGER) AS hour,
        COUNT(*) AS tx_count, COALESCE(SUM(total),0) AS revenue
        FROM sales WHERE {cond}
        GROUP BY hour ORDER BY hour""")
    # Fill all 24 hours
    by_hour={r["hour"]:{"tx_count":r["tx_count"],"revenue":round(r["revenue"],2)} for r in rows}
    result=[{"hour":h,"label":f"{h:02d}:00",
             "tx_count":by_hour.get(h,{}).get("tx_count",0),
             "revenue":by_hour.get(h,{}).get("revenue",0)} for h in range(24)]
    return jsonify(result)

@app.route("/api/expiring")
@login_req
def api_expiring():
    days=int(request.args.get("days",3))
    rows=qry(f"""SELECT id,name,barcode,quantity,unit_type,expiry_date FROM products
        WHERE expiry_date IS NOT NULL AND TRIM(expiry_date)!=''
        AND date(expiry_date) BETWEEN date('now') AND date('now','+{days} days')
        ORDER BY date(expiry_date),name""")
    return jsonify([dict(r) for r in rows])

@app.route("/api/change-password", methods=["POST"])
@login_req
def api_change_password():
    d = request.get_json()
    uid = session.get("uid")
    user = qry("SELECT * FROM users WHERE id=?", (uid,), one=True)
    if user["password_hash"] != hash_pw(d.get("old_password",'')):
        return jsonify({"error": "Грешна текуща парола."}), 401
    new_pw = (d.get("new_password") or "").strip()
    if len(new_pw) < 4:
        return jsonify({"error": "Паролата трябва да е поне 4 символа."}), 400
    exe("UPDATE users SET password_hash=? WHERE id=?", (hash_pw(new_pw), uid))
    return jsonify({"ok": True})

@app.route("/api/users",methods=["POST"])
@login_req
def api_user_create():
    if session.get("role")!="admin": return jsonify({"error":"Forbidden"}),403
    d=request.get_json(); u=(d.get("username") or "").strip(); pw=d.get("password") or ""
    if not u or not pw: return jsonify({"error":"Задължителни полета."}),400
    try:
        exe("INSERT INTO users(username,password_hash,role,full_name) VALUES(?,?,?,?)",
            (u,hash_pw(pw),d.get("role") or "staff",d.get("full_name") or u))
        return jsonify({"ok":True})
    except sqlite3.IntegrityError: return jsonify({"error":"Потребителят съществува."}),409

@app.route("/api/users/<int:uid>",methods=["DELETE"])
@login_req
def api_user_delete(uid):
    if session.get("role")!="admin": return jsonify({"error":"Forbidden"}),403
    if uid==session.get("uid"): return jsonify({"error":"Не можеш да изтриеш себе си."}),400
    exe("DELETE FROM users WHERE id=?",(uid,)); return jsonify({"ok":True})

@app.context_processor
def ctx(): return {"eur_rate":EUR_RATE,"now":datetime.now()}

if __name__=="__main__":
    init_db()
    app.run(debug=True,host="0.0.0.0",port=5000)
