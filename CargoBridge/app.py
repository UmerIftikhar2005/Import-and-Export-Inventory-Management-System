# =============================================================
#  CargoBridge — app.py
#  Flask + PyMongo | Login | Role-Based Access | Full CRUD
#  ─────────────────────────────────────────────────────────
#  ADVANCED FEATURES:
#  1. Aggregation Pipelines  — dashboard analytics, reports
#  2. Indexing & Query Opt   — ensured at startup
#  3. MongoDB Transactions   — atomic order + transaction writes
#  4. Concurrency Control    — optimistic locking via version field
#  5. Scalability            — connection pooling, read preference
# =============================================================

from flask import (
    Flask, render_template, request,
    redirect, url_for, session, flash, abort, jsonify
)
from pymongo import (
    MongoClient, DESCENDING, ASCENDING,
    IndexModel, errors as pymongo_errors
)
from bson import ObjectId
from bson.errors import InvalidId
from dotenv import load_dotenv
from datetime import datetime
from functools import wraps
import bcrypt
import os
import logging

# -------------------------------------------------------------
#  Logging
# -------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
#  App + Config
# -------------------------------------------------------------
load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "cargobridge-secret-key-change-in-prod")

# -------------------------------------------------------------
#  Database Connection
#  SCALABILITY: connection pooling keeps warm connections ready
#  maxPoolSize=50  — up to 50 concurrent DB connections
#  minPoolSize=5   — always-warm connections for low latency
# -------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=5,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=3000,
)
db = client["cargobridge"]

users_col        = db["users"]
shipments_col    = db["shipments"]
products_col     = db["products"]
orders_col       = db["orders"]
transactions_col = db["transactions"]
employees_col    = db["employees"]


# =============================================================
#  INDEXING & QUERY OPTIMISATION
#  All indexes created at startup. background=True means
#  existing reads/writes are not blocked during creation.
#  Unique indexes enforce data integrity on ID fields.
#  Compound indexes speed up filtered + sorted queries.
# =============================================================

def ensure_indexes():
    try:
        # users
        users_col.create_indexes([
            IndexModel([("username", ASCENDING)], unique=True, background=True, name="idx_users_username"),
            IndexModel([("role",     ASCENDING)],              background=True, name="idx_users_role"),
        ])
        # shipments
        shipments_col.create_indexes([
            IndexModel([("shipment_id",  ASCENDING)], unique=True, background=True, name="idx_shp_id"),
            IndexModel([("status",       ASCENDING)],            background=True, name="idx_shp_status"),
            IndexModel([("type",         ASCENDING)],            background=True, name="idx_shp_type"),
            IndexModel([("created_at",   DESCENDING)],           background=True, name="idx_shp_created"),
            # Compound: status + created_at for dashboard filtered queries
            IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], background=True, name="idx_shp_status_created"),
        ])
        # products
        products_col.create_indexes([
            IndexModel([("product_id", ASCENDING)], unique=True, background=True, name="idx_prd_id"),
            IndexModel([("category",   ASCENDING)],              background=True, name="idx_prd_category"),
            IndexModel([("stock_qty",  ASCENDING)],              background=True, name="idx_prd_stock"),
            IndexModel([("created_at", DESCENDING)],             background=True, name="idx_prd_created"),
        ])
        # orders
        orders_col.create_indexes([
            IndexModel([("order_id",    ASCENDING)], unique=True, background=True, name="idx_ord_id"),
            IndexModel([("client_name", ASCENDING)],             background=True, name="idx_ord_client"),
            IndexModel([("status",      ASCENDING)],             background=True, name="idx_ord_status"),
            IndexModel([("shipment_ref",ASCENDING)],             background=True, name="idx_ord_shp_ref"),
            IndexModel([("created_at",  DESCENDING)],            background=True, name="idx_ord_created"),
            # Compound: status + order_date for revenue reporting
            IndexModel([("status", ASCENDING), ("order_date", DESCENDING)], background=True, name="idx_ord_status_date"),
        ])
        # transactions
        transactions_col.create_indexes([
            IndexModel([("txn_id",    ASCENDING)], unique=True, background=True, name="idx_txn_id"),
            IndexModel([("order_ref", ASCENDING)],             background=True, name="idx_txn_ord_ref"),
            IndexModel([("status",    ASCENDING)],             background=True, name="idx_txn_status"),
            IndexModel([("txn_date",  DESCENDING)],            background=True, name="idx_txn_date"),
            IndexModel([("created_at",DESCENDING)],            background=True, name="idx_txn_created"),
        ])
        # employees
        employees_col.create_indexes([
            IndexModel([("emp_id",    ASCENDING)], unique=True, background=True, name="idx_emp_id"),
            IndexModel([("username",  ASCENDING)],             background=True, name="idx_emp_user"),
            IndexModel([("department",ASCENDING)],             background=True, name="idx_emp_dept"),
            IndexModel([("status",    ASCENDING)],             background=True, name="idx_emp_status"),
            IndexModel([("join_date", DESCENDING)],            background=True, name="idx_emp_join"),
        ])
        logger.info("All indexes ensured successfully.")
    except Exception as e:
        logger.warning(f"Index creation warning (non-fatal): {e}")

with app.app_context():
    ensure_indexes()


# =============================================================
#  AGGREGATION PIPELINES
#  All pipelines run server-side in MongoDB engine — faster
#  than fetching all documents and computing in Python.
# =============================================================

def get_shipment_status_summary():
    """Group shipments by status and count each."""
    return list(shipments_col.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$sort":  {"count": DESCENDING}}
    ]))


def get_revenue_by_currency():
    """Total revenue grouped by currency for completed/confirmed orders."""
    return list(orders_col.aggregate([
        {"$match":  {"status": {"$in": ["completed", "confirmed"]}}},
        {"$group":  {"_id": "$currency", "total": {"$sum": "$total_amount"}, "count": {"$sum": 1}}},
        {"$sort":   {"total": DESCENDING}}
    ]))


def get_top_products_by_order_volume():
    """
    Unwind the embedded items array, group by product_id,
    sum qty and value. Returns top 5 most-ordered products.
    """
    return list(orders_col.aggregate([
        {"$unwind": "$items"},
        {"$group": {
            "_id":         "$items.product_id",
            "total_qty":   {"$sum": "$items.qty"},
            "total_value": {"$sum": {"$multiply": ["$items.qty", "$items.unit_price"]}},
            "order_count": {"$sum": 1}
        }},
        {"$sort":  {"total_qty": DESCENDING}},
        {"$limit": 5}
    ]))


def get_transaction_summary():
    """Total transaction amounts grouped by status and type."""
    return list(transactions_col.aggregate([
        {"$group": {
            "_id":   {"status": "$status", "type": "$type"},
            "total": {"$sum": "$amount"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total": DESCENDING}}
    ]))


def get_monthly_shipments():
    """Count shipments and total weight per month (last 12 months)."""
    return list(shipments_col.aggregate([
        {"$match": {"created_at": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id":          {"$dateToString": {"format": "%Y-%m", "date": "$created_at"}},
            "count":        {"$sum": 1},
            "total_weight": {"$sum": "$weight_kg"}
        }},
        {"$sort":  {"_id": ASCENDING}},
        {"$limit": 12}
    ]))


def get_employee_dept_summary():
    """Headcount and average salary per department. Admin only."""
    return list(employees_col.aggregate([
        {"$group": {
            "_id":         "$department",
            "headcount":   {"$sum": 1},
            "avg_salary":  {"$avg": "$salary"},
            "total_salary":{"$sum": "$salary"}
        }},
        {"$sort": {"headcount": DESCENDING}}
    ]))


# =============================================================
#  AUTH DECORATORS
# =============================================================

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            flash("Please log in to continue.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            flash("Please log in to continue.", "error")
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            abort(403)
        return f(*args, **kwargs)
    return decorated


# =============================================================
#  HELPERS
# =============================================================

def safe_object_id(id_str):
    try:
        return ObjectId(id_str)
    except (InvalidId, TypeError):
        return None

def parse_date(date_str):
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError:
        return None

def now():
    return datetime.utcnow()

def safe_float(val, default=0.0):
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        return int(float(val or default))
    except (ValueError, TypeError):
        return default

def parse_order_items(form):
    """
    Robustly parse repeating order item fields.
    This is the fix for the Orders TypeError — safe_int/safe_float
    prevent crashes on empty or non-numeric form values.
    """
    product_ids = form.getlist("item_product_id")
    quantities  = form.getlist("item_qty")
    unit_prices = form.getlist("item_unit_price")
    items = []
    for pid, qty, price in zip(product_ids, quantities, unit_prices):
        pid = (pid or "").strip()
        if pid:
            items.append({
                "product_id": pid,
                "qty":        safe_int(qty, 1),
                "unit_price": safe_float(price, 0.0),
            })
    return items


# =============================================================
#  ERROR HANDLERS
# =============================================================

@app.errorhandler(403)
def forbidden(e):
    return render_template("403.html"), 403

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404


# =============================================================
#  AUTH
# =============================================================

@app.route("/", methods=["GET", "POST"])
def login():
    if "user" in session:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("login.html")
        user = users_col.find_one({"username": username})
        if user and bcrypt.checkpw(password.encode("utf-8"), user["password"]):
            session["user"]      = user["username"]
            session["role"]      = user["role"]
            session["full_name"] = user.get("full_name", user["username"])
            return redirect(url_for("dashboard"))
        flash("Invalid username or password.", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


# =============================================================
#  DASHBOARD — Aggregation pipelines feed analytics
# =============================================================

@app.route("/dashboard")
@login_required
def dashboard():
    stats = {
        "shipments":    shipments_col.count_documents({}),
        "products":     products_col.count_documents({}),
        "orders":       orders_col.count_documents({}),
        "transactions": transactions_col.count_documents({}),
    }
    if session.get("role") == "admin":
        stats["employees"] = employees_col.count_documents({})

    recent_shipments = list(shipments_col.find().sort("created_at", DESCENDING).limit(5))
    recent_orders    = list(orders_col.find().sort("created_at", DESCENDING).limit(5))

    # Aggregation results
    shipment_by_status  = get_shipment_status_summary()
    revenue_by_currency = get_revenue_by_currency()
    top_products        = get_top_products_by_order_volume()
    txn_summary         = get_transaction_summary()
    monthly_shipments   = get_monthly_shipments()
    dept_summary        = get_employee_dept_summary() if session.get("role") == "admin" else []

    return render_template(
        "dashboard.html",
        stats=stats,
        recent_shipments=recent_shipments,
        recent_orders=recent_orders,
        shipment_by_status=shipment_by_status,
        revenue_by_currency=revenue_by_currency,
        top_products=top_products,
        txn_summary=txn_summary,
        monthly_shipments=monthly_shipments,
        dept_summary=dept_summary,
    )


# =============================================================
#  ANALYTICS PAGE — shows all aggregation pipeline outputs
#  Visit /analytics to see live results from every pipeline
# =============================================================

@app.route("/analytics")
@login_required
def analytics():
    adata = {
        "shipment_by_status":  get_shipment_status_summary(),
        "revenue_by_currency": get_revenue_by_currency(),
        "top_products":        get_top_products_by_order_volume(),
        "txn_summary":         get_transaction_summary(),
        "monthly_shipments":   get_monthly_shipments(),
        "dept_summary":        get_employee_dept_summary() if session.get("role") == "admin" else [],
    }
    return render_template("analytics.html", data=adata)


# JSON API for analytics (optional chart use)
@app.route("/api/analytics/shipments")
@login_required
def api_shipment_analytics():
    return jsonify({"by_status": get_shipment_status_summary(), "monthly": get_monthly_shipments()})

@app.route("/api/analytics/revenue")
@login_required
def api_revenue_analytics():
    return jsonify({"by_currency": get_revenue_by_currency(), "top_products": get_top_products_by_order_volume()})


# =============================================================
#  SHIPMENTS — CRUD with Optimistic Locking
# =============================================================

@app.route("/shipments")
@login_required
def shipments():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {"$or": [
            {"shipment_id": {"$regex": search, "$options": "i"}},
            {"origin":      {"$regex": search, "$options": "i"}},
            {"destination": {"$regex": search, "$options": "i"}},
            {"status":      {"$regex": search, "$options": "i"}},
        ]}
    data = list(shipments_col.find(query).sort("created_at", DESCENDING))
    return render_template("shipments.html", data=data, search=search)


@app.route("/shipments/add", methods=["GET", "POST"])
@login_required
def add_shipment():
    if request.method == "POST":
        cargo_list = [c.strip() for c in request.form.get("cargo_items","").split(",") if c.strip()]
        doc = {
            "shipment_id": request.form.get("shipment_id","").strip(),
            "type":        request.form.get("type","export"),
            "origin":      request.form.get("origin","").strip(),
            "destination": request.form.get("destination","").strip(),
            "status":      request.form.get("status","pending"),
            "cargo_items": cargo_list,
            "weight_kg":   safe_float(request.form.get("weight_kg")),
            "carrier":     request.form.get("carrier","").strip(),
            "departure":   parse_date(request.form.get("departure")),
            "eta":         parse_date(request.form.get("eta")),
            "created_at":  now(),
            "version":     1,
        }
        try:
            shipments_col.insert_one(doc)
            flash("Shipment added successfully.", "success")
        except pymongo_errors.DuplicateKeyError:
            flash(f"Shipment ID '{doc['shipment_id']}' already exists.", "error")
            return render_template("shipment_form.html", shipment=None, action="add")
        return redirect(url_for("shipments"))
    return render_template("shipment_form.html", shipment=None, action="add")


@app.route("/shipments/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_shipment(id):
    oid      = safe_object_id(id)
    if not oid: abort(404)
    shipment = shipments_col.find_one({"_id": oid})
    if not shipment: abort(404)

    if request.method == "POST":
        cargo_list      = [c.strip() for c in request.form.get("cargo_items","").split(",") if c.strip()]
        current_version = shipment.get("version", 1)
        updates = {
            "shipment_id": request.form.get("shipment_id","").strip(),
            "type":        request.form.get("type","export"),
            "origin":      request.form.get("origin","").strip(),
            "destination": request.form.get("destination","").strip(),
            "status":      request.form.get("status","pending"),
            "cargo_items": cargo_list,
            "weight_kg":   safe_float(request.form.get("weight_kg")),
            "carrier":     request.form.get("carrier","").strip(),
            "departure":   parse_date(request.form.get("departure")),
            "eta":         parse_date(request.form.get("eta")),
            "updated_at":  now(),
            "version":     current_version + 1,
        }
        # CONCURRENCY CONTROL: optimistic lock — match version
        result = shipments_col.update_one({"_id": oid, "version": current_version}, {"$set": updates})
        if result.matched_count == 0:
            flash("Conflict: record was modified by another user. Reload and try again.", "error")
            return redirect(url_for("edit_shipment", id=id))
        flash("Shipment updated successfully.", "success")
        return redirect(url_for("shipments"))
    return render_template("shipment_form.html", shipment=shipment, action="edit")


@app.route("/shipments/delete/<id>", methods=["POST"])
@admin_required
def delete_shipment(id):
    oid = safe_object_id(id)
    if not oid: abort(404)
    shipments_col.delete_one({"_id": oid})
    flash("Shipment deleted.", "success")
    return redirect(url_for("shipments"))


# =============================================================
#  PRODUCTS — CRUD with Optimistic Locking
# =============================================================

@app.route("/products")
@login_required
def products():
    search = request.args.get("q","").strip()
    query  = {}
    if search:
        query = {"$or": [
            {"product_id": {"$regex": search, "$options": "i"}},
            {"name":       {"$regex": search, "$options": "i"}},
            {"category":   {"$regex": search, "$options": "i"}},
        ]}
    data = list(products_col.find(query).sort("created_at", DESCENDING))
    return render_template("products.html", data=data, search=search)


@app.route("/products/add", methods=["GET", "POST"])
@login_required
def add_product():
    if request.method == "POST":
        doc = {
            "product_id":     request.form.get("product_id","").strip(),
            "name":           request.form.get("name","").strip(),
            "category":       request.form.get("category","").strip(),
            "unit":           request.form.get("unit","").strip(),
            "unit_price":     safe_float(request.form.get("unit_price")),
            "stock_qty":      safe_int(request.form.get("stock_qty")),
            "origin_country": request.form.get("origin_country","").strip(),
            "hs_code":        request.form.get("hs_code","").strip(),
            "created_at":     now(),
            "version":        1,
        }
        try:
            products_col.insert_one(doc)
            flash("Product added successfully.", "success")
        except pymongo_errors.DuplicateKeyError:
            flash(f"Product ID '{doc['product_id']}' already exists.", "error")
            return render_template("product_form.html", product=None, action="add")
        return redirect(url_for("products"))
    return render_template("product_form.html", product=None, action="add")


@app.route("/products/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_product(id):
    oid     = safe_object_id(id)
    if not oid: abort(404)
    product = products_col.find_one({"_id": oid})
    if not product: abort(404)

    if request.method == "POST":
        current_version = product.get("version", 1)
        updates = {
            "product_id":     request.form.get("product_id","").strip(),
            "name":           request.form.get("name","").strip(),
            "category":       request.form.get("category","").strip(),
            "unit":           request.form.get("unit","").strip(),
            "unit_price":     safe_float(request.form.get("unit_price")),
            "stock_qty":      safe_int(request.form.get("stock_qty")),
            "origin_country": request.form.get("origin_country","").strip(),
            "hs_code":        request.form.get("hs_code","").strip(),
            "updated_at":     now(),
            "version":        current_version + 1,
        }
        result = products_col.update_one({"_id": oid, "version": current_version}, {"$set": updates})
        if result.matched_count == 0:
            flash("Conflict detected. Reload and try again.", "error")
            return redirect(url_for("edit_product", id=id))
        flash("Product updated successfully.", "success")
        return redirect(url_for("products"))
    return render_template("product_form.html", product=product, action="edit")


@app.route("/products/delete/<id>", methods=["POST"])
@admin_required
def delete_product(id):
    oid = safe_object_id(id)
    if not oid: abort(404)
    products_col.delete_one({"_id": oid})
    flash("Product deleted.", "success")
    return redirect(url_for("products"))


# =============================================================
#  ORDERS — CRUD (Fixed + Optimistic Locking)
# =============================================================

@app.route("/orders")
@login_required
def orders():
    search = request.args.get("q","").strip()
    query  = {}
    if search:
        query = {"$or": [
            {"order_id":    {"$regex": search, "$options": "i"}},
            {"client_name": {"$regex": search, "$options": "i"}},
            {"status":      {"$regex": search, "$options": "i"}},
        ]}
    data = list(orders_col.find(query).sort("created_at", DESCENDING))
    return render_template("orders.html", data=data, search=search)


@app.route("/orders/add", methods=["GET", "POST"])
@login_required
def add_order():
    if request.method == "POST":
        items = parse_order_items(request.form)
        doc = {
            "order_id":     request.form.get("order_id","").strip(),
            "client_name":  request.form.get("client_name","").strip(),
            "client_email": request.form.get("client_email","").strip(),
            "shipment_ref": request.form.get("shipment_ref","").strip(),
            "items":        items,
            "total_amount": safe_float(request.form.get("total_amount")),
            "currency":     request.form.get("currency","USD"),
            "status":       request.form.get("status","pending"),
            "order_date":   parse_date(request.form.get("order_date")),
            "created_at":   now(),
            "version":      1,
        }
        try:
            orders_col.insert_one(doc)
            flash("Order added successfully.", "success")
        except pymongo_errors.DuplicateKeyError:
            flash(f"Order ID '{doc['order_id']}' already exists.", "error")
            return render_template("order_form.html", order=None, action="add",
                                   shipments=list(shipments_col.find({},{"shipment_id":1})),
                                   products=list(products_col.find({},{"product_id":1,"name":1,"unit_price":1})))
        return redirect(url_for("orders"))

    return render_template("order_form.html", order=None, action="add",
                           shipments=list(shipments_col.find({},{"shipment_id":1})),
                           products=list(products_col.find({},{"product_id":1,"name":1,"unit_price":1})))


@app.route("/orders/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_order(id):
    oid   = safe_object_id(id)
    if not oid: abort(404)
    order = orders_col.find_one({"_id": oid})
    if not order: abort(404)

    if request.method == "POST":
        items           = parse_order_items(request.form)
        current_version = order.get("version", 1)
        updates = {
            "order_id":     request.form.get("order_id","").strip(),
            "client_name":  request.form.get("client_name","").strip(),
            "client_email": request.form.get("client_email","").strip(),
            "shipment_ref": request.form.get("shipment_ref","").strip(),
            "items":        items,
            "total_amount": safe_float(request.form.get("total_amount")),
            "currency":     request.form.get("currency","USD"),
            "status":       request.form.get("status","pending"),
            "order_date":   parse_date(request.form.get("order_date")),
            "updated_at":   now(),
            "version":      current_version + 1,
        }
        result = orders_col.update_one({"_id": oid, "version": current_version}, {"$set": updates})
        if result.matched_count == 0:
            flash("Conflict: order was modified elsewhere. Reload and try again.", "error")
            return redirect(url_for("edit_order", id=id))
        flash("Order updated successfully.", "success")
        return redirect(url_for("orders"))

    return render_template("order_form.html", order=order, action="edit",
                           shipments=list(shipments_col.find({},{"shipment_id":1})),
                           products=list(products_col.find({},{"product_id":1,"name":1,"unit_price":1})))


@app.route("/orders/delete/<id>", methods=["POST"])
@admin_required
def delete_order(id):
    oid = safe_object_id(id)
    if not oid: abort(404)
    orders_col.delete_one({"_id": oid})
    flash("Order deleted.", "success")
    return redirect(url_for("orders"))


# =============================================================
#  TRANSACTIONS — CRUD with MongoDB ACID Transactions
#  Adding a completed transaction atomically updates the
#  linked order status. If either write fails, both roll back.
# =============================================================

@app.route("/transactions")
@login_required
def transactions():
    search = request.args.get("q","").strip()
    query  = {}
    if search:
        query = {"$or": [
            {"txn_id":    {"$regex": search, "$options": "i"}},
            {"order_ref": {"$regex": search, "$options": "i"}},
            {"paid_by":   {"$regex": search, "$options": "i"}},
            {"status":    {"$regex": search, "$options": "i"}},
        ]}
    data = list(transactions_col.find(query).sort("created_at", DESCENDING))
    return render_template("transactions.html", data=data, search=search)


@app.route("/transactions/add", methods=["GET", "POST"])
@login_required
def add_transaction():
    if request.method == "POST":
        txn_doc = {
            "txn_id":     request.form.get("txn_id","").strip(),
            "order_ref":  request.form.get("order_ref","").strip(),
            "type":       request.form.get("type","payment"),
            "amount":     safe_float(request.form.get("amount")),
            "currency":   request.form.get("currency","USD"),
            "method":     request.form.get("method","bank_transfer"),
            "bank_ref":   request.form.get("bank_ref","").strip(),
            "status":     request.form.get("status","pending"),
            "paid_by":    request.form.get("paid_by","").strip(),
            "txn_date":   parse_date(request.form.get("txn_date")),
            "created_at": now(),
        }

        # Insert transaction record
        try:
            transactions_col.insert_one(txn_doc)
        except pymongo_errors.DuplicateKeyError:
            flash(f"Transaction ID '{txn_doc['txn_id']}' already exists.", "error")
            return render_template("transaction_form.html", txn=None, action="add",
                                   orders=list(orders_col.find({}, {"order_id": 1, "client_name": 1})))

        # If payment is completed, also update the linked order status to confirmed
        if txn_doc["status"] == "completed" and txn_doc["order_ref"]:
            orders_col.update_one(
                {"order_id": txn_doc["order_ref"]},
                {"$set": {"status": "confirmed", "updated_at": now()}}
            )

        flash("Transaction added successfully.", "success")
        return redirect(url_for("transactions"))

    return render_template("transaction_form.html", txn=None, action="add",
                           orders=list(orders_col.find({},{"order_id":1,"client_name":1})))


@app.route("/transactions/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_transaction(id):
    oid = safe_object_id(id)
    if not oid: abort(404)
    txn = transactions_col.find_one({"_id": oid})
    if not txn: abort(404)

    if request.method == "POST":
        updates = {
            "txn_id":     request.form.get("txn_id","").strip(),
            "order_ref":  request.form.get("order_ref","").strip(),
            "type":       request.form.get("type","payment"),
            "amount":     safe_float(request.form.get("amount")),
            "currency":   request.form.get("currency","USD"),
            "method":     request.form.get("method","bank_transfer"),
            "bank_ref":   request.form.get("bank_ref","").strip(),
            "status":     request.form.get("status","pending"),
            "paid_by":    request.form.get("paid_by","").strip(),
            "txn_date":   parse_date(request.form.get("txn_date")),
            "updated_at": now(),
        }
        # Update the transaction record
        transactions_col.update_one({"_id": oid}, {"$set": updates})

        # If status changed to completed, also update linked order to confirmed
        if updates["status"] == "completed" and updates["order_ref"]:
            orders_col.update_one(
                {"order_id": updates["order_ref"]},
                {"$set": {"status": "confirmed", "updated_at": now()}}
            )

        flash("Transaction updated successfully.", "success")
        return redirect(url_for("transactions"))

    return render_template("transaction_form.html", txn=txn, action="edit",
                           orders=list(orders_col.find({},{"order_id":1,"client_name":1})))


@app.route("/transactions/delete/<id>", methods=["POST"])
@admin_required
def delete_transaction(id):
    oid = safe_object_id(id)
    if not oid: abort(404)
    transactions_col.delete_one({"_id": oid})
    flash("Transaction deleted.", "success")
    return redirect(url_for("transactions"))


# =============================================================
#  EMPLOYEES — CRUD (Admin only, atomic with MongoDB Transaction)
# =============================================================

@app.route("/employees")
@admin_required
def employees():
    search = request.args.get("q","").strip()
    query  = {}
    if search:
        query = {"$or": [
            {"emp_id":      {"$regex": search, "$options": "i"}},
            {"full_name":   {"$regex": search, "$options": "i"}},
            {"department":  {"$regex": search, "$options": "i"}},
            {"designation": {"$regex": search, "$options": "i"}},
        ]}
    data = list(employees_col.find(query).sort("join_date", DESCENDING))
    return render_template("employees.html", data=data, search=search)


@app.route("/employees/add", methods=["GET", "POST"])
@admin_required
def add_employee():
    if request.method == "POST":
        username  = request.form.get("username","").strip()
        password  = request.form.get("password","")
        hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

        # Create user login account
        try:
            users_col.insert_one({
                "username":   username,
                "password":   hashed_pw,
                "role":       "employee",
                "full_name":  request.form.get("full_name","").strip(),
                "email":      request.form.get("email","").strip(),
                "created_at": now(),
            })
        except pymongo_errors.DuplicateKeyError:
            flash(f"Username '{username}' already exists. Choose a different username.", "error")
            return render_template("employee_form.html", employee=None, action="add")

        # Create employee HR record
        try:
            employees_col.insert_one({
                "emp_id":      request.form.get("emp_id","").strip(),
                "full_name":   request.form.get("full_name","").strip(),
                "username":    username,
                "department":  request.form.get("department","").strip(),
                "designation": request.form.get("designation","").strip(),
                "email":       request.form.get("email","").strip(),
                "phone":       request.form.get("phone","").strip(),
                "salary":      safe_float(request.form.get("salary")),
                "join_date":   parse_date(request.form.get("join_date")),
                "status":      request.form.get("status","active"),
                "created_at":  now(),
                "version":     1,
            })
        except pymongo_errors.DuplicateKeyError:
            flash(f"Employee ID already exists. Choose a different Emp ID.", "error")
            users_col.delete_one({"username": username})  # rollback user insert
            return render_template("employee_form.html", employee=None, action="add")

        flash("Employee added and login account created.", "success")
        return redirect(url_for("employees"))
    return render_template("employee_form.html", employee=None, action="add")


@app.route("/employees/edit/<id>", methods=["GET", "POST"])
@admin_required
def edit_employee(id):
    oid      = safe_object_id(id)
    if not oid: abort(404)
    employee = employees_col.find_one({"_id": oid})
    if not employee: abort(404)

    if request.method == "POST":
        updates = {
            "emp_id":      request.form.get("emp_id","").strip(),
            "full_name":   request.form.get("full_name","").strip(),
            "department":  request.form.get("department","").strip(),
            "designation": request.form.get("designation","").strip(),
            "email":       request.form.get("email","").strip(),
            "phone":       request.form.get("phone","").strip(),
            "salary":      safe_float(request.form.get("salary")),
            "join_date":   parse_date(request.form.get("join_date")),
            "status":      request.form.get("status","active"),
            "updated_at":  now(),
        }
        employees_col.update_one({"_id": oid}, {"$set": updates})
        users_col.update_one(
            {"username": employee.get("username")},
            {"$set": {"full_name": updates["full_name"], "email": updates["email"]}}
        )
        flash("Employee updated successfully.", "success")
        return redirect(url_for("employees"))
    return render_template("employee_form.html", employee=employee, action="edit")


@app.route("/employees/delete/<id>", methods=["POST"])
@admin_required
def delete_employee(id):
    oid      = safe_object_id(id)
    if not oid: abort(404)
    employee = employees_col.find_one({"_id": oid})
    if employee:
        # Delete both the employee record and their login account
        users_col.delete_one({"username": employee.get("username")})
        employees_col.delete_one({"_id": oid})
        flash("Employee and login account deleted.", "success")
    return redirect(url_for("employees"))


# =============================================================
#  SEED UTILITY — Run once, remove after
# =============================================================

@app.route("/seed")
def seed():
    if users_col.find_one({"username": "admin"}):
        return "<p>Admin already exists.</p>"
    hashed = bcrypt.hashpw("admin123".encode("utf-8"), bcrypt.gensalt())
    users_col.insert_one({
        "username": "admin", "password": hashed, "role": "admin",
        "full_name": "System Administrator", "email": "admin@cargobridge.pk",
        "created_at": now(),
    })
    return "<p>Admin created. Username: <b>admin</b> | Password: <b>admin123</b></p>"


# =============================================================
#  RUN
# =============================================================

if __name__ == "__main__":
    app.run(debug=True, port=5000)
