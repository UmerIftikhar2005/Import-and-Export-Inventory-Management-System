# =============================================================
#  CargoBridge — app.py
#  Flask + PyMongo | Login | Role-Based Access | Full CRUD
#  Collections: shipments, products, orders, transactions, employees
# =============================================================

from flask import (
    Flask, render_template, request,
    redirect, url_for, session, flash, abort
)
from pymongo import MongoClient, DESCENDING
from bson import ObjectId
from bson.errors import InvalidId
from dotenv import load_dotenv
from datetime import datetime
from functools import wraps
import bcrypt
import os

# -------------------------------------------------------------
#  App + Config
# -------------------------------------------------------------
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "cargobridge-secret-key-change-in-prod")

# -------------------------------------------------------------
#  Database Connection
# -------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client    = MongoClient(MONGO_URI)
db        = client["cargobridge"]

# Collection handles
users_col        = db["users"]
shipments_col    = db["shipments"]
products_col     = db["products"]
orders_col       = db["orders"]
transactions_col = db["transactions"]
employees_col    = db["employees"]


# =============================================================
#  DECORATORS — Auth Guards
# =============================================================

def login_required(f):
    """Redirect to login if no active session."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            flash("Please log in to continue.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Allow only admin role. Returns 403 for employees."""
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
    """Return ObjectId or None if invalid."""
    try:
        return ObjectId(id_str)
    except (InvalidId, TypeError):
        return None


def parse_date(date_str):
    """Parse HTML date input (YYYY-MM-DD) to datetime. Returns None if blank."""
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError:
        return None


def now():
    return datetime.utcnow()


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
#  AUTH — Login / Logout
# =============================================================

@app.route("/", methods=["GET", "POST"])
def login():
    # Already logged in → go to dashboard
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
        else:
            flash("Invalid username or password.", "error")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


# =============================================================
#  DASHBOARD
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

    recent_shipments = list(
        shipments_col.find().sort("created_at", DESCENDING).limit(5)
    )
    recent_orders = list(
        orders_col.find().sort("created_at", DESCENDING).limit(5)
    )
    return render_template(
        "dashboard.html",
        stats=stats,
        recent_shipments=recent_shipments,
        recent_orders=recent_orders
    )


# =============================================================
#  SHIPMENTS — Full CRUD
# =============================================================

@app.route("/shipments")
@login_required
def shipments():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {
            "$or": [
                {"shipment_id": {"$regex": search, "$options": "i"}},
                {"origin":      {"$regex": search, "$options": "i"}},
                {"destination": {"$regex": search, "$options": "i"}},
                {"status":      {"$regex": search, "$options": "i"}},
            ]
        }
    data = list(shipments_col.find(query).sort("created_at", DESCENDING))
    return render_template("shipments.html", data=data, search=search)


@app.route("/shipments/add", methods=["GET", "POST"])
@login_required
def add_shipment():
    if request.method == "POST":
        cargo_raw   = request.form.get("cargo_items", "")
        cargo_list  = [c.strip() for c in cargo_raw.split(",") if c.strip()]

        doc = {
            "shipment_id":  request.form.get("shipment_id", "").strip(),
            "type":         request.form.get("type", "export"),
            "origin":       request.form.get("origin", "").strip(),
            "destination":  request.form.get("destination", "").strip(),
            "status":       request.form.get("status", "pending"),
            "cargo_items":  cargo_list,
            "weight_kg":    float(request.form.get("weight_kg") or 0),
            "carrier":      request.form.get("carrier", "").strip(),
            "departure":    parse_date(request.form.get("departure")),
            "eta":          parse_date(request.form.get("eta")),
            "created_at":   now(),
        }
        shipments_col.insert_one(doc)
        flash("Shipment added successfully.", "success")
        return redirect(url_for("shipments"))

    return render_template("shipment_form.html", shipment=None, action="add")


@app.route("/shipments/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_shipment(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    shipment = shipments_col.find_one({"_id": oid})
    if not shipment:
        abort(404)

    if request.method == "POST":
        cargo_raw  = request.form.get("cargo_items", "")
        cargo_list = [c.strip() for c in cargo_raw.split(",") if c.strip()]

        updates = {
            "shipment_id":  request.form.get("shipment_id", "").strip(),
            "type":         request.form.get("type", "export"),
            "origin":       request.form.get("origin", "").strip(),
            "destination":  request.form.get("destination", "").strip(),
            "status":       request.form.get("status", "pending"),
            "cargo_items":  cargo_list,
            "weight_kg":    float(request.form.get("weight_kg") or 0),
            "carrier":      request.form.get("carrier", "").strip(),
            "departure":    parse_date(request.form.get("departure")),
            "eta":          parse_date(request.form.get("eta")),
            "updated_at":   now(),
        }
        shipments_col.update_one({"_id": oid}, {"$set": updates})
        flash("Shipment updated successfully.", "success")
        return redirect(url_for("shipments"))

    return render_template("shipment_form.html", shipment=shipment, action="edit")


@app.route("/shipments/delete/<id>", methods=["POST"])
@admin_required
def delete_shipment(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)
    shipments_col.delete_one({"_id": oid})
    flash("Shipment deleted.", "success")
    return redirect(url_for("shipments"))


# =============================================================
#  PRODUCTS — Full CRUD
# =============================================================

@app.route("/products")
@login_required
def products():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {
            "$or": [
                {"product_id": {"$regex": search, "$options": "i"}},
                {"name":       {"$regex": search, "$options": "i"}},
                {"category":   {"$regex": search, "$options": "i"}},
            ]
        }
    data = list(products_col.find(query).sort("created_at", DESCENDING))
    return render_template("products.html", data=data, search=search)


@app.route("/products/add", methods=["GET", "POST"])
@login_required
def add_product():
    if request.method == "POST":
        doc = {
            "product_id":       request.form.get("product_id", "").strip(),
            "name":             request.form.get("name", "").strip(),
            "category":         request.form.get("category", "").strip(),
            "unit":             request.form.get("unit", "").strip(),
            "unit_price":       float(request.form.get("unit_price") or 0),
            "stock_qty":        int(request.form.get("stock_qty") or 0),
            "origin_country":   request.form.get("origin_country", "").strip(),
            "hs_code":          request.form.get("hs_code", "").strip(),
            "created_at":       now(),
        }
        products_col.insert_one(doc)
        flash("Product added successfully.", "success")
        return redirect(url_for("products"))

    return render_template("product_form.html", product=None, action="add")


@app.route("/products/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_product(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    product = products_col.find_one({"_id": oid})
    if not product:
        abort(404)

    if request.method == "POST":
        updates = {
            "product_id":     request.form.get("product_id", "").strip(),
            "name":           request.form.get("name", "").strip(),
            "category":       request.form.get("category", "").strip(),
            "unit":           request.form.get("unit", "").strip(),
            "unit_price":     float(request.form.get("unit_price") or 0),
            "stock_qty":      int(request.form.get("stock_qty") or 0),
            "origin_country": request.form.get("origin_country", "").strip(),
            "hs_code":        request.form.get("hs_code", "").strip(),
            "updated_at":     now(),
        }
        products_col.update_one({"_id": oid}, {"$set": updates})
        flash("Product updated successfully.", "success")
        return redirect(url_for("products"))

    return render_template("product_form.html", product=product, action="edit")


@app.route("/products/delete/<id>", methods=["POST"])
@admin_required
def delete_product(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)
    products_col.delete_one({"_id": oid})
    flash("Product deleted.", "success")
    return redirect(url_for("products"))


# =============================================================
#  ORDERS — Full CRUD
# =============================================================

@app.route("/orders")
@login_required
def orders():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {
            "$or": [
                {"order_id":    {"$regex": search, "$options": "i"}},
                {"client_name": {"$regex": search, "$options": "i"}},
                {"status":      {"$regex": search, "$options": "i"}},
            ]
        }
    data = list(orders_col.find(query).sort("created_at", DESCENDING))
    return render_template("orders.html", data=data, search=search)


@app.route("/orders/add", methods=["GET", "POST"])
@login_required
def add_order():
    if request.method == "POST":
        # Parse line items from repeating form fields
        product_ids  = request.form.getlist("item_product_id")
        quantities   = request.form.getlist("item_qty")
        unit_prices  = request.form.getlist("item_unit_price")

        items = []
        for pid, qty, price in zip(product_ids, quantities, unit_prices):
            if pid.strip():
                items.append({
                    "product_id": pid.strip(),
                    "qty":        int(qty or 0),
                    "unit_price": float(price or 0),
                })

        doc = {
            "order_id":     request.form.get("order_id", "").strip(),
            "client_name":  request.form.get("client_name", "").strip(),
            "client_email": request.form.get("client_email", "").strip(),
            "shipment_ref": request.form.get("shipment_ref", "").strip(),
            "items":        items,
            "total_amount": float(request.form.get("total_amount") or 0),
            "currency":     request.form.get("currency", "USD"),
            "status":       request.form.get("status", "pending"),
            "order_date":   parse_date(request.form.get("order_date")),
            "created_at":   now(),
        }
        orders_col.insert_one(doc)
        flash("Order added successfully.", "success")
        return redirect(url_for("orders"))

    shipments = list(shipments_col.find({}, {"shipment_id": 1}))
    products  = list(products_col.find({}, {"product_id": 1, "name": 1, "unit_price": 1}))
    return render_template("order_form.html", order=None, action="add",
                           shipments=shipments, products=products)


@app.route("/orders/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_order(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    order = orders_col.find_one({"_id": oid})
    if not order:
        abort(404)

    if request.method == "POST":
        product_ids = request.form.getlist("item_product_id")
        quantities  = request.form.getlist("item_qty")
        unit_prices = request.form.getlist("item_unit_price")

        items = []
        for pid, qty, price in zip(product_ids, quantities, unit_prices):
            if pid.strip():
                items.append({
                    "product_id": pid.strip(),
                    "qty":        int(qty or 0),
                    "unit_price": float(price or 0),
                })

        updates = {
            "order_id":     request.form.get("order_id", "").strip(),
            "client_name":  request.form.get("client_name", "").strip(),
            "client_email": request.form.get("client_email", "").strip(),
            "shipment_ref": request.form.get("shipment_ref", "").strip(),
            "items":        items,
            "total_amount": float(request.form.get("total_amount") or 0),
            "currency":     request.form.get("currency", "USD"),
            "status":       request.form.get("status", "pending"),
            "order_date":   parse_date(request.form.get("order_date")),
            "updated_at":   now(),
        }
        orders_col.update_one({"_id": oid}, {"$set": updates})
        flash("Order updated successfully.", "success")
        return redirect(url_for("orders"))

    shipments = list(shipments_col.find({}, {"shipment_id": 1}))
    products  = list(products_col.find({}, {"product_id": 1, "name": 1, "unit_price": 1}))
    return render_template("order_form.html", order=order, action="edit",
                           shipments=shipments, products=products)


@app.route("/orders/delete/<id>", methods=["POST"])
@admin_required
def delete_order(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)
    orders_col.delete_one({"_id": oid})
    flash("Order deleted.", "success")
    return redirect(url_for("orders"))


# =============================================================
#  TRANSACTIONS — Full CRUD
# =============================================================

@app.route("/transactions")
@login_required
def transactions():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {
            "$or": [
                {"txn_id":    {"$regex": search, "$options": "i"}},
                {"order_ref": {"$regex": search, "$options": "i"}},
                {"paid_by":   {"$regex": search, "$options": "i"}},
                {"status":    {"$regex": search, "$options": "i"}},
            ]
        }
    data = list(transactions_col.find(query).sort("created_at", DESCENDING))
    return render_template("transactions.html", data=data, search=search)


@app.route("/transactions/add", methods=["GET", "POST"])
@login_required
def add_transaction():
    if request.method == "POST":
        doc = {
            "txn_id":     request.form.get("txn_id", "").strip(),
            "order_ref":  request.form.get("order_ref", "").strip(),
            "type":       request.form.get("type", "payment"),
            "amount":     float(request.form.get("amount") or 0),
            "currency":   request.form.get("currency", "USD"),
            "method":     request.form.get("method", "bank_transfer"),
            "bank_ref":   request.form.get("bank_ref", "").strip(),
            "status":     request.form.get("status", "pending"),
            "paid_by":    request.form.get("paid_by", "").strip(),
            "txn_date":   parse_date(request.form.get("txn_date")),
            "created_at": now(),
        }
        transactions_col.insert_one(doc)
        flash("Transaction added successfully.", "success")
        return redirect(url_for("transactions"))

    orders = list(orders_col.find({}, {"order_id": 1, "client_name": 1}))
    return render_template("transaction_form.html", txn=None, action="add", orders=orders)


@app.route("/transactions/edit/<id>", methods=["GET", "POST"])
@login_required
def edit_transaction(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    txn = transactions_col.find_one({"_id": oid})
    if not txn:
        abort(404)

    if request.method == "POST":
        updates = {
            "txn_id":     request.form.get("txn_id", "").strip(),
            "order_ref":  request.form.get("order_ref", "").strip(),
            "type":       request.form.get("type", "payment"),
            "amount":     float(request.form.get("amount") or 0),
            "currency":   request.form.get("currency", "USD"),
            "method":     request.form.get("method", "bank_transfer"),
            "bank_ref":   request.form.get("bank_ref", "").strip(),
            "status":     request.form.get("status", "pending"),
            "paid_by":    request.form.get("paid_by", "").strip(),
            "txn_date":   parse_date(request.form.get("txn_date")),
            "updated_at": now(),
        }
        transactions_col.update_one({"_id": oid}, {"$set": updates})
        flash("Transaction updated successfully.", "success")
        return redirect(url_for("transactions"))

    orders = list(orders_col.find({}, {"order_id": 1, "client_name": 1}))
    return render_template("transaction_form.html", txn=txn, action="edit", orders=orders)


@app.route("/transactions/delete/<id>", methods=["POST"])
@admin_required
def delete_transaction(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)
    transactions_col.delete_one({"_id": oid})
    flash("Transaction deleted.", "success")
    return redirect(url_for("transactions"))


# =============================================================
#  EMPLOYEES — Full CRUD (Admin only)
# =============================================================

@app.route("/employees")
@admin_required
def employees():
    search = request.args.get("q", "").strip()
    query  = {}
    if search:
        query = {
            "$or": [
                {"emp_id":      {"$regex": search, "$options": "i"}},
                {"full_name":   {"$regex": search, "$options": "i"}},
                {"department":  {"$regex": search, "$options": "i"}},
                {"designation": {"$regex": search, "$options": "i"}},
            ]
        }
    data = list(employees_col.find(query).sort("join_date", DESCENDING))
    return render_template("employees.html", data=data, search=search)


@app.route("/employees/add", methods=["GET", "POST"])
@admin_required
def add_employee():
    if request.method == "POST":
        username  = request.form.get("username", "").strip()
        password  = request.form.get("password", "")
        hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

        # Create user account in users collection
        users_col.insert_one({
            "username":   username,
            "password":   hashed_pw,
            "role":       "employee",
            "full_name":  request.form.get("full_name", "").strip(),
            "email":      request.form.get("email", "").strip(),
            "created_at": now(),
        })

        # Create employee record
        doc = {
            "emp_id":       request.form.get("emp_id", "").strip(),
            "full_name":    request.form.get("full_name", "").strip(),
            "username":     username,
            "department":   request.form.get("department", "").strip(),
            "designation":  request.form.get("designation", "").strip(),
            "email":        request.form.get("email", "").strip(),
            "phone":        request.form.get("phone", "").strip(),
            "salary":       float(request.form.get("salary") or 0),
            "join_date":    parse_date(request.form.get("join_date")),
            "status":       request.form.get("status", "active"),
            "created_at":   now(),
        }
        employees_col.insert_one(doc)
        flash("Employee added and login account created.", "success")
        return redirect(url_for("employees"))

    return render_template("employee_form.html", employee=None, action="add")


@app.route("/employees/edit/<id>", methods=["GET", "POST"])
@admin_required
def edit_employee(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    employee = employees_col.find_one({"_id": oid})
    if not employee:
        abort(404)

    if request.method == "POST":
        updates = {
            "emp_id":      request.form.get("emp_id", "").strip(),
            "full_name":   request.form.get("full_name", "").strip(),
            "department":  request.form.get("department", "").strip(),
            "designation": request.form.get("designation", "").strip(),
            "email":       request.form.get("email", "").strip(),
            "phone":       request.form.get("phone", "").strip(),
            "salary":      float(request.form.get("salary") or 0),
            "join_date":   parse_date(request.form.get("join_date")),
            "status":      request.form.get("status", "active"),
            "updated_at":  now(),
        }
        employees_col.update_one({"_id": oid}, {"$set": updates})

        # Sync full_name + email to users collection as well
        users_col.update_one(
            {"username": employee.get("username")},
            {"$set": {
                "full_name": updates["full_name"],
                "email":     updates["email"],
            }}
        )
        flash("Employee updated successfully.", "success")
        return redirect(url_for("employees"))

    return render_template("employee_form.html", employee=employee, action="edit")


@app.route("/employees/delete/<id>", methods=["POST"])
@admin_required
def delete_employee(id):
    oid = safe_object_id(id)
    if not oid:
        abort(404)

    employee = employees_col.find_one({"_id": oid})
    if employee:
        # Also remove the login account
        users_col.delete_one({"username": employee.get("username")})
        employees_col.delete_one({"_id": oid})

    flash("Employee and their login account deleted.", "success")
    return redirect(url_for("employees"))


# =============================================================
#  SEED UTILITY — Run once to create admin account
#  Access: GET /seed  (remove this route in production!)
# =============================================================

@app.route("/seed")
def seed():
    if users_col.find_one({"username": "admin"}):
        return "<p>Admin already exists. Remove /seed route before deploying.</p>"

    hashed = bcrypt.hashpw("admin123".encode("utf-8"), bcrypt.gensalt())
    users_col.insert_one({
        "username":   "admin",
        "password":   hashed,
        "role":       "admin",
        "full_name":  "System Administrator",
        "email":      "admin@cargobridge.pk",
        "created_at": now(),
    })
    return "<p>Admin created. Username: <b>admin</b> | Password: <b>admin123</b>. Remove this route now.</p>"


# =============================================================
#  RUN
# =============================================================

if __name__ == "__main__":
    app.run(debug=True, port=5000)