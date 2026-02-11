#!/usr/bin/env python3
"""
ganesh_dairy_pro_v_6.py

Ganesh Dairy Pro — v6.0
- Dashboard shows records filtered by shift (defaults to current shift).
- Adds "Generate Monthly Bill" button to create a monthly bill for an individual farmer.
  The bill can be either printed directly or saved as a PDF.
- Safe None-handling when formatting numeric fields.
- No background image dependency.
"""
import hashlib

def hash_password(p: str) -> str:
    return hashlib.sha256(p.encode('utf-8')).hexdigest()

import os
import sys
import sqlite3
from datetime import date, datetime, timedelta
import hashlib
import tempfile
import webbrowser
import tkinter as tk
from tkinter import ttk, messagebox
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Optional ttkbootstrap for nicer UI; falls back to plain ttk/tk
try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
    TB = True
    THEME = "cosmo"
except Exception:
    TB = False
    THEME = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "dairy.db")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
BILLS_DIR = os.path.join(REPORTS_DIR, "bills")


def ensure_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(BILLS_DIR, exist_ok=True)


def normalize_date_input(d: str):
    """Accept and normalize date formats:
       DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, YYYY/MM/DD.
       Returns YYYY-MM-DD
    """
    if not d:
        raise ValueError("Empty date")

    s = str(d).strip()

    # acceptable date formats
    fmts = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"]

    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            pass

    # ISO fallback
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    raise ValueError(f"Invalid date format: {d}")


def get_advance_balance(farmer_code: int, up_to_month: str = None):
    """Return advance balance for farmer up to (but not including) the given month (YYYY-MM)."""
    conn = get_conn()
    cur = conn.cursor()

    # total advances
    cur.execute(
        "SELECT IFNULL(SUM(amount),0) as total_adv FROM advances WHERE farmer_code=?",
        (farmer_code,)
    )
    adv_row = cur.fetchone()
    total_adv = adv_row['total_adv'] if adv_row else 0.0

    # total deductions
    if up_to_month:
        cur.execute(
            "SELECT IFNULL(SUM(amount),0) as total_ded "
            "FROM advance_deductions WHERE farmer_code=? AND month<?",
            (farmer_code, up_to_month)
        )
    else:
        cur.execute(
            "SELECT IFNULL(SUM(amount),0) as total_ded "
            "FROM advance_deductions WHERE farmer_code=?",
            (farmer_code,)
        )

    ded_row = cur.fetchone()
    total_ded = ded_row['total_ded'] if ded_row else 0.0

    cur.close()
    conn.close()

    return round(float(total_adv) - float(total_ded), 2)


def record_advance_deduction(farmer_code: int, month: str, amount: float, note: str = ''):
    """Record an advance deduction."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        today = date.today().strftime('%Y-%m-%d')

        cur.execute(
            "INSERT INTO advance_deductions (farmer_code, date, month, amount, note) "
            "VALUES (?,?,?,?,?)",
            (farmer_code, today, month, amount, note)
        )

        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print("Advance Deduction Error:", e)
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return False


def get_shift_for_time(dt=None):
    if dt is None:
        dt = datetime.now()
    h = dt.hour
    # Morning shift: 6 AM to 3:59 PM
    if 6 <= h < 16:
        return "Morning"
    # Evening shift: 4 PM to 5:59 AM
    else:
        return "Evening"


def open_folder(path):
    try:
        if sys.platform.startswith('win'):
            os.startfile(path)
        elif sys.platform == 'darwin':
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')
    except Exception as e:
        messagebox.showerror("Error", f"Cannot open folder:\n{e}")


# DB helpers
def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    cur.close()
    return cols


def init_db_and_migrate():
    # ensure folders for reports & bills exist
    ensure_dirs()

    created = not os.path.exists(DB_FILE)
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password_hash TEXT
    )
    """)

    # farmers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS farmers (
        farmer_code INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        village TEXT,
        contact TEXT,
        category TEXT DEFAULT 'Cow'
    )
    """)

    # milk records
    cur.execute("""
    CREATE TABLE IF NOT EXISTS milk_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farmer_code INTEGER,
        date TEXT,
        shift TEXT,
        litres REAL,
        fat REAL,
        snf REAL,
        rate REAL,
        amount REAL,
        category TEXT
    )
    """)

    # advances
    cur.execute("""
    CREATE TABLE IF NOT EXISTS advances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farmer_code INTEGER,
        date TEXT,
        reason TEXT,
        amount REAL
    )
    """)

    # rate table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rate_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        fat REAL,
        snf REAL,
        rate REAL
    )
    """)

    # sales
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        dairy_name TEXT,
        category TEXT,
        litres REAL,
        fat REAL,
        rate REAL,
        amount REAL
    )
    """)

    # advance deductions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS advance_deductions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farmer_code INTEGER,
        date TEXT,
        month TEXT,  /* YYYY-MM */
        amount REAL,
        note TEXT
    )
    """)

    # ensure admin exists
    cur.execute("SELECT COUNT(*) AS c FROM users")
    c = cur.fetchone()['c']
    if c == 0:
        cur.execute(
            "INSERT INTO users (username, password_hash) VALUES (?,?)",
            ('admin', hash_password('admin123'))
        )
    else:
        cur.execute("SELECT password_hash FROM users WHERE username=?", ('admin',))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (?,?)",
                ('admin', hash_password('admin123'))
            )
        else:
            if row['password_hash'] is None:
                cur.execute(
                    "UPDATE users SET password_hash=? WHERE username=?",
                    (hash_password('admin123'), 'admin')
                )

    # populate sample rates if empty
    cur.execute("SELECT COUNT(*) AS c FROM rate_table")
    if cur.fetchone()['c'] == 0:
        cow_base_fat = 3.0
        cow_base_rate = 30.0
        buf_base_fat = 5.0
        buf_base_rate = 45.0
        entries = []

        # cow slabs
        f = cow_base_fat
        while f <= 6.0 + 1e-9:
            rate = round(cow_base_rate + (f - cow_base_fat) * 5.0, 2)
            entries.append(('Cow', round(f, 1), 8.0, rate))
            f = round(f + 0.1, 1)

        # buffalo slabs
        f = buf_base_fat
        while f <= 11.0 + 1e-9:
            rate = round(buf_base_rate + (f - buf_base_fat) * 5.0, 2)
            entries.append(('Buffalo', round(f, 1), 8.0, rate))
            f = round(f + 0.1, 1)

        cur.executemany(
            "INSERT INTO rate_table (category, fat, snf, rate) VALUES (?,?,?,?)",
            entries
        )

    conn.commit()
    cur.close()
    conn.close()

    if created:
        print("Database created and initialized.")


# get smallest available farmer code (reuse gaps)
def get_next_farmer_code(conn):
    cur = conn.cursor()
    cur.execute("SELECT farmer_code FROM farmers ORDER BY farmer_code")
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    n = 1
    for code in rows:
        if code == n:
            n += 1
        elif code > n:
            break
    return n


# rate lookup
def find_rate_for(category: str, fat: float, snf: float):
    conn = get_conn()
    cur = conn.cursor()

    # default category if None
    if not category:
        category = "Cow"

    try:
        fat_val = float(fat)
    except Exception:
        fat_val = 0.0

    fat_r = round(fat_val * 10) / 10.0

    # try exact fat match, closest SNF
    cur.execute(
        "SELECT rate FROM rate_table WHERE category=? AND fat=? "
        "ORDER BY ABS(snf-?) LIMIT 1",
        (category, fat_r, snf)
    )
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        return round(row['rate'], 2)

    # fallback: closest fat
    cur.execute(
        "SELECT fat, snf, rate FROM rate_table WHERE category=? "
        "ORDER BY ABS(fat-?) LIMIT 1",
        (category, fat_r)
    )
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        return round(row['rate'], 2)

    cur.close()
    conn.close()

    # final fallback formula
    if category.lower().startswith('buf'):
        base_fat = 5.0
        base_rate = 45.0
    else:
        base_fat = 3.0
        base_rate = 30.0

    rate = base_rate + (fat_val - base_fat) * 5.0
    return round(max(0.0, rate), 2)


# --- Late/missed milk recorder (can be called from GUI) ---
def record_milk_late(farmer_code, date_str, shift, litres, fat, snf, category):
    """
    Record milk entry after the shift is over (insert or update).
    Accepts date_str in DD/MM/YYYY or YYYY-MM-DD.
    Stores YYYY-MM-DD in DB.
    Returns True on success, False on failure.
    """
    conn = None
    cur = None
    try:
        # Normalize date
        date_db = normalize_date_input(date_str)

        conn = get_conn()
        cur = conn.cursor()

        # Validate farmer
        cur.execute("SELECT name FROM farmers WHERE farmer_code=?", (farmer_code,))
        f = cur.fetchone()
        if not f:
            print("Invalid farmer code")
            return False

        # Validate numeric values
        fat = float(fat)
        snf = float(snf)
        litres = float(litres)

        if fat < 0 or snf < 0 or litres < 0:
            raise ValueError("Negative values are not allowed for FAT, SNF, or Litres.")

        # Calculate rate & amount
        rate = find_rate_for(category, fat, snf)
        amount = round(litres * rate, 2)

        # Check if record already exists
        cur.execute(
            """SELECT id FROM milk_records
               WHERE farmer_code=? AND date=? AND shift=? AND category=?""",
            (farmer_code, date_db, shift, category),
        )
        ex = cur.fetchone()

        if ex:
            # UPDATE existing record
            cur.execute(
                """UPDATE milk_records
                   SET litres=?, fat=?, snf=?, rate=?, amount=?
                   WHERE farmer_code=? AND date=? AND shift=? AND category=?""",
                (litres, fat, snf, rate, amount,
                 farmer_code, date_db, shift, category),
            )
        else:
            # INSERT new record
            cur.execute(
                """INSERT INTO milk_records
                   (farmer_code, date, shift, litres, fat, snf, rate, amount, category)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (farmer_code, date_db, shift, litres, fat, snf, rate, amount, category),
            )

        conn.commit()
        return True

    except Exception as e:
        print("Milk Late Entry Error:", e)   # Important for debugging
        return False

    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass





    # ------------------------------ #
    # Fetch Farmer Details
    # ------------------------------ #
    def fetch_farmer(event=None):
        code = farmer_code.get().strip()
        if not code.isdigit():
            farmer_info.config(text="⚠️ Invalid code format", foreground="gray")
            return

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, village, category FROM farmers WHERE farmer_code=?", (code,))
        data = cur.fetchone()
        cur.close()
        conn.close()

        if data:
            farmer_info.config(
                text=f"👤 {data['name']} | 🏡 {data['village'] or ''} | {data['category']}",
                foreground="green",
            )
            if data["category"] and data["category"].lower() in ["cow", "buffalo"]:
                cat_cb.set(data["category"])
        else:
            farmer_info.config(text="❌ Farmer not found", foreground="red")

    farmer_code.bind("<FocusOut>", fetch_farmer)
    farmer_code.bind("<Return>", fetch_farmer)

    # ------------------------------ #
    # Auto Amount Update
    # ------------------------------ #
    def update_amount(event=None):
        try:
            l = float(litres.get() or 0)
            f = float(fat.get() or 0)
            s = float(snf.get() or 8.0)
            c = cat_cb.get()
            rate = find_rate_for(c, f, s)
            amt = round(l * rate, 2)
            amount_label.config(text=f"₹{amt:.2f}")
        except Exception:
            amount_label.config(text="₹0.00")

    litres.bind("<KeyRelease>", update_amount)
    fat.bind("<KeyRelease>", update_amount)
    snf.bind("<KeyRelease>", update_amount)
    cat_cb.bind("<<ComboboxSelected>>", update_amount)

    # ------------------------------ #
    # Save Missed Record
    # ------------------------------ #
    def save_record():
        try:
            code = int(farmer_code.get())
            d = date_entry.get().strip()
            s = shift_cb.get()
            c = cat_cb.get()
            l = float(litres.get())
            f = float(fat.get())
            sn = float(snf.get())

            # Normalize date using global helper
            try:
                entered_date = normalize_date_input(d)
            except Exception:
                messagebox.showerror("Invalid Date", "Date must be DD/MM/YYYY or YYYY-MM-DD")
                return

            # Prevent future dates
            if datetime.strptime(entered_date, "%Y-%m-%d").date() > date.today():
                messagebox.showerror("Invalid Date", "❌ Cannot record for a future date.")
                return

            if l < 0 or f < 0 or sn < 0:
                messagebox.showerror("Invalid Input", "❌ Litres, FAT, and SNF cannot be negative.")
                return

            # Save using helper function
            if record_milk_late(code, entered_date, s, l, f, sn, c):
                messagebox.showinfo("Success", f"Missed milk recorded for {d} ({s}, {c}).")
                win.destroy()
            else:
                messagebox.showerror("Error", "Failed to record. Check farmer code or inputs.")
        except ValueError:
            messagebox.showerror("Error", "Invalid input: ensure numeric values for code/litres/fat/snf")
        except Exception as e:
            messagebox.showerror("Error", f"Invalid input: {e}")

    ttk.Button(frm, text="💾 Save Record", command=save_record)\
        .grid(row=9, column=0, columnspan=2, pady=15)


class DairyApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Shree Ganesh Dairy")
        try:
            self.root.state('zoomed')
        except Exception:
            try:
                self.root.attributes('-zoomed', True)
            except Exception:
                pass
        if TB:
            self.style = tb.Style(THEME)
        else:
            self.style = None
        init_db_and_migrate()
        self.current_user = None
        # default shift filter: show only current shift
        self.current_shift_filter = get_shift_for_time()
        self.create_welcome_ui()
    def create_welcome_ui(self):
        for w in self.root.winfo_children():
            w.destroy()

        self.root.configure(bg="#f8f9fa")

        frame = tk.Frame(self.root, bg="#f8f9fa")
        frame.place(relx=0.5, rely=0.5, anchor="center")

        # --- Main Title ---
        title = tk.Label(
            frame,
            text="WELCOME TO\nDAIRY MANAGEMENT SYSTEM\n(SHREE GANESH DAIRY TALBID)",
            font=("Segoe UI", 34, "bold"),
            fg="#1a5276",
            bg="#f8f9fa",
            justify="center"
        )
        title.pack(pady=(0, 50))

        # --- Developer Info ---
        dev = tk.Label(
            frame,
            text="DEVELOPED BY - PRATIK RAMESH JADHAV (MCA-II)",
            font=("Segoe UI", 20, "italic"),
            fg="#2e4053",
            bg="#f8f9fa"
        )
        dev.pack(pady=(0, 20))

        # --- Guide Info ---
        guide = tk.Label(
            frame,
            text="UNDER THE GUIDANCE OF - PROF. DR. P. P. PATIL",
            font=("Segoe UI", 20, "italic"),
            fg="#2e4053",
            bg="#f8f9fa"
        )
        guide.pack(pady=(0, 40))

        # --- Continue Button ---
        continue_btn = tk.Button(
            frame,
            text="Continue →",
            font=("Segoe UI", 18, "bold"),
            bg="#3498db",
            fg="white",
            activebackground="#5dade2",
            relief="flat",
            padx=30,
            pady=10,
            cursor="hand2",
            command=self.create_login_ui
        )
        continue_btn.pack(pady=10)

        # Allow pressing Enter to continue
        self.root.bind("<Return>", lambda e: self.create_login_ui())
    def _focus(self, widget):
        try:
            widget.focus_set()
        except Exception:
            pass


    def create_login_ui(self):
        for w in self.root.winfo_children():
            w.destroy()
        self.root.configure(bg="#f8f9fa")

        container = tk.Frame(self.root, bg="#f8f9fa")
        container.place(relx=0.5, rely=0.5, anchor="center")

        title_label = tk.Label(
            container,
            text="🥛 Shree Ganesh Dairy",
            font=("Segoe UI", 42, "bold"),
            fg="#1a5276",
            bg="#f8f9fa"
        )
        title_label.pack(pady=(0, 10))

        tagline = tk.Label(
            container,
            text="Pure Milk. Honest Service.",
            font=("Segoe UI", 14, "italic"),
            fg="#5d6d7e",
            bg="#f8f9fa"
        )
        tagline.pack(pady=(0, 25))

        shadow = tk.Frame(container, bg="#d6eaf8", bd=0)
        shadow.pack(pady=10)
        card = tk.Frame(
            shadow,
            bg="#fefefe",
            padx=50,
            pady=45,
            highlightbackground="#aed6f1",
            highlightthickness=2
        )
        card.pack(padx=3, pady=3)

        tk.Label(
            card,
            text="Login to Continue",
            font=("Segoe UI", 16, "bold"),
            fg="#2c3e50",
            bg="#fefefe"
        ).grid(row=0, column=0, columnspan=2, pady=(0, 25))

        tk.Label(card, text="Username:", font=("Segoe UI", 11), bg="#fefefe").grid(
            row=1, column=0, sticky="e", padx=8, pady=8
        )
        self.login_user = ttk.Entry(card, width=30)
        self.login_user.grid(row=1, column=1, pady=8)
        self.login_user.insert(0, "admin")

        tk.Label(card, text="Password:", font=("Segoe UI", 11), bg="#fefefe").grid(
            row=2, column=0, sticky="e", padx=8, pady=8
        )
        pass_frame = tk.Frame(card, bg="#fefefe")
        pass_frame.grid(row=2, column=1, pady=8)
        self.login_pass = ttk.Entry(pass_frame, width=27, show="*")
        self.login_pass.pack(side="left", padx=(0, 6))

        def toggle_password():
            if self.login_pass.cget("show") == "*":
                self.login_pass.config(show="")
                eye_btn.config(text="🙈")
            else:
                self.login_pass.config(show="*")
                eye_btn.config(text="👁️")

        eye_btn = tk.Button(
            pass_frame,
            text="👁️",
            font=("Segoe UI", 10),
            bg="#fefefe",
            relief="flat",
            command=toggle_password,
            cursor="hand2"
        )
        eye_btn.pack(side="right")

        self.remember_var = tk.BooleanVar()
        remember_chk = tk.Checkbutton(
            card,
            text="Remember Me",
            variable=self.remember_var,
            font=("Segoe UI", 10),
            bg="#fefefe"
        )
        remember_chk.grid(row=3, column=1, sticky="w", pady=(2, 10))

        login_btn = tk.Button(
            card,
            text="Login",
            font=("Segoe UI", 11, "bold"),
            bg="#3498db",
            fg="white",
            activebackground="#5dade2",
            relief="flat",
            padx=22,
            pady=7,
            width=15,
            cursor="hand2",
            command=self.do_login
        )
        login_btn.grid(row=4, column=0, columnspan=2, pady=(15, 5))

        def on_enter(e):
            login_btn.config(bg="#5dade2")

        def on_leave(e):
            login_btn.config(bg="#3498db")

        login_btn.bind("<Enter>", on_enter)
        login_btn.bind("<Leave>", on_leave)

        forgot_lbl = tk.Label(
            card,
            text="Forgot password?",
            font=("Segoe UI", 9, "underline"),
            fg="#2980b9",
            bg="#fefefe",
            cursor="hand2"
        )
        forgot_lbl.grid(row=5, column=0, columnspan=2, pady=(6, 0))
        forgot_lbl.bind(
            "<Button-1>",
            lambda e: messagebox.showinfo("Info", "Password reset feature coming soon!")
        )

        self.root.bind("<Return>", lambda e: self.do_login())

    def do_login(self):
        username = self.login_user.get().strip()
        password = self.login_pass.get().strip()

        if not username or not password:
            messagebox.showerror("Error", "Enter both username and password")
            return

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE username=?", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and row["password_hash"] == hash_password(password):
            self.current_user = username
            self.root.unbind("<Return>")  # unbind Enter key after login
            self.create_main_ui()
        else:
            messagebox.showerror("Login Failed", "Invalid username or password")

    def create_main_ui(self):
        for w in self.root.winfo_children():
            w.destroy()

        header = (
            tb.Frame(self.root, bootstyle='info')
            if TB else tk.Frame(self.root, bg='#e0e0e0')
        )
        header.pack(fill='x', side='top')

        title_lbl = (
            tb.Label(header, text='🐄 Shree Ganesh Dairy Talbid',
                     font=('Segoe UI', 14, 'bold'))
            if TB else tk.Label(header, text='Shree Ganesh Dairy',
                                font=('Segoe UI', 14, 'bold'))
        )
        title_lbl.pack(side='left', padx=12, pady=8)

        corner = tb.Frame(header) if TB else tk.Frame(header)
        corner.pack(side='right', padx=8)

        # Core buttons
        mf_btn = (
            tb.Button(corner, text='Manage Farmers',
                      bootstyle='secondary-outline', width=16,
                      command=self.manage_farmers_window)
            if TB else tk.Button(corner, text='Manage Farmers',
                                 width=16, command=self.manage_farmers_window)
        )
        mf_btn.pack(side='right', padx=6)

        adv_btn = (
            tb.Button(corner, text='Record Advance',
                      bootstyle='secondary-outline', width=14,
                      command=self.record_advance_popup)
            if TB else tk.Button(corner, text='Record Advance',
                                 width=14, command=self.record_advance_popup)
        )
        adv_btn.pack(side='right', padx=6)

        sale_btn = (
            tb.Button(corner, text='Record Sale',
                      bootstyle='secondary-outline', width=12,
                      command=self.record_sale_popup)
            if TB else tk.Button(corner, text='Record Sale',
                                 width=12, command=self.record_sale_popup)
        )
        sale_btn.pack(side='right', padx=6)
        # NEW: Sales Report button (for milk sold in Sales tab)
        sales_report_btn = (
            tb.Button(
                corner, text='Sales Report',
                bootstyle='secondary-outline', width=14,
                command=self.select_month_for_sales_report
            )
            if TB else tk.Button(
                corner, text='Sales Report',
                width=14,
                command=self.select_month_for_sales_report
            )
        )
        sales_report_btn.pack(side='right', padx=6)

        rate_btn = (
            tb.Button(corner, text='Rate Table',
                      bootstyle='secondary-outline', width=12,
                      command=self.rate_table_window)
            if TB else tk.Button(corner, text='Rate Table',
                                 width=12, command=self.rate_table_window)
        )
        rate_btn.pack(side='right', padx=6)

        report_btn = (
            tb.Button(corner, text='Generate Report',
                      bootstyle='secondary-outline', width=14,
                      command=self.select_month_for_report)
            if TB else tk.Button(corner, text='Generate Report',
                                 width=14, command=self.select_month_for_report)
        )
        report_btn.pack(side='right', padx=6)

        # New: Generate Monthly Bill button
        bill_btn = (
            tb.Button(corner, text='Create Monthly Bill',
                      bootstyle='secondary-outline', width=16,
                      command=self.generate_monthly_bill_popup)
            if TB else tk.Button(corner, text='Create Monthly Bill',
                                 width=16,
                                 command=self.generate_monthly_bill_popup)
        )
        bill_btn.pack(side='right', padx=6)

        openr_btn = (
            tb.Button(corner, text='Open Reports',
                      bootstyle='secondary-outline', width=12,
                      command=lambda: open_folder(REPORTS_DIR))
            if TB else tk.Button(corner, text='Open Reports',
                                 width=12,
                                 command=lambda: open_folder(REPORTS_DIR))
        )
        openr_btn.pack(side='right', padx=6)

        # Start New Shift + Auth buttons
        authfrm = tb.Frame(header) if TB else tk.Frame(header)
        authfrm.pack(side='right', padx=8)

        start_shift_btn = (
            tb.Button(authfrm, text='Start New Shift',
                      bootstyle='primary-outline', width=16,
                      command=self.start_new_shift)
            if TB else tk.Button(authfrm, text='Start New Shift',
                                 width=16, command=self.start_new_shift)
        )
        start_shift_btn.pack(side='right', padx=4)

        chbtn = (
            tb.Button(authfrm, text='Change Password',
                      bootstyle='warning-outline', width=14,
                      command=self.change_password_popup)
            if TB else tk.Button(authfrm, text='Change Password',
                                 command=self.change_password_popup)
        )
        chbtn.pack(side='right', padx=4)

        lobtn = (
            tb.Button(authfrm, text='Logout',
                      bootstyle='danger-outline', width=10,
                      command=self.create_login_ui)
            if TB else tk.Button(authfrm, text='Logout',
                                 command=self.create_login_ui)
        )
        lobtn.pack(side='right')

               # --- QUICK ENTRY SECTION ---
        quick_section = (
            tb.Frame(self.root, padding=8)
            if TB else tk.Frame(self.root, padx=8, pady=8)
        )
        quick_section.pack(fill="x", pady=(0, 5))

        # Blue banner
        banner = tk.Frame(quick_section, bg="#1a5276")
        banner.grid(row=0, column=0, columnspan=14, sticky="ew")
        tk.Label(
            banner,
            text="QUICK MILK ENTRY ",
            font=("Segoe UI", 20, "bold"),
            fg="white",
            bg="#1a5276",
            pady=10,
        ).pack(fill="x")

        # Quick entry form
        quick = (
            tb.Frame(quick_section, padding=10)
            if TB else tk.Frame(quick_section, padx=10, pady=10)
        )
        quick.grid(row=1, column=0, columnspan=14, sticky="ew")

        # ---- Farmer list for dropdown (Code - Name) ----
        try:
            conn = get_conn()          # uses your existing DB helper
            cur = conn.cursor()
            cur.execute(
                "SELECT farmer_code, name FROM farmers ORDER BY name"
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print("Error loading farmers for dropdown:", e)
            rows = []

        # e.g. ["1 - Ramesh", "2 - Suresh", ...]
        self.farmer_display_list = [f"{code} - {name}" for code, name in rows]

        # ---------------- Row 1 ----------------
        tk.Label(quick, text="Farmer Code:").grid(
            row=0, column=0, sticky="e", padx=6, pady=6
        )
        self.e_code = ttk.Entry(quick, width=12)
        self.e_code.grid(row=0, column=1, padx=4, pady=6)
        self.e_code.bind("<Return>", lambda e: self._on_code_enter())
        self.e_code.bind("<FocusOut>", lambda e: self.fetch_farmer_and_update())

        # 🔽 NEW: Farmer dropdown (Code - Name)
        tk.Label(quick, text="Farmer (Code - Name):").grid(
            row=0, column=2, sticky="e", padx=6
        )
        self.farmer_combo = ttk.Combobox(
            quick,
            values=self.farmer_display_list,
            width=26,
            state="readonly",
        )
        self.farmer_combo.grid(row=0, column=3, padx=4, pady=6, sticky="w")
        self.farmer_combo.set("Select Farmer")
        self.farmer_combo.bind("<<ComboboxSelected>>", self.on_farmer_selected)

        tk.Label(quick, text="Category:").grid(
            row=0, column=4, sticky="e", padx=6
        )
        self.cat_placeholder = tk.Frame(quick)
        self.cat_placeholder.grid(row=0, column=5, padx=4)

        tk.Label(quick, text="Date:").grid(
            row=0, column=6, sticky="e", padx=6
        )
        self.e_date = ttk.Entry(quick, width=14)
        self.e_date.grid(row=0, column=7, padx=4)
        self.e_date.insert(0, date.today().strftime("%d/%m/%Y"))

        tk.Label(quick, text="Shift:").grid(
            row=0, column=8, sticky="e", padx=6
        )
        self.e_shift = ttk.Combobox(
            quick, values=["Morning", "Evening"], width=12, state="readonly"
        )
        self.e_shift.grid(row=0, column=9, padx=4)
        self.e_shift.set(get_shift_for_time())

        tk.Label(quick, text="Rate (₹/L):").grid(
            row=0, column=10, sticky="e", padx=6
        )
        self.e_rate = ttk.Entry(quick, width=14)
        self.e_rate.grid(row=0, column=11, padx=4)

        calc_btn = (
            tb.Button(
                quick,
                text="Calculate",
                bootstyle="info",
                width=12,
                command=self.quick_calculate,
            )
            if TB
            else tk.Button(
                quick,
                text="Calculate",
                width=12,
                command=self.quick_calculate,
            )
        )
        calc_btn.grid(row=0, column=12, padx=10)

        # ---------------- Row 2 ----------------
        tk.Label(quick, text="Litres:").grid(
            row=1, column=0, sticky="e", padx=6, pady=6
        )
        self.e_litres = ttk.Entry(quick, width=14)
        self.e_litres.grid(row=1, column=1, padx=4, pady=6)

        tk.Label(quick, text="Fat (%):").grid(
            row=1, column=2, sticky="e", padx=6
        )
        self.e_fat = ttk.Entry(quick, width=14)
        self.e_fat.grid(row=1, column=3, padx=4)

        tk.Label(quick, text="SNF:").grid(
            row=1, column=4, sticky="e", padx=6
        )
        self.e_snf = ttk.Entry(quick, width=14)
        self.e_snf.grid(row=1, column=5, padx=4)

        tk.Label(quick, text="Amount (₹):").grid(
            row=1, column=8, sticky="e", padx=6
        )
        self.e_amount = ttk.Entry(quick, width=14)
        self.e_amount.grid(row=1, column=9, padx=4)

        save_btn = (
            tb.Button(
                quick,
                text="Save Record",
                bootstyle="success",
                width=12,
                command=self.quick_save_record,
            )
            if TB
            else tk.Button(
                quick,
                text="Save Record",
                width=12,
                command=self.quick_save_record,
            )
        )
        save_btn.grid(row=1, column=10, padx=10)

        miss_btn = (
            tb.Button(
                quick,
                text="📥 Missed Milk",
                bootstyle="info-outline",
                width=12,
                command=lambda app=self: record_missed_milk(app),
            )
            if TB
            else tk.Button(
                quick,
                text="📥 Missed Milk",
                command=lambda app=self: record_missed_milk(app),
            )
        )
        miss_btn.grid(row=1, column=11, padx=6)

        # ---------------- Row 3 ----------------
        # Shift filter selector
        tk.Label(quick, text="Show Shift:", font=("Segoe UI", 9)).grid(
            row=2, column=0, sticky="e", padx=6, pady=6
        )
        self.shift_filter_cb = ttk.Combobox(
            quick,
            values=["Morning", "Evening", "All"],
            width=12,
            state="readonly",
        )
        self.shift_filter_cb.grid(row=2, column=1, padx=4, pady=6)
        self.shift_filter_cb.set(
            self.current_shift_filter
            if self.current_shift_filter in ("Morning", "Evening")
            else "All"
        )
        self.shift_filter_cb.bind(
            "<<ComboboxSelected>>", lambda e: self.on_shift_filter_change()
        )


        # Dashboard summary
        dash = (
            tb.Frame(self.root, padding=8)
            if TB else tk.Frame(self.root, padx=8, pady=8)
        )
        dash.pack(fill='x')

        self.summary_vars = {
            'farmers': tk.StringVar(value='Farmers: --'),
            'today_litres': tk.StringVar(value='Milk Today: -- L'),
            'today_amount': tk.StringVar(value='Amount Today: ₹--'),
            'sales_today': tk.StringVar(value='Sales Today: -- L'),
            'records': tk.StringVar(value='Total Records: --')
        }

        tk.Label(dash, textvariable=self.summary_vars['farmers'],
                 font=('Segoe UI', 10, 'bold')).grid(row=0, column=0, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['today_litres'],
                 font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['sales_today'],
                 font=('Segoe UI', 10, 'bold')).grid(row=0, column=2, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['today_amount'],
                 font=('Segoe UI', 10, 'bold')).grid(row=0, column=3, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['records'],
                 font=('Segoe UI', 10, 'bold')).grid(row=0, column=4, padx=8, sticky='w')

        # Notebook: Milk / Advances / Sales
        nb = tb.Notebook(self.root) if TB else ttk.Notebook(self.root)
        nb.pack(fill='both', expand=True, padx=12, pady=12)

        # Milk records tab
        t1 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb)
        nb.add(t1, text='Milk Records')
        cols = ('id', 'date', 'farmer_code', 'farmer_name', 'category',
                'shift', 'litres', 'fat', 'snf', 'rate', 'amount')
        self.records_tv = ttk.Treeview(t1, columns=cols, show='headings', height=18)
        for c in cols:
            self.records_tv.heading(
                c, text=c.replace('_', ' ').title(),
                command=lambda _c=c: self.sort_records(_c)
            )
            self.records_tv.column(c, width=100, anchor='center')
        self.records_tv.pack(fill='both', expand=True, side='left')
        vs = ttk.Scrollbar(t1, orient='vertical', command=self.records_tv.yview)
        vs.pack(side='right', fill='y')
        self.records_tv.configure(yscroll=vs.set)

        # Advances tab
        t2 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb)
        nb.add(t2, text='Advances')
        adv_cols = ('id', 'date', 'farmer_code', 'farmer_name', 'reason', 'amount')
        self.adv_tv = ttk.Treeview(t2, columns=adv_cols, show='headings', height=12)
        for c in adv_cols:
            self.adv_tv.heading(c, text=c.replace('_', ' ').title())
            self.adv_tv.column(c, width=120, anchor='center')
        self.adv_tv.pack(fill='both', expand=True)

        # Sales tab
        t3 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb)
        nb.add(t3, text='Sales')
        sales_cols = ('id', 'date', 'dairy_name', 'category', 'litres', 'fat', 'rate', 'amount')
        self.sales_tv = ttk.Treeview(t3, columns=sales_cols, show='headings', height=12)
        for c in sales_cols:
            self.sales_tv.heading(c, text=c.replace('_', ' ').title())
            self.sales_tv.column(c, width=110, anchor='center')
        self.sales_tv.pack(fill='both', expand=True)

        self.e_code.focus_set()
        self.load_all()

        

    def generate_monthly_bill_popup(self):
        """Popup to generate monthly bill for a single farmer with optional partial advance deduction."""
        popup = tk.Toplevel(self.root)
        popup.title("Generate Monthly Bill")
        popup.geometry("500x420")
        popup.resizable(True, True)   # or (True, False) if you only want horizontal


        frm = ttk.Frame(popup, padding=12)
        frm.pack(fill='both', expand=True)

        tk.Label(frm, text='Farmer Code:').grid(row=0, column=0, sticky='e', pady=6)
        code_e = ttk.Entry(frm)
        code_e.grid(row=0, column=1, pady=6)

        tk.Label(frm, text='Month:').grid(row=1, column=0, sticky='e', pady=6)

        months = [f"{y}-{m:02d}" for y in range(2020, date.today().year + 2) for m in range(1, 13)]
        default_month = date.today().strftime('%Y-%m')

        month_var = tk.StringVar(value=default_month)
        month_cb = ttk.Combobox(
            frm,
            values=sorted(list(set(months))),
            textvariable=month_var,
            state='readonly',
            width=12
        )
        month_cb.grid(row=1, column=1, pady=6)

        tk.Label(frm, text='Farmer Name:').grid(row=2, column=0, sticky='e', pady=6)
        name_lbl = tk.Label(frm, text='-', anchor='w')
        name_lbl.grid(row=2, column=1, sticky='w')

        tk.Label(frm, text='Earnings (₹):').grid(row=3, column=0, sticky='e', pady=6)
        earn_lbl = tk.Label(frm, text='0.00', anchor='w')
        earn_lbl.grid(row=3, column=1, sticky='w')

        tk.Label(frm, text='Advance Balance (before month):').grid(row=4, column=0, sticky='e', pady=6)
        bal_lbl = tk.Label(frm, text='0.00', anchor='w')
        bal_lbl.grid(row=4, column=1, sticky='w')

        tk.Label(frm, text='Deduction this month (₹):').grid(row=5, column=0, sticky='e', pady=6)
        ded_e = ttk.Entry(frm)
        ded_e.grid(row=5, column=1, pady=6)
        ded_e.insert(0, '0.00')

        # ✅ FETCH INFO
        def fetch_info(event=None):
            code_txt = code_e.get().strip()
            if not code_txt.isdigit():
                messagebox.showerror('Error', 'Farmer code must be numeric')
                return

            code = int(code_txt)

            conn = get_conn()
            cur = conn.cursor()
            cur.execute('SELECT name FROM farmers WHERE farmer_code=?', (code,))
            r = cur.fetchone()

            if not r:
                cur.close()
                conn.close()
                messagebox.showerror('Error', f'Farmer {code} not found')
                return

            name_lbl.config(text=r['name'])
            month = month_var.get()

            # ✅ Monthly earnings
            cur.execute(
                "SELECT IFNULL(SUM(amount),0) as total FROM milk_records "
                "WHERE farmer_code=? AND strftime('%Y-%m', date)=?",
                (code, month)
            )
            row = cur.fetchone()
            earnings = row['total'] if row else 0.0
            earn_lbl.config(text=f"{earnings:.2f}")

            # ✅ Advance balance
            bal = get_advance_balance(code, up_to_month=month)
            bal_lbl.config(text=f"{bal:.2f}")

            default_ded = min(bal, earnings)
            ded_e.delete(0, 'end')
            ded_e.insert(0, f"{default_ded:.2f}")

            cur.close()
            conn.close()

        tk.Button(frm, text='Fetch', command=fetch_info).grid(row=6, column=0, pady=12)

        # ✅ GENERATE & SAVE BILL
        def generate_and_save():
            code_txt = code_e.get().strip()
            if not code_txt.isdigit():
                messagebox.showerror('Error', 'Farmer code must be numeric')
                return

            code = int(code_txt)
            month = month_var.get()

            try:
                ded_amt = float(ded_e.get().strip())
            except Exception:
                messagebox.showerror('Error', 'Invalid deduction amount')
                return

            conn = None
            cur = None
            try:
                conn = get_conn()
                cur = conn.cursor()

                # ✅ Earnings again
                cur.execute(
                    "SELECT IFNULL(SUM(amount),0) as total FROM milk_records "
                    "WHERE farmer_code=? AND strftime('%Y-%m', date)=?",
                    (code, month)
                )
                row = cur.fetchone()
                earnings = row['total'] if row else 0.0

                prev_bal = get_advance_balance(code, up_to_month=month)

                if ded_amt < 0:
                    messagebox.showerror('Error', 'Deduction cannot be negative')
                    return
                if ded_amt > prev_bal:
                    messagebox.showerror('Error', 'Deduction cannot exceed advance balance')
                    return
                if ded_amt > earnings:
                    if not messagebox.askyesno('Confirm', 'Deduction exceeds earnings. Continue?'):
                        return

                # ✅ Record deduction
                ok = record_advance_deduction(code, month, ded_amt, note=f'User deduction for {month}')
                if not ok:
                    messagebox.showerror('Error', 'Failed to record deduction')
                    return

                net = round(earnings - ded_amt, 2)

                # ✅ PDF GENERATION
                try:
                    from reportlab.lib.pagesizes import letter
                    from reportlab.pdfgen import canvas as pdfcanvas

                    bill_name = f"Bill_{code}_{month}.pdf"
                    bill_path = os.path.join(BILLS_DIR, bill_name)

                    c = pdfcanvas.Canvas(bill_path, pagesize=letter)
                    c.setFont('Helvetica-Bold', 14)
                    c.drawString(72, 720, 'Shree Ganesh Dairy - Monthly Bill')

                    c.setFont('Helvetica', 11)
                    c.drawString(72, 690, f'Farmer Code: {code}')
                    c.drawString(72, 670, f'Farmer Name: {name_lbl.cget("text")}')
                    c.drawString(72, 650, f'Month: {month}')
                    c.drawString(72, 630, f'Earnings: ₹{earnings:.2f}')
                    c.drawString(72, 610, f'Advance Deducted: ₹{ded_amt:.2f}')
                    c.drawString(72, 590, f'Net Payable: ₹{net:.2f}')

                    c.showPage()
                    c.save()

                    messagebox.showinfo('Saved', f'Bill saved: {bill_path}')
                    open_folder(BILLS_DIR)
                    popup.destroy()
                    self.load_all()

                except Exception as e:
                    messagebox.showerror('Error', f'Failed to generate bill: {e}')

            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        tk.Button(frm, text='Generate Bill', command=generate_and_save).grid(row=6, column=1, pady=12)
    def on_shift_filter_change(self):
        """
        Called when user changes the 'Show Shift' combobox (Morning/Evening/All).
        Updates current_shift_filter and reloads dashboard + tables.
        """
        val = self.shift_filter_cb.get()

        # For 'Morning' or 'Evening' we filter, for 'All' we remove filter
        if val in ("Morning", "Evening"):
            self.current_shift_filter = val
        else:
            self.current_shift_filter = None

        # Recalculate summary + reload records with new filter
        self.load_all()

      # Start New Shift
    def start_new_shift(self):
        if not messagebox.askyesno(
            "Confirm",
            "Start new shift? This will clear quick-entry fields and filter dashboard to today's current shift."
        ):
            return

        # clear fields
        self.e_code.delete(0, 'end')
        for ch in self.cat_placeholder.winfo_children():
            ch.destroy()

        self.e_litres.delete(0, 'end')
        self.e_fat.delete(0, 'end')
        self.e_snf.delete(0, 'end')
        self.e_rate.delete(0, 'end')
        self.e_amount.delete(0, 'end')

        # Use DD/MM/YYYY in entry (GUI format)
        today_display = date.today().strftime('%d/%m/%Y')
        self.e_date.delete(0, 'end')
        self.e_date.insert(0, today_display)

        shift_now = get_shift_for_time()
        try:
            self.e_shift.set(shift_now)
        except Exception:
            pass

        # apply filter
        self.current_shift_filter = shift_now
        try:
            self.shift_filter_cb.set(shift_now)
        except Exception:
            pass

        self.load_all()

    # Fetch farmer info and update category widget
    def fetch_farmer_and_update(self):
        code_txt = self.e_code.get().strip()
        for ch in self.cat_placeholder.winfo_children():
            ch.destroy()

        if not code_txt:
            lbl = tk.Label(self.cat_placeholder, text='Unknown', font=('Segoe UI', 10))
            lbl.pack()
            self.entry_category_widget = None
            return

        try:
            code = int(code_txt)
        except ValueError:
            lbl = tk.Label(self.cat_placeholder, text='Invalid', font=('Segoe UI', 10))
            lbl.pack()
            self.entry_category_widget = None
            return

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, category FROM farmers WHERE farmer_code=?", (code,))
        r = cur.fetchone()
        cur.close()
        conn.close()

        if not r:
            lbl = tk.Label(self.cat_placeholder, text='Not Found', font=('Segoe UI', 10))
            lbl.pack()
            self.entry_category_widget = None
            return

        cat = r['category'] or 'Cow'
        if cat == 'Both':
            cb = ttk.Combobox(self.cat_placeholder, values=['Cow', 'Buffalo'], state='readonly', width=10)
            cb.pack()
            cb.set('Cow')
            self.entry_category_widget = cb
        else:
            lbl = tk.Label(self.cat_placeholder, text=cat, font=('Segoe UI', 10, 'bold'))
            lbl.pack()
            self.entry_category_widget = None
    def on_farmer_selected(self, event=None):
        """
        When a farmer is selected from the combobox, extract the code
        and update the Farmer Code entry, then reload farmer details.
        """
        text = self.farmer_combo.get()      # e.g. "1 - Ramesh Patil"
        if " - " not in text:
            return

        farmer_code = text.split(" - ", 1)[0].strip()

        # Put code into Farmer Code entry
        self.e_code.delete(0, tk.END)
        self.e_code.insert(0, farmer_code)

        # Use your existing logic to update category/rate/etc.
        self.fetch_farmer_and_update()

    def quick_calculate(self):
        try:
            fat = float(self.e_fat.get().strip()) if self.e_fat.get().strip() else 0.0
            # Use 8.0 as safe default SNF
            snf = float(self.e_snf.get().strip()) if self.e_snf.get().strip() else 8.0
            litres = float(self.e_litres.get().strip()) if self.e_litres.get().strip() else 0.0
        except ValueError:
            messagebox.showerror("Error", "Litres/FAT/SNF must be numeric")
            return

        cat = 'Cow'
        code_txt = self.e_code.get().strip()

        if code_txt:
            try:
                code = int(code_txt)
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("SELECT category FROM farmers WHERE farmer_code=?", (code,))
                r = cur.fetchone()
                cur.close()
                conn.close()

                if r and r['category']:
                    if r['category'] == 'Both':
                        if getattr(self, 'entry_category_widget', None) and isinstance(self.entry_category_widget, ttk.Combobox):
                            cat = self.entry_category_widget.get()
                        else:
                            cat = 'Cow'
                    else:
                        cat = r['category']
            except Exception:
                pass

        rate = find_rate_for(cat, round(fat, 1), snf)
        amt = round(litres * rate, 2)

        self.e_rate.delete(0, 'end')
        self.e_rate.insert(0, f"{rate:.2f}")
        self.e_amount.delete(0, 'end')
        self.e_amount.insert(0, f"{amt:.2f}")

    def quick_save_record(self):
        code_txt = self.e_code.get().strip()
        if not code_txt:
            messagebox.showerror("Error", "Enter Farmer Code")
            self.e_code.focus_set()
            return

        try:
            code = int(code_txt)
        except ValueError:
            messagebox.showerror("Error", "Farmer Code must be integer")
            self.e_code.focus_set()
            return

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, category FROM farmers WHERE farmer_code=?", (code,))
        farmer = cur.fetchone()

        if not farmer:
            cur.close()
            conn.close()
            messagebox.showerror("Error", f"Farmer code {code} not found. Use Manage Farmers to add.")
            return

        farmer_cat = farmer["category"] or "Cow"
        if farmer_cat == "Both":
            if getattr(self, "entry_category_widget", None) and isinstance(self.entry_category_widget, ttk.Combobox):
                category = self.entry_category_widget.get() or "Cow"
            else:
                messagebox.showerror("Error", "Select category for this entry")
                cur.close()
                conn.close()
                return
        else:
            category = farmer_cat

        date_txt = self.e_date.get().strip()
        try:
            # Use global normalizer (accepts DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, etc.)
            date_db = normalize_date_input(date_txt)
        except Exception:
            messagebox.showerror("Error", "Date must be in DD/MM/YYYY or YYYY-MM-DD format")
            cur.close()
            conn.close()
            return

        try:
            shift = self.e_shift.get().strip() or get_shift_for_time()
            litres = float(self.e_litres.get().strip())
            fat = round(float(self.e_fat.get().strip()), 1)
            # Use 8.0 as safe default SNF if empty
            snf = float(self.e_snf.get().strip()) if self.e_snf.get().strip() else 8.0
        except ValueError:
            messagebox.showerror("Error", "Numeric fields invalid")
            cur.close()
            conn.close()
            return

        # --- Prevent negative values ---
        if litres < 0 or fat < 0 or snf < 0:
            messagebox.showerror("Invalid Input", "❌ Litres, FAT, and SNF cannot be negative.")
            cur.close()
            conn.close()
            return

        if category == "Cow" and not (0.0 <= fat <= 8.0):
            messagebox.showwarning("Warning", "Cow FAT seems out of expected range (0–8). Continue?")
        if category == "Buffalo" and not (0.0 <= fat <= 12.0):
            messagebox.showwarning("Warning", "Buffalo FAT seems out of expected range (0–12). Continue?")

        try:
            rate = float(self.e_rate.get().strip()) if self.e_rate.get().strip() else find_rate_for(category, fat, snf)
        except ValueError:
            rate = find_rate_for(category, fat, snf)

        amount = round(litres * rate, 2)

        try:
            cur.execute(
                "INSERT INTO milk_records "
                "(farmer_code, date, shift, litres, fat, snf, rate, amount, category) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (code, date_db, shift, litres, fat, snf, rate, amount, category)
            )
            conn.commit()
            cur.close()
            conn.close()
            messagebox.showinfo("Saved", "Milk record saved")

            # --- Clear all quick-entry fields ---
            self.e_code.delete(0, "end")
            for ch in self.cat_placeholder.winfo_children():
                ch.destroy()
            self.e_litres.delete(0, "end")
            self.e_fat.delete(0, "end")
            self.e_snf.delete(0, "end")
            self.e_rate.delete(0, "end")
            self.e_amount.delete(0, "end")

            # Focus back for next entry
            self.e_code.focus_set()

            # Reload dashboard
            self.load_all()
        except sqlite3.Error as e:
            messagebox.showerror("DB Error", str(e))
            cur.close()
            conn.close()

    def _on_code_enter(self):
        """When Enter is pressed in Farmer Code field — fetch farmer info and move through all entry fields."""
        self.fetch_farmer_and_update()
        self._focus(self.e_litres)

        # --- Bind field navigation + auto-calculation ---
        self.e_litres.bind("<Return>", lambda e: [self.quick_calculate(), self._focus(self.e_fat)])
        self.e_fat.bind("<Return>", lambda e: [self.quick_calculate(), self._focus(self.e_snf)])
        self.e_snf.bind("<Return>", lambda e: [self.quick_calculate(), self._focus(self.e_rate)])
        self.e_rate.bind("<Return>", lambda e: [self.quick_calculate(), self._focus(self.e_amount)])
        self.e_amount.bind("<Return>", lambda e: self._confirm_and_save())

    def _confirm_and_save(self):
        """Ask before saving the milk record automatically."""
        if messagebox.askyesno("Confirm", "Save this milk record?"):
            self.quick_calculate()
            self.quick_save_record()


        # Manage farmers window (add/edit/delete) - delete removes related data and frees code for reuse
    def manage_farmers_window(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root)
        win.title("Manage Farmers")
        win.geometry("820x520")

        frm = tb.Frame(win, padding=8) if TB else tk.Frame(win, padx=8, pady=8)
        frm.pack(fill='both', expand=True)

        tree = ttk.Treeview(
            frm,
            columns=('farmer_code', 'name', 'village', 'contact', 'category'),
            show='headings'
        )
        for c in ('farmer_code', 'name', 'village', 'contact', 'category'):
            tree.heading(c, text=c.replace('_', ' ').title())
            tree.column(c, width=140, anchor='center')
        tree.pack(fill='both', expand=True, padx=6, pady=6)

        def load_farmers():
            for it in tree.get_children():
                tree.delete(it)
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT farmer_code,name,village,contact,category FROM farmers ORDER BY farmer_code")
            for r in cur.fetchall():
                tree.insert(
                    '',
                    'end',
                    values=(
                        r['farmer_code'],
                        r['name'],
                        r['village'] or '',
                        r['contact'] or '',
                        r['category'] or 'Cow'
                    )
                )
            cur.close()
            conn.close()

        load_farmers()

        ctrl = tb.Frame(win) if TB else tk.Frame(win)
        ctrl.pack(fill='x')

        def add_farmer():
            sub = tb.Toplevel(win) if TB else tk.Toplevel(win)
            sub.title("Add Farmer")
            sub.geometry("420x260")

            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12)
            f2.pack(fill='both', expand=True)

            conn = get_conn()
            next_code = get_next_farmer_code(conn)

            tk.Label(f2, text="Farmer Code:").grid(row=0, column=0, sticky='e', pady=6)
            tk.Label(f2, text=str(next_code), font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, sticky='w')

            tk.Label(f2, text="Name:").grid(row=1, column=0, sticky='e', pady=6)
            name_e = ttk.Entry(f2)
            name_e.grid(row=1, column=1, pady=6)

            tk.Label(f2, text="Village:").grid(row=2, column=0, sticky='e', pady=6)
            village_e = ttk.Entry(f2)
            village_e.grid(row=2, column=1, pady=6)

            tk.Label(f2, text="Contact:").grid(row=3, column=0, sticky='e', pady=6)
            contact_e = ttk.Entry(f2)
            contact_e.grid(row=3, column=1, pady=6)

            tk.Label(f2, text="Category:").grid(row=4, column=0, sticky='e', pady=6)
            cat_cb = ttk.Combobox(f2, values=['Cow', 'Buffalo', 'Both'], state='readonly')
            cat_cb.grid(row=4, column=1, pady=6)
            cat_cb.set('Cow')

            def do_add():
                name = name_e.get().strip()
                if not name:
                    messagebox.showerror("Error", "Name required")
                    return
                village = village_e.get().strip()
                contact = contact_e.get().strip()
                category = cat_cb.get().strip() or 'Cow'
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO farmers (farmer_code, name, village, contact, category) "
                        "VALUES (?,?,?,?,?)",
                        (next_code, name, village, contact, category)
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    messagebox.showinfo("Saved", f"Farmer added with Code {next_code}")
                    sub.destroy()
                    load_farmers()
                except sqlite3.IntegrityError:
                    messagebox.showerror("DB Error", "Farmer code conflict — try again")
                    conn.close()
                except sqlite3.Error as e:
                    messagebox.showerror("DB Error", str(e))
                    conn.close()

            if TB:
                tb.Button(f2, text='Save', bootstyle='success', command=do_add).grid(
                    row=5, column=0, columnspan=2, pady=8
                )
            else:
                tk.Button(f2, text='Save', command=do_add).grid(
                    row=5, column=0, columnspan=2, pady=8
                )

        def edit_farmer():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("Select", "Select a farmer to edit")
                return
            vals = tree.item(sel[0], 'values')
            fcode = vals[0]

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT name,village,contact,category FROM farmers WHERE farmer_code=?", (fcode,))
            r = cur.fetchone()
            cur.close()
            conn.close()

            if not r:
                messagebox.showerror("Error", "Farmer not found")
                return

            sub = tb.Toplevel(win) if TB else tk.Toplevel(win)
            sub.title(f"Edit Farmer {fcode}")
            sub.geometry("420x260")

            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12)
            f2.pack(fill='both', expand=True)

            tk.Label(f2, text="Farmer Code:").grid(row=0, column=0, sticky='e', pady=6)
            tk.Label(f2, text=str(fcode), font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, sticky='w')

            tk.Label(f2, text="Name:").grid(row=1, column=0, sticky='e', pady=6)
            name_e = ttk.Entry(f2)
            name_e.grid(row=1, column=1, pady=6)
            name_e.insert(0, r['name'])

            tk.Label(f2, text="Village:").grid(row=2, column=0, sticky='e', pady=6)
            village_e = ttk.Entry(f2)
            village_e.grid(row=2, column=1, pady=6)
            village_e.insert(0, r['village'] or '')

            tk.Label(f2, text="Contact:").grid(row=3, column=0, sticky='e', pady=6)
            contact_e = ttk.Entry(f2)
            contact_e.grid(row=3, column=1, pady=6)
            contact_e.insert(0, r['contact'] or '')

            tk.Label(f2, text="Category:").grid(row=4, column=0, sticky='e', pady=6)
            cat_cb = ttk.Combobox(f2, values=['Cow', 'Buffalo', 'Both'], state='readonly')
            cat_cb.grid(row=4, column=1, pady=6)
            cat_cb.set(r['category'] or 'Cow')

            def do_save():
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE farmers SET name=?,village=?,contact=?,category=? WHERE farmer_code=?",
                        (
                            name_e.get().strip(),
                            village_e.get().strip(),
                            contact_e.get().strip(),
                            cat_cb.get().strip(),
                            fcode
                        )
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    messagebox.showinfo("Saved", "Farmer updated")
                    sub.destroy()
                    load_farmers()
                    self.load_all()
                except sqlite3.Error as e:
                    messagebox.showerror("DB Error", str(e))

            if TB:
                tb.Button(f2, text='Save', bootstyle='success', command=do_save).grid(
                    row=5, column=0, columnspan=2, pady=8
                )
            else:
                tk.Button(f2, text='Save', command=do_save).grid(
                    row=5, column=0, columnspan=2, pady=8
                )

        def delete_farmer():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("Select", "Select a farmer to delete")
                return

            fcode = tree.item(sel[0], 'values')[0]

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT name FROM farmers WHERE farmer_code=?", (fcode,))
            r = cur.fetchone()

            if not r:
                cur.close()
                conn.close()
                messagebox.showerror("Error", "Farmer not found")
                return

            name = r['name']

            if not messagebox.askyesno(
                "Confirm",
                f"Delete farmer {fcode} ({name}) and ALL related records? This cannot be undone."
            ):
                cur.close()
                conn.close()
                return

            try:
                # Delete all related data as well
                cur.execute("DELETE FROM milk_records WHERE farmer_code=?", (fcode,))
                cur.execute("DELETE FROM advances WHERE farmer_code=?", (fcode,))
                cur.execute("DELETE FROM advance_deductions WHERE farmer_code=?", (fcode,))
                cur.execute("DELETE FROM farmers WHERE farmer_code=?", (fcode,))

                conn.commit()
                cur.close()
                conn.close()
                load_farmers()
                self.load_all()
                messagebox.showinfo("Deleted", f"Farmer {fcode} and related data deleted")
            except sqlite3.Error as e:
                messagebox.showerror("DB Error", str(e))
                cur.close()
                conn.close()

        if TB:
            tb.Button(ctrl, text='Add', bootstyle='success', command=add_farmer).pack(side='left', padx=6)
            tb.Button(ctrl, text='Edit', bootstyle='info', command=edit_farmer).pack(side='left', padx=6)
            tb.Button(ctrl, text='Delete', bootstyle='danger', command=delete_farmer).pack(side='left', padx=6)
            tb.Button(ctrl, text='Close', bootstyle='secondary', command=win.destroy).pack(side='right', padx=6)
        else:
            tk.Button(ctrl, text='Add', command=add_farmer).pack(side='left', padx=6)
            tk.Button(ctrl, text='Edit', command=edit_farmer).pack(side='left', padx=6)
            tk.Button(ctrl, text='Delete', command=delete_farmer).pack(side='left', padx=6)
            tk.Button(ctrl, text='Close', command=win.destroy).pack(side='right', padx=6)

    # Rate table window
    def rate_table_window(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root)
        win.title("Rate Table")
        win.geometry("560x420")

        frm = tb.Frame(win, padding=8) if TB else tk.Frame(win, padx=8, pady=8)
        frm.pack(fill='both', expand=True)

        tree = ttk.Treeview(frm, columns=('id', 'category', 'fat', 'snf', 'rate'), show='headings', height=14)
        for c in ('id', 'category', 'fat', 'snf', 'rate'):
            tree.heading(c, text=c.upper())
            tree.column(c, width=100, anchor='center')
        tree.pack(fill='both', expand=True, padx=6, pady=6)

        btnfrm = tb.Frame(win) if TB else tk.Frame(win)
        btnfrm.pack(fill='x')

        def load_table():
            for it in tree.get_children():
                tree.delete(it)
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id,category,fat,snf,rate FROM rate_table ORDER BY category, fat")
            for r in cur.fetchall():
                fat_v = r['fat'] if r['fat'] is not None else 0.0
                snf_v = r['snf'] if r['snf'] is not None else 0.0
                rate_v = r['rate'] if r['rate'] is not None else 0.0
                tree.insert(
                    '',
                    'end',
                    values=(
                        r['id'],
                        r['category'],
                        f"{fat_v:.1f}",
                        f"{snf_v:.1f}",
                        f"{rate_v:.2f}"
                    )
                )
            cur.close()
            conn.close()

        def add_rate():
            sub = tb.Toplevel(win) if TB else tk.Toplevel(win)
            sub.title("Add Rate")
            sub.geometry("360x240")

            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12)
            f2.pack(fill='both', expand=True)

            tk.Label(f2, text='Category:').grid(row=0, column=0, sticky='e', pady=6)
            cat_cb = ttk.Combobox(f2, values=['Cow', 'Buffalo'], state='readonly')
            cat_cb.grid(row=0, column=1, pady=6)
            cat_cb.set('Cow')

            tk.Label(f2, text='FAT:').grid(row=1, column=0, sticky='e', pady=6)
            fat_e = ttk.Entry(f2)
            fat_e.grid(row=1, column=1, pady=6)

            tk.Label(f2, text='SNF:').grid(row=2, column=0, sticky='e', pady=6)
            snf_e = ttk.Entry(f2)
            snf_e.grid(row=2, column=1, pady=6)

            tk.Label(f2, text='Rate:').grid(row=3, column=0, sticky='e', pady=6)
            rate_e = ttk.Entry(f2)
            rate_e.grid(row=3, column=1, pady=6)

            def do_add():
                try:
                    cat = cat_cb.get().strip()
                    f = float(fat_e.get().strip())
                    s = float(snf_e.get().strip())
                    r_v = float(rate_e.get().strip())
                except ValueError:
                    messagebox.showerror("Error", "Numeric values required")
                    return
                conn = get_conn()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO rate_table (category,fat,snf,rate) VALUES (?,?,?,?)",
                    (cat, round(f, 1), s, r_v)
                )
                conn.commit()
                cur.close()
                conn.close()
                sub.destroy()
                load_table()

            if TB:
                tb.Button(f2, text='Save', bootstyle='success', command=do_add).grid(
                    row=4, column=0, columnspan=2, pady=8
                )
            else:
                tk.Button(f2, text='Save', command=do_add).grid(
                    row=4, column=0, columnspan=2, pady=8
                )

        def del_rate():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("Select", "Select a rate row")
                return
            rid = tree.item(sel[0], 'values')[0]
            if not messagebox.askyesno("Confirm", "Delete selected rate?"):
                return
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM rate_table WHERE id=?", (rid,))
            conn.commit()
            cur.close()
            conn.close()
            load_table()

        if TB:
            tb.Button(btnfrm, text='Add', bootstyle='success', command=add_rate).pack(side='left', padx=6)
            tb.Button(btnfrm, text='Delete', bootstyle='danger', command=del_rate).pack(side='left', padx=6)
            tb.Button(btnfrm, text='Close', bootstyle='secondary', command=win.destroy).pack(side='right', padx=6)
        else:
            tk.Button(btnfrm, text='Add', command=add_rate).pack(side='left', padx=6)
            tk.Button(btnfrm, text='Delete', command=del_rate).pack(side='left', padx=6)
            tk.Button(btnfrm, text='Close', command=win.destroy).pack(side='right', padx=6)

        load_table()


    def record_advance_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root)
        win.title("Record Advance")
        win.geometry("420x260")

        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12)
        frm.pack(fill='both', expand=True)

        tk.Label(frm, text='Farmer Code:').grid(row=0, column=0, sticky='e', pady=6)
        code_e = ttk.Entry(frm)
        code_e.grid(row=0, column=1, pady=6)

        # CHANGE LABEL + DEFAULT DATE FORMAT
        tk.Label(frm, text='Date (DD/MM/YYYY):').grid(row=1, column=0, sticky='e', pady=6)
        date_e = ttk.Entry(frm)
        date_e.grid(row=1, column=1, pady=6)
        date_e.insert(0, date.today().strftime('%d/%m/%Y'))   # FIXED

        tk.Label(frm, text='Reason:').grid(row=2, column=0, sticky='e', pady=6)
        reason_e = ttk.Entry(frm)
        reason_e.grid(row=2, column=1, pady=6)

        tk.Label(frm, text='Amount (₹):').grid(row=3, column=0, sticky='e', pady=6)
        amt_e = ttk.Entry(frm)
        amt_e.grid(row=3, column=1, pady=6)

        def save_adv():
            code_txt = code_e.get().strip()
            if not code_txt:
                messagebox.showerror("Error", "Enter Farmer Code")
                return
            try:
                code = int(code_txt)
            except ValueError:
                messagebox.showerror("Error", "Farmer Code must be integer")
                return

            # DATE + AMOUNT VALIDATION
            try:
                date_txt = date_e.get().strip()
                date_db = normalize_date_input(date_txt)
                amount = float(amt_e.get().strip())
            except ValueError:
                messagebox.showerror("Error", "Check date format (DD/MM/YYYY) or amount")
                return
            except Exception:
                messagebox.showerror("Error", "Check date/amount format")
                return

            if amount < 0:
                messagebox.showerror("Error", "Advance amount cannot be negative")
                return

            conn = get_conn()
            cur = conn.cursor()

            cur.execute("SELECT name FROM farmers WHERE farmer_code=?", (code,))
            f = cur.fetchone()
            if not f:
                cur.close()
                conn.close()
                messagebox.showerror("Error", "Farmer not found")
                return

            try:
                cur.execute(
                    "INSERT INTO advances (farmer_code, date, reason, amount) "
                    "VALUES (?, ?, ?, ?)",
                    (code, date_db, reason_e.get().strip(), amount)
                )
                conn.commit()
                cur.close()
                conn.close()
                messagebox.showinfo("Saved", "Advance recorded")
                win.destroy()
                self.load_all()
            except sqlite3.Error as e:
                messagebox.showerror("DB Error", str(e))
                cur.close()
                conn.close()

        if TB:
            tb.Button(frm, text='Save Advance', bootstyle='success', command=save_adv)\
                .grid(row=4, column=0, columnspan=2, pady=10)
        else:
            tk.Button(frm, text='Save Advance', command=save_adv)\
                .grid(row=4, column=0, columnspan=2, pady=10)

    def record_sale_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root)
        win.title("Record Sale")
        win.geometry("420x320")

        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12)
        frm.pack(fill='both', expand=True)

        # --- DATE FIELD (DD/MM/YYYY) ---
        tk.Label(frm, text='Date (DD/MM/YYYY):').grid(row=0, column=0, sticky='e', pady=6)
        date_e = ttk.Entry(frm)
        date_e.grid(row=0, column=1, pady=6)
        date_e.insert(0, date.today().strftime('%d/%m/%Y'))   # fixed

        tk.Label(frm, text='Dairy Name:').grid(row=1, column=0, sticky='e', pady=6)
        dairy_e = ttk.Entry(frm)
        dairy_e.grid(row=1, column=1, pady=6)

        tk.Label(frm, text='Category:').grid(row=2, column=0, sticky='e', pady=6)
        cat_cb = ttk.Combobox(frm, values=['Cow', 'Buffalo'], state='readonly')
        cat_cb.grid(row=2, column=1, pady=6)
        cat_cb.set('Cow')

        tk.Label(frm, text='Litres:').grid(row=3, column=0, sticky='e', pady=6)
        litres_e = ttk.Entry(frm)
        litres_e.grid(row=3, column=1, pady=6)

        tk.Label(frm, text='Fat (%):').grid(row=4, column=0, sticky='e', pady=6)
        fat_e = ttk.Entry(frm)
        fat_e.grid(row=4, column=1, pady=6)

        tk.Label(frm, text='Rate (₹/L):').grid(row=5, column=0, sticky='e', pady=6)
        rate_e = ttk.Entry(frm)
        rate_e.grid(row=5, column=1, pady=6)

        tk.Label(frm, text='Amount (₹):').grid(row=6, column=0, sticky='e', pady=6)
        amount_e = ttk.Entry(frm)
        amount_e.grid(row=6, column=1, pady=6)

        # --- CALCULATE BUTTON ---
        def calc_sale():
            try:
                l = float(litres_e.get().strip()) if litres_e.get().strip() else 0.0
                r = float(rate_e.get().strip()) if rate_e.get().strip() else 0.0
            except ValueError:
                messagebox.showerror("Error", "Enter numeric litres/rate")
                return

            amount_e.delete(0, 'end')
            amount_e.insert(0, f"{l * r:.2f}")

        # --- SAVE SALE ---
        def save_sale():
            try:
                d_txt = date_e.get().strip()
                # validate and normalize
                d_sql = normalize_date_input(d_txt)

                dairy = dairy_e.get().strip()
                cat = cat_cb.get().strip()
                l = float(litres_e.get().strip())
                fat = float(fat_e.get().strip()) if fat_e.get().strip() else 0.0
                rate = float(rate_e.get().strip()) if rate_e.get().strip() else 0.0

                amt = (
                    float(amount_e.get().strip())
                    if amount_e.get().strip()
                    else round(l * rate, 2)
                )
            except ValueError as ve:
                messagebox.showerror("Error", f"Check fields: {ve}")
                return
            except Exception:
                messagebox.showerror("Error", "Check all fields (date/numbers)")
                return

            if l < 0 or rate < 0 or amt < 0:
                messagebox.showerror("Error", "Litres/Rate/Amount cannot be negative")
                return

            conn = get_conn()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO sales (date, dairy_name, category, litres, fat, rate, amount)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (d_sql, dairy, cat, l, fat, rate, amt)
                )

                conn.commit()
                cur.close()
                conn.close()

                messagebox.showinfo("Saved", "Sale recorded")
                win.destroy()
                self.load_all()

            except sqlite3.Error as e:
                messagebox.showerror("DB Error", str(e))
                cur.close()
                conn.close()

        if TB:
            tb.Button(frm, text='Calculate', bootstyle='info', command=calc_sale)\
                .grid(row=7, column=0, pady=8)
            tb.Button(frm, text='Save Sale', bootstyle='success', command=save_sale)\
                .grid(row=7, column=1, pady=8)
        else:
            tk.Button(frm, text='Calculate', command=calc_sale)\
                .grid(row=7, column=0, pady=8)
            tk.Button(frm, text='Save Sale', command=save_sale)\
                .grid(row=7, column=1, pady=8)

    def select_month_for_report(self):
        """Popup for selecting month and year before generating consolidated report."""
        popup = tk.Toplevel(self.root)
        popup.title("Select Month")
        popup.geometry("320x220")
        popup.resizable(False, False)
        popup.configure(bg="#f0f0f0")

        tk.Label(
            popup,
            text="📅 Select Month and Year",
            font=("Segoe UI", 13, "bold"),
            bg="#f0f0f0",
            fg="#1a5276"
        ).pack(pady=10)

        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        month_var = tk.StringVar(value=months[date.today().month - 1])
        year_var = tk.StringVar(value=str(date.today().year))

        month_cb = ttk.Combobox(
            popup,
            values=months,
            textvariable=month_var,
            state="readonly",
            width=15
        )
        month_cb.pack(pady=5)

        year_cb = ttk.Combobox(
            popup,
            values=[str(y) for y in range(2020, date.today().year + 2)],
            textvariable=year_var,
            state="readonly",
            width=8
        )
        year_cb.pack(pady=5)

        def generate_selected_month():
            m_index = months.index(month_var.get()) + 1
            month_str = f"{int(year_var.get()):04d}-{m_index:02d}"
            popup.destroy()
            self.generate_consolidated_monthly_report(month_str)

        ttk.Button(popup, text="Generate Report", command=generate_selected_month).pack(pady=15)
    def select_month_for_sales_report(self):
        """Popup for selecting month & year for Sales (milk sold) report."""
        popup = tk.Toplevel(self.root)
        popup.title("Select Month – Sales Report")
        popup.geometry("320x220")
        popup.resizable(False, False)
        popup.configure(bg="#f0f0f0")

        tk.Label(
            popup,
            text="🧾 Milk Sales – Select Month & Year",
            font=("Segoe UI", 12, "bold"),
            bg="#f0f0f0",
            fg="#1a5276"
        ).pack(pady=10)

        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        month_var = tk.StringVar(value=months[date.today().month - 1])
        year_var = tk.StringVar(value=str(date.today().year))

        month_cb = ttk.Combobox(
            popup,
            values=months,
            textvariable=month_var,
            state="readonly",
            width=15
        )
        month_cb.pack(pady=5)

        year_cb = ttk.Combobox(
            popup,
            values=[str(y) for y in range(2020, date.today().year + 2)],
            textvariable=year_var,
            state="readonly",
            width=8
        )
        year_cb.pack(pady=5)

        def generate_selected_month():
            m_index = months.index(month_var.get()) + 1
            month_str = f"{int(year_var.get()):04d}-{m_index:02d}"  # YYYY-MM
            popup.destroy()
            self.generate_sales_report(month_str)

        ttk.Button(popup, text="Generate Sales Report", command=generate_selected_month)\
            .pack(pady=15)
    def generate_sales_report(self, month_str=None):
        """
        Generate monthly sales report (milk sold in 'Sales' tab).
        Creates a PDF with all sales rows for that month + totals.
        """
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas as pdfcanvas
        from datetime import datetime

        conn = get_conn()
        cur = conn.cursor()

        # Default: current month if not provided
        if not month_str:
            month_str = date.today().strftime("%Y-%m")

        # Fetch all sales for the given month
        cur.execute(
            """
            SELECT date, dairy_name, category, litres, fat, rate, amount
            FROM sales
            WHERE strftime('%Y-%m', date)=?
            ORDER BY date, dairy_name
            """,
            (month_str,)
        )
        rows = cur.fetchall()

        if not rows:
            cur.close()
            conn.close()
            messagebox.showinfo(
                "No Data",
                f"No sales records found for {month_str}."
            )
            return

        # Prepare file path
        report_name = f"Sales_Report_{month_str}.pdf"
        report_path = os.path.join(REPORTS_DIR, report_name)

        c = pdfcanvas.Canvas(report_path, pagesize=letter)
        width, height = letter

        # For nice "Month Year" display
        try:
            nice_month = datetime.strptime(month_str + "-01", "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            nice_month = month_str

        def draw_header():
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, height - 50, "Shree Ganesh Dairy – Milk Sales Report")

            c.setFont("Helvetica", 10)
            c.drawString(50, height - 65, f"Month: {nice_month}")
            c.drawRightString(
                width - 50,
                height - 65,
                date.today().strftime("Printed on: %d-%m-%Y")
            )

            # Table header
            y_header = height - 90
            c.setFont("Helvetica-Bold", 9)
            c.drawString(50,  y_header, "Date")
            c.drawString(110, y_header, "Dairy")
            c.drawString(260, y_header, "Cat")
            c.drawRightString(330, y_header, "Litres")
            c.drawRightString(380, y_header, "Fat")
            c.drawRightString(430, y_header, "Rate")
            c.drawRightString(500, y_header, "Amount")

            c.line(45, y_header - 3, width - 45, y_header - 3)
            return y_header - 15

        y = draw_header()
        c.setFont("Helvetica", 9)

        total_litres = 0.0
        total_amount = 0.0

        for r in rows:
            if y < 60:   # new page
                c.showPage()
                y = draw_header()
                c.setFont("Helvetica", 9)

            # Safe values
            raw_date = r["date"]
            try:
                disp_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%d-%m-%Y")
            except Exception:
                disp_date = raw_date

            dairy = (r["dairy_name"] or "")[:20]
            cat = r["category"] or ""
            litres = r["litres"] or 0.0
            fat = r["fat"] or 0.0
            rate = r["rate"] or 0.0
            amount = r["amount"] or 0.0

            total_litres += litres
            total_amount += amount

            # Row
            c.drawString(50,  y, disp_date)
            c.drawString(110, y, dairy)
            c.drawString(260, y, cat)
            c.drawRightString(330, y, f"{litres:.2f}")
            c.drawRightString(380, y, f"{fat:.1f}")
            c.drawRightString(430, y, f"{rate:.2f}")
            c.drawRightString(500, y, f"{amount:.2f}")

            y -= 14

        # Totals row
        if y < 80:
            c.showPage()
            y = draw_header()
            c.setFont("Helvetica", 9)

        c.line(45, y, width - 45, y)
        y -= 14
        c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, "TOTAL")
        c.drawRightString(330, y, f"{total_litres:.2f} L")
        c.drawRightString(500, y, f"₹{total_amount:.2f}")

        c.showPage()
        c.save()

        cur.close()
        conn.close()

        messagebox.showinfo(
            "Report Generated",
            f"Sales report saved:\n{report_path}"
        )
        # Open reports folder so user can see it
        open_folder(REPORTS_DIR)

    def generate_consolidated_monthly_report(self, month_str=None):
        """Generate clean compact A4 monthly report (header 1-line, tables touching, no wasted space)."""
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
        )
        from reportlab.lib.units import mm
        from datetime import date, datetime
        import webbrowser

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT farmer_code, name, category FROM farmers ORDER BY farmer_code")
        farmers = cur.fetchall()

        if not farmers:
            cur.close()
            conn.close()
            messagebox.showinfo("Info", "No farmers found to generate report.")
            return

        today = date.today()
        full_date_str = today.strftime("%d %B %Y")
        if not month_str:
            month_str = today.strftime("%Y-%m")

        report_path = os.path.join(REPORTS_DIR, f"Combined_Report_{month_str}.pdf")

        # --- DOCUMENT SETUP ---
        doc = SimpleDocTemplate(
            report_path,
            pagesize=A4,
            leftMargin=25,
            rightMargin=25,
            topMargin=25,
            bottomMargin=25
        )

        elements = []

        # --- STYLES ---
        header_style = ParagraphStyle(
            "Header",
            fontSize=14,
            alignment=1,
            leading=16,
            spaceAfter=2,
            fontName="Helvetica-Bold"
        )
        sub_style = ParagraphStyle(
            "Sub",
            fontSize=9,
            alignment=1,
            textColor=colors.darkgray,
            spaceAfter=4
        )
        normal_style = ParagraphStyle(
            "Normal",
            fontSize=9,
            leading=11,
            spaceAfter=2
        )

        base_table_style = TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.4, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
        ])

        # --- GENERATE PER FARMER ---
        for f in farmers:
            fcode, fname, cat = f["farmer_code"], f["name"], f["category"]
            categories = ["Cow", "Buffalo"] if str(cat).lower() == "both" else [cat]

            # 🔹 Fetch all advances ONCE per farmer for the month
            cur.execute(
                """SELECT date, reason, amount FROM advances
                WHERE farmer_code=? AND strftime('%Y-%m', date)=?
                ORDER BY date""",
                (fcode, month_str)
            )
            all_adv = cur.fetchall()
            all_adv_total = sum((a["amount"] or 0.0) for a in all_adv)

            for idx, category in enumerate(categories):
                is_primary_category = (idx == 0)  # only first category will carry the advance

                # Fetch milk records
                cur.execute(
                    """SELECT date, shift, litres, fat, snf, rate, amount
                    FROM milk_records
                    WHERE farmer_code=? AND category=? AND strftime('%Y-%m', date)=?
                    ORDER BY date, shift""",
                    (fcode, category, month_str)
                )
                rows = cur.fetchall()
                if not rows:
                    continue

                # Organize by date
                rec = {}
                for r in rows:
                    d = datetime.strptime(r["date"], "%Y-%m-%d").strftime("%d-%b")
                    if d not in rec:
                        rec[d] = {"M": ["-"] * 5, "E": ["-"] * 5}

                    litres_v = r['litres'] if r['litres'] is not None else 0.0
                    fat_v = r['fat'] if r['fat'] is not None else 0.0
                    snf_v = r['snf'] if r['snf'] is not None else 0.0
                    rate_v = r['rate'] if r['rate'] is not None else 0.0
                    amount_v = r['amount'] if r['amount'] is not None else 0.0

                    vals = [
                        f"{litres_v:.2f}",
                        f"{fat_v:.1f}",
                        f"{snf_v:.1f}",
                        f"{rate_v:.2f}",
                        f"{amount_v:.2f}"
                    ]

                    if str(r["shift"]).startswith("M"):
                        rec[d]["M"] = vals
                    else:
                        rec[d]["E"] = vals

                # Build Milk data
                milk_data = [[
                    "Date", "M-Ltr", "M-Fat", "M-SNF", "M-Rate", "M-Amt",
                    "E-Ltr", "E-Fat", "E-SNF", "E-Rate", "E-Amt"
                ]]

                for d in sorted(rec.keys(), key=lambda x: datetime.strptime(x, "%d-%b")):
                    milk_data.append([d] + rec[d]["M"] + rec[d]["E"])

                total_lit = sum((r["litres"] or 0.0) for r in rows)
                total_amt = sum((r["amount"] or 0.0) for r in rows)

                # 🔹 Decide advances for this category
                if is_primary_category:
                    adv = all_adv
                    adv_total = all_adv_total
                else:
                    adv = []          # no advances on second category
                    adv_total = 0.0

                net_pay = total_amt - adv_total

                # --- HEADER (one line) ---
                header_text = "<b>Shree Ganesh Dairy — Monthly Farmer Report</b>"
                elements.append(Paragraph(header_text, header_style))

                info = (
                    f"<b>Name:</b> {fname} &nbsp;&nbsp;&nbsp; "
                    f"<b>Code:</b> {fcode} &nbsp;&nbsp;&nbsp; "
                    f"<b>Category:</b> {category} &nbsp;&nbsp;&nbsp; "
                    f"<b>Date:</b> {full_date_str}"
                )
                elements.append(Paragraph(info, normal_style))

                # No large spacer, only minimal
                elements.append(Spacer(1, 3))

                # --- MILK TABLE ---
                milk_table = Table(milk_data, repeatRows=1)
                milk_table.setStyle(base_table_style)
                milk_table.setStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d9eaf7")),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold')
                ])
                elements.append(milk_table)

                # Tight spacing
                elements.append(Spacer(1, 4))

                # --- ADVANCE TABLE ---
                elements.append(Paragraph("<b>Advance Details</b>", normal_style))

                adv_data = [["Date", "Reason", "Amount (₹)"]]
                if adv:
                    for a in adv:
                        adv_date = datetime.strptime(a["date"], "%Y-%m-%d").strftime("%d-%b")
                        adv_reason = a["reason"] or ""
                        adv_amt = a["amount"] if a["amount"] is not None else 0.0
                        adv_data.append([
                            adv_date,
                            adv_reason,
                            f"{adv_amt:.2f}"
                        ])
                else:
                    # if second category but farmer has some advances -> show info
                    if (not is_primary_category) and all_adv_total > 0:
                        adv_data.append(["-", "Advance adjusted in first category", "-"])
                    else:
                        adv_data.append(["-", "No advances recorded", "-"])

                adv_table = Table(adv_data, colWidths=[65, 260, 70])
                adv_table.setStyle(base_table_style)
                adv_table.setStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d9eaf7")),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold')
                ])
                elements.append(adv_table)

                elements.append(Spacer(1, 4))

                # --- TOTAL SUMMARY TABLE ---
                total_data = [
                    ["Total Milk (L)", f"{total_lit:.2f}"],
                    ["Total Amount (₹)", f"{total_amt:.2f}"],
                    ["Total Advances (₹)", f"{adv_total:.2f}"],
                    ["Net Payable (₹)", f"{net_pay:.2f}"]
                ]

                total_table = Table(total_data, colWidths=[180, 120])
                total_table.setStyle([
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1a5276")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
                    ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                ])
                elements.append(total_table)

                # Page break for next farmer / category
                elements.append(PageBreak())

        # close DB after we’re done fetching
        cur.close()
        conn.close()

        doc.build(elements)
        messagebox.showinfo("Report Generated", f"Combined monthly report saved:\n{report_path}")
        webbrowser.open_new(report_path)



    def change_password_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root)
        win.title("Change Password")
        win.geometry("420x200")

        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12)
        frm.pack(fill='both', expand=True)

        tk.Label(frm, text='Current Password:').grid(row=0, column=0, sticky='e', pady=6)
        cur_e = ttk.Entry(frm, show='*')
        cur_e.grid(row=0, column=1, pady=6)

        tk.Label(frm, text='New Password:').grid(row=1, column=0, sticky='e', pady=6)
        new_e = ttk.Entry(frm, show='*')
        new_e.grid(row=1, column=1, pady=6)

        tk.Label(frm, text='Confirm New:').grid(row=2, column=0, sticky='e', pady=6)
        conf_e = ttk.Entry(frm, show='*')
        conf_e.grid(row=2, column=1, pady=6)

        def do_change():
            curp = cur_e.get().strip()
            newp = new_e.get().strip()
            conf = conf_e.get().strip()

            if not (curp and newp and conf):
                messagebox.showerror("Error", "Fill all fields")
                return
            if newp != conf:
                messagebox.showerror("Error", "Passwords do not match")
                return

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT password_hash FROM users WHERE username=?", (self.current_user,))
            row = cur.fetchone()

            if not row or row['password_hash'] != hash_password(curp):
                messagebox.showerror("Error", "Current password incorrect")
                cur.close()
                conn.close()
                return

            cur.execute("UPDATE users SET password_hash=? WHERE username=?", (hash_password(newp), self.current_user))
            conn.commit()
            cur.close()
            conn.close()
            messagebox.showinfo("Saved", "Password changed")
            win.destroy()

        if TB:
            tb.Button(frm, text='Change Password', bootstyle='success', command=do_change)\
                .grid(row=3, column=0, columnspan=2, pady=8)
        else:
            tk.Button(frm, text='Change Password', command=do_change)\
                .grid(row=3, column=0, columnspan=2, pady=8)

    # Load all data and refresh UI; respects current_shift_filter when set
    def load_all(self):
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM farmers")
        total_farmers = cur.fetchone()['c'] or 0

        today = date.today().strftime('%Y-%m-%d')

        # Use shift filter only for Morning/Evening, not for 'All'/None
        use_shift_filter = self.current_shift_filter in ('Morning', 'Evening')

        # query milk and sales depending on shift filter
        if use_shift_filter:
            cur.execute(
                "SELECT category, SUM(litres) AS l, SUM(amount) AS a "
                "FROM milk_records WHERE date=? AND shift=? GROUP BY category",
                (today, self.current_shift_filter)
            )
            milk_rows = cur.fetchall()
            cur.execute(
                "SELECT category, SUM(litres) AS l FROM sales WHERE date=? GROUP BY category",
                (today,)
            )
            sales_rows = cur.fetchall()
        else:
            cur.execute(
                "SELECT category, SUM(litres) AS l, SUM(amount) AS a "
                "FROM milk_records WHERE date=? GROUP BY category",
                (today,)
            )
            milk_rows = cur.fetchall()
            cur.execute(
                "SELECT category, SUM(litres) AS l FROM sales WHERE date=? GROUP BY category",
                (today,)
            )
            sales_rows = cur.fetchall()

        milk_by_cat = {
            r['category'] or 'Unknown': {
                'litres': r['l'] or 0.0,
                'amount': r['a'] or 0.0
            }
            for r in milk_rows
        }

        total_today = sum(v['litres'] for v in milk_by_cat.values())
        total_amount_today = sum(v['amount'] for v in milk_by_cat.values())

        sales_by_cat = {r['category'] or 'Unknown': r['l'] or 0.0 for r in sales_rows}
        total_sales_today = sum(sales_by_cat.values())

        total_records = cur.execute("SELECT COUNT(*) AS c FROM milk_records").fetchone()['c'] or 0

        self.summary_vars['farmers'].set(f"Farmers: {total_farmers}")

        def cat_breakdown_str(d):
            parts = []
            for k in ('Cow', 'Buffalo'):
                if k in d:
                    parts.append(f"{k}: {d[k]['litres'] if isinstance(d[k], dict) else d[k]:.2f} L")
            return ' | '.join(parts) if parts else 'None'

        milk_break = cat_breakdown_str(milk_by_cat)

        if use_shift_filter:
            self.summary_vars['today_litres'].set(
                f"Milk Today ({self.current_shift_filter}): {total_today:.2f} L ({milk_break})"
            )
        else:
            self.summary_vars['today_litres'].set(
                f"Milk Today: {total_today:.2f} L ({milk_break})"
            )

        sales_parts = []
        for k in ('Cow', 'Buffalo'):
            if k in sales_by_cat:
                sales_parts.append(f"{k}: {sales_by_cat[k]:.2f} L")
        sales_break = ' | '.join(sales_parts) if sales_parts else 'None'

        self.summary_vars['sales_today'].set(
            f"Sales Today: {total_sales_today:.2f} L ({sales_break})"
        )
        self.summary_vars['today_amount'].set(f"Amount Today: ₹{total_amount_today:.2f}")
        self.summary_vars['records'].set(f"Total Records: {total_records}")

        # reload records (respect shift filter)
        for it in self.records_tv.get_children():
            self.records_tv.delete(it)

        if use_shift_filter:
            cur.execute(
                """
                SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,
                       m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
                FROM milk_records m
                LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
                WHERE m.date=? AND m.shift=?
                ORDER BY m.date DESC, m.id DESC LIMIT 1000
                """,
                (today, self.current_shift_filter)
            )
        else:
            cur.execute(
                """
                SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,
                       m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
                FROM milk_records m
                LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
                ORDER BY m.date DESC, m.id DESC LIMIT 1000
                """
            )

        for r in cur.fetchall():
            # Safe formatting: if a numeric field is None, display 0.00
            litres_v = r['litres'] if r['litres'] is not None else 0.0
            fat_v = r['fat'] if r['fat'] is not None else 0.0
            snf_v = r['snf'] if r['snf'] is not None else 0.0
            rate_v = r['rate'] if r['rate'] is not None else 0.0
            amount_v = r['amount'] if r['amount'] is not None else 0.0

            self.records_tv.insert(
                '',
                'end',
                values=(
                    r['id'],
                    r['date'],
                    r['farmer_code'] or '',
                    r['farmer_name'] or '',
                    r['category'] or '',
                    r['shift'],
                    f"{litres_v:.2f}",
                    f"{fat_v:.2f}",
                    f"{snf_v:.2f}",
                    f"{rate_v:.2f}",
                    f"{amount_v:.2f}"
                )
            )

        # advances
        for it in self.adv_tv.get_children():
            self.adv_tv.delete(it)
        cur.execute(
            """
            SELECT a.id,a.date,a.farmer_code,f.name AS farmer_name,a.reason,a.amount
            FROM advances a
            LEFT JOIN farmers f ON a.farmer_code=f.farmer_code
            ORDER BY a.date DESC, a.id DESC LIMIT 500
            """
        )
        for r in cur.fetchall():
            self.adv_tv.insert(
                '',
                'end',
                values=(
                    r['id'],
                    r['date'],
                    r['farmer_code'] or '',
                    r['farmer_name'] or '',
                    r['reason'] or '',
                    f"{(r['amount'] or 0.0):.2f}"
                )
            )

        # sales
        for it in self.sales_tv.get_children():
            self.sales_tv.delete(it)
        cur.execute(
            "SELECT id,date,dairy_name,category,litres,fat,rate,amount "
            "FROM sales ORDER BY date DESC, id DESC LIMIT 500"
        )
        for r in cur.fetchall():
            self.sales_tv.insert(
                '',
                'end',
                values=(
                    r['id'],
                    r['date'],
                    r['dairy_name'] or '',
                    r['category'] or '',
                    f"{(r['litres'] or 0.0):.2f}",
                    f"{(r['fat'] or 0.0):.2f}",
                    f"{(r['rate'] or 0.0):.2f}",
                    f"{(r['amount'] or 0.0):.2f}"
                )
            )

        cur.close()
        conn.close()

    def sort_records(self, col):
        colmap = {
            'id': 'm.id',
            'date': 'm.date',
            'farmer_code': 'm.farmer_code',
            'farmer_name': 'f.name',
            'category': 'm.category',
            'shift': 'm.shift',
            'litres': 'm.litres',
            'fat': 'm.fat',
            'snf': 'm.snf',
            'rate': 'm.rate',
            'amount': 'm.amount'
        }
        dbcol = colmap.get(col, 'm.date')

        conn = get_conn()
        cur = conn.cursor()
        sql = f"""
        SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,
               m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
        FROM milk_records m
        LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
        ORDER BY {dbcol} DESC LIMIT 1000
        """
        cur.execute(sql)

        for it in self.records_tv.get_children():
            self.records_tv.delete(it)

        for r in cur.fetchall():
            litres_v = r['litres'] if r['litres'] is not None else 0.0
            fat_v = r['fat'] if r['fat'] is not None else 0.0
            snf_v = r['snf'] if r['snf'] is not None else 0.0
            rate_v = r['rate'] if r['rate'] is not None else 0.0
            amount_v = r['amount'] if r['amount'] is not None else 0.0

            self.records_tv.insert(
                '',
                'end',
                values=(
                    r['id'],
                    r['date'],
                    r['farmer_code'] or '',
                    r['farmer_name'] or '',
                    r['category'] or '',
                    r['shift'],
                    f"{litres_v:.2f}",
                    f"{fat_v:.2f}",
                    f"{snf_v:.2f}",
                    f"{rate_v:.2f}",
                    f"{amount_v:.2f}"
                )
            )

        cur.close()
        conn.close()




# main
def main():
    ensure_dirs()
    root = tb.Window(themename=THEME) if TB else tk.Tk()
    app = DairyApp(root)
    root.mainloop()


# --- GUI helper to record missed milk (invoked from dashboard) ---
def record_missed_milk(app):
    """Open a popup (same style as Record Milk) to record missed milk entries.
    Allows past and current dates.
    """
    win = tk.Toplevel(app.root)
    win.title("📥 Record Missed Milk (Power Failure Entry)")
    win.geometry("430x500")
    win.resizable(False, False)

    frm = ttk.Frame(win, padding=15)
    frm.pack(fill="both", expand=True)

    # Farmer Code
    ttk.Label(frm, text="Farmer Code:").grid(row=0, column=0, sticky="w", pady=5)
    farmer_code = ttk.Entry(frm)
    farmer_code.grid(row=0, column=1, pady=5)

    # Farmer Info display
    farmer_info = ttk.Label(frm, text="", foreground="gray", font=("Segoe UI", 9))
    farmer_info.grid(row=1, column=0, columnspan=2, sticky="w", pady=2)

    # Date (DD/MM/YYYY)
    ttk.Label(frm, text="Date (DD/MM/YYYY):").grid(row=2, column=0, sticky="w", pady=5)
    date_entry = ttk.Entry(frm)
    date_entry.insert(0, date.today().strftime("%d/%m/%Y"))
    date_entry.grid(row=2, column=1, pady=5)

    # Shift
    ttk.Label(frm, text="Shift:").grid(row=3, column=0, sticky="w", pady=5)
    shift_cb = ttk.Combobox(frm, values=["Morning", "Evening"], state="readonly")
    shift_cb.current(0)
    shift_cb.grid(row=3, column=1, pady=5)

    # Category
    ttk.Label(frm, text="Category:").grid(row=4, column=0, sticky="w", pady=5)
    cat_cb = ttk.Combobox(frm, values=["Cow", "Buffalo"], state="readonly")
    cat_cb.current(0)
    cat_cb.grid(row=4, column=1, pady=5)

    # Litres
    ttk.Label(frm, text="Litres:").grid(row=5, column=0, sticky="w", pady=5)
    litres = ttk.Entry(frm)
    litres.grid(row=5, column=1, pady=5)

    # Fat
    ttk.Label(frm, text="Fat:").grid(row=6, column=0, sticky="w", pady=5)
    fat = ttk.Entry(frm)
    fat.grid(row=6, column=1, pady=5)

    # SNF
    ttk.Label(frm, text="SNF:").grid(row=7, column=0, sticky="w", pady=5)
    snf = ttk.Entry(frm)
    snf.grid(row=7, column=1, pady=5)

    # Amount label
    ttk.Label(frm, text="Amount (Auto):").grid(row=8, column=0, sticky="w", pady=5)
    amount_label = ttk.Label(frm, text="₹0.00", font=("Helvetica", 10, "bold"))
    amount_label.grid(row=8, column=1, sticky="w", pady=5)

    # Fetch farmer details
    def fetch_farmer(event=None):
        code = farmer_code.get().strip()
        if not code.isdigit():
            farmer_info.config(text="⚠️ Invalid code format", foreground="gray")
            return
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, village, category FROM farmers WHERE farmer_code=?", (int(code),))
        data = cur.fetchone()
        cur.close()
        conn.close()
        if data:
            farmer_info.config(
                text=f"👤 {data['name']} | 🏡 {data['village']} | {data['category']}",
                foreground="green"
            )
            if data["category"].lower() in ["cow", "buffalo"]:
                cat_cb.set(data["category"])
        else:
            farmer_info.config(text="❌ Farmer not found", foreground="red")

    farmer_code.bind("<FocusOut>", fetch_farmer)
    farmer_code.bind("<Return>", fetch_farmer)

    # Auto-calc amount
    def update_amount(event=None):
        try:
            l = float(litres.get())
            f = float(fat.get())
            s = float(snf.get())
            c = cat_cb.get()
            rate = find_rate_for(c, f, s)
            amt = round(l * rate, 2)
            amount_label.config(text=f"₹{amt:.2f}")
        except Exception:
            amount_label.config(text="₹0.00")

    litres.bind("<KeyRelease>", update_amount)
    fat.bind("<KeyRelease>", update_amount)
    snf.bind("<KeyRelease>", update_amount)
    cat_cb.bind("<<ComboboxSelected>>", update_amount)

    def save_record():
        try:
            code = int(farmer_code.get())
            d = date_entry.get().strip()
            s = shift_cb.get()
            c = cat_cb.get()
            l = float(litres.get())
            f = float(fat.get())
            sn = float(snf.get())

            ok = record_milk_late(code, d, s, l, f, sn, c)
            if ok:
                messagebox.showinfo("Success", f"Missed milk recorded for {d} ({s}, {c}).")
                win.destroy()
                try:
                    app.load_all()
                except Exception:
                    pass
            else:
                messagebox.showerror("Error", "Failed to record. Check farmer code or inputs.")
        except Exception as e:
            messagebox.showerror("Error", f"Invalid input: {e}")

    if TB:
        tb.Button(frm, text="Save Record", bootstyle="success", command=save_record)\
            .grid(row=9, column=0, columnspan=2, pady=15)
    else:
        ttk.Button(frm, text="Save Record", command=save_record)\
            .grid(row=9, column=0, columnspan=2, pady=15)


if __name__ == '__main__':
    main()
