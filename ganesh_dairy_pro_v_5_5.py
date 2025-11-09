#!/usr/bin/env python3
"""
ganesh_dairy_pro_v5.5.py

Ganesh Dairy Pro — v5.5
- Auto-clears Farmer Code and all quick-entry fields after successfully saving a milk record,
  and focuses Farmer Code so the operator can immediately enter the next record.
- Includes Start New Shift button (left of Logout), farmer deletion that removes related data,
  category-aware quick-entry (Cow/Buffalo/Both), sales, rate table, reports, and DB migrations.

Run: python ganesh_dairy_pro_v5.5.py
"""

import os
import sys
import sqlite3
from datetime import date, datetime
import hashlib
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


def ensure_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)


def hash_password(p: str) -> str:
    return hashlib.sha256(p.encode('utf-8')).hexdigest()


def get_shift_for_time(dt=None):
    if dt is None:
        dt = datetime.now()
    h = dt.hour
    if 4 <= h <= 11:
        return "Morning"
    if 12 <= h <= 22:
        return "Evening"
    return "Morning"


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
    # farmers with explicit farmer_code primary key so we can reuse codes
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

    # ensure admin exists
    cur.execute("SELECT COUNT(*) AS c FROM users")
    c = cur.fetchone()['c']
    if c == 0:
        cur.execute("INSERT INTO users (username, password_hash) VALUES (?,?)", ('admin', hash_password('admin123')))
    else:
        cur.execute("SELECT password_hash FROM users WHERE username=?", ('admin',))
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO users (username, password_hash) VALUES (?,?)", ('admin', hash_password('admin123')))
        else:
            if row['password_hash'] is None:
                cur.execute("UPDATE users SET password_hash=? WHERE username=?", (hash_password('admin123'), 'admin'))

    # populate sample rates if empty
    cur.execute("SELECT COUNT(*) AS c FROM rate_table")
    if cur.fetchone()['c'] == 0:
        cow_base_fat = 3.0; cow_base_rate = 30.0
        buf_base_fat = 5.0; buf_base_rate = 45.0
        entries = []
        f = cow_base_fat
        while f <= 6.0 + 1e-9:
            rate = round(cow_base_rate + (f - cow_base_fat) * 5.0, 2)
            entries.append(('Cow', round(f,1), 8.0, rate))
            f = round(f + 0.1, 1)
        f = buf_base_fat
        while f <= 11.0 + 1e-9:
            rate = round(buf_base_rate + (f - buf_base_fat) * 5.0, 2)
            entries.append(('Buffalo', round(f,1), 8.0, rate))
            f = round(f + 0.1, 1)
        cur.executemany("INSERT INTO rate_table (category, fat, snf, rate) VALUES (?,?,?,?)", entries)

    conn.commit()
    cur.close(); conn.close()
    ensure_dirs()
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
    conn = get_conn(); cur = conn.cursor()
    fat_r = round(fat * 10) / 10.0
    cur.execute("SELECT rate FROM rate_table WHERE category=? AND fat=? ORDER BY ABS(snf-?) LIMIT 1", (category, fat_r, snf))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return round(row['rate'], 2)
    cur.execute("SELECT fat, snf, rate FROM rate_table WHERE category=? ORDER BY ABS(fat-?) LIMIT 1", (category, fat_r))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return round(row['rate'], 2)
    cur.close(); conn.close()
    if category.lower().startswith('buf'):
        base_fat = 5.0; base_rate = 45.0
    else:
        base_fat = 3.0; base_rate = 30.0
    rate = base_rate + (fat - base_fat) * 5.0
    return round(max(0.0, rate), 2)

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
        # shift filter - when set, load_all shows only today's that shift
        self.current_shift_filter = None
        self.create_login_ui()

    def create_login_ui(self):
        for w in self.root.winfo_children(): w.destroy()
        self.root.configure(bg="#f8f9fa")
        container = tk.Frame(self.root, bg="#f8f9fa")
        container.place(relx=0.5, rely=0.5, anchor="center")
        title_label = tk.Label(container, text="🥛 Shree Ganesh Dairy", font=("Segoe UI", 42, "bold"), fg="#1a5276", bg="#f8f9fa")
        title_label.pack(pady=(0, 10))
        tagline = tk.Label(container, text="Pure Milk. Honest Service.", font=("Segoe UI", 14, "italic"), fg="#5d6d7e", bg="#f8f9fa")
        tagline.pack(pady=(0, 25))
        shadow = tk.Frame(container, bg="#d6eaf8", bd=0); shadow.pack(pady=10)
        card = tk.Frame(shadow, bg="#fefefe", padx=50, pady=45, highlightbackground="#aed6f1", highlightthickness=2); card.pack(padx=3, pady=3)
        tk.Label(card, text="Login to Continue", font=("Segoe UI", 16, "bold"), fg="#2c3e50", bg="#fefefe").grid(row=0, column=0, columnspan=2, pady=(0, 25))
        tk.Label(card, text="Username:", font=("Segoe UI", 11), bg="#fefefe").grid(row=1, column=0, sticky="e", padx=8, pady=8)
        self.login_user = ttk.Entry(card, width=30); self.login_user.grid(row=1, column=1, pady=8); self.login_user.insert(0, "admin")
        tk.Label(card, text="Password:", font=("Segoe UI", 11), bg="#fefefe").grid(row=2, column=0, sticky="e", padx=8, pady=8)
        pass_frame = tk.Frame(card, bg="#fefefe"); pass_frame.grid(row=2, column=1, pady=8)
        self.login_pass = ttk.Entry(pass_frame, width=27, show="*"); self.login_pass.pack(side="left", padx=(0, 6))
        def toggle_password():
            if self.login_pass.cget("show") == "*":
                self.login_pass.config(show=""); eye_btn.config(text="🙈")
            else:
                self.login_pass.config(show="*"); eye_btn.config(text="👁️")
        eye_btn = tk.Button(pass_frame, text="👁️", font=("Segoe UI", 10), bg="#fefefe", relief="flat", command=toggle_password, cursor="hand2"); eye_btn.pack(side="right")
        self.remember_var = tk.BooleanVar(); remember_chk = tk.Checkbutton(card, text="Remember Me", variable=self.remember_var, font=("Segoe UI", 10), bg="#fefefe"); remember_chk.grid(row=3, column=1, sticky="w", pady=(2, 10))
        login_btn = tk.Button(card, text="Login", font=("Segoe UI", 11, "bold"), bg="#3498db", fg="white", activebackground="#5dade2", relief="flat", padx=22, pady=7, width=15, cursor="hand2", command=self.do_login)
        login_btn.grid(row=4, column=0, columnspan=2, pady=(15, 5))
        def on_enter(e): login_btn.config(bg="#5dade2")
        def on_leave(e): login_btn.config(bg="#3498db")
        login_btn.bind("<Enter>", on_enter); login_btn.bind("<Leave>", on_leave)
        forgot_lbl = tk.Label(card, text="Forgot password?", font=("Segoe UI", 9, "underline"), fg="#2980b9", bg="#fefefe", cursor="hand2")
        forgot_lbl.grid(row=5, column=0, columnspan=2, pady=(6, 0)); forgot_lbl.bind("<Button-1>", lambda e: messagebox.showinfo("Info", "Password reset feature coming soon!"))
        self.root.bind("<Return>", lambda e: self.do_login())

    def do_login(self):
        username = self.login_user.get().strip(); password = self.login_pass.get().strip()
        if not username or not password:
            messagebox.showerror("Error", "Enter both username and password"); return
        conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT password_hash FROM users WHERE username=?", (username,))
        row = cur.fetchone(); cur.close(); conn.close()
        if row and row["password_hash"] == hash_password(password):
            self.current_user = username; self.create_main_ui()
        else:
            messagebox.showerror("Login Failed", "Invalid username or password")

    def create_main_ui(self):
        for w in self.root.winfo_children(): w.destroy()
        header = tb.Frame(self.root, bootstyle='info') if TB else tk.Frame(self.root, bg='#e0e0e0'); header.pack(fill='x', side='top')
        title_lbl = tb.Label(header, text='🐄 Shree Ganesh Dairy', font=('Segoe UI', 14, 'bold')) if TB else tk.Label(header, text='Shree Ganesh Dairy', font=('Segoe UI', 14, 'bold'))
        title_lbl.pack(side='left', padx=12, pady=8)
        corner = tb.Frame(header) if TB else tk.Frame(header); corner.pack(side='right', padx=8)
        mf_btn = tb.Button(corner, text='Manage Farmers', bootstyle='secondary-outline', width=16, command=self.manage_farmers_window) if TB else tk.Button(corner, text='Manage Farmers', width=16, command=self.manage_farmers_window)
        mf_btn.pack(side='right', padx=6)
        adv_btn = tb.Button(corner, text='Record Advance', bootstyle='secondary-outline', width=14, command=self.record_advance_popup) if TB else tk.Button(corner, text='Record Advance', width=14, command=self.record_advance_popup)
        adv_btn.pack(side='right', padx=6)
        sale_btn = tb.Button(corner, text='Record Sale', bootstyle='secondary-outline', width=12, command=self.record_sale_popup) if TB else tk.Button(corner, text='Record Sale', width=12, command=self.record_sale_popup)
        sale_btn.pack(side='right', padx=6)
        rate_btn = tb.Button(corner, text='Rate Table', bootstyle='secondary-outline', width=12, command=self.rate_table_window) if TB else tk.Button(corner, text='Rate Table', width=12, command=self.rate_table_window)
        rate_btn.pack(side='right', padx=6)
        report_btn = tb.Button(corner, text='Generate Report', bootstyle='secondary-outline', width=14, command=self.generate_consolidated_monthly_report) if TB else tk.Button(corner, text='Generate Report', width=14, command=self.generate_consolidated_monthly_report)
        report_btn.pack(side='right', padx=6)
        openr_btn = tb.Button(corner, text='Open Reports', bootstyle='secondary-outline', width=12, command=lambda: open_folder(REPORTS_DIR)) if TB else tk.Button(corner, text='Open Reports', width=12, command=lambda: open_folder(REPORTS_DIR))
        openr_btn.pack(side='right', padx=6)

        # Start New Shift button left of Logout
        authfrm = tb.Frame(header) if TB else tk.Frame(header); authfrm.pack(side='right', padx=8)
        start_shift_btn = tb.Button(authfrm, text='Start New Shift', bootstyle='primary-outline', width=16, command=self.start_new_shift) if TB else tk.Button(authfrm, text='Start New Shift', width=16, command=self.start_new_shift)
        start_shift_btn.pack(side='right', padx=4)
        chbtn = tb.Button(authfrm, text='Change Password', bootstyle='warning-outline', width=14, command=self.change_password_popup) if TB else tk.Button(authfrm, text='Change Password', command=self.change_password_popup)
        chbtn.pack(side='right', padx=4)
        lobtn = tb.Button(authfrm, text='Logout', bootstyle='danger-outline', width=10, command=self.create_login_ui) if TB else tk.Button(authfrm, text='Logout', command=self.create_login_ui)
        lobtn.pack(side='right')

        # Quick Entry
        quick = tb.Frame(self.root, padding=8) if TB else tk.Frame(self.root, padx=8, pady=8); quick.pack(fill='x')
        tk.Label(quick, text='Farmer Code:').grid(row=0, column=0, sticky='e', padx=6, pady=6)
        self.e_code = ttk.Entry(quick, width=12); self.e_code.grid(row=0, column=1, padx=4, pady=6)
        tk.Label(quick, text='Category:').grid(row=0, column=2, sticky='e', padx=6)
        self.cat_placeholder = tk.Frame(quick); self.cat_placeholder.grid(row=0, column=3, padx=4)
        tk.Label(quick, text='Date:').grid(row=0, column=4, sticky='e', padx=6)
        self.e_date = ttk.Entry(quick, width=14); self.e_date.grid(row=0, column=5, padx=4); self.e_date.insert(0, date.today().strftime('%Y-%m-%d'))
        tk.Label(quick, text='Shift:').grid(row=0, column=6, sticky='e', padx=6)
        self.e_shift = ttk.Combobox(quick, values=['Morning', 'Evening'], width=12, state='readonly'); self.e_shift.grid(row=0, column=7, padx=4); self.e_shift.set(get_shift_for_time())

        tk.Label(quick, text='Litres:').grid(row=1, column=0, sticky='e', padx=6, pady=6)
        self.e_litres = ttk.Entry(quick, width=14); self.e_litres.grid(row=1, column=1, padx=4)
        tk.Label(quick, text='Fat (%):').grid(row=1, column=2, sticky='e', padx=6)
        self.e_fat = ttk.Entry(quick, width=14); self.e_fat.grid(row=1, column=3, padx=4)
        tk.Label(quick, text='SNF:').grid(row=1, column=4, sticky='e', padx=6)
        self.e_snf = ttk.Entry(quick, width=14); self.e_snf.grid(row=1, column=5, padx=4)
        tk.Label(quick, text='Rate (₹/L):').grid(row=0, column=8, sticky='e', padx=6)
        self.e_rate = ttk.Entry(quick, width=14); self.e_rate.grid(row=0, column=9, padx=4)
        tk.Label(quick, text='Amount (₹):').grid(row=1, column=8, sticky='e', padx=6)
        self.e_amount = ttk.Entry(quick, width=14); self.e_amount.grid(row=1, column=9, padx=4)

        calc_btn = tb.Button(quick, text='Calculate', bootstyle='info', width=12, command=self.quick_calculate) if TB else tk.Button(quick, text='Calculate', width=12, command=self.quick_calculate)
        calc_btn.grid(row=0, column=10, padx=10)
        save_btn = tb.Button(quick, text='Save Record', bootstyle='success', width=12, command=self.quick_save_record) if TB else tk.Button(quick, text='Save Record', width=12, command=self.quick_save_record)
        save_btn.grid(row=1, column=10, padx=10)

        # Bindings
        self.e_code.bind('<Return>', lambda e: self.fetch_farmer_and_update())
        self.e_code.bind('<FocusOut>', lambda e: self.fetch_farmer_and_update())
        self.e_code.bind('<Return>', lambda e: self._focus(self.e_litres))
        self.e_litres.bind('<Return>', lambda e: self._focus(self.e_fat))
        self.e_fat.bind('<Return>', lambda e: (self.quick_calculate(), self._focus(self.e_snf)))
        self.e_snf.bind('<Return>', lambda e: (self.quick_calculate(), self._focus(save_btn)))
        save_btn.bind('<Return>', lambda e: self.quick_save_record()); save_btn.bind('<space>', lambda e: self.quick_save_record())

        # Dashboard
        dash = tb.Frame(self.root, padding=8) if TB else tk.Frame(self.root, padx=8, pady=8); dash.pack(fill='x')
        self.summary_vars = {
            'farmers': tk.StringVar(value='Farmers: --'),
            'today_litres': tk.StringVar(value='Milk Today: -- L'),
            'today_amount': tk.StringVar(value='Amount Today: ₹--'),
            'sales_today': tk.StringVar(value='Sales Today: -- L'),
            'records': tk.StringVar(value='Total Records: --')
        }
        tk.Label(dash, textvariable=self.summary_vars['farmers'], font=('Segoe UI', 10, 'bold')).grid(row=0, column=0, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['today_litres'], font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['sales_today'], font=('Segoe UI', 10, 'bold')).grid(row=0, column=2, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['today_amount'], font=('Segoe UI', 10, 'bold')).grid(row=0, column=3, padx=8, sticky='w')
        tk.Label(dash, textvariable=self.summary_vars['records'], font=('Segoe UI', 10, 'bold')).grid(row=0, column=4, padx=8, sticky='w')

        # Notebook
        nb = tb.Notebook(self.root) if TB else ttk.Notebook(self.root); nb.pack(fill='both', expand=True, padx=12, pady=12)
        t1 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb); nb.add(t1, text='Milk Records')
        cols = ('id','date','farmer_code','farmer_name','category','shift','litres','fat','snf','rate','amount')
        self.records_tv = ttk.Treeview(t1, columns=cols, show='headings', height=18)
        for c in cols:
            self.records_tv.heading(c, text=c.replace('_',' ').title(), command=lambda _c=c: self.sort_records(_c))
            self.records_tv.column(c, width=100, anchor='center')
        self.records_tv.pack(fill='both', expand=True, side='left')
        vs = ttk.Scrollbar(t1, orient='vertical', command=self.records_tv.yview); vs.pack(side='right', fill='y'); self.records_tv.configure(yscroll=vs.set)

        t2 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb); nb.add(t2, text='Advances')
        adv_cols = ('id','date','farmer_code','farmer_name','reason','amount')
        self.adv_tv = ttk.Treeview(t2, columns=adv_cols, show='headings', height=12)
        for c in adv_cols:
            self.adv_tv.heading(c, text=c.replace('_',' ').title()); self.adv_tv.column(c, width=120, anchor='center')
        self.adv_tv.pack(fill='both', expand=True)

        t3 = tb.Frame(nb, padding=8) if TB else tk.Frame(nb); nb.add(t3, text='Sales')
        sales_cols = ('id','date','dairy_name','category','litres','fat','rate','amount')
        self.sales_tv = ttk.Treeview(t3, columns=sales_cols, show='headings', height=12)
        for c in sales_cols:
            self.sales_tv.heading(c, text=c.replace('_',' ').title()); self.sales_tv.column(c, width=110, anchor='center')
        self.sales_tv.pack(fill='both', expand=True)

        self.e_code.focus_set()
        self.load_all()

    def _focus(self, w):
        try:
            w.focus_set()
        except Exception:
            pass

    # Start New Shift
    def start_new_shift(self):
        if not messagebox.askyesno("Confirm", "Start new shift? This will clear quick-entry fields and filter dashboard to today's current shift."):
            return
        # clear fields
        self.e_code.delete(0, 'end')
        for ch in self.cat_placeholder.winfo_children(): ch.destroy()
        self.e_litres.delete(0, 'end'); self.e_fat.delete(0, 'end'); self.e_snf.delete(0, 'end'); self.e_rate.delete(0, 'end'); self.e_amount.delete(0, 'end')
        today = date.today().strftime('%Y-%m-%d')
        self.e_date.delete(0, 'end'); self.e_date.insert(0, today)
        shift_now = get_shift_for_time()
        try:
            self.e_shift.set(shift_now)
        except Exception:
            pass
        # apply filter
        self.current_shift_filter = shift_now
        self.load_all()

    # Fetch farmer info and update category widget
    def fetch_farmer_and_update(self):
        code_txt = self.e_code.get().strip()
        for ch in self.cat_placeholder.winfo_children(): ch.destroy()
        if not code_txt:
            lbl = tk.Label(self.cat_placeholder, text='Unknown', font=('Segoe UI',10)); lbl.pack(); self.entry_category_widget = None; return
        try:
            code = int(code_txt)
        except ValueError:
            lbl = tk.Label(self.cat_placeholder, text='Invalid', font=('Segoe UI',10)); lbl.pack(); self.entry_category_widget = None; return
        conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT name, category FROM farmers WHERE farmer_code=?", (code,))
        r = cur.fetchone(); cur.close(); conn.close()
        if not r:
            lbl = tk.Label(self.cat_placeholder, text='Not Found', font=('Segoe UI',10)); lbl.pack(); self.entry_category_widget = None; return
        cat = r['category'] or 'Cow'
        if cat == 'Both':
            cb = ttk.Combobox(self.cat_placeholder, values=['Cow','Buffalo'], state='readonly', width=10); cb.pack(); cb.set('Cow'); self.entry_category_widget = cb
        else:
            lbl = tk.Label(self.cat_placeholder, text=cat, font=('Segoe UI',10,'bold')); lbl.pack(); self.entry_category_widget = None

    def quick_calculate(self):
        try:
            fat = float(self.e_fat.get().strip()) if self.e_fat.get().strip() else 0.0
            snf = float(self.e_snf.get().strip()) if self.e_snf.get().strip() else 0.0
            litres = float(self.e_litres.get().strip()) if self.e_litres.get().strip() else 0.0
        except ValueError:
            messagebox.showerror("Error", "Litres/FAT/SNF must be numeric"); return
        cat = 'Cow'
        code_txt = self.e_code.get().strip()
        if code_txt:
            try:
                code = int(code_txt)
                conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT category FROM farmers WHERE farmer_code=?", (code,))
                r = cur.fetchone(); cur.close(); conn.close()
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
        rate = find_rate_for(cat, round(fat,1), snf)
        amt = round(litres * rate, 2)
        self.e_rate.delete(0, 'end'); self.e_rate.insert(0, f"{rate:.2f}")
        self.e_amount.delete(0, 'end'); self.e_amount.insert(0, f"{amt:.2f}")

    def quick_save_record(self):
        code_txt = self.e_code.get().strip()
        if not code_txt:
            messagebox.showerror("Error", "Enter Farmer Code"); self.e_code.focus_set(); return
        try:
            code = int(code_txt)
        except ValueError:
            messagebox.showerror("Error", "Farmer Code must be integer"); self.e_code.focus_set(); return
        conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT name, category FROM farmers WHERE farmer_code=?", (code,))
        farmer = cur.fetchone()
        if not farmer:
            cur.close(); conn.close(); messagebox.showerror("Error", f"Farmer code {code} not found. Use Manage Farmers to add."); return
        farmer_cat = farmer['category'] or 'Cow'
        if farmer_cat == 'Both':
            if getattr(self, 'entry_category_widget', None) and isinstance(self.entry_category_widget, ttk.Combobox):
                category = self.entry_category_widget.get() or 'Cow'
            else:
                messagebox.showerror("Error", "Select category for this entry"); cur.close(); conn.close(); return
        else:
            category = farmer_cat
        date_txt = self.e_date.get().strip()
        try:
            datetime.strptime(date_txt, "%Y-%m-%d")
        except Exception:
            messagebox.showerror("Error", "Date must be YYYY-MM-DD"); cur.close(); conn.close(); return
        try:
            shift = self.e_shift.get().strip() or get_shift_for_time()
            litres = float(self.e_litres.get().strip())
            fat = round(float(self.e_fat.get().strip()), 1)
            snf = float(self.e_snf.get().strip()) if self.e_snf.get().strip() else 0.0
        except ValueError:
            messagebox.showerror("Error", "Numeric fields invalid"); cur.close(); conn.close(); return
        if category == 'Cow' and not (0.0 <= fat <= 8.0):
            messagebox.showwarning("Warning", "Cow FAT seems out of expected range (0-8). Continue?")
        if category == 'Buffalo' and not (0.0 <= fat <= 12.0):
            messagebox.showwarning("Warning", "Buffalo FAT seems out of expected range (0-12). Continue?")
        try:
            rate = float(self.e_rate.get().strip()) if self.e_rate.get().strip() else find_rate_for(category, fat, snf)
        except ValueError:
            rate = find_rate_for(category, fat, snf)
        amount = round(litres * rate, 2)
        try:
            cur.execute(
                "INSERT INTO milk_records (farmer_code, date, shift, litres, fat, snf, rate, amount, category) VALUES (?,?,?,?,?,?,?,?,?)",
                (code, date_txt, shift, litres, fat, snf, rate, amount, category)
            )
            conn.commit()
            cur.close(); conn.close()
            messagebox.showinfo("Saved", "Milk record saved")
            # --- NEW BEHAVIOR: clear ALL quick-entry fields including Farmer Code ---
            self.e_code.delete(0, 'end')
            for ch in self.cat_placeholder.winfo_children(): ch.destroy()
            self.e_litres.delete(0, 'end')
            self.e_fat.delete(0, 'end')
            self.e_snf.delete(0, 'end')
            self.e_rate.delete(0, 'end')
            self.e_amount.delete(0, 'end')
            # focus back to farmer code ready for next record
            self.e_code.focus_set()
            # reload views
            self.load_all()
        except sqlite3.Error as e:
            messagebox.showerror("DB Error", str(e)); cur.close(); conn.close()

    # Manage farmers window (add/edit/delete) - delete removes related data and frees code for reuse
    def manage_farmers_window(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); win.title("Manage Farmers"); win.geometry("820x520")
        frm = tb.Frame(win, padding=8) if TB else tk.Frame(win, padx=8, pady=8); frm.pack(fill='both', expand=True)
        tree = ttk.Treeview(frm, columns=('farmer_code','name','village','contact','category'), show='headings')
        for c in ('farmer_code','name','village','contact','category'):
            tree.heading(c, text=c.replace('_',' ').title()); tree.column(c, width=140, anchor='center')
        tree.pack(fill='both', expand=True, padx=6, pady=6)
        def load_farmers():
            for it in tree.get_children(): tree.delete(it)
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT farmer_code,name,village,contact,category FROM farmers ORDER BY farmer_code")
            for r in cur.fetchall():
                tree.insert('', 'end', values=(r['farmer_code'], r['name'], r['village'] or '', r['contact'] or '', r['category'] or 'Cow'))
            cur.close(); conn.close()
        load_farmers()
        ctrl = tb.Frame(win) if TB else tk.Frame(win); ctrl.pack(fill='x')
        def add_farmer():
            sub = tb.Toplevel(win) if TB else tk.Toplevel(win); sub.title("Add Farmer"); sub.geometry("420x260")
            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12); f2.pack(fill='both', expand=True)
            conn = get_conn()
            next_code = get_next_farmer_code(conn)
            tk.Label(f2, text="Farmer Code:").grid(row=0, column=0, sticky='e', pady=6)
            tk.Label(f2, text=str(next_code), font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, sticky='w')
            tk.Label(f2, text="Name:").grid(row=1, column=0, sticky='e', pady=6); name_e = ttk.Entry(f2); name_e.grid(row=1, column=1, pady=6)
            tk.Label(f2, text="Village:").grid(row=2, column=0, sticky='e', pady=6); village_e = ttk.Entry(f2); village_e.grid(row=2, column=1, pady=6)
            tk.Label(f2, text="Contact:").grid(row=3, column=0, sticky='e', pady=6); contact_e = ttk.Entry(f2); contact_e.grid(row=3, column=1, pady=6)
            tk.Label(f2, text="Category:").grid(row=4, column=0, sticky='e', pady=6)
            cat_cb = ttk.Combobox(f2, values=['Cow','Buffalo','Both'], state='readonly'); cat_cb.grid(row=4, column=1, pady=6); cat_cb.set('Cow')
            def do_add():
                name = name_e.get().strip()
                if not name: messagebox.showerror("Error", "Name required"); return
                village = village_e.get().strip(); contact = contact_e.get().strip(); category = cat_cb.get().strip() or 'Cow'
                try:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO farmers (farmer_code, name, village, contact, category) VALUES (?,?,?,?,?)", (next_code, name, village, contact, category))
                    conn.commit(); cur.close(); conn.close(); messagebox.showinfo("Saved", f"Farmer added with Code {next_code}"); sub.destroy(); load_farmers()
                except sqlite3.IntegrityError:
                    messagebox.showerror("DB Error", "Farmer code conflict — try again"); conn.close()
                except sqlite3.Error as e:
                    messagebox.showerror("DB Error", str(e)); conn.close()
            tb.Button(f2, text='Save', bootstyle='success', command=do_add).grid(row=5, column=0, columnspan=2, pady=8) if TB else tk.Button(f2, text='Save', command=do_add).grid(row=5, column=0, columnspan=2, pady=8)
        def edit_farmer():
            sel = tree.selection();
            if not sel: messagebox.showwarning("Select", "Select a farmer to edit"); return
            vals = tree.item(sel[0], 'values'); fcode = vals[0]
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT name,village,contact,category FROM farmers WHERE farmer_code=?", (fcode,)); r = cur.fetchone(); cur.close(); conn.close()
            if not r: messagebox.showerror("Error", "Farmer not found"); return
            sub = tb.Toplevel(win) if TB else tk.Toplevel(win); sub.title(f"Edit Farmer {fcode}"); sub.geometry("420x260")
            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12); f2.pack(fill='both', expand=True)
            tk.Label(f2, text="Farmer Code:").grid(row=0,column=0,sticky='e',pady=6); tk.Label(f2, text=str(fcode), font=('Segoe UI',10,'bold')).grid(row=0,column=1,sticky='w')
            tk.Label(f2, text="Name:").grid(row=1,column=0,sticky='e',pady=6); name_e = ttk.Entry(f2); name_e.grid(row=1,column=1,pady=6); name_e.insert(0, r['name'])
            tk.Label(f2, text="Village:").grid(row=2,column=0,sticky='e',pady=6); village_e = ttk.Entry(f2); village_e.grid(row=2,column=1,pady=6); village_e.insert(0, r['village'] or '')
            tk.Label(f2, text="Contact:").grid(row=3,column=0,sticky='e',pady=6); contact_e = ttk.Entry(f2); contact_e.grid(row=3,column=1,pady=6); contact_e.insert(0, r['contact'] or '')
            tk.Label(f2, text="Category:").grid(row=4,column=0,sticky='e',pady=6); cat_cb = ttk.Combobox(f2, values=['Cow','Buffalo','Both'], state='readonly'); cat_cb.grid(row=4,column=1,pady=6); cat_cb.set(r['category'] or 'Cow')
            def do_save():
                try:
                    conn = get_conn(); cur = conn.cursor(); cur.execute("UPDATE farmers SET name=?,village=?,contact=?,category=? WHERE farmer_code=?", (name_e.get().strip(), village_e.get().strip(), contact_e.get().strip(), cat_cb.get().strip(), fcode)); conn.commit(); cur.close(); conn.close(); messagebox.showinfo("Saved", "Farmer updated"); sub.destroy(); load_farmers(); self.load_all()
                except sqlite3.Error as e:
                    messagebox.showerror("DB Error", str(e))
            tb.Button(f2, text='Save', bootstyle='success', command=do_save).grid(row=5,column=0,columnspan=2,pady=8) if TB else tk.Button(f2, text='Save', command=do_save).grid(row=5,column=0,columnspan=2,pady=8)
        def delete_farmer():
            sel = tree.selection();
            if not sel: messagebox.showwarning("Select", "Select a farmer to delete"); return
            fcode = tree.item(sel[0], 'values')[0]
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT name FROM farmers WHERE farmer_code=?", (fcode,)); r = cur.fetchone()
            if not r: cur.close(); conn.close(); messagebox.showerror("Error", "Farmer not found"); return
            name = r['name']
            if not messagebox.askyesno("Confirm", f"Delete farmer {fcode} ({name}) and ALL related records? This cannot be undone."):
                cur.close(); conn.close(); return
            try:
                cur.execute("DELETE FROM milk_records WHERE farmer_code=?", (fcode,))
                cur.execute("DELETE FROM advances WHERE farmer_code=?", (fcode,))
                cur.execute("DELETE FROM farmers WHERE farmer_code=?", (fcode,))
                conn.commit(); cur.close(); conn.close(); load_farmers(); self.load_all(); messagebox.showinfo("Deleted", f"Farmer {fcode} and related data deleted")
            except sqlite3.Error as e:
                messagebox.showerror("DB Error", str(e)); cur.close(); conn.close()
        tb.Button(ctrl, text='Add', bootstyle='success', command=add_farmer).pack(side='left', padx=6) if TB else tk.Button(ctrl, text='Add', command=add_farmer).pack(side='left', padx=6)
        tb.Button(ctrl, text='Edit', bootstyle='info', command=edit_farmer).pack(side='left', padx=6) if TB else tk.Button(ctrl, text='Edit', command=edit_farmer).pack(side='left', padx=6)
        tb.Button(ctrl, text='Delete', bootstyle='danger', command=delete_farmer).pack(side='left', padx=6) if TB else tk.Button(ctrl, text='Delete', command=delete_farmer).pack(side='left', padx=6)
        tb.Button(ctrl, text='Close', bootstyle='secondary', command=win.destroy).pack(side='right', padx=6) if TB else tk.Button(ctrl, text='Close', command=win.destroy).pack(side='right', padx=6)

    # Rate table, advances, sales, reports & change password are similar to prior versions
    def rate_table_window(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); win.title("Rate Table"); win.geometry("560x420")
        frm = tb.Frame(win, padding=8) if TB else tk.Frame(win, padx=8, pady=8); frm.pack(fill='both', expand=True)
        tree = ttk.Treeview(frm, columns=('id','category','fat','snf','rate'), show='headings', height=14)
        for c in ('id','category','fat','snf','rate'):
            tree.heading(c, text=c.upper()); tree.column(c, width=100, anchor='center')
        tree.pack(fill='both', expand=True, padx=6, pady=6)
        btnfrm = tb.Frame(win) if TB else tk.Frame(win); btnfrm.pack(fill='x')
        def load_table():
            for it in tree.get_children(): tree.delete(it)
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT id,category,fat,snf,rate FROM rate_table ORDER BY category, fat")
            for r in cur.fetchall():
                tree.insert('', 'end', values=(r['id'], r['category'], f"{r['fat']:.1f}", f"{r['snf']:.1f}", f"{r['rate']:.2f}"))
            cur.close(); conn.close()
        def add_rate():
            sub = tb.Toplevel(win) if TB else tk.Toplevel(win); sub.title("Add Rate"); sub.geometry("360x240")
            f2 = tb.Frame(sub, padding=12) if TB else tk.Frame(sub, padx=12, pady=12); f2.pack(fill='both', expand=True)
            tk.Label(f2, text='Category:').grid(row=0,column=0,sticky='e',pady=6); cat_cb = ttk.Combobox(f2, values=['Cow','Buffalo'], state='readonly'); cat_cb.grid(row=0,column=1,pady=6); cat_cb.set('Cow')
            tk.Label(f2, text='FAT:').grid(row=1,column=0,sticky='e',pady=6); fat_e = ttk.Entry(f2); fat_e.grid(row=1,column=1,pady=6)
            tk.Label(f2, text='SNF:').grid(row=2,column=0,sticky='e',pady=6); snf_e = ttk.Entry(f2); snf_e.grid(row=2,column=1,pady=6)
            tk.Label(f2, text='Rate:').grid(row=3,column=0,sticky='e',pady=6); rate_e = ttk.Entry(f2); rate_e.grid(row=3,column=1,pady=6)
            def do_add():
                try:
                    cat = cat_cb.get().strip(); f = float(fat_e.get().strip()); s = float(snf_e.get().strip()); r = float(rate_e.get().strip())
                except ValueError:
                    messagebox.showerror("Error", "Numeric values required"); return
                conn = get_conn(); cur = conn.cursor(); cur.execute("INSERT INTO rate_table (category,fat,snf,rate) VALUES (?,?,?,?)", (cat, round(f,1), s, r)); conn.commit(); cur.close(); conn.close(); sub.destroy(); load_table()
            tb.Button(f2, text='Save', bootstyle='success', command=do_add).grid(row=4,column=0,columnspan=2,pady=8) if TB else tk.Button(f2, text='Save', command=do_add).grid(row=4,column=0,columnspan=2,pady=8)
        def del_rate():
            sel = tree.selection();
            if not sel: messagebox.showwarning("Select","Select a rate row"); return
            rid = tree.item(sel[0],'values')[0]
            if not messagebox.askyesno("Confirm","Delete selected rate?"): return
            conn = get_conn(); cur = conn.cursor(); cur.execute("DELETE FROM rate_table WHERE id=?", (rid,)); conn.commit(); cur.close(); conn.close(); load_table()
        tb.Button(btnfrm, text='Add', bootstyle='success', command=add_rate).pack(side='left', padx=6) if TB else tk.Button(btnfrm, text='Add', command=add_rate).pack(side='left', padx=6)
        tb.Button(btnfrm, text='Delete', bootstyle='danger', command=del_rate).pack(side='left', padx=6) if TB else tk.Button(btnfrm, text='Delete', command=del_rate).pack(side='left', padx=6)
        tb.Button(btnfrm, text='Close', bootstyle='secondary', command=win.destroy).pack(side='right', padx=6) if TB else tk.Button(btnfrm, text='Close', command=win.destroy).pack(side='right', padx=6)
        load_table()

    def record_advance_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); win.title("Record Advance"); win.geometry("420x260")
        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12); frm.pack(fill='both', expand=True)
        tk.Label(frm, text='Farmer Code:').grid(row=0,column=0,sticky='e',pady=6); code_e = ttk.Entry(frm); code_e.grid(row=0,column=1,pady=6)
        tk.Label(frm, text='Date (YYYY-MM-DD):').grid(row=1,column=0,sticky='e',pady=6); date_e = ttk.Entry(frm); date_e.grid(row=1,column=1,pady=6); date_e.insert(0, date.today().strftime('%Y-%m-%d'))
        tk.Label(frm, text='Reason:').grid(row=2,column=0,sticky='e',pady=6); reason_e = ttk.Entry(frm); reason_e.grid(row=2,column=1,pady=6)
        tk.Label(frm, text='Amount (₹):').grid(row=3,column=0,sticky='e',pady=6); amt_e = ttk.Entry(frm); amt_e.grid(row=3,column=1,pady=6)
        def save_adv():
            code_txt = code_e.get().strip()
            if not code_txt: messagebox.showerror("Error","Enter Farmer Code"); return
            try: code = int(code_txt)
            except ValueError: messagebox.showerror("Error","Farmer Code must be integer"); return
            try: date_txt = date_e.get().strip(); datetime.strptime(date_txt, "%Y-%m-%d"); amount = float(amt_e.get().strip())
            except Exception: messagebox.showerror("Error","Check date/amount format"); return
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT name FROM farmers WHERE farmer_code=?", (code,)); f = cur.fetchone()
            if not f: cur.close(); conn.close(); messagebox.showerror("Error","Farmer not found"); return
            try: cur.execute("INSERT INTO advances (farmer_code,date,reason,amount) VALUES (?,?,?,?)", (code, date_txt, reason_e.get().strip(), amount)); conn.commit(); cur.close(); conn.close(); messagebox.showinfo("Saved","Advance recorded"); win.destroy(); self.load_all()
            except sqlite3.Error as e: messagebox.showerror("DB Error", str(e)); cur.close(); conn.close()
        tb.Button(frm, text='Save Advance', bootstyle='success', command=save_adv).grid(row=4,column=0,columnspan=2,pady=10) if TB else tk.Button(frm, text='Save Advance', command=save_adv).grid(row=4,column=0,columnspan=2,pady=10)

    def record_sale_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); win.title("Record Sale"); win.geometry("420x320")
        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12); frm.pack(fill='both', expand=True)
        tk.Label(frm, text='Date (YYYY-MM-DD):').grid(row=0,column=0,sticky='e',pady=6); date_e = ttk.Entry(frm); date_e.grid(row=0,column=1,pady=6); date_e.insert(0, date.today().strftime('%Y-%m-%d'))
        tk.Label(frm, text='Dairy Name:').grid(row=1,column=0,sticky='e',pady=6); dairy_e = ttk.Entry(frm); dairy_e.grid(row=1,column=1,pady=6)
        tk.Label(frm, text='Category:').grid(row=2,column=0,sticky='e',pady=6); cat_cb = ttk.Combobox(frm, values=['Cow','Buffalo'], state='readonly'); cat_cb.grid(row=2,column=1,pady=6); cat_cb.set('Cow')
        tk.Label(frm, text='Litres:').grid(row=3,column=0,sticky='e',pady=6); litres_e = ttk.Entry(frm); litres_e.grid(row=3,column=1,pady=6)
        tk.Label(frm, text='Fat (%):').grid(row=4,column=0,sticky='e',pady=6); fat_e = ttk.Entry(frm); fat_e.grid(row=4,column=1,pady=6)
        tk.Label(frm, text='Rate (₹/L):').grid(row=5,column=0,sticky='e',pady=6); rate_e = ttk.Entry(frm); rate_e.grid(row=5,column=1,pady=6)
        tk.Label(frm, text='Amount (₹):').grid(row=6,column=0,sticky='e',pady=6); amount_e = ttk.Entry(frm); amount_e.grid(row=6,column=1,pady=6)
        def calc_sale():
            try:
                l = float(litres_e.get().strip()) if litres_e.get().strip() else 0.0
                r = float(rate_e.get().strip()) if rate_e.get().strip() else 0.0
            except ValueError:
                messagebox.showerror("Error","Enter numeric litres/rate"); return
            amount_e.delete(0,'end'); amount_e.insert(0, f"{l*r:.2f}")
        def save_sale():
            try:
                d = date_e.get().strip(); datetime.strptime(d, "%Y-%m-%d")
                dairy = dairy_e.get().strip(); cat = cat_cb.get().strip(); l = float(litres_e.get().strip()); fat = float(fat_e.get().strip()) if fat_e.get().strip() else 0.0; rate = float(rate_e.get().strip()) if rate_e.get().strip() else 0.0; amt = float(amount_e.get().strip()) if amount_e.get().strip() else round(l*rate,2)
            except Exception:
                messagebox.showerror("Error","Check all fields (date/numbers)"); return
            conn = get_conn(); cur = conn.cursor();
            try:
                cur.execute("INSERT INTO sales (date,dairy_name,category,litres,fat,rate,amount) VALUES (?,?,?,?,?,?,?)", (d, dairy, cat, l, fat, rate, amt)); conn.commit(); cur.close(); conn.close(); messagebox.showinfo("Saved","Sale recorded"); win.destroy(); self.load_all()
            except sqlite3.Error as e:
                messagebox.showerror("DB Error", str(e)); cur.close(); conn.close()
        tb.Button(frm, text='Calculate', bootstyle='info', command=calc_sale).grid(row=7,column=0,pady=8) if TB else tk.Button(frm, text='Calculate', command=calc_sale).grid(row=7,column=0,pady=8)
        tb.Button(frm, text='Save Sale', bootstyle='success', command=save_sale).grid(row=7,column=1,pady=8) if TB else tk.Button(frm, text='Save Sale', command=save_sale).grid(row=7,column=1,pady=8)

    def generate_consolidated_monthly_report(self):
        def do_popup():
            month = m_e.get().strip()
            if not month: messagebox.showerror("Error","Enter month (YYYY-MM)"); return
            try: datetime.strptime(month + "-01", "%Y-%m-%d")
            except Exception: messagebox.showerror("Error","Invalid month format"); return
            popup.destroy(); self._generate_report(month)
        popup = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); popup.title("Generate Monthly Consolidated Report"); popup.geometry("420x140")
        frm = tb.Frame(popup, padding=12) if TB else tk.Frame(popup, padx=12, pady=12); frm.pack(fill='both', expand=True)
        tk.Label(frm, text='Month (YYYY-MM):').grid(row=0,column=0,sticky='e',pady=8); m_e = ttk.Entry(frm); m_e.grid(row=0,column=1,pady=8); m_e.insert(0, date.today().strftime('%Y-%m'))
        tb.Button(frm, text='Generate', bootstyle='success', command=do_popup).grid(row=1,column=0,columnspan=2,pady=8) if TB else tk.Button(frm, text='Generate', command=do_popup).grid(row=1,column=0,columnspan=2,pady=8)

    def _generate_report(self, ym):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT farmer_code, name FROM farmers ORDER BY farmer_code"); farmers = cur.fetchall()
        if not farmers: messagebox.showinfo("Info","No farmers to report"); cur.close(); conn.close(); return
        fname = os.path.join(REPORTS_DIR, f"monthly_report_{ym}.pdf")
        c = canvas.Canvas(fname, pagesize=letter); y = 750
        c.setFont("Helvetica-Bold", 14); c.drawString(60, y, f"Dairy Monthly Consolidated Report - {ym}")
        y -= 24; c.setFont("Helvetica", 10); c.drawString(60, y, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        y -= 18; c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, "Code"); c.drawString(90, y, "Farmer"); c.drawString(260, y, "Total L"); c.drawString(330, y, "Avg FAT"); c.drawString(400, y, "Avg SNF"); c.drawString(470, y, "Amount"); c.drawString(540, y, "Advances"); c.drawString(610, y, "Net")
        y -= 14; c.setFont("Helvetica", 10)
        for f in farmers:
            fcode = f['farmer_code']; fname = f['name']
            cur.execute("SELECT SUM(litres) AS tl, SUM(amount) AS ta, AVG(fat) AS af, AVG(snf) AS asnf FROM milk_records WHERE farmer_code=? AND date LIKE ?", (fcode, ym + "%"))
            r = cur.fetchone(); total_l = r['tl'] or 0.0; total_amt = r['ta'] or 0.0; avg_fat = r['af'] or 0.0; avg_snf = r['asnf'] or 0.0
            cur.execute("SELECT SUM(amount) AS adv FROM advances WHERE farmer_code=? AND date LIKE ?", (fcode, ym + "%"))
            adv = cur.fetchone()['adv'] or 0.0
            net = total_amt - adv
            c.drawString(50, y, str(fcode)); c.drawString(90, y, (fname[:20] + '...') if len(fname) > 20 else fname)
            c.drawRightString(320, y, f"{total_l:.2f}"); c.drawRightString(390, y, f"{avg_fat:.2f}"); c.drawRightString(460, y, f"{avg_snf:.2f}"); c.drawRightString(530, y, f"{total_amt:.2f}"); c.drawRightString(600, y, f"{adv:.2f}"); c.drawRightString(680, y, f"{net:.2f}")
            y -= 16
            if y < 100: c.showPage(); y = 750
        c.showPage(); y = 750; c.setFont("Helvetica-Bold", 12); c.drawString(60, y, "Sales Summary")
        y -= 20; c.setFont("Helvetica", 10)
        cur.execute("SELECT category, SUM(litres) AS l, SUM(amount) AS a FROM sales WHERE date LIKE ? GROUP BY category", (ym + "%",))
        for r in cur.fetchall():
            c.drawString(60, y, f"{r['category']}: {r['l'] or 0:.2f} L, Amount: ₹{r['a'] or 0:.2f}"); y -= 14
            if y < 100: c.showPage(); y = 750
        cur.close(); conn.close(); c.save(); messagebox.showinfo("Saved", f"Report saved:\n{fname}"); open_folder(REPORTS_DIR)

    def change_password_popup(self):
        win = tb.Toplevel(self.root) if TB else tk.Toplevel(self.root); win.title("Change Password"); win.geometry("420x200")
        frm = tb.Frame(win, padding=12) if TB else tk.Frame(win, padx=12, pady=12); frm.pack(fill='both', expand=True)
        tk.Label(frm, text='Current Password:').grid(row=0,column=0,sticky='e',pady=6); cur_e = ttk.Entry(frm, show='*'); cur_e.grid(row=0,column=1,pady=6)
        tk.Label(frm, text='New Password:').grid(row=1,column=0,sticky='e',pady=6); new_e = ttk.Entry(frm, show='*'); new_e.grid(row=1,column=1,pady=6)
        tk.Label(frm, text='Confirm New:').grid(row=2,column=0,sticky='e',pady=6); conf_e = ttk.Entry(frm, show='*'); conf_e.grid(row=2,column=1,pady=6)
        def do_change():
            curp = cur_e.get().strip(); newp = new_e.get().strip(); conf = conf_e.get().strip()
            if not (curp and newp and conf): messagebox.showerror("Error","Fill all fields"); return
            if newp != conf: messagebox.showerror("Error","Passwords do not match"); return
            conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT password_hash FROM users WHERE username=?", (self.current_user,)); row = cur.fetchone()
            if not row or row['password_hash'] != hash_password(curp): messagebox.showerror("Error","Current password incorrect"); cur.close(); conn.close(); return
            cur.execute("UPDATE users SET password_hash=? WHERE username=?", (hash_password(newp), self.current_user)); conn.commit(); cur.close(); conn.close(); messagebox.showinfo("Saved","Password changed"); win.destroy()
        tb.Button(frm, text='Change Password', bootstyle='success', command=do_change).grid(row=3,column=0,columnspan=2,pady=8) if TB else tk.Button(frm, text='Change Password', command=do_change).grid(row=3,column=0,columnspan=2,pady=8)

    # Load all data and refresh UI; respects current_shift_filter when set
    def load_all(self):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM farmers"); total_farmers = cur.fetchone()['c'] or 0
        today = date.today().strftime('%Y-%m-%d')
        # query milk and sales depending on shift filter
        if self.current_shift_filter:
            cur.execute("SELECT category, SUM(litres) AS l, SUM(amount) AS a FROM milk_records WHERE date=? AND shift=? GROUP BY category", (today, self.current_shift_filter))
            milk_rows = cur.fetchall()
            cur.execute("SELECT category, SUM(litres) AS l FROM sales WHERE date=? GROUP BY category", (today,))
            sales_rows = cur.fetchall()
        else:
            cur.execute("SELECT category, SUM(litres) AS l, SUM(amount) AS a FROM milk_records WHERE date=? GROUP BY category", (today,))
            milk_rows = cur.fetchall()
            cur.execute("SELECT category, SUM(litres) AS l FROM sales WHERE date=? GROUP BY category", (today,))
            sales_rows = cur.fetchall()
        milk_by_cat = {r['category'] or 'Unknown': {'litres': r['l'] or 0.0, 'amount': r['a'] or 0.0} for r in milk_rows}
        total_today = sum(v['litres'] for v in milk_by_cat.values())
        total_amount_today = sum(v['amount'] for v in milk_by_cat.values())
        sales_by_cat = {r['category'] or 'Unknown': r['l'] or 0.0 for r in sales_rows}
        total_sales_today = sum(sales_by_cat.values())
        total_records = cur.execute("SELECT COUNT(*) AS c FROM milk_records").fetchone()['c'] or 0
        self.summary_vars['farmers'].set(f"Farmers: {total_farmers}")
        def cat_breakdown_str(d):
            parts = []
            for k in ('Cow','Buffalo'):
                if k in d:
                    parts.append(f"{k}: {d[k]['litres'] if isinstance(d[k], dict) else d[k]:.2f} L")
            return ' | '.join(parts) if parts else 'None'
        milk_break = cat_breakdown_str(milk_by_cat)
        if self.current_shift_filter:
            self.summary_vars['today_litres'].set(f"Milk Today ({self.current_shift_filter}): {total_today:.2f} L ({milk_break})")
        else:
            self.summary_vars['today_litres'].set(f"Milk Today: {total_today:.2f} L ({milk_break})")
        sales_parts = []
        for k in ('Cow','Buffalo'):
            if k in sales_by_cat:
                sales_parts.append(f"{k}: {sales_by_cat[k]:.2f} L")
        sales_break = ' | '.join(sales_parts) if sales_parts else 'None'
        self.summary_vars['sales_today'].set(f"Sales Today: {total_sales_today:.2f} L ({sales_break})")
        self.summary_vars['today_amount'].set(f"Amount Today: ₹{total_amount_today:.2f}")
        self.summary_vars['records'].set(f"Total Records: {total_records}")
        # reload records (respect shift filter)
        for it in self.records_tv.get_children(): self.records_tv.delete(it)
        if self.current_shift_filter:
            cur.execute("""
            SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
            FROM milk_records m LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
            WHERE m.date=? AND m.shift=?
            ORDER BY m.date DESC, m.id DESC LIMIT 1000
            """, (today, self.current_shift_filter))
        else:
            cur.execute("""
            SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
            FROM milk_records m LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
            ORDER BY m.date DESC, m.id DESC LIMIT 1000
            """)
        for r in cur.fetchall():
            self.records_tv.insert('', 'end', values=(r['id'], r['date'], r['farmer_code'] or '', r['farmer_name'] or '', r['category'] or '', r['shift'], f"{r['litres']:.2f}", f"{r['fat']:.2f}", f"{r['snf']:.2f}", f"{r['rate']:.2f}", f"{r['amount']:.2f}"))
        # advances
        for it in self.adv_tv.get_children(): self.adv_tv.delete(it)
        cur.execute("""
        SELECT a.id,a.date,a.farmer_code,f.name AS farmer_name,a.reason,a.amount
        FROM advances a LEFT JOIN farmers f ON a.farmer_code=f.farmer_code
        ORDER BY a.date DESC, a.id DESC LIMIT 500
        """)
        for r in cur.fetchall():
            self.adv_tv.insert('', 'end', values=(r['id'], r['date'], r['farmer_code'] or '', r['farmer_name'] or '', r['reason'] or '', f"{r['amount']:.2f}"))
        # sales
        for it in self.sales_tv.get_children(): self.sales_tv.delete(it)
        cur.execute("SELECT id,date,dairy_name,category,litres,fat,rate,amount FROM sales ORDER BY date DESC, id DESC LIMIT 500")
        for r in cur.fetchall():
            self.sales_tv.insert('', 'end', values=(r['id'], r['date'], r['dairy_name'] or '', r['category'] or '', f"{r['litres']:.2f}", f"{r['fat']:.2f}", f"{r['rate']:.2f}", f"{r['amount']:.2f}"))
        cur.close(); conn.close()

    def sort_records(self, col):
        colmap = {
            'id':'m.id','date':'m.date','farmer_code':'m.farmer_code','farmer_name':'f.name','category':'m.category','shift':'m.shift',
            'litres':'m.litres','fat':'m.fat','snf':'m.snf','rate':'m.rate','amount':'m.amount'
        }
        dbcol = colmap.get(col, 'm.date')
        conn = get_conn(); cur = conn.cursor()
        sql = f"""
        SELECT m.id,m.date,m.farmer_code,f.name AS farmer_name,m.category,m.shift,m.litres,m.fat,m.snf,m.rate,m.amount
        FROM milk_records m LEFT JOIN farmers f ON m.farmer_code=f.farmer_code
        ORDER BY {dbcol} DESC LIMIT 1000
        """
        cur.execute(sql)
        for it in self.records_tv.get_children(): self.records_tv.delete(it)
        for r in cur.fetchall():
            self.records_tv.insert('', 'end', values=(r['id'], r['date'], r['farmer_code'] or '', r['farmer_name'] or '', r['category'] or '', r['shift'], f"{r['litres']:.2f}", f"{r['fat']:.2f}", f"{r['snf']:.2f}", f"{r['rate']:.2f}", f"{r['amount']:.2f}"))
        cur.close(); conn.close()

# main
def main():
    ensure_dirs()
    root = tb.Window(themename=THEME) if TB else tk.Tk()
    app = DairyApp(root)
    root.mainloop()

if __name__ == '__main__':
    main()
import base64

for fn in ["bg.jpeg", "bg2.jpeg"]:
    with open(fn, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    print(f"\n----- {fn} -----\n{b64}\n")
