import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import queue
import threading
import time
import concurrent.futures
from datetime import datetime
import os
import csv
import tkinter.font as tkFont
import json
import re
from decimal import Decimal, InvalidOperation
from azure.cosmos import CosmosClient, exceptions
import logging
logger = logging.getLogger("cosmos_upload")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler("cosmos_upload.log", encoding="utf-8")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
SETTINGS_FILE = "freight_loader_settings.json"
DEFAULT_SETTINGS = {
    "connection_string": "",
    "database_name": "soh",
    "container_name": "dropshipPricing",
    "allow_partial_upload": False,
    "log_level": "INFO",
    "max_workers": 4
}
app_state = DEFAULT_SETTINGS.copy()
class PriorityQueue:
    def __init__(self):
        self._queue = queue.PriorityQueue()
        self._items = {}
    def put(self, file_path, priority, task_id):
        if file_path not in self._items:
            priority_value = self._get_priority_value(priority)
            self._queue.put((priority_value, task_id, file_path))
            self._items[file_path] = (priority_value, task_id)
    def get(self, timeout=None):
        priority_value, task_id, file_path = self._queue.get(timeout=timeout)
        if file_path in self._items:
            del self._items[file_path]
        return file_path
    def task_done(self):
        self._queue.task_done()
    def empty(self):
        return self._queue.empty()
    def contains(self, file_path):
        return file_path in self._items
    def _get_priority_value(self, priority_label):
        return {"High": 1, "Medium": 2, "Low": 3}.get(priority_label, 2)
    def get_priority_label(self, priority_value):
        return {1: "High", 2: "Medium", 3: "Low"}.get(priority_value, "Medium")
    def get_all_paths(self):
        return list(item[2] for item in self._queue.queue)
RE_AU_POSTCODE = re.compile(r"^\d{4}$")
INVALID_CHARS = set('=\\@^;|,\':?"{}~[]`')
CSV_FIELD_ALIASES = {
    "sku": ["sku", "SKU", "Sku"],
    "postCode": ["postCode", "postcode", "post_code", "Postcode", "postal_code", "PostalCode"],
    "price": ["price", "Price", "unit_price", "UnitPrice"]
}
def normalize_str(v):
    if v is None:
        return ""
    return str(v).strip()
def is_valid_sku(s):
    if not s or not s.isascii():
        return False, "sku must be ASCII and non-empty"
    if any(c in INVALID_CHARS for c in s):
        return False, "sku contains one or more invalid characters"
    if "  " in s:
        return False, "sku contains multiple consecutive spaces"
    if len(s) > 128:
        return False, "sku too long (>128 chars)"
    return True, ""
def is_valid_postcode(pc):
    if not RE_AU_POSTCODE.match(pc):
        return False, "postCode must be 4 digits (AU)"
    return True, ""
def normalize_price(p):
    if p is None or str(p).strip() == "":
        return False, None, "price missing"
    try:
        d = Decimal(str(p)).quantize(Decimal("0.01"))
        if d < 0:
            return False, None, "price must be >= 0"
        return True, format(d, "f"), ""
    except (InvalidOperation, ValueError):
        return False, None, "price must be numeric (up to 2 decimals)"
def field_from_row(row, logical_key):
    for k in CSV_FIELD_ALIASES[logical_key]:
        if k in row and str(row[k]).strip() != "":
            return row[k]
    return None
def build_doc(sku, postcode, price):
    doc_id = f"{sku}{postcode}"
    return {
        "id": doc_id,
        "postCode": str(postcode),
        "price": str(price),
        "sku": str(sku),
        "message": ""
    }
def write_error_report(error_rows, report_path):
    headers = ["row", "context", "error"]
    try:
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for e in error_rows:
                w.writerow({
                    "row": e.get("row"),
                    "context": e.get("context", ""),
                    "error": e.get("error", "")
                })
    except Exception as e:
        logger.error("Failed to write error report: %s", e)
def validate_csv(file_path):
    valid_docs = []
    errors = []
    warnings = []
    seen_ids = set()
    with open(file_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            errors.append({"row": 1, "context": "header", "error": "Missing header row"})
            return valid_docs, errors, warnings
        missing_min = []
        for key in ["sku", "postCode", "price"]:
            if not any(alias in reader.fieldnames for alias in CSV_FIELD_ALIASES[key]):
                missing_min.append(key)
        if missing_min:
            errors.append({
                "row": 1,
                "context": "header",
                "error": f"Missing required columns: {', '.join(missing_min)}"
            })
            return valid_docs, errors, warnings
        for idx, row in enumerate(reader, start=2):
            raw_sku = normalize_str(field_from_row(row, "sku"))
            raw_pc = normalize_str(field_from_row(row, "postCode"))
            raw_price = normalize_str(field_from_row(row, "price"))
            if not raw_sku and not raw_pc and not raw_price:
                continue 
            ok_sku, sku_err = is_valid_sku(raw_sku)
            ok_pc, pc_err = is_valid_postcode(raw_pc)
            ok_price, norm_price, price_err = normalize_price(raw_price)
            errs = []
            if not raw_sku: errs.append("sku missing")
            if not raw_pc: errs.append("postCode missing")
            if not raw_price: errs.append("price missing")
            if raw_sku and not ok_sku: errs.append(sku_err)
            if raw_pc and not ok_pc: errs.append(pc_err)
            if raw_price and not ok_price: errs.append(price_err)
            if errs:
                errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
                continue
            doc_id = f"{raw_sku}{raw_pc}"
            if doc_id in seen_ids:
                errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
                continue
            seen_ids.add(doc_id)
            doc = build_doc(raw_sku, raw_pc, norm_price)
            valid_docs.append(doc)
    return valid_docs, errors, warnings
def validate_json(file_path):
    valid_docs = []
    errors = []
    warnings = []
    seen_ids = set()
    def validate_obj(obj, idx_for_report):
        raw_sku = normalize_str(obj.get("sku"))
        if not raw_sku:
            for k in CSV_FIELD_ALIASES["sku"]:
                if k in obj:
                    raw_sku = normalize_str(obj[k]); break
        raw_pc = normalize_str(obj.get("postCode") or obj.get("postcode") or obj.get("post_code"))
        raw_price_val = obj.get("price")
        if raw_price_val is None:
            for k in ["Price", "unit_price", "UnitPrice"]:
                if k in obj:
                    raw_price_val = obj[k]; break
        raw_price = normalize_str(raw_price_val)
        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)
        errs = []
        if not raw_sku: errs.append("sku missing")
        if not raw_pc: errs.append("postCode missing")
        if raw_price == "": errs.append("price missing")
        if raw_sku and not ok_sku: errs.append(sku_err)
        if raw_pc and not ok_pc: errs.append(pc_err)
        if raw_price != "" and not ok_price: errs.append(price_err)
        if errs:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            return
        doc_id = f"{raw_sku}{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            return
        seen_ids.add(doc_id)
        doc = build_doc(raw_sku, raw_pc, norm_price)
        valid_docs.append(doc)
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for i, obj in enumerate(data, start=1):
                if not isinstance(obj, dict):
                    errors.append({"row": i, "context": "", "error": "Each item must be a JSON object"})
                    continue
                validate_obj(obj, i)
            return valid_docs, errors, warnings
        else:
            warnings.append("Top-level JSON is not an array; falling back to NDJSON parser.")
    except json.JSONDecodeError:
        warnings.append("JSON is not an array; attempting NDJSON (one JSON object per line).")
    try:
        with open(file_path, encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        errors.append({"row": i, "context": "", "error": "Line is not a JSON object"})
                        continue
                    validate_obj(obj, i)
                except json.JSONDecodeError as e:
                    errors.append({"row": i, "context": "", "error": f"Invalid JSON: {e}"})
    except Exception as e:
        errors.append({"row": 0, "context": "", "error": f"Error reading file line-by-line: {e}"})
    return valid_docs, errors, warnings
def validate_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return validate_csv(file_path)
    elif ext == ".json":
        return validate_json(file_path)
    else:
        return [], [{"row": 0, "context": "", "error": "Unsupported file type. Use CSV or JSON."}], []
def upload_item_with_retry(doc, container, MAX_RETRIES=5):
    retry_count = 0
    backoff_time = 1.0
    while retry_count < MAX_RETRIES:
        try:
            container.upsert_item(doc)
            ru = Decimal("0")
            try:
                ru_val = container.client_connection.last_response_headers.get("x-ms-request-charge")
                if ru_val is not None:
                    ru = Decimal(str(ru_val))
            except Exception:
                pass
            return True, ru, None
        except exceptions.CosmosResourceThrottleError:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                error_msg = f"Max retries reached after throttling. Skipping item {doc['id']}."
                return False, Decimal("0"), error_msg
            logger.warning(f"Throttled on {doc['id']}. Retrying in {backoff_time:.1f}s (Attempt {retry_count}/{MAX_RETRIES}).")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)
        except Exception as e:
            error_msg = f"Fatal error uploading {doc['id']}: {e}"
            return False, Decimal("0"), error_msg
    return False, Decimal("0"), f"Failed to upload {doc['id']} after all attempts (unknown error)."
class TaskManagerApp:
    def __init__(self, root, num_workers=4):
        self.root = root
        self.root.title("Freight Matrix Task Manager")
        self.load_settings()
        self.num_workers = app_state.get("max_workers", num_workers)
        self.file_queue = PriorityQueue() 
        self.running = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers)
        self.task_rows = {}          
        self.task_metadata = {}      
        self.task_counter = 0
        self.worker_threads = []
        self.stop_event = threading.Event()
        self.paused_tasks = {}       
        self.canceled_tasks = {}     
        self.single_sku_entry = None
        self.single_postcode_entry = None
        self.single_price_entry = None
        self.log_area = None
        self.setup_ui()
        self.configure_styles()
        self.root.protocol("WM_DELETE_WINDOW", self.on_exit)
    def apply_log_level(self, level_name: str):
        level = logging.INFO
        if level_name.upper() == "DEBUG":
            level = logging.DEBUG
        elif level_name.upper() == "WARNING":
            level = logging.WARNING
        elif level_name.upper() == "ERROR":
            level = logging.ERROR
        logger.setLevel(level)
    def load_settings(self):
        global app_state
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for k, v in DEFAULT_SETTINGS.items():
                    if k == "max_workers":
                        try:
                            app_state[k] = int(data.get(k, v))
                        except (ValueError, TypeError):
                            app_state[k] = v
                    else:
                        app_state[k] = data.get(k, v)
                self.apply_log_level(app_state.get("log_level", "INFO"))
                logger.info("Settings loaded from %s", SETTINGS_FILE)
            except Exception as e:
                logger.error("Failed to load settings: %s", e)
        else:
            self.apply_log_level(app_state.get("log_level", "INFO"))
    def save_settings(self):
        try:
            save_data = app_state.copy()
            save_data['max_workers'] = int(save_data.get('max_workers', DEFAULT_SETTINGS['max_workers']))
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=2)
            logger.info("Settings saved to %s", SETTINGS_FILE)
            messagebox.showinfo("Settings", "Settings saved successfully.")
        except Exception as e:
            logger.error("Failed to save settings: %s", e)
            messagebox.showerror("Settings", f"Failed to save settings:\n{e}")
    def get_cosmos_container(self):
        conn = app_state["connection_string"].strip()
        dbname = app_state["database_name"].strip()
        cname = app_state["container_name"].strip()
        client = CosmosClient.from_connection_string(conn)
        db = client.get_database_client(dbname)
        return db.get_container_client(cname)
    def preflight_cosmos_connection(self):
        try:
            client = CosmosClient.from_connection_string(app_state["connection_string"].strip())
            db = client.get_database_client(app_state["database_name"].strip())
            _ = db.read()
            container = db.get_container_client(app_state["container_name"].strip())
            _ = container.read()
            return True, None
        except Exception as e:
            return False, e
    def test_connection_action(self):
        ok, err = self.preflight_cosmos_connection()
        if ok:
            messagebox.showinfo("Connection Test", "Connection successful!")
        else:
            messagebox.showerror("Connection Test Failed", str(err))
    def apply_settings_from_ui(self, conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var):
        global app_state
        app_state["connection_string"] = conn_var.get().strip()
        app_state["database_name"] = db_var.get().strip()
        app_state["container_name"] = cont_var.get().strip()
        app_state["allow_partial_upload"] = bool(partial_var.get())
        app_state["log_level"] = loglevel_var.get()
        try:
            new_workers = int(max_workers_var.get())
            if new_workers != self.num_workers:
                 messagebox.showwarning("Worker Change", f"Max workers changed from {self.num_workers} to {new_workers}. Restart the queue to apply.")
            self.num_workers = new_workers
            app_state["max_workers"] = new_workers
        except ValueError:
            app_state["max_workers"] = DEFAULT_SETTINGS["max_workers"]
            logger.error("Invalid value for Max Workers. Defaulting to %d.", app_state["max_workers"])
        self.apply_log_level(app_state["log_level"])
    def upload_single_sku_price(self, raw_sku, raw_pc, raw_price):
        if not self.log_area:
             messagebox.showerror("Error", "Log area not initialized.")
             return
        self.log_area.insert(tk.END, "--- Starting Single SKU Update ---\n")
        self.log_area.see(tk.END)
        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)
        errs = []
        if not ok_sku: errs.append(f"SKU error: {sku_err}")
        if not ok_pc: errs.append(f"Postcode error: {pc_err}")
        if not ok_price: errs.append(f"Price error: {price_err}")
        if errs:
            msg = f"Validation failed:\n- " + "\n- ".join(errs)
            self.log_area.insert(tk.END, msg + "\n")
            self.log_area.see(tk.END)
            messagebox.showerror("Validation Error", msg)
            return
        ok_conn, err_conn = self.preflight_cosmos_connection()
        if not ok_conn:
            msg = f"Cosmos connection failed. Please verify connection string / DB / container.\nDetails: {err_conn}"
            self.log_area.insert(tk.END, msg + "\n")
            self.log_area.see(tk.END)
            messagebox.showerror("Connection Error", msg)
            return
        doc = build_doc(raw_sku, raw_pc, norm_price)
        container = self.get_cosmos_container()
        self.log_area.insert(tk.END, f"Attempting upload for ID: {doc['id']}...\n")
        self.log_area.see(tk.END)
        success, ru, error_msg = upload_item_with_retry(doc, container, MAX_RETRIES=5)
        if success:
            final_msg = f"SUCCESS: Item {doc['id']} updated. RU charge: {ru:.2f}"
            self.log_area.insert(tk.END, final_msg + "\n")
            messagebox.showinfo("Success", final_msg)
        else:
            final_msg = f"FAILURE: Item {doc['id']} failed. Reason: {error_msg}"
            self.log_area.insert(tk.END, final_msg + "\n")
            messagebox.showerror("Upload Failed", final_msg)
        self.log_area.see(tk.END)
    def configure_styles(self):
        self.style = ttk.Style()
        self.task_table.tag_configure('queued', foreground='gray', font='Arial 8')
        self.task_table.tag_configure('processing', foreground='blue', font='Arial 8 bold')
        self.task_table.tag_configure('paused', foreground='#ff8c00', font='Arial 8') 
        self.task_table.tag_configure('completed', foreground='green', font='Arial 8 bold') 
        self.task_table.tag_configure('failed', foreground='red', font='Arial 8 bold') 
        self.task_table.tag_configure('canceled', foreground='purple', font='Arial 8 bold') 
        self.progress_styles = {}
        for i in range(101):
            tag_name = f"progress_{i}"
            progress_width = int(i / 100.0 * 1000)
            progress_color = '#a6e3a6'
            background_color = '#e5e5e5'
            if i == 100:
                self.style.configure(tag_name, 
                                     fieldbackground=[("selected", "SystemHighlight"), ("!selected", "green")], 
                                     foreground=[("selected", "white"), ("!selected", "black")])
            else:
                self.style.configure(tag_name, 
                                     fieldbackground=[
                                         ("selected", "SystemHighlight"), 
                                         ("!selected", 
                                          [('progress_bg.Tredge', 0, progress_width, progress_color), 
                                           ('progress_bg.Tredge', progress_width, 1000, background_color)]
                                         )
                                     ], 
                                     foreground=[("selected", "white"), ("!selected", "black")])
            self.task_table.tag_configure(tag_name, background="", font='Arial 8 bold')
            self.progress_styles[i] = tag_name
    def setup_ui(self):
        self.root.geometry("1000x600")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        task_container = ttk.Frame(self.notebook, padding="10 10 10 10")
        self.notebook.add(task_container, text="🏃 Queue Manager")
        task_container.rowconfigure(2, weight=1)
        task_container.columnconfigure(0, weight=1)
        settings_frame = ttk.LabelFrame(task_container, text="⚙️ Queue Settings", padding=5)
        settings_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10), columnspan=2)
        ttk.Label(settings_frame, text="Default Priority:").grid(row=0, column=0, padx=5, pady=2, sticky="w")
        self.default_priority = tk.StringVar(value="Medium")
        self.priority_combo = ttk.Combobox(settings_frame, textvariable=self.default_priority, 
                                           values=["High", "Medium", "Low"], state="readonly", width=10)
        self.priority_combo.grid(row=0, column=1, padx=5, pady=2, sticky="w")
        ttk.Label(settings_frame, text="File Filter (e.g., csv,json):").grid(row=0, column=2, padx=(20,5), pady=2, sticky="w")
        self.file_filter = tk.StringVar(value="csv,json") # Default to supported types
        ttk.Entry(settings_frame, textvariable=self.file_filter, width=15).grid(row=0, column=3, padx=5, pady=2, sticky="w")
        self.recursive_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(settings_frame, text="Recursive Search", variable=self.recursive_var).grid(row=0, column=4, padx=(20, 5), pady=2, sticky="w")
        controls = ttk.Frame(task_container)
        controls.grid(row=1, column=0, sticky="ew", pady=(0, 10), columnspan=2)
        ttk.Button(controls, text="➕ Add File", command=self.add_file).grid(row=0, column=0, padx=5)
        ttk.Button(controls, text="📁 Add Folder", command=self.add_folder).grid(row=0, column=1, padx=5)
        self.start_stop_button = ttk.Button(controls, text="▶️ Start Queue", command=self.toggle_workers, style='TButton', width=15)
        self.start_stop_button.grid(row=0, column=2, padx=(20, 5))
        ttk.Button(controls, text="⏸️ Pause Selected", command=lambda: self.control_task("Pause")).grid(row=0, column=3, padx=5)
        ttk.Button(controls, text="▶️ Resume Selected", command=lambda: self.control_task("Resume")).grid(row=0, column=4, padx=5)
        ttk.Button(controls, text="🛑 Cancel Selected", command=lambda: self.control_task("Cancel")).grid(row=0, column=5, padx=5)
        ttk.Button(controls, text="🗑️ Clear Queue", command=self.clear_queue).grid(row=0, column=6, padx=(20, 5))
        ttk.Button(controls, text="❌ Exit Safely", command=self.on_exit).grid(row=0, column=7, padx=5)
        task_tab = ttk.Frame(task_container)
        task_tab.grid(row=2, column=0, sticky="nsew")
        task_tab.grid_rowconfigure(0, weight=1)
        task_tab.grid_columnconfigure(0, weight=1)
        self.task_table = ttk.Treeview(task_tab, columns=("id", "file", "priority", "size", "modified", "status", "progress", "started"), show="headings", height=15)
        cols = {"id": 60, "file": 300, "priority": 80, "size": 80, "modified": 120, "status": 90, "progress": 100, "started": 120}
        for col, width in cols.items():
            heading_text = col.title().replace('Id', 'ID')
            self.task_table.heading(col, text=heading_text, command=lambda _col=col: self.treeview_sort_column(self.task_table, _col, False))
            self.task_table.column(col, width=width, anchor="w")
        self.task_table.column("id", anchor="center")
        self.task_table.column("priority", anchor="center")
        self.task_table.column("status", anchor="center")
        self.task_table.column("progress", anchor="center")
        self.task_table.grid(row=0, column=0, sticky="nsew")
        self.task_table.bind("<Double-1>", lambda e: self.on_header_double_click(e, self.task_table))
        task_scroll = ttk.Scrollbar(task_tab, orient="vertical", command=self.task_table.yview)
        task_scroll.grid(row=0, column=1, sticky="ns")
        self.task_table.configure(yscrollcommand=task_scroll.set)
        history_tab = ttk.Frame(self.notebook, padding="10 10 10 10")
        history_tab.grid_rowconfigure(0, weight=1)
        history_tab.grid_columnconfigure(0, weight=1)
        self.notebook.add(history_tab, text="✅ History")
        self.history_table = ttk.Treeview(history_tab, columns=("id", "file", "priority", "completed", "duration", "status"), show="headings", height=15)
        history_cols = {"id": 60, "file": 350, "priority": 80, "completed": 150, "duration": 100, "status": 100}
        for col, width in history_cols.items():
            heading_text = col.title().replace('Id', 'ID')
            self.history_table.heading(col, text=heading_text, command=lambda _col=col: self.treeview_sort_column(self.history_table, _col, False))
            self.history_table.column(col, width=width, anchor="w")
        self.history_table.column("id", anchor="center")
        self.history_table.column("priority", anchor="center")
        self.history_table.column("duration", anchor="center")
        self.history_table.column("status", anchor="center")
        self.history_table.grid(row=0, column=0, sticky="nsew")
        self.history_table.bind("<Double-1>", lambda e: self.on_header_double_click(e, self.history_table))
        history_scroll = ttk.Scrollbar(history_tab, orient="vertical", command=self.history_table.yview)
        history_scroll.grid(row=0, column=1, sticky="ns")
        self.history_table.configure(yscrollcommand=history_scroll.set)
        export_button = ttk.Button(history_tab, text="💾 Export History to CSV", command=self.export_history)
        export_button.grid(row=1, column=0, columnspan=2, pady=5, sticky="e")
        self.autofit_columns(self.history_table)
        tab_upload = ttk.Frame(self.notebook, padding="10 10 10 10")
        self.notebook.add(tab_upload, text="Azure Upload")
        left = ttk.Frame(tab_upload, padding=10)
        left.grid(row=0, column=0, sticky="nsw")
        right = ttk.Frame(tab_upload, padding=10)
        right.grid(row=0, column=1, sticky="nsew")
        tab_upload.columnconfigure(1, weight=1)
        tab_upload.rowconfigure(0, weight=1)
        ttk.Label(left, text="SKU:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        self.single_sku_entry = ttk.Entry(left, width=28)
        self.single_sku_entry.grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(left, text="Postcode:").grid(row=1, column=0, sticky="e", padx=4, pady=4)
        self.single_postcode_entry = ttk.Entry(left, width=28)
        self.single_postcode_entry.grid(row=1, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(left, text="Price:").grid(row=2, column=0, sticky="e", padx=4, pady=4)
        self.single_price_entry = ttk.Entry(left, width=28)
        self.single_price_entry.grid(row=2, column=1, sticky="w", padx=4, pady=4)
        send_button = ttk.Button(left, text="Single SKU Update",
                                 command=lambda: self.upload_single_sku_price(self.single_sku_entry.get(), self.single_postcode_entry.get(), self.single_price_entry.get()))
        send_button.grid(row=3, column=0, columnspan=2, sticky="ew", padx=4, pady=(8, 4))
        ttk.Label(left, text="Bulk uploads are managed\nvia the 'Queue Manager' tab.", foreground='blue').grid(row=4, column=0, columnspan=2, sticky="ew", padx=4, pady=4)
        ttk.Label(right, text="Single Upload Log:").pack(fill="x")
        self.log_area = scrolledtext.ScrolledText(right, width=80, height=24)
        self.log_area.pack(fill="both", expand=True)
        tab_settings = ttk.Frame(self.notebook, padding="10 10 10 10")
        self.notebook.add(tab_settings, text="System Settings")
        conn_var = tk.StringVar(value=app_state["connection_string"])
        db_var = tk.StringVar(value=app_state["database_name"])
        cont_var = tk.StringVar(value=app_state["container_name"])
        partial_var = tk.IntVar(value=1 if app_state["allow_partial_upload"] else 0)
        loglevel_var = tk.StringVar(value=app_state["log_level"])
        max_workers_var = tk.IntVar(value=self.num_workers) # Use the current max workers count
        tab_settings.columnconfigure(1, weight=1)
        ttk.Label(tab_settings, text="Connection String:").grid(row=0, column=0, sticky="e", padx=6, pady=6)
        conn_entry = ttk.Entry(tab_settings, textvariable=conn_var, width=80)
        conn_entry.grid(row=0, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(tab_settings, text="Database Name:").grid(row=1, column=0, sticky="e", padx=6, pady=6)
        db_entry = ttk.Entry(tab_settings, textvariable=db_var, width=40)
        db_entry.grid(row=1, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(tab_settings, text="Container Name:").grid(row=2, column=0, sticky="e", padx=6, pady=6)
        cont_entry = ttk.Entry(tab_settings, textvariable=cont_var, width=40)
        cont_entry.grid(row=2, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(tab_settings, text="Max Concurrent Threads:").grid(row=3, column=0, sticky="e", padx=6, pady=6)
        max_workers_entry = ttk.Entry(tab_settings, textvariable=max_workers_var, width=15)
        max_workers_entry.grid(row=3, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(tab_settings, text="Log Level:").grid(row=4, column=0, sticky="e", padx=6, pady=6)
        loglevel_combo = ttk.Combobox(tab_settings, textvariable=loglevel_var, values=["DEBUG", "INFO", "WARNING", "ERROR"], width=12, state="readonly")
        loglevel_combo.grid(row=4, column=1, sticky="w", padx=6, pady=6)
        partial_check = ttk.Checkbutton(tab_settings, text="Allow partial upload if validation errors exist", variable=partial_var)
        partial_check.grid(row=5, column=1, sticky="w", padx=6, pady=6)
        btn_frame = ttk.Frame(tab_settings)
        btn_frame.grid(row=6, column=1, sticky="w", padx=6, pady=10)
        apply_btn = ttk.Button(btn_frame, text="Apply (No Save)", 
                               command=lambda: (self.apply_settings_from_ui(conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var), messagebox.showinfo("Settings", "Settings applied.")))
        apply_btn.grid(row=0, column=0, padx=4)
        save_btn = ttk.Button(btn_frame, text="Save Settings", 
                              command=lambda: (self.apply_settings_from_ui(conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var), self.save_settings()))
        save_btn.grid(row=0, column=1, padx=4)
        test_btn = ttk.Button(btn_frame, text="Test Connection", command=self.test_connection_action)
        test_btn.grid(row=0, column=2, padx=4)
        help_text = (
            "Tips:\n"
            "- Enter the exact Primary connection string from Azure Portal.\n"
            "- 'Apply' loads settings for the current session; 'Save' persists to app_settings.json.\n"
            "- **Max Concurrent Threads** sets the worker limit for **all** bulk tasks.\n"
            "- All uploads use concurrent threads with automatic retry logic."
        )
        ttk.Label(tab_settings, text=help_text, foreground="#555", justify=tk.LEFT).grid(row=7, column=0, columnspan=2, sticky="w", padx=6, pady=6)
    def start_workers(self):
        if not self.running:
            self.running = True
            self.stop_event.clear()
            if self.executor._shutdown or self.executor._max_workers != self.num_workers:
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) 
            self.worker_threads.clear()
            for _ in range(self.num_workers):
                t = threading.Thread(target=self.worker_loop, daemon=True)
                t.start()
                self.worker_threads.append(t)
            for file_path in list(self.paused_tasks.keys()):
                self.root.after(0, lambda fp=file_path: self.control_task_internal(fp, "Resume"))
            self.start_stop_button.config(text="⏸️ Stop Queue")
    def stop_workers(self):
        if self.running:
            self.running = False
            self.stop_event.set()
            self.root.after(0, self.update_queued_to_paused)
            self.start_stop_button.config(text="▶️ Start Queue")
    def toggle_workers(self):
        if self.running:
            self.stop_workers()
        else:
            self.start_workers()
    def update_queued_to_paused(self):
        for file_path in list(self.task_rows.keys()):
            row_id = self.task_rows.get(file_path)
            if row_id:
                status = self.task_table.item(row_id, "values")[5] 
                if status == "Queued":
                    self.update_status(file_path, "Paused")
                    self.paused_tasks[file_path] = True 
    def clear_queue(self):
        if self.running:
            messagebox.showerror("Error", "Stop the queue before clearing it.")
            return
        if not self.file_queue.empty() or self.task_rows:
            if not messagebox.askyesno("Clear Queue", "Are you sure you want to clear all pending tasks?"):
                return
        self.paused_tasks.clear()
        self.canceled_tasks.clear()
        for path in self.file_queue.get_all_paths():
            try:
                self.file_queue.get(timeout=0)
                self.file_queue.task_done()
            except queue.Empty:
                pass
        for row_id in self.task_table.get_children():
            self.task_table.delete(row_id)
        self.task_rows.clear()
        self.task_metadata.clear()
        messagebox.showinfo("Queue Cleared", "All pending tasks have been removed.")
    def control_task(self, action):
        selected_items = self.task_table.selection()
        if not selected_items:
            messagebox.showinfo("Selection Error", "Please select one or more tasks.")
            return
        for item_id in selected_items:
            vals = self.task_table.item(item_id, "values")
            file_name = vals[1] 
            file_path = next((k for k, v in self.task_rows.items() if v == item_id and os.path.basename(k) == file_name), None)
            if file_path:
                self.control_task_internal(file_path, action)
    def control_task_internal(self, file_path, action):
        row_id = self.task_rows.get(file_path)
        if not row_id: return
        if action == "Pause":
            if file_path not in self.canceled_tasks:
                self.paused_tasks[file_path] = True
                self.update_status(file_path, "Paused")
        elif action == "Resume":
            if file_path in self.paused_tasks and file_path not in self.canceled_tasks:
                del self.paused_tasks[file_path]
                self.update_status(file_path, "Queued")
                priority = self.task_metadata[file_path]["priority"]
                task_id = self.task_metadata[file_path]["id"]
                self.file_queue.put(file_path, priority, task_id)
        elif action == "Cancel":
            if self.file_queue.contains(file_path):
                pass
            self.canceled_tasks[file_path] = True
            self.update_status(file_path, "Cancelling...")
            self.root.after(500, lambda: self.finish_task(file_path, "Canceled"))
    def add_file(self):
        priority = self.default_priority.get()
        file_path = filedialog.askopenfilename(filetypes=[("Data files", "*.csv;*.json"), ("All files", "*.*")]) 
        if file_path:
            self.add_task_to_queue(file_path, priority)
    def add_folder(self):
        priority = self.default_priority.get()
        folder_path = filedialog.askdirectory()
        if folder_path:
            is_recursive = self.recursive_var.get()
            extensions = [ext.strip().lower() for ext in self.file_filter.get().split(',') if ext.strip()]
            if is_recursive:
                for root, _, files in os.walk(folder_path):
                    for entry in files:
                        file_path = os.path.join(root, entry)
                        self.check_and_add_file(file_path, priority, extensions)
            else:
                for entry in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, entry)
                    if os.path.isfile(file_path):
                        self.check_and_add_file(file_path, priority, extensions)
    def check_and_add_file(self, file_path, priority, extensions):
        if file_path in self.task_rows: return
        if extensions:
            ext = os.path.splitext(file_path)[1].lstrip('.').lower()
            if ext not in extensions:
                return 
        self.add_task_to_queue(file_path, priority)
    def add_task_to_queue(self, file_path, priority):
        if file_path in self.task_rows:
            messagebox.showinfo("Duplicate", f"File already in queue: {os.path.basename(file_path)}")
            return
        self.task_counter += 1
        task_id = self.task_counter
        started = datetime.now()
        try:
            file_size = os.path.getsize(file_path)
            modified_ts = os.path.getmtime(file_path)
            modified_date = datetime.fromtimestamp(modified_ts).strftime("%Y-%m-%d")
        except:
            file_size = 0
            modified_date = "N/A"
        file_size_str = self.format_file_size(file_size)
        row_id = self.task_table.insert("", tk.END, 
                                        values=(task_id, os.path.basename(file_path), priority, file_size_str, modified_date, "Queued", "0%", started.strftime("%Y-%m-%d %H:%M:%S")),
                                        tags=('queued', 'progress_0'))
        self.task_rows[file_path] = row_id
        self.task_metadata[file_path] = {
            "id": task_id, 
            "start": started, 
            "priority": priority, 
            "size": file_size_str, 
            "modified": modified_date,
            "total_docs": 0,
            "uploaded_docs": 0
        }
        self.file_queue.put(file_path, priority, task_id)
    def format_file_size(self, size_bytes):
        if size_bytes == 0: return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = 0
        while size_bytes >= 1024 and i < len(size_name) - 1:
            size_bytes /= 1024
            i += 1
        return f"{size_bytes:,.1f} {size_name[i]}"
    def worker_loop(self):
        while self.running:
            try:
                file_path = self.file_queue.get(timeout=0.1)
                if not self.running:
                    priority = self.task_metadata.get(file_path, {}).get("priority", "Medium")
                    task_id = self.task_metadata.get(file_path, {}).get("id", 0)
                    self.file_queue.put(file_path, priority, task_id) 
                    break
                if file_path in self.canceled_tasks or file_path in self.paused_tasks:
                    self.file_queue.task_done()
                    continue
                self.executor.submit(self.process_file, file_path)
                self.file_queue.task_done()
            except queue.Empty:
                if self.stop_event.is_set():
                    break
                continue
    def process_file(self, file_path):
        if not self.running or file_path in self.canceled_tasks or file_path in self.paused_tasks:
            self.root.after(0, lambda: self.update_status(file_path, "Paused" if file_path in self.paused_tasks else "Queued"))
            return
        try:
            self.root.after(0, lambda: self.update_status(file_path, "Processing"))
            ok_conn, err_conn = self.preflight_cosmos_connection()
            if not ok_conn:
                self.root.after(0, lambda: messagebox.showerror("Connection Error", f"Cosmos connection failed for {os.path.basename(file_path)}:\n{err_conn}"))
                self.root.after(0, lambda: self.finish_task(file_path, "Failed"))
                return
            valid_docs, errors, warnings = validate_file(file_path)
            for w in warnings:
                logger.warning(f"File {os.path.basename(file_path)}: {w}")
            report_path = f"{file_path}.errors.csv"
            allow_partial = app_state.get("allow_partial_upload", False)
            if errors and not allow_partial:
                write_error_report(errors, report_path)
                logger.error(f"File {os.path.basename(file_path)}: Validation failed with {len(errors)} errors (strict mode).")
                self.root.after(0, lambda: self.finish_task(file_path, "Failed"))
                return
            if errors and allow_partial:
                write_error_report(errors, report_path)
                logger.warning(f"File {os.path.basename(file_path)}: Partial mode. Errors: {len(errors)}. Uploading {len(valid_docs)} valid rows.")
            if not valid_docs:
                logger.info(f"File {os.path.basename(file_path)}: No valid documents to upload.")
                self.root.after(0, lambda: self.finish_task(file_path, "Completed"))
                return
            self.task_metadata[file_path]["total_docs"] = len(valid_docs)
            container = self.get_cosmos_container()
            uploaded = 0
            total_ru = Decimal("0")
            MAX_RETRIES = 5
            futures = []
            for i, doc in enumerate(valid_docs, start=1):
                if not self.running or file_path in self.canceled_tasks or file_path in self.paused_tasks:
                    self.root.after(0, lambda: self.update_status(file_path, "Paused" if file_path in self.paused_tasks else "Cancelling..."))
                    return
                success, ru, error_msg = upload_item_with_retry(doc, container, MAX_RETRIES)
                if success:
                    uploaded += 1
                    total_ru += ru
                else:
                    logger.error(f"Failed item in bulk {doc['id']}: {error_msg}")
                if i % 50 == 0 or i == len(valid_docs):
                    progress_value = int((i / len(valid_docs)) * 100)
                    self.task_metadata[file_path]["uploaded_docs"] = uploaded
                    self.root.after(0, lambda val=progress_value: self.update_progress(file_path, val))
            if file_path not in self.canceled_tasks and file_path not in self.paused_tasks:
                logger.info(f"Bulk upload of {os.path.basename(file_path)} complete. Uploaded: {uploaded}/{len(valid_docs)}. Total RU: {total_ru:.2f}")
                self.root.after(0, lambda: self.finish_task(file_path, "Completed"))
        except Exception as e:
            logger.error(f"Task execution failed for {os.path.basename(file_path)}: {e}")
            self.root.after(0, lambda: self.finish_task(file_path, "Failed"))
    def update_status(self, file_path, status):
        row_id = self.task_rows.get(file_path)
        if row_id:
            vals = list(self.task_table.item(row_id, "values"))
            vals[5] = status 
            current_tags = list(self.task_table.item(row_id, "tags"))
            new_tags = [t for t in current_tags if not t in ('queued', 'processing', 'paused', 'completed', 'failed', 'canceled')]
            if status == "Queued":
                new_tags.append('queued')
            elif status == "Processing":
                new_tags.append('processing')
            elif status == "Paused":
                new_tags.append('paused')
            self.task_table.item(row_id, values=vals, tags=tuple(new_tags))
    def update_progress(self, file_path, value):
        row_id = self.task_rows.get(file_path)
        if row_id:
            vals = list(self.task_table.item(row_id, "values"))
            meta = self.task_metadata.get(file_path, {})
            uploaded = meta.get("uploaded_docs", 0)
            total = meta.get("total_docs", 0)
            if total > 0:
                vals[6] = f"{uploaded}/{total} ({value}%)" # Show actual count/total
            else:
                vals[6] = f"{value}%" 
            current_tags = list(self.task_table.item(row_id, "tags"))
            new_tags = [t for t in current_tags if not t.startswith('progress_')]
            new_tags.append(self.progress_styles.get(value, 'progress_0'))
            self.task_table.item(row_id, values=vals, tags=tuple(new_tags))
    def finish_task(self, file_path, status):
        self.paused_tasks.pop(file_path, None)
        self.canceled_tasks.pop(file_path, None)
        if file_path in self.task_rows:
            row_id = self.task_rows.pop(file_path)
            current_vals = self.task_table.item(row_id, "values") 
            if file_path in self.task_metadata:
                metadata = self.task_metadata.pop(file_path)
                task_id = metadata["id"]
                start_time = metadata["start"]
                priority = metadata["priority"]
                end_time = datetime.now()
                duration = round((end_time - start_time).total_seconds(), 2)
            else:
                task_id = 0
                priority = "N/A"
                end_time = datetime.now()
                duration = 0
            tag = status.lower()
            self.history_table.insert("", tk.END, values=(task_id, os.path.basename(file_path), priority, end_time.strftime("%Y-%m-%d %H:%M:%S"), duration, status), tags=(tag))
            self.autofit_columns(self.history_table)
            progress_tag = 'progress_100' if status == "Completed" else 'progress_0'
            self.task_table.item(row_id, values=(current_vals[0], current_vals[1], current_vals[2], current_vals[3], current_vals[4], status, "100%", current_vals[7]), tags=(tag, progress_tag))
            self.root.after(500, lambda: self.task_table.delete(row_id))
    def export_history(self):
        save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")], title="Save History as CSV")
        if save_path:
            try:
                with open(save_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Task ID", "File Name", "Priority", "Completed At", "Duration (s)", "Finish Status"])
                    for row_id in self.history_table.get_children():
                        values = self.history_table.item(row_id, "values")
                        writer.writerow(values)
                messagebox.showinfo("Export Successful", f"History exported to {save_path}")
            except Exception as e:
                messagebox.showerror("Export Failed", f"Error exporting history: {e}")
    def on_exit(self):
        if not self.file_queue.empty() or self.running:
            if not messagebox.askyesno("Exit", "Queue still has pending tasks or workers are running. Exit anyway?"):
                return
        self.stop_workers()
        self.executor.shutdown(wait=False)
        for t in self.worker_threads:
            if t.is_alive():
                 t.join(timeout=0.1)
        self.root.destroy()
    def autofit_columns(self, treeview):
        font = tkFont.Font(font=('TkDefaultFont', 9))
        for col in treeview["columns"]:
            max_width = font.measure(treeview.heading(col, "text")) + 10
            for row_id in treeview.get_children():
                val = treeview.set(row_id, col)
                if val:
                    width = font.measure(str(val))
                    if width > max_width:
                        max_width = width
            treeview.column(col, width=max_width + 10 if max_width < 50 else max_width + 10)
    def autofit_column(self, treeview, col_id):
        font = tkFont.Font(font=('TkDefaultFont', 9))
        col_index = int(col_id.replace("#", "")) - 1
        col_name = treeview["columns"][col_index]
        max_width = font.measure(treeview.heading(col_name, "text")) + 10
        for row_id in treeview.get_children():
            val = treeview.set(row_id, col_name)
            if val:
                width = font.measure(str(val))
                if width > max_width:
                    max_width = width
        treeview.column(col_name, width=max_width + 10 if max_width < 50 else max_width + 10)
    def on_header_double_click(self, event, treeview):
        region = treeview.identify_region(event.x, event.y)
        if region == "heading":
            col_id = treeview.identify_column(event.x)
            self.autofit_column(treeview, col_id)
    def treeview_sort_column(self, tv, col, reverse):
        items = [(tv.set(k, col), k) for k in tv.get_children('')]
        if col == "priority":
            priority_map = {"High": 1, "Medium": 2, "Low": 3, "N/A": 4}
            items.sort(key=lambda t: priority_map.get(t[0], 4), reverse=reverse)
        else:
            try:
                items.sort(key=lambda t: float(t[0].strip('%').split('/')[0] if isinstance(t[0], str) and ('%' in t[0] or '/' in t[0]) else t[0]), reverse=reverse)
            except ValueError:
                items.sort(key=lambda t: t[0], reverse=reverse)
        for index, (_, k) in enumerate(items):
            tv.move(k, '', index)
        tv.heading(col, command=lambda: self.treeview_sort_column(tv, col, not reverse))
if __name__ == "__main__":
    root = tk.Tk()
    app = TaskManagerApp(root)
    root.mainloop()
