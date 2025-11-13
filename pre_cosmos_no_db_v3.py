import os
import csv
import json
import math
import threading
import queue
import platform
import subprocess
from datetime import datetime
from collections import defaultdict
from statistics import mean

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS, WARNING

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".csv_task_manager_settings.json")

# ---------------------------------------------------------------------------
# VALIDATION + NORMALISATION
# ---------------------------------------------------------------------------

CSV_FIELD_ALIASES = {
    "sku": ["sku", "productcode", "productCode", "product_code", "product id", "productid"],
    "postCode": ["postcode", "postCode", "post_code", "post code", "zip", "zip_code"],
    "price": ["price", "unit_price", "unitPrice", "unitprice", "unit price", "amount"],
}


def normalize_str(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v).strip()
    return str(v).strip().strip('"').strip()


def _lower_keys(d: dict) -> dict:
    return {(k or "").strip().lower(): v for k, v in d.items()}


def field_from_row(row: dict, key: str):
    for alias in CSV_FIELD_ALIASES.get(key, []):
        a = alias.lower()
        if a in row:
            return row.get(a)
    return None


def is_valid_sku(s: str):
    s = normalize_str(s)
    if not s:
        return False, "sku empty"
    for ch in s:
        if not (ch.isalnum() or ch in "-_./"):
            return False, "sku has invalid characters"
    if len(s) > 64:
        return False, "sku too long"
    return True, ""


def is_valid_postcode(pc: str):
    """
    Handle missing leading zero:
    - '200' -> '0200'
    - must be all digits and end up 4 digits
    """
    pc_raw = normalize_str(pc)
    if not pc_raw:
        return False, "", "postCode empty"
    if not pc_raw.isdigit():
        return False, "", "postCode must be numeric"
    if len(pc_raw) == 3:
        pc_norm = "0" + pc_raw
    elif len(pc_raw) == 4:
        pc_norm = pc_raw
    else:
        return False, "", "postCode must be 4 digits"
    return True, pc_norm, ""


def normalize_price(p: str):
    s = normalize_str(p)
    if s == "":
        return False, 0.0, "price empty"
    clean = s.replace(",", "")
    for sym in "$€£AUDaud ":
        clean = clean.replace(sym, "")
    try:
        val = float(clean)
    except Exception:
        return False, 0.0, "price not a number"
    if val < 0:
        return False, 0.0, "price negative"
    if math.isinf(val) or math.isnan(val):
        return False, 0.0, "price not finite"
    return True, round(val, 2), ""


def build_doc(raw_sku: str, norm_pc: str, price_val: float):
    return {
        "sku": normalize_str(raw_sku),
        "postCode": normalize_str(norm_pc),
        "price": float(price_val),
    }


def _validate_from_reader(reader: csv.DictReader):
    valid_docs, errors, warnings = [], [], []
    seen_ids = set()

    raw_fieldnames = reader.fieldnames or []
    fieldnames_lc = [(h or "").strip().lower() for h in raw_fieldnames]
    if not fieldnames_lc:
        errors.append({"row": 1, "context": "header", "error": "Missing header row"})
        return valid_docs, errors, warnings

    missing_min = []
    for key in ["sku", "postCode", "price"]:
        if not any(alias.lower() in fieldnames_lc for alias in CSV_FIELD_ALIASES[key]):
            missing_min.append(key)
    if missing_min:
        errors.append({"row": 1, "context": "header", "error": f"Missing required columns: {', '.join(missing_min)}"})
        return valid_docs, errors, warnings

    for idx, row in enumerate(reader, start=2):
        row = _lower_keys(row)
        raw_sku = normalize_str(field_from_row(row, "sku"))
        raw_pc = normalize_str(field_from_row(row, "postCode"))
        raw_price = normalize_str(field_from_row(row, "price"))

        # skip empty lines
        if not raw_sku and not raw_pc and not raw_price:
            continue

        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, norm_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)

        errs = []
        if not raw_sku:
            errs.append("sku missing")
        if not raw_pc:
            errs.append("postCode missing")
        if not raw_price:
            errs.append("price missing")
        if raw_sku and not ok_sku:
            errs.append(sku_err)
        if raw_pc and not ok_pc:
            errs.append(pc_err)
        if raw_price and not ok_price:
            errs.append(price_err)

        if errs:
            errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            continue

        doc_id = f"{raw_sku}|{norm_pc}"
        if doc_id in seen_ids:
            errors.append(
                {"row": idx, "context": f"sku={raw_sku}, postCode={norm_pc}", "error": "Duplicate id within file"}
            )
            continue
        seen_ids.add(doc_id)

        valid_docs.append(build_doc(raw_sku, norm_pc, norm_price))

    return valid_docs, errors, warnings


def validate_csv(file_path):
    try:
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return _validate_from_reader(reader)
    except Exception as e:
        return [], [{"row": 0, "context": "file", "error": f"Read error: {e}"}], []


def validate_json(file_path):
    valid_docs, errors, warnings = [], [], []
    seen_ids = set()

    def validate_obj(obj, idx_for_report):
        obj_lc = _lower_keys(obj)
        raw_sku = normalize_str(field_from_row(obj_lc, "sku"))
        raw_pc = normalize_str(field_from_row(obj_lc, "postCode"))
        raw_price = normalize_str(field_from_row(obj_lc, "price"))

        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, norm_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)

        errs = []
        if not raw_sku:
            errs.append("sku missing")
        if not raw_pc:
            errs.append("postCode missing")
        if raw_price == "":
            errs.append("price missing")
        if raw_sku and not ok_sku:
            errs.append(sku_err)
        if raw_pc and not ok_pc:
            errs.append(pc_err)
        if raw_price != "" and not ok_price:
            errs.append(price_err)

        if errs:
            errors.append(
                {"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)}
            )
            return

        doc_id = f"{raw_sku}|{norm_pc}"
        if doc_id in seen_ids:
            errors.append(
                {"row": idx_for_report, "context": f"sku={raw_sku}, postCode={norm_pc}", "error": "Duplicate id within file"}
            )
            return

        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, norm_pc, norm_price))

    # Try top-level array first
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

    # NDJSON fallback
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
    if ext == ".json":
        return validate_json(file_path)
    return [], [{"row": 0, "context": "", "error": "Unsupported file type. Use CSV or JSON."}], []


def compute_stats(valid_docs, errors, warnings):
    total_rows_est = len(valid_docs) + len(errors)
    dup_count = sum(1 for e in errors if "Duplicate id" in str(e.get("error", "")))
    uniq_skus = len({d["sku"] for d in valid_docs})
    prices = [d["price"] for d in valid_docs]
    pmin = min(prices) if prices else None
    pmax = max(prices) if prices else None
    pavg = round(mean(prices), 6) if prices else None
    warn_count = len(warnings)

    return {
        "rows_total": total_rows_est,
        "rows_valid": len(valid_docs),
        "rows_invalid": len(errors),
        "duplicates": dup_count,
        "unique_skus": uniq_skus,
        "price_min": pmin,
        "price_max": pmax,
        "price_avg": pavg,
        "warnings": warn_count,
    }


# ---------------------------------------------------------------------------
# TASK MANAGER APP
# ---------------------------------------------------------------------------

class TaskManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Task Manager + CSV/JSON Validator & Batch Export")
        self.root.geometry("1000x420")
        self.style = Style(theme="darkly")

        # Queue + worker
        self.task_queue = queue.Queue()
        self.stop_flag = threading.Event()
        self.processing = False
        self.workers = []

        # Locks
        self.aggregate_lock = threading.Lock()
        self.conflict_lock = threading.Lock()

        # Mapping file path -> Treeview item
        self.path_to_item = {}
        self.column_default_widths = {}
        self.col_visibility_vars = {}

        # Tk variables (settings)
        self.folder_var = tk.StringVar(value=os.path.abspath("."))
        self.output_folder_var = tk.StringVar(value=os.path.abspath("export"))
        self.vendor_id_var = tk.StringVar(value="vendor_001")
        self.include_vendor_var = tk.BooleanVar(value=True)

        self.enable_batch_var = tk.BooleanVar(value=True)
        self.batch_mode_var = tk.StringVar(value="rows")  # rows | group
        self.rows_per_file_var = tk.IntVar(value=1000)
        self.group_column_var = tk.StringVar(value="postCode")

        self.export_csv_var = tk.BooleanVar(value=True)
        self.export_json_var = tk.BooleanVar(value=True)
        self.open_folder_after_var = tk.BooleanVar(value=False)

        self.num_workers_var = tk.IntVar(value=2)

        # Aggregates + conflicts
        self._reset_aggregate()

        # Load settings (if exist)
        self.load_settings()

        # Build UI
        self._build_ui()

        # Close handler
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # ------------------------------------------------------------------ UI

    def _build_ui(self):
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        notebook = ttk.Notebook(self.root)
        notebook.grid(row=0, column=0, sticky="nsew")

        # Tabs
        self.tab_queue = ttk.Frame(notebook, padding=10)
        notebook.add(self.tab_queue, text="Queue")

        self.tab_settings = ttk.Frame(notebook, padding=10)
        notebook.add(self.tab_settings, text="Settings")

        self.tab_conflicts = ttk.Frame(notebook, padding=10)
        notebook.add(self.tab_conflicts, text="Conflicts")

        self.tab_logs = ttk.Frame(notebook, padding=10)
        notebook.add(self.tab_logs, text="Logs")

        # Build contents
        self._build_queue_tab(self.tab_queue)
        self._build_settings_tab(self.tab_settings)
        self._build_conflicts_tab(self.tab_conflicts)
        self._build_logs_tab(self.tab_logs)

    def _build_queue_tab(self, parent):
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        # Folder selection row
        folder_frame = ttk.Frame(parent)
        folder_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        folder_frame.columnconfigure(1, weight=1)

        ttk.Label(folder_frame, text="Source folder:").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.folder_entry = ttk.Entry(folder_frame, textvariable=self.folder_var)
        self.folder_entry.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        ttk.Button(folder_frame, text="Browse", command=self.browse_folder, bootstyle=PRIMARY)\
            .grid(row=0, column=2, padx=(0, 4))
        ttk.Button(folder_frame, text="Scan for CSV/JSON", command=self.scan_folder, bootstyle=INFO)\
            .grid(row=0, column=3)

        # Treeview for files
        cols = (
            "file",
            "status",
            "rows_total",
            "rows_valid",
            "rows_invalid",
            "duplicates",
            "unique_skus",
            "price_min",
            "price_max",
            "price_avg",
            "warnings",
        )
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 8))
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self.file_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=16)
        for c in cols:
            heading = c.replace("_", " ").title()
            self.file_tree.heading(c, text=heading)
            if c == "file":
                width = 260
            elif c == "status":
                width = 90
            else:
                width = 90
            self.file_tree.column(c, width=width, anchor="center")
            self.column_default_widths[c] = width
        self.file_tree.grid(row=0, column=0, sticky="nsew")

        sy = ttk.Scrollbar(tree_frame, orient="vertical", command=self.file_tree.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self.file_tree.configure(yscrollcommand=sy.set)

        # Controls row
        controls = ttk.Frame(parent)
        controls.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        controls.columnconfigure(3, weight=1)

        ttk.Button(controls, text="Start Queue", command=self.start_queue, bootstyle=SUCCESS, width=16)\
            .grid(row=0, column=0, padx=(0, 6))
        ttk.Button(controls, text="Stop After Current", command=self.stop_queue, bootstyle=WARNING, width=18)\
            .grid(row=0, column=1, padx=(0, 6))
        ttk.Button(controls, text="Clear Table", command=self.clear_table, width=14)\
            .grid(row=0, column=2, padx=(0, 6))
        ttk.Button(controls, text="Open Output Folder", command=self.open_output_folder, width=20)\
            .grid(row=0, column=3, sticky="e")
        ttk.Button(controls, text="Export Table to Excel", command=self.export_table_to_excel, width=23)\
            .grid(row=0, column=4, padx=(6, 0))

    def _build_settings_tab(self, parent):
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        # Scrollable canvas
        canvas = tk.Canvas(parent, highlightthickness=0)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        scroll_frame = ttk.Frame(canvas)
        scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")

        # Mouse wheel bindings
        def _on_mousewheel(event):
            # Windows / MacOS
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _on_linux_scroll(event):
            # Linux uses Button-4 and Button-5 events
            if event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")

        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", _on_linux_scroll)
        canvas.bind_all("<Button-5>", _on_linux_scroll)

        scroll_frame.columnconfigure(1, weight=1)

        # Output config
        out_group = ttk.LabelFrame(scroll_frame, text="Output", padding=10)
        out_group.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        out_group.columnconfigure(1, weight=1)

        ttk.Label(out_group, text="Output folder:").grid(row=0, column=0, sticky="w")
        self.out_label = ttk.Label(out_group, text=self.output_folder_var.get(), anchor="w")
        self.out_label.grid(row=0, column=1, sticky="ew", padx=(6, 6))
        ttk.Button(out_group, text="Browse", command=self.choose_output_folder).grid(row=0, column=2, sticky="w")

        ttk.Label(out_group, text="Formats:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        fmt_row = ttk.Frame(out_group)
        fmt_row.grid(row=1, column=1, sticky="w", pady=(6, 0))
        ttk.Checkbutton(fmt_row, text="Export CSV", variable=self.export_csv_var).pack(side="left")
        ttk.Checkbutton(fmt_row, text="Export JSON", variable=self.export_json_var).pack(side="left", padx=(10, 0))

        ttk.Checkbutton(
            out_group,
            text="Open output folder when queue finishes",
            variable=self.open_folder_after_var,
        ).grid(row=2, column=1, sticky="w", pady=(6, 0))

        # Vendor
        vendor_group = ttk.LabelFrame(scroll_frame, text="Vendor", padding=10)
        vendor_group.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        vendor_group.columnconfigure(1, weight=1)

        ttk.Label(vendor_group, text="Vendor ID:").grid(row=0, column=0, sticky="w")
        ttk.Entry(vendor_group, textvariable=self.vendor_id_var, width=24).grid(
            row=0, column=1, sticky="w", padx=(6, 6)
        )
        ttk.Checkbutton(
            vendor_group,
            text="Include vendor_id in CSV/JSON",
            variable=self.include_vendor_var,
        ).grid(row=0, column=2, sticky="w")

        # Batch
        batch_group = ttk.LabelFrame(scroll_frame, text="Batch Export", padding=10)
        batch_group.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        batch_group.columnconfigure(1, weight=1)

        ttk.Checkbutton(
            batch_group,
            text="Enable batch export",
            variable=self.enable_batch_var,
        ).grid(row=0, column=0, columnspan=3, sticky="w")

        ttk.Label(batch_group, text="Mode:").grid(row=1, column=0, sticky="w")
        mode_row = ttk.Frame(batch_group)
        mode_row.grid(row=1, column=1, sticky="w")

        ttk.Radiobutton(
            mode_row,
            text="Rows per file",
            variable=self.batch_mode_var,
            value="rows",
        ).pack(side="left")
        ttk.Radiobutton(
            mode_row,
            text="Group by column",
            variable=self.batch_mode_var,
            value="group",
        ).pack(side="left", padx=(10, 0))

        ttk.Label(batch_group, text="Rows per file:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(batch_group, textvariable=self.rows_per_file_var, width=10).grid(
            row=2, column=1, sticky="w", pady=(6, 0)
        )

        ttk.Label(batch_group, text="Group column:").grid(row=3, column=0, sticky="w", pady=(6, 0))
        group_combo = ttk.Combobox(
            batch_group,
            textvariable=self.group_column_var,
            state="readonly",
            values=["postCode", "sku"],
            width=16,
        )
        group_combo.grid(row=3, column=1, sticky="w", pady=(6, 0))

        # Concurrency
        conc_group = ttk.LabelFrame(scroll_frame, text="Concurrency", padding=10)
        conc_group.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        conc_group.columnconfigure(1, weight=1)

        ttk.Label(conc_group, text="Number of worker threads:").grid(row=0, column=0, sticky="w")
        try:
            spin = ttk.Spinbox(
                conc_group,
                from_=1,
                to=16,
                textvariable=self.num_workers_var,
                width=6,
            )
        except AttributeError:
            spin = ttk.Entry(conc_group, textvariable=self.num_workers_var, width=6)
        spin.grid(row=0, column=1, sticky="w", padx=(6, 0))

        # Queue Columns visibility
        cols_group = ttk.LabelFrame(scroll_frame, text="Queue Columns View", padding=10)
        cols_group.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        cols = self.file_tree["columns"]
        for idx, col in enumerate(cols):
            var = tk.BooleanVar(value=True)
            self.col_visibility_vars[col] = var
            cb = ttk.Checkbutton(
                cols_group,
                text=col.replace("_", " ").title(),
                variable=var,
                command=lambda c=col: self._toggle_column_visibility(c),
            )
            cb.grid(row=idx // 3, column=idx % 3, sticky="w", padx=(0, 10), pady=2)

        # Save settings button
        ttk.Button(scroll_frame, text="Save Settings", command=self.save_settings, bootstyle=SUCCESS, width=20)\
            .grid(row=5, column=0, sticky="w", pady=(10, 0))

    def _build_conflicts_tab(self, parent):
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        cols = ("sku", "postCode", "prices", "files")
        self.conflicts_tree = ttk.Treeview(parent, columns=cols, show="headings", height=20)
        for c in cols:
            heading = c.replace("_", " ").title()
            width = 120 if c in ("sku", "postCode") else 500
            self.conflicts_tree.heading(c, text=heading)
            self.conflicts_tree.column(c, width=width, anchor="w")
        self.conflicts_tree.grid(row=0, column=0, sticky="nsew")

        sy = ttk.Scrollbar(parent, orient="vertical", command=self.conflicts_tree.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self.conflicts_tree.configure(yscrollcommand=sy.set)

    def _build_logs_tab(self, parent):
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        log_frame = ttk.LabelFrame(parent, text="Log", padding=8)
        log_frame.grid(row=0, column=0, sticky="nsew")
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, height=12, wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_sy = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_sy.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_sy.set)

    # ------------------------------------------------------------------ Folder & scanning

    def browse_folder(self):
        path = filedialog.askdirectory(initialdir=self.folder_var.get() or ".")
        if not path:
            return
        self.folder_var.set(path)
        self.log(f"Selected folder: {path}")

    def scan_folder(self):
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Scan", "Invalid folder.")
            return

        self.clear_table()
        self._reset_aggregate()

        exts = {".csv", ".json"}
        count = 0
        for name in sorted(os.listdir(folder)):
            full = os.path.join(folder, name)
            if not os.path.isfile(full):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in exts:
                continue
            item_id = self.file_tree.insert(
                "",
                "end",
                values=(name, "Queued", "", "", "", "", "", "", "", "", "")
            )
            self.path_to_item[full] = item_id
            count += 1

        self.log(f"Scanned folder: {folder} ({count} CSV/JSON files queued)")

    def clear_table(self):
        for iid in self.file_tree.get_children():
            self.file_tree.delete(iid)
        self.path_to_item.clear()
        self._reset_aggregate()
        self._clear_conflicts_table()

    # ------------------------------------------------------------------ Queue control

    def start_queue(self):
        if self.processing:
            messagebox.showinfo("Queue", "Queue is already running.")
            return

        if not self.path_to_item:
            messagebox.showinfo("Queue", "No files in the queue. Scan a folder first.")
            return

        # Rebuild queue from table
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
            except queue.Empty:
                break

        for path, item_id in self.path_to_item.items():
            vals = self.file_tree.item(item_id, "values")
            status = vals[1]
            if status not in ("Processing", "Done"):
                self.task_queue.put(path)
                self._set_status(path, "Queued")

        self.stop_flag.clear()
        self.processing = True
        self._reset_aggregate()
        self._clear_conflicts_table()

        # Start workers
        self.workers = []
        try:
            n = int(self.num_workers_var.get())
        except Exception:
            n = 1
        n = max(1, min(n, 16))

        for _ in range(n):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            self.workers.append(t)
            t.start()

        self.log(f"Queue started with {n} worker(s).")
        self._schedule_monitor()

    def stop_queue(self):
        if not self.processing:
            messagebox.showinfo("Queue", "Queue is not running.")
            return
        self.stop_flag.set()
        self.log("Stop requested – will stop after current file(s).")

    # ------------------------------------------------------------------ Worker & monitor

    def _worker_loop(self):
        while not self.stop_flag.is_set():
            try:
                file_path = self.task_queue.get(timeout=0.5)
            except queue.Empty:
                break

            self._set_status(file_path, "Processing")
            try:
                self._process_single_file(file_path)
                if not self.stop_flag.is_set():
                    self._set_status(file_path, "Done")
            except Exception as e:
                self.log(f"[ERROR] {os.path.basename(file_path)}: {e}")
                self._set_status(file_path, "Error")
            finally:
                self.task_queue.task_done()

            if self.stop_flag.is_set():
                break

    def _schedule_monitor(self):
        self.root.after(500, self._monitor_workers)

    def _monitor_workers(self):
        if not self.processing:
            return
        alive = any(t.is_alive() for t in self.workers)
        if alive:
            self._schedule_monitor()
        else:
            self.processing = False
            self._log_summary()
            if self.open_folder_after_var.get():
                try:
                    folder = self.output_folder_var.get().strip() or os.path.abspath("export")
                    self._open_folder(folder)
                except Exception:
                    pass

    # ------------------------------------------------------------------ Single file processing

    def _process_single_file(self, file_path):
        start_ts = datetime.now()
        fname = os.path.basename(file_path)

        valid_docs, errors, warnings = validate_file(file_path)
        stats = compute_stats(valid_docs, errors, warnings)

        # Update aggregate stats
        self._update_aggregate(stats)

        # Conflicts / cross-file comparisons
        self._update_conflicts(file_path, valid_docs)

        # Export batch output for this file
        self._export_for_file(file_path, valid_docs, errors)

        elapsed = (datetime.now() - start_ts).total_seconds()
        self.log(
            f"Processed {fname}: "
            f"{stats['rows_valid']} valid, {stats['rows_invalid']} invalid, "
            f"{stats['duplicates']} duplicates, time={elapsed:.2f}s"
        )

        # Update tree row with stats
        self._update_tree_row(file_path, stats)

    # ------------------------------------------------------------------ Export helpers

    def _timestamp(self):
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _sanitize_group(self, s: str) -> str:
        out = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)
        return out[:80] if out else "UNK"

    def _render_path(self, folder, base, batch, group, ts, ext):
        pattern = "{base}_{batch}_{group}_{ts}.{ext}"
        vals = {
            "base": base,
            "batch": batch,
            "group": group if group else "all",
            "ts": ts,
            "ext": ext.lstrip("."),
        }
        return os.path.join(folder, pattern.format(**vals))

    def _export_for_file(self, file_path, valid_docs, errors):
        if not valid_docs and not errors:
            return

        vendor_id = self.vendor_id_var.get().strip()
        include_vendor = self.include_vendor_var.get()

        base_name = os.path.splitext(os.path.basename(file_path))[0]
        base_name_snake = base_name.lower().replace("-", "_").replace(" ", "_")

        export_folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        os.makedirs(export_folder, exist_ok=True)

        # Convert docs to export rows, keep postCode as normalized 4-digit
        csv_rows = [
            {
                "postCode": d["postCode"],
                "sku": d["sku"],
                "price": d["price"],
                **({"vendor_id": vendor_id} if include_vendor else {}),
            }
            for d in valid_docs
        ]
        json_rows = [
            {
                "postCode": d["postCode"],
                "sku": d["sku"],
                "price": d["price"],
                **({"vendor_id": vendor_id} if include_vendor else {}),
            }
            for d in valid_docs
        ]

        ts = self._timestamp()

        if self.enable_batch_var.get() and valid_docs:
            mode = self.batch_mode_var.get()
            if mode == "rows":
                try:
                    chunk_size = max(1, int(self.rows_per_file_var.get()))
                except Exception:
                    chunk_size = 1000
                self._export_by_rows(
                    export_folder, base_name_snake, csv_rows, json_rows, chunk_size, ts, include_vendor
                )
            else:
                group_col = (self.group_column_var.get() or "").strip()
                self._export_by_group(
                    export_folder, base_name_snake, csv_rows, json_rows, group_col, ts, include_vendor
                )
        else:
            # Single file
            self._export_single(export_folder, base_name_snake, csv_rows, json_rows, ts, include_vendor)

        # Error file
        if errors:
            err_path = os.path.join(export_folder, f"{base_name_snake}_errors.csv")
            try:
                with open(err_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["row", "context", "error"])
                    writer.writeheader()
                    writer.writerows(errors)
            except Exception as e:
                self.log(f"[WARN] Failed to write error file for {base_name}: {e}")

    def _export_single(self, folder, base, csv_rows, json_rows, ts, include_vendor):
        fields = ["postCode", "sku", "price"] + (["vendor_id"] if include_vendor else [])

        if self.export_csv_var.get() and csv_rows:
            csv_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                writer.writerows(csv_rows)

        if self.export_json_var.get() and json_rows:
            json_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_rows, f, indent=4)

    def _export_by_rows(self, folder, base, csv_rows, json_rows, chunk_size, ts, include_vendor):
        total = len(csv_rows)
        if total == 0:
            return

        fields = ["postCode", "sku", "price"] + (["vendor_id"] if include_vendor else [])
        parts = (total + chunk_size - 1) // chunk_size

        for i in range(parts):
            start, end = i * chunk_size, min((i + 1) * chunk_size, total)
            batch_id = f"part{(i + 1):03d}"
            csv_chunk = csv_rows[start:end]
            json_chunk = json_rows[start:end]

            if self.export_csv_var.get():
                csv_path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(csv_chunk)

            if self.export_json_var.get():
                json_path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="json")
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(json_chunk, f, indent=4)

    def _export_by_group(self, folder, base, csv_rows, json_rows, group_col, ts, include_vendor):
        key = group_col.strip()
        if key not in ("postCode", "sku", "price"):
            key = "postCode"

        fields = ["postCode", "sku", "price"] + (["vendor_id"] if include_vendor else [])

        def get_key(d):
            if key == "postCode":
                return d.get("postCode", "") or "UNK"
            if key == "sku":
                return d.get("sku", "") or "UNK"
            if key == "price":
                return str(d.get("price", "")) or "UNK"
            return "UNK"

        groups_csv = defaultdict(list)
        groups_json = defaultdict(list)

        for c, j in zip(csv_rows, json_rows):
            gval = get_key(c)
            groups_csv[gval].append(c)
            groups_json[gval].append(j)

        for gval, rows_csv in groups_csv.items():
            safe_group = self._sanitize_group(gval)
            if self.export_csv_var.get():
                csv_path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(rows_csv)

            if self.export_json_var.get():
                rows_json = groups_json[gval]
                json_path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="json")
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(rows_json, f, indent=4)

    # ------------------------------------------------------------------ Tree + log helpers

    def _set_status(self, file_path, status):
        def _inner():
            item_id = self.path_to_item.get(file_path)
            if not item_id:
                return
            vals = list(self.file_tree.item(item_id, "values"))
            vals[1] = status
            self.file_tree.item(item_id, values=vals)

        self.root.after(0, _inner)

    def _update_tree_row(self, file_path, stats):
        def _inner():
            item_id = self.path_to_item.get(file_path)
            if not item_id:
                return
            vals = list(self.file_tree.item(item_id, "values"))
            vals[2] = stats["rows_total"]
            vals[3] = stats["rows_valid"]
            vals[4] = stats["rows_invalid"]
            vals[5] = stats["duplicates"]
            vals[6] = stats["unique_skus"]
            vals[7] = stats["price_min"] if stats["price_min"] is not None else ""
            vals[8] = stats["price_max"] if stats["price_max"] is not None else ""
            vals[9] = stats["price_avg"] if stats["price_avg"] is not None else ""
            vals[10] = stats["warnings"]
            self.file_tree.item(item_id, values=vals)

        self.root.after(0, _inner)

    def _toggle_column_visibility(self, col):
        visible = self.col_visibility_vars[col].get()

        def _inner():
            if visible:
                width = self.column_default_widths.get(col, 90)
                self.file_tree.column(col, width=width, stretch=True)
            else:
                self.file_tree.column(col, width=0, stretch=False)

        self.root.after(0, _inner)

    def log(self, text: str):
        def _inner():
            self.log_text.insert("end", text + "\n")
            self.log_text.see("end")

        self.root.after(0, _inner)

    # ------------------------------------------------------------------ Aggregate & conflicts

    def _reset_aggregate(self):
        self.aggregate = {
            "files": 0,
            "rows_total": 0,
            "rows_valid": 0,
            "rows_invalid": 0,
            "duplicates": 0,
            "warnings": 0,
        }
        # conflicts: (sku, postCode) -> { price: set(files) }
        self.conflict_map = {}

    def _update_aggregate(self, stats):
        with self.aggregate_lock:
            self.aggregate["files"] += 1
            self.aggregate["rows_total"] += stats["rows_total"]
            self.aggregate["rows_valid"] += stats["rows_valid"]
            self.aggregate["rows_invalid"] += stats["rows_invalid"]
            self.aggregate["duplicates"] += stats["duplicates"]
            self.aggregate["warnings"] += stats["warnings"]

    def _update_conflicts(self, file_path, docs):
        fname = os.path.basename(file_path)
        with self.conflict_lock:
            for d in docs:
                key = (d["sku"], d["postCode"])
                price = float(d["price"])
                if key not in self.conflict_map:
                    self.conflict_map[key] = {price: {fname}}
                else:
                    price_map = self.conflict_map[key]
                    if price not in price_map:
                        price_map[price] = {fname}
                    else:
                        price_map[price].add(fname)

    def _clear_conflicts_table(self):
        if hasattr(self, "conflicts_tree"):
            for iid in self.conflicts_tree.get_children():
                self.conflicts_tree.delete(iid)

    def _populate_conflicts_table(self):
        self._clear_conflicts_table()
        rows = []

        with self.conflict_lock:
            for (sku, pc), price_map in self.conflict_map.items():
                if len(price_map) <= 1:
                    continue
                prices_list = sorted(price_map.keys())
                prices_str = ", ".join(str(p) for p in prices_list)
                files_details = []
                for price, files in price_map.items():
                    for f in sorted(files):
                        files_details.append(f"{f} (price={price})")
                files_str = "; ".join(files_details)
                rows.append((sku, pc, prices_str, files_str))

        def _inner():
            for r in rows:
                self.conflicts_tree.insert("", "end", values=r)

        self.root.after(0, _inner)

    def _log_summary(self):
        total_files = self.aggregate["files"]
        conflicts_count = 0
        with self.conflict_lock:
            for price_map in self.conflict_map.values():
                if len(price_map) > 1:
                    conflicts_count += 1

        if total_files == 0:
            self.log("Queue finished – no files processed.")
        else:
            self.log(
                "Queue finished.\n"
                f"Files processed: {total_files}\n"
                f"Rows total: {self.aggregate['rows_total']}\n"
                f"Rows valid: {self.aggregate['rows_valid']}\n"
                f"Rows invalid: {self.aggregate['rows_invalid']}\n"
                f"Duplicates: {self.aggregate['duplicates']}\n"
                f"Warnings: {self.aggregate['warnings']}\n"
                f"Conflicting sku/postCode pairs across files: {conflicts_count}"
            )
        self._populate_conflicts_table()

    # ------------------------------------------------------------------ Output folder helpers

    def choose_output_folder(self):
        path = filedialog.askdirectory(initialdir=self.output_folder_var.get() or ".")
        if not path:
            return
        self.output_folder_var.set(path)
        self.out_label.config(text=path)
        self.log(f"Output folder set to: {path}")

    def open_output_folder(self):
        folder = self.output_folder_var.get().strip()
        if not folder:
            folder = os.path.abspath("export")
        os.makedirs(folder, exist_ok=True)
        try:
            self._open_folder(folder)
        except Exception as e:
            messagebox.showerror("Open Folder", f"Failed to open folder:\n{e}")

    def _open_folder(self, path):
        sysname = platform.system()
        if sysname == "Windows":
            subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sysname == "Darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])

    # ------------------------------------------------------------------ Export table to Excel

    def export_table_to_excel(self):
        items = self.file_tree.get_children()
        if not items:
            messagebox.showinfo("Export", "No rows in the table to export.")
            return

        cols = self.file_tree["columns"]
        data = []
        for iid in items:
            vals = self.file_tree.item(iid, "values")
            row = {cols[i]: vals[i] for i in range(len(cols))}
            data.append(row)

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile="queue_stats.xlsx",
        )
        if not file_path:
            return

        try:
            import pandas as pd
        except ImportError:
            # Fallback to CSV
            alt = file_path
            if alt.lower().endswith(".xlsx"):
                alt = alt[:-5] + ".csv"
            try:
                with open(alt, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=cols)
                    writer.writeheader()
                    writer.writerows(data)
                messagebox.showwarning(
                    "Export",
                    f"pandas not installed; exported as CSV instead:\n{alt}\n"
                    "Install pandas + openpyxl for true Excel export.",
                )
            except Exception as e:
                messagebox.showerror("Export", f"Failed to export table:\n{e}")
            return

        try:
            df = pd.DataFrame(data, columns=cols)
            df.to_excel(file_path, index=False)
            messagebox.showinfo("Export", f"Exported table to:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Export", f"Failed to export table:\n{e}")

    # ------------------------------------------------------------------ Settings persistence

    def save_settings(self):
        cfg = {
            "folder": self.folder_var.get(),
            "output_folder": self.output_folder_var.get(),
            "vendor_id": self.vendor_id_var.get(),
            "include_vendor": bool(self.include_vendor_var.get()),
            "enable_batch": bool(self.enable_batch_var.get()),
            "batch_mode": self.batch_mode_var.get(),
            "rows_per_file": int(self.rows_per_file_var.get() or 1000),
            "group_column": self.group_column_var.get(),
            "export_csv": bool(self.export_csv_var.get()),
            "export_json": bool(self.export_json_var.get()),
            "open_folder_after": bool(self.open_folder_after_var.get()),
            "num_workers": int(self.num_workers_var.get() or 1),
        }
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=4)
            self.log(f"Settings saved to: {CONFIG_PATH}")
        except Exception as e:
            messagebox.showerror("Settings", f"Failed to save settings:\n{e}")

    def load_settings(self):
        if not os.path.exists(CONFIG_PATH):
            return
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            return

        self.folder_var.set(cfg.get("folder", self.folder_var.get()))
        self.output_folder_var.set(cfg.get("output_folder", self.output_folder_var.get()))
        self.vendor_id_var.set(cfg.get("vendor_id", self.vendor_id_var.get()))
        self.include_vendor_var.set(bool(cfg.get("include_vendor", self.include_vendor_var.get())))
        self.enable_batch_var.set(bool(cfg.get("enable_batch", self.enable_batch_var.get())))
        self.batch_mode_var.set(cfg.get("batch_mode", self.batch_mode_var.get()))
        self.rows_per_file_var.set(int(cfg.get("rows_per_file", self.rows_per_file_var.get())))
        self.group_column_var.set(cfg.get("group_column", self.group_column_var.get()))
        self.export_csv_var.set(bool(cfg.get("export_csv", self.export_csv_var.get())))
        self.export_json_var.set(bool(cfg.get("export_json", self.export_json_var.get())))
        self.open_folder_after_var.set(bool(cfg.get("open_folder_after", self.open_folder_after_var.get())))
        self.num_workers_var.set(int(cfg.get("num_workers", self.num_workers_var.get())))

    # ------------------------------------------------------------------ Close

    def on_close(self):
        try:
            self.save_settings()
        except Exception:
            pass
        self.stop_flag.set()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = TaskManagerApp(root)
    root.mainloop()
