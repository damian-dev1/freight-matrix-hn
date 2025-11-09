# pre_cosmos_db_standalone_validator.py


import os
import csv
import json
import math
import sqlite3
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS, DANGER, WARNING
import subprocess
import platform
from datetime import datetime
from collections import defaultdict
from statistics import mean

# ==================== Validation helpers & config ====================

CSV_FIELD_ALIASES = {
    "sku": ["sku", "productcode", "productCode", "product_code", "product id", "productid"],
    "postCode": ["postcode", "postCode", "post_code", "post code", "zip", "zip_code"],
    "price": ["price", "unit_price", "unitPrice", "unitprice", "unit price", "amount"]
}

def normalize_str(v):
    if v is None: return ""
    if isinstance(v, (int, float)): return str(v).strip()
    return str(v).strip().strip('"').strip()

def _lower_keys(d: dict) -> dict:
    return {(k or "").strip().lower(): v for k, v in d.items()}

def field_from_row(row: dict, key: str):
    for alias in CSV_FIELD_ALIASES.get(key, []):
        a = alias.lower()
        if a in row: return row.get(a)
    return None

def is_valid_sku(s: str):
    s = normalize_str(s)
    if not s: return False, "sku empty"
    for ch in s:
        if not (ch.isalnum() or ch in "-_./"): return False, "sku has invalid characters"
    if len(s) > 64: return False, "sku too long"
    return True, ""

def is_valid_postcode(pc: str):
    pc = normalize_str(pc)
    if not pc: return False, "postCode empty"
    if not pc.isdigit() or len(pc) != 4: return False, "postCode must be 4 digits"
    return True, ""

def normalize_price(p: str):
    s = normalize_str(p)
    if s == "": return False, 0.0, "price empty"
    clean = s.replace(",", "")
    for sym in "$€£AUDaud ":
        clean = clean.replace(sym, "")
    try:
        val = float(clean)
    except Exception:
        return False, 0.0, "price not a number"
    if val < 0: return False, 0.0, "price negative"
    if math.isinf(val) or math.isnan(val): return False, 0.0, "price not finite"
    return True, round(val, 2), ""

def build_doc(raw_sku: str, raw_pc: str, price_val: float):
    return {"sku": normalize_str(raw_sku), "postCode": normalize_str(raw_pc), "price": float(price_val)}

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

        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            continue

        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))

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
        raw_pc  = normalize_str(field_from_row(obj_lc, "postCode"))
        raw_price = normalize_str(field_from_row(obj_lc, "price"))
        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)
        errs = []
        if not raw_sku: errs.append("sku missing")
        if not raw_pc:  errs.append("postCode missing")
        if raw_price == "": errs.append("price missing")
        if raw_sku and not ok_sku: errs.append(sku_err)
        if raw_pc  and not ok_pc:  errs.append(pc_err)
        if raw_price != "" and not ok_price: errs.append(price_err)
        if errs:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            return
        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            return
        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))
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
                if not line: continue
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
    if ext == ".csv":  return validate_csv(file_path)
    if ext == ".json": return validate_json(file_path)
    return [], [{"row": 0, "context": "", "error": "Unsupported file type. Use CSV or JSON."}], []

def validate_pasted_csv_text(text: str):
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return [], [{"row": 1, "context": "header", "error": "No content"}], []
    reader = csv.DictReader(lines)
    return _validate_from_reader(reader)

# ==================== App (Notebook UI + SQLite + Runs Browser) ====================

class CSVConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CSV/JSON → Clean CSV & JSON (Validation + SQLite + Runs Browser)")
        self.root.geometry("540x400")
        self.root.resizable(True, True)

        self.style = Style(theme="darkly")

        # State
        self.file_path = None
        self.last_valid_docs = []
        self.last_errors = []
        self.last_warnings = []
        self.headers = []
        self.cached_stats = {}

        # Export config
        self.export_csv_var  = tk.BooleanVar(value=True)
        self.export_json_var = tk.BooleanVar(value=True)

        # Batch config
        self.enable_batch_var  = tk.BooleanVar(value=True)
        self.batch_mode_var    = tk.StringVar(value="rows")  # 'rows' or 'group'
        self.rows_per_file_var = tk.IntVar(value=1000)
        self.group_column_var  = tk.StringVar(value="postcode")

        # Output config
        self.filename_pattern_var = tk.StringVar(value="{base}_{batch}_{group}_{ts}.{ext}")
        self.output_folder_var    = tk.StringVar(value=os.path.abspath("export"))
        self.open_folder_after_var= tk.BooleanVar(value=True)

        # Vendor ID
        self.vendor_id_var = tk.StringVar(value="vendor_001")
        self.include_vendor_in_files_var = tk.BooleanVar(value=True)

        # SQLite config
        default_db = os.path.abspath("export_runs.sqlite")
        self.enable_sqlite_save_var = tk.BooleanVar(value=True)
        self.db_path_var = tk.StringVar(value=default_db)

        # Runs browser filters
        self.runs_filter_vendor_var = tk.StringVar(value="")
        self.runs_limit_var = tk.IntVar(value=200)

        self._ensure_export_dir()
        self._ensure_db_schema()
        self._build_ui()

    # -------------------- UI --------------------
    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self.root)
        notebook.grid(row=0, column=0, sticky="nsew")

        # Tabs
        self.tab_source = ttk.Frame(notebook, padding=12)
        self._build_tab_source(self.tab_source)
        notebook.add(self.tab_source, text="Source")

        self.tab_preview = ttk.Frame(notebook, padding=12)
        self._build_tab_preview(self.tab_preview)
        notebook.add(self.tab_preview, text="Preview")

        self.tab_batch = ttk.Frame(notebook, padding=12)
        self._build_tab_batch(self.tab_batch)
        notebook.add(self.tab_batch, text="Batch")

        self.tab_output = ttk.Frame(notebook, padding=12)
        self._build_tab_output(self.tab_output)
        notebook.add(self.tab_output, text="Output")

        self.tab_db = ttk.Frame(notebook, padding=12)
        self._build_tab_db(self.tab_db)
        notebook.add(self.tab_db, text="Database")

        self.tab_runs = ttk.Frame(notebook, padding=12)
        self._build_tab_runs(self.tab_runs)
        notebook.add(self.tab_runs, text="Runs")

        self.tab_export = ttk.Frame(notebook, padding=12)
        self._build_tab_export(self.tab_export)
        notebook.add(self.tab_export, text="Export")

    def _build_tab_source(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)

        file_frame = ttk.LabelFrame(parent, text="File", padding=10)
        file_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)

        ttk.Label(file_frame, text="Selected:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.file_label = ttk.Label(file_frame, text="No file selected", anchor="w")
        self.file_label.grid(row=0, column=1, sticky="ew")
        ttk.Button(file_frame, text="Select CSV/JSON", command=self.load_file, bootstyle=PRIMARY)\
            .grid(row=0, column=2, padx=(8, 0))

        paste_frame = ttk.LabelFrame(parent, text="Paste CSV Content", padding=10)
        paste_frame.grid(row=1, column=0, sticky="nsew")
        paste_frame.columnconfigure(0, weight=1)
        paste_frame.rowconfigure(0, weight=1)

        self.paste_box = tk.Text(paste_frame, wrap=tk.NONE, height=8)
        self.paste_box.grid(row=0, column=0, sticky="nsew")
        paste_scroll_y = ttk.Scrollbar(paste_frame, orient="vertical", command=self.paste_box.yview)
        paste_scroll_y.grid(row=0, column=1, sticky="ns")
        paste_scroll_x = ttk.Scrollbar(paste_frame, orient="horizontal", command=self.paste_box.xview)
        paste_scroll_x.grid(row=1, column=0, sticky="ew")
        self.paste_box.config(yscrollcommand=paste_scroll_y.set, xscrollcommand=paste_scroll_x.set)

        actions = ttk.Frame(parent)
        actions.grid(row=2, column=0, sticky="ew", pady=10)
        ttk.Button(actions, text="Validate & Preview", command=self.preview_data, bootstyle=INFO).pack(side="left")

        tips = ttk.Label(
            parent,
            text="Tip: Select a CSV/JSON file or paste CSV with headers above. Validation runs before preview/export.",
            anchor="w"
        )
        tips.grid(row=4, column=0, sticky="w", pady=(8, 0))

    def _build_tab_preview(self, parent):
        parent.columnconfigure(0, weight=3)
        parent.columnconfigure(1, weight=2)
        parent.rowconfigure(1, weight=1)

        hdr = ttk.Frame(parent); hdr.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(hdr, text="Preview uses validated rows only (first 100 shown).").pack(side="left")

        preview_group = ttk.LabelFrame(parent, text="Preview (first 100 valid rows)", padding=10)
        preview_group.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        preview_group.columnconfigure(0, weight=1); preview_group.rowconfigure(0, weight=1)

        self.preview_box = tk.Text(preview_group, wrap=tk.NONE)
        self.preview_box.grid(row=0, column=0, sticky="nsew")
        pv_sy = ttk.Scrollbar(preview_group, orient="vertical", command=self.preview_box.yview); pv_sy.grid(row=0, column=1, sticky="ns")
        pv_sx = ttk.Scrollbar(preview_group, orient="horizontal", command=self.preview_box.xview); pv_sx.grid(row=1, column=0, sticky="ew")
        self.preview_box.config(yscrollcommand=pv_sy.set, xscrollcommand=pv_sx.set)

        stats_group = ttk.LabelFrame(parent, text="Data Quality / Stats", padding=10)
        stats_group.grid(row=1, column=1, sticky="nsew")
        stats_group.columnconfigure(0, weight=1); stats_group.rowconfigure(0, weight=1)

        self.stats_box = tk.Text(stats_group, height=12, wrap=tk.WORD, state="disabled")
        self.stats_box.grid(row=0, column=0, sticky="nsew")
        st_sy = ttk.Scrollbar(stats_group, orient="vertical", command=self.stats_box.yview); st_sy.grid(row=0, column=1, sticky="ns")
        self.stats_box.config(yscrollcommand=st_sy.set)

    def _build_tab_batch(self, parent):
        parent.columnconfigure(0, weight=1)
        lf = ttk.LabelFrame(parent, text="Batch Export Configuration", padding=10)
        lf.grid(row=0, column=0, sticky="nsew"); lf.columnconfigure(1, weight=1)

        ttk.Checkbutton(lf, text="Enable batch export", variable=self.enable_batch_var,
                        command=self._toggle_batch_controls).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        ttk.Label(lf, text="Mode:").grid(row=1, column=0, sticky="w")
        row_mode = ttk.Frame(lf); row_mode.grid(row=1, column=1, sticky="w", pady=(0, 4))
        ttk.Radiobutton(row_mode, text="Rows per file", value="rows", variable=self.batch_mode_var,
                        command=self._on_mode_change).pack(side="left")
        ttk.Radiobutton(row_mode, text="Group by column", value="group", variable=self.batch_mode_var,
                        command=self._on_mode_change).pack(side="left", padx=(10, 0))
        ttk.Label(lf, text="Rows per file:").grid(row=2, column=0, sticky="w")
        self.rows_entry = ttk.Entry(lf, width=10, textvariable=self.rows_per_file_var); self.rows_entry.grid(row=2, column=1, sticky="w", pady=(0, 4))
        ttk.Label(lf, text="Group column:").grid(row=3, column=0, sticky="w")
        self.group_combo = ttk.Combobox(lf, width=20, state="readonly", textvariable=self.group_column_var,
                                        values=["postcode", "productcode", "sku", "state"])
        self.group_combo.grid(row=3, column=1, sticky="w")
        self._toggle_batch_controls(); self._on_mode_change()

    def _build_tab_output(self, parent):
        parent.columnconfigure(1, weight=1)

        formats = ttk.LabelFrame(parent, text="Formats", padding=10)
        formats.grid(row=0, column=0, columnspan=2, sticky="ew")
        ttk.Checkbutton(formats, text="Export CSV",  variable=self.export_csv_var).pack(side="left")
        ttk.Checkbutton(formats, text="Export JSON", variable=self.export_json_var).pack(side="left", padx=(10, 0))

        vendor = ttk.LabelFrame(parent, text="Vendor", padding=10)
        vendor.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10,0))
        ttk.Label(vendor, text="Vendor ID:").grid(row=0, column=0, sticky="w")
        ttk.Entry(vendor, textvariable=self.vendor_id_var, width=24).grid(row=0, column=1, sticky="w", padx=(8,8))
        ttk.Checkbutton(vendor, text="Include vendor_id in CSV/JSON", variable=self.include_vendor_in_files_var)\
            .grid(row=0, column=2, sticky="w")

        out = ttk.LabelFrame(parent, text="Destination & Naming", padding=10)
        out.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        out.columnconfigure(1, weight=1)
        ttk.Label(out, text="Output folder:").grid(row=0, column=0, sticky="w")
        self.out_label = ttk.Label(out, text=self.output_folder_var.get(), anchor="w"); self.out_label.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(out, text="Browse", command=self._choose_output_folder).grid(row=0, column=2, sticky="w")
        ttk.Label(out, text="Filename pattern:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.pattern_entry = ttk.Entry(out, textvariable=self.filename_pattern_var)
        self.pattern_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(out, text="Tokens: {base} {batch} {group} {ts} {ext}").grid(row=2, column=1, sticky="w", pady=(6, 0))
        ttk.Checkbutton(out, text="Open folder after export", variable=self.open_folder_after_var)\
            .grid(row=3, column=1, sticky="w", pady=(8, 0))

    def _build_tab_db(self, parent):
        parent.columnconfigure(1, weight=1)
        lf = ttk.LabelFrame(parent, text="SQLite Settings", padding=10)
        lf.grid(row=0, column=0, sticky="ew")
        lf.columnconfigure(1, weight=1)

        ttk.Checkbutton(lf, text="Enable save to SQLite", variable=self.enable_sqlite_save_var)\
            .grid(row=0, column=0, sticky="w", pady=(0,8), columnspan=3)
        ttk.Label(lf, text="Database file:").grid(row=1, column=0, sticky="w")
        self.db_path_label = ttk.Label(lf, text=self.db_path_var.get(), anchor="w"); self.db_path_label.grid(row=1, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(lf, text="Browse", command=self._choose_db_path).grid(row=1, column=2, sticky="w")
        ttk.Button(parent, text="Save to DB Now", command=self.save_to_sqlite, bootstyle=SUCCESS, width=20)\
            .grid(row=1, column=0, sticky="w", pady=(12,0))

        schema_note = ("Schema: runs, records, errors. We store per-run stats, each valid record with vendor_id, "
                       "and all validation errors.")
        ttk.Label(parent, text=schema_note, wraplength=900, anchor="w").grid(row=2, column=0, sticky="w", pady=(10,0))

    def _build_tab_runs(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)

        # Filters / actions
        top = ttk.Frame(parent); top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(top, text="Vendor filter:").pack(side="left")
        ttk.Entry(top, textvariable=self.runs_filter_vendor_var, width=20).pack(side="left", padx=(6, 12))
        ttk.Label(top, text="Limit:").pack(side="left")
        ttk.Entry(top, textvariable=self.runs_limit_var, width=7).pack(side="left", padx=(6, 12))
        ttk.Button(top, text="Refresh", command=self._runs_refresh, bootstyle=INFO).pack(side="left")
        ttk.Button(top, text="Open DB Folder", command=self._open_db_folder).pack(side="left", padx=(8, 0))

        # Treeview
        cols = ("run_id","created_at","source_name","vendor_id","rows_total","rows_valid","rows_invalid",
                "duplicates","unique_skus","price_min","price_max","price_avg","warnings")
        self.runs_tree = ttk.Treeview(parent, columns=cols, show="headings", height=18)
        for c in cols:
            self.runs_tree.heading(c, text=c.replace("_"," ").title(), command=lambda col=c: self._sort_runs(col, False))
            width = 110 if c not in ("created_at","source_name","vendor_id") else 160
            self.runs_tree.column(c, width=width, anchor="center")
        self.runs_tree.grid(row=2, column=0, sticky="nsew")

        sy = ttk.Scrollbar(parent, orient="vertical", command=self.runs_tree.yview); sy.grid(row=2, column=1, sticky="ns")
        self.runs_tree.configure(yscrollcommand=sy.set)

        # Buttons
        btns = ttk.Frame(parent); btns.grid(row=3, column=0, sticky="ew", pady=(10,0))
        ttk.Button(btns, text="View Records", command=self._runs_view_records, bootstyle=PRIMARY).pack(side="left")
        ttk.Button(btns, text="View Errors", command=self._runs_view_errors, bootstyle=WARNING).pack(side="left", padx=(8,0))
        ttk.Button(btns, text="Re-Export Selected", command=self._runs_reexport_selected, bootstyle=SUCCESS).pack(side="left", padx=(8,0))
        ttk.Button(btns, text="Diff Two Runs (A Δ B)", command=self._runs_diff_two, bootstyle=DANGER).pack(side="left", padx=(8,0))

        self._runs_refresh()

    def _build_tab_export(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        ttk.Label(parent, text="Click Export to generate files per configuration. If SQLite is enabled, the run is also saved to DB.")\
            .grid(row=0, column=0, sticky="w")
        btn_row = ttk.Frame(parent); btn_row.grid(row=1, column=0, sticky="n")
        ttk.Button(btn_row, text="Export Files", command=self.export_files, bootstyle=SUCCESS, width=20).pack(pady=(20, 8))
        self.status_label = ttk.Label(parent, text="", foreground="lightgreen", anchor="center")
        self.status_label.grid(row=2, column=0, sticky="ew", pady=(8, 0))

    # -------------------- Helpers --------------------
    def _ensure_export_dir(self):
        folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        self.output_folder_var.set(folder)
        os.makedirs(folder, exist_ok=True)

    def _choose_output_folder(self):
        path = filedialog.askdirectory(initialdir=self.output_folder_var.get())
        if path:
            self.output_folder_var.set(path)
            self.out_label.config(text=path)

    def _toggle_batch_controls(self):
        enabled = self.enable_batch_var.get()
        rows_state  = "normal"   if (enabled and self.batch_mode_var.get() == "rows")  else "disabled"
        group_state = "readonly" if (enabled and self.batch_mode_var.get() == "group") else "disabled"
        self.rows_entry.configure(state=rows_state)
        self.group_combo.configure(state=group_state)

    def _on_mode_change(self): self._toggle_batch_controls()

    def _update_group_columns(self, headers):
        if headers:
            self.group_combo.configure(values=headers)
            cur = self.group_column_var.get()
            if cur not in headers:
                self.group_column_var.set("postcode" if "postcode" in headers else headers[0])

    # -------------------- Source / Preview --------------------
    def load_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV/JSON files", "*.csv *.json")])
        if not file_path: return
        self.file_path = file_path
        self.file_label.config(text=os.path.basename(file_path))
        self.status_label.config(text="File selected. Click 'Validate & Preview'.")

    def preview_data(self):
        valid_docs, errors, warnings = [], [], []
        headers = []
        if self.file_path:
            ext = os.path.splitext(self.file_path)[1].lower()
            if ext == ".csv":
                with open(self.file_path, newline="", encoding="utf-8-sig") as f:
                    rdr = csv.DictReader(f); headers = [(h or "").strip().lower() for h in (rdr.fieldnames or [])]
                valid_docs, errors, warnings = validate_file(self.file_path)
            elif ext == ".json":
                valid_docs, errors, warnings = validate_file(self.file_path)
                headers = ["sku", "postCode", "price"]
            else:
                messagebox.showerror("Error", "Unsupported file type."); return
        else:
            txt = self.paste_box.get("1.0", tk.END).strip()
            if not txt:
                messagebox.showerror("Error", "No file selected or pasted content."); return
            valid_docs, errors, warnings = validate_pasted_csv_text(txt)
            first = [ln for ln in txt.splitlines() if ln.strip()][:1]
            headers = [h.strip().lower() for h in next(csv.reader(first))] if first else ["sku","postCode","price"]

        self.last_valid_docs, self.last_errors, self.last_warnings = valid_docs, errors, warnings
        self.headers = headers; self._update_group_columns(headers)

        # Preview (first 100)
        self.preview_box.delete("1.0", tk.END)
        header_line = "postCode,sku,price,vendor_id\n"
        self.preview_box.insert(tk.END, header_line)
        v_id = self.vendor_id_var.get().strip()
        for doc in valid_docs[:100]:
            self.preview_box.insert(tk.END, f"{doc['postCode']},{doc['sku']},{doc['price']},{v_id}\n")

        # Stats
        total_rows_est = len(valid_docs) + len(errors)
        dup_count = sum(1 for e in errors if "Duplicate id" in e.get("error", ""))
        uniq_skus = len(set(d["sku"] for d in valid_docs))
        prices = [d["price"] for d in valid_docs]
        pmin = min(prices) if prices else None
        pmax = max(prices) if prices else None
        pavg = round(mean(prices), 6) if prices else None
        warn_count = len(warnings)

        self.cached_stats = {
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

        issues_preview = [f"Row {e.get('row')}: {e.get('context','')} -> {e.get('error')}" for e in errors[:50]]
        price_stats = "" if pmin is None else f"Price min/max/avg: {pmin} / {pmax} / {pavg}"
        stats_text = (
            f"Rows (estimated): {total_rows_est}\n"
            f"Valid rows: {len(valid_docs)}\n"
            f"Invalid rows: {len(errors)}\n"
            f"Duplicates: {dup_count}\n"
            f"Unique SKUs: {uniq_skus}\n"
            f"{price_stats}\n"
            f"Warnings: {warn_count}\n"
            + ("\nIssues (first 50):\n" + "\n".join(issues_preview) if issues_preview else "")
        )
        self.stats_box.config(state="normal"); self.stats_box.delete("1.0", tk.END)
        self.stats_box.insert(tk.END, stats_text); self.stats_box.config(state="disabled")
        self.stats_box.config(foreground="red" if len(errors) > 0 else "lightgreen")
        self.status_label.config(text="Validation complete. Review Preview & Stats.")

    # -------------------- Export & DB --------------------
    def export_files(self):
        if not self.last_valid_docs and not self.last_errors:
            self.preview_data()
        if not self.last_valid_docs and self.last_errors:
            messagebox.showerror("Error", "No valid rows to export (all invalid). Check errors in Preview tab.")
            return

        vendor_id = self.vendor_id_var.get().strip()
        include_vendor = self.include_vendor_in_files_var.get()

        # Build rows including vendor_id if selected
        csv_rows = [{
            "postCode": d["postCode"].lstrip("0") if d["postCode"] else "",
            "sku": d["sku"],
            "price": d["price"],
            **({"vendor_id": vendor_id} if include_vendor else {})
        } for d in self.last_valid_docs]

        json_rows = [{
            "postCode": d["postCode"],
            "sku": d["sku"],
            "price": d["price"],
            **({"vendor_id": vendor_id} if include_vendor else {})
        } for d in self.last_valid_docs]

        base_name = os.path.splitext(os.path.basename(self.file_path or 'pasted'))[0]
        base_name_snake = base_name.lower().replace("-", "_").replace(" ", "_")
        export_folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        os.makedirs(export_folder, exist_ok=True)

        if self.enable_batch_var.get():
            mode = self.batch_mode_var.get()
            if mode == "rows":
                try: chunk = max(1, int(self.rows_per_file_var.get()))
                except Exception: chunk = 1000
                self._export_by_rows(export_folder, base_name_snake, csv_rows, json_rows, chunk)
            else:
                group_col = (self.group_column_var.get() or "").strip()
                self._export_by_group_valid(export_folder, base_name_snake, json_rows, csv_rows, group_col)
        else:
            self._export_single(export_folder, base_name_snake, csv_rows, json_rows)

        # Write error file
        if self.last_errors:
            error_path = os.path.join(export_folder, f"{base_name_snake}_errors.csv")
            try:
                with open(error_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["row", "context", "error"])
                    writer.writeheader(); writer.writerows(self.last_errors)
            except Exception as e:
                messagebox.showwarning("Warning", f"Failed to write error file:\n{e}")

        # Optional DB save
        if self.enable_sqlite_save_var.get():
            self.save_to_sqlite()

        try:
            if self.open_folder_after_var.get(): self._open_folder(export_folder)
        except Exception: pass

        self.status_label.config(text="Export completed.")
        messagebox.showinfo("Success", f"Export completed.\nFiles saved in:\n{export_folder}")

    def save_to_sqlite(self):
        if not self.enable_sqlite_save_var.get():
            messagebox.showinfo("SQLite", "SQLite saving is disabled."); return
        self._ensure_db_schema()
        db_path = self.db_path_var.get().strip()
        if not db_path:
            messagebox.showerror("SQLite", "No database path configured."); return
        if not self.last_valid_docs and not self.last_errors:
            self.preview_data()

        vendor_id = self.vendor_id_var.get().strip()
        stats = self._compute_stats_for_db()

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO runs (created_at, source_name, vendor_id,
                                  rows_total, rows_valid, rows_invalid, duplicates,
                                  unique_skus, price_min, price_max, price_avg, warnings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(timespec="seconds"),
                os.path.basename(self.file_path or "pasted"),
                vendor_id,
                stats["rows_total"], stats["rows_valid"], stats["rows_invalid"], stats["duplicates"],
                stats["unique_skus"], stats["price_min"], stats["price_max"], stats["price_avg"], stats["warnings"]
            ))
            run_id = cur.lastrowid

            if self.last_valid_docs:
                rec_rows = [(run_id, vendor_id, d["sku"], d["postCode"], float(d["price"])) for d in self.last_valid_docs]
                cur.executemany("INSERT INTO records (run_id, vendor_id, sku, postCode, price) VALUES (?, ?, ?, ?, ?)", rec_rows)

            if self.last_errors:
                err_rows = [(run_id, int(e.get("row") or 0), str(e.get("context") or ""), str(e.get("error") or "")) for e in self.last_errors]
                cur.executemany("INSERT INTO errors (run_id, row_no, context, error) VALUES (?, ?, ?, ?)", err_rows)

            conn.commit(); conn.close()
            self.status_label.config(text=f"Saved to SQLite: run_id={run_id}")
            messagebox.showinfo("SQLite", f"Saved run to database:\n{db_path}\nrun_id={run_id}")
            self._runs_refresh()
        except Exception as e:
            messagebox.showerror("SQLite", f"Failed to save to database:\n{e}")

    # -------- writers --------
    def _export_single(self, folder, base, csv_rows, json_rows):
        ts = self._timestamp()
        if self.export_csv_var.get():
            csv_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="csv")
            fields = ["postCode", "sku", "price"] + (["vendor_id"] if self.include_vendor_in_files_var.get() else [])
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader(); writer.writerows(csv_rows)
        if self.export_json_var.get():
            json_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_rows, f, indent=4)

    def _export_by_rows(self, folder, base, csv_rows, json_rows, chunk_size):
        total = len(csv_rows)
        if total == 0: return
        ts = self._timestamp()
        parts = (total + chunk_size - 1) // chunk_size
        fields = ["postCode", "sku", "price"] + (["vendor_id"] if self.include_vendor_in_files_var.get() else [])
        for i in range(parts):
            start, end = i * chunk_size, min((i+1) * chunk_size, total)
            batch_id = f"part{(i+1):03d}"
            csv_chunk, json_chunk = csv_rows[start:end], json_rows[start:end]
            if self.export_csv_var.get():
                csv_path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="csv")
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader(); writer.writerows(csv_chunk)
            if self.export_json_var.get():
                json_path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="json")
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(json_chunk, f, indent=4)

    def _export_by_group_valid(self, folder, base, json_rows, csv_rows, group_col):
        ts = self._timestamp()
        key_lower = (group_col or "").lower()
        def key_for_json(d):
            if key_lower in ("postcode", "post_code", "post code"): return d.get("postCode", "") or "UNK"
            if key_lower == "sku": return d.get("sku", "") or "UNK"
            if key_lower == "price": return str(d.get("price", "")) or "UNK"
            return str(d.get(key_lower, "") or "UNK")
        groups_json, groups_csv = defaultdict(list), defaultdict(list)
        for j, c in zip(json_rows, csv_rows):
            g = key_for_json(j).strip() or "UNK"
            groups_json[g].append(j); groups_csv[g].append(c)

        fields = ["postCode", "sku", "price"] + (["vendor_id"] if self.include_vendor_in_files_var.get() else [])
        for gval, rows_csv in groups_csv.items():
            safe_group = self._sanitize_group(gval); batch_id = "group"
            if self.export_csv_var.get():
                csv_path = self._render_path(folder, base, batch=batch_id, group=safe_group, ts=ts, ext="csv")
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader(); writer.writerows(rows_csv)
            if self.export_json_var.get():
                rows_json = groups_json[gval]
                json_path = self._render_path(folder, base, batch=batch_id, group=safe_group, ts=ts, ext="json")
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(rows_json, f, indent=4)

    # -------- naming / system helpers --------
    def _render_path(self, folder, base, batch, group, ts, ext):
        pattern = (self.filename_pattern_var.get() or "{base}_{batch}_{group}_{ts}.{ext}").strip()
        vals = {"base": base, "batch": batch, "group": group if group else "all", "ts": ts, "ext": ext.lstrip(".")}
        return os.path.join(folder, pattern.format(**vals))

    def _timestamp(self): return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _sanitize_group(self, s: str) -> str:
        out = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)
        return out[:80] if out else "UNK"

    def _open_folder(self, path):
        sys = platform.system()
        if sys == "Windows": subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sys == "Darwin": subprocess.Popen(["open", path])
        else: subprocess.Popen(["xdg-open", path])

    # -------- SQLite helpers --------
    def _choose_db_path(self):
        path = filedialog.asksaveasfilename(defaultextension=".sqlite",
                                            filetypes=[("SQLite DB", "*.sqlite *.db")],
                                            initialfile=os.path.basename(self.db_path_var.get() or "export_runs.sqlite"))
        if path:
            self.db_path_var.set(path)
            self.db_path_label.config(text=path)
            self._ensure_db_schema()
            self._runs_refresh()

    def _ensure_db_schema(self):
        if not self.enable_sqlite_save_var.get(): return
        db_path = self.db_path_var.get().strip()
        if not db_path: return
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at  TEXT NOT NULL,
            source_name TEXT,
            vendor_id   TEXT,
            rows_total  INTEGER,
            rows_valid  INTEGER,
            rows_invalid INTEGER,
            duplicates  INTEGER,
            unique_skus INTEGER,
            price_min   REAL,
            price_max   REAL,
            price_avg   REAL,
            warnings    INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id    INTEGER NOT NULL,
            vendor_id TEXT,
            sku       TEXT NOT NULL,
            postCode  TEXT NOT NULL,
            price     REAL NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id  INTEGER NOT NULL,
            row_no  INTEGER,
            context TEXT,
            error   TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_records_run ON records(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_records_vendor ON records(vendor_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_records_sku ON records(sku)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_records_pc ON records(postCode)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_run ON errors(run_id)")
        conn.commit(); conn.close()

    def _compute_stats_for_db(self):
        if self.cached_stats:
            return self.cached_stats
        prices = [d["price"] for d in self.last_valid_docs]
        pmin = min(prices) if prices else None
        pmax = max(prices) if prices else None
        pavg = round(mean(prices), 6) if prices else None
        dup_count = sum(1 for e in self.last_errors if "Duplicate id" in e.get("error", ""))
        return {
            "rows_total": len(self.last_valid_docs) + len(self.last_errors),
            "rows_valid": len(self.last_valid_docs),
            "rows_invalid": len(self.last_errors),
            "duplicates": dup_count,
            "unique_skus": len(set(d["sku"] for d in self.last_valid_docs)),
            "price_min": pmin, "price_max": pmax, "price_avg": pavg,
            "warnings": len(self.last_warnings),
        }

    # -------- Runs Browser actions --------
    def _runs_refresh(self):
        self._ensure_db_schema()
        db = self.db_path_var.get().strip()
        if not db or not os.path.exists(db):
            for i in self.runs_tree.get_children(): self.runs_tree.delete(i)
            return
        vendor = self.runs_filter_vendor_var.get().strip()
        limit = max(1, int(self.runs_limit_var.get() or 200))
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            if vendor:
                cur.execute("""
                    SELECT run_id, created_at, source_name, vendor_id, rows_total, rows_valid, rows_invalid,
                           duplicates, unique_skus, price_min, price_max, price_avg, warnings
                    FROM runs
                    WHERE vendor_id LIKE ?
                    ORDER BY datetime(created_at) DESC
                    LIMIT ?
                """, (f"%{vendor}%", limit))
            else:
                cur.execute("""
                    SELECT run_id, created_at, source_name, vendor_id, rows_total, rows_valid, rows_invalid,
                           duplicates, unique_skus, price_min, price_max, price_avg, warnings
                    FROM runs
                    ORDER BY datetime(created_at) DESC
                    LIMIT ?
                """, (limit,))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            messagebox.showerror("Runs", f"Failed to load runs:\n{e}")
            return

        for i in self.runs_tree.get_children(): self.runs_tree.delete(i)
        for r in rows:
            self.runs_tree.insert("", "end", values=r)

    def _sort_runs(self, col, reverse):
        data = [(self.runs_tree.set(k, col), k) for k in self.runs_tree.get_children("")]
        # try numeric sort
        try:
            data.sort(key=lambda t: float(t[0]) if t[0] not in ("", None) else float("-inf"), reverse=reverse)
        except Exception:
            data.sort(key=lambda t: t[0], reverse=reverse)
        for idx, item in enumerate(data):
            self.runs_tree.move(item[1], "", idx)
        # toggle sort direction on header click
        self.runs_tree.heading(col, command=lambda: self._sort_runs(col, not reverse))

    def _runs_get_selected_ids(self):
        sels = self.runs_tree.selection()
        ids = []
        for s in sels:
            try:
                vals = self.runs_tree.item(s, "values")
                ids.append(int(vals[0]))
            except Exception:
                pass
        return ids

    def _runs_view_records(self):
        ids = self._runs_get_selected_ids()
        if not ids:
            messagebox.showinfo("Runs", "Select a run first.")
            return
        run_id = ids[0]
        rows = self._db_fetch_records(run_id)
        self._popup_records(rows, title=f"Records (run_id={run_id})")

    def _runs_view_errors(self):
        ids = self._runs_get_selected_ids()
        if not ids:
            messagebox.showinfo("Runs", "Select a run first.")
            return
        run_id = ids[0]
        rows = self._db_fetch_errors(run_id)
        self._popup_errors(rows, title=f"Errors (run_id={run_id})")

    def _runs_reexport_selected(self):
        ids = self._runs_get_selected_ids()
        if not ids:
            messagebox.showinfo("Runs", "Select a run first.")
            return
        if len(ids) > 1:
            messagebox.showinfo("Runs", "Select only one run to re-export.")
            return
        run_id = ids[0]
        docs = self._db_fetch_records_as_docs(run_id)  # [{"postCode":..., "sku":..., "price":..., "vendor_id":...}]
        if not docs:
            messagebox.showinfo("Runs", "No records in selected run.")
            return

        include_vendor = self.include_vendor_in_files_var.get()
        csv_rows = [{"postCode": d["postCode"].lstrip("0") if d["postCode"] else "",
                     "sku": d["sku"], "price": d["price"],
                     **({"vendor_id": d.get("vendor_id","")} if include_vendor else {})} for d in docs]
        json_rows = [{"postCode": d["postCode"], "sku": d["sku"], "price": d["price"],
                      **({"vendor_id": d.get("vendor_id","")} if include_vendor else {})} for d in docs]

        export_folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        os.makedirs(export_folder, exist_ok=True)
        base = f"run_{run_id}"

        if self.enable_batch_var.get():
            mode = self.batch_mode_var.get()
            if mode == "rows":
                try: chunk = max(1, int(self.rows_per_file_var.get()))
                except Exception: chunk = 1000
                self._export_by_rows(export_folder, base, csv_rows, json_rows, chunk)
            else:
                group_col = (self.group_column_var.get() or "").strip()
                self._export_by_group_valid(export_folder, base, json_rows, csv_rows, group_col)
        else:
            self._export_single(export_folder, base, csv_rows, json_rows)

        if self.open_folder_after_var.get():
            try: self._open_folder(export_folder)
            except Exception: pass
        self.status_label.config(text=f"Re-exported run_id={run_id}")
        messagebox.showinfo("Runs", f"Re-export complete for run_id={run_id}")

    def _runs_diff_two(self):
        ids = self._runs_get_selected_ids()
        if len(ids) != 2:
            messagebox.showinfo("Runs", "Select exactly two runs (A then B).")
            return
        a_id, b_id = ids[0], ids[1]
        a_docs = self._db_fetch_records_as_docs(a_id)
        b_docs = self._db_fetch_records_as_docs(b_id)
        a_set = {(d["sku"], d["postCode"]) for d in a_docs}
        b_set = {(d["sku"], d["postCode"]) for d in b_docs}

        added = sorted(list(b_set - a_set))
        removed = sorted(list(a_set - b_set))
        # price deltas for intersection
        a_map = {(d["sku"], d["postCode"]): d["price"] for d in a_docs}
        b_map = {(d["sku"], d["postCode"]): d["price"] for d in b_docs}
        common = sorted(list(a_set & b_set))
        deltas = []
        for key in common:
            pa, pb = a_map.get(key), b_map.get(key)
            if pa != pb:
                deltas.append((key[0], key[1], pa, pb, round(pb - pa, 6)))
        deltas.sort(key=lambda x: (x[0], x[1]))

        # show popup with summary lists
        txt = []
        txt.append(f"A = run {a_id}, B = run {b_id}")
        txt.append(f"Added in B (not in A): {len(added)}")
        for s, pc in added[:500]: txt.append(f"  + {s} @ {pc}")
        if len(added) > 500: txt.append("  … (truncated)")

        txt.append(f"\nRemoved in B (present in A only): {len(removed)}")
        for s, pc in removed[:500]: txt.append(f"  - {s} @ {pc}")
        if len(removed) > 500: txt.append("  … (truncated)")

        txt.append(f"\nPrice changes on common pairs: {len(deltas)} (sku, postCode, A_price, B_price, Δ)")
        for s, pc, pa, pb, d in deltas[:500]:
            txt.append(f"  ~ {s} @ {pc} : {pa} → {pb}  (Δ {d})")
        if len(deltas) > 500: txt.append("  … (truncated)")

        self._popup_text("\n".join(txt), title=f"Run Diff: {a_id} Δ {b_id}")

    # ---- DB fetch utilities for runs browser ----
    def _db_fetch_records(self, run_id:int):
        db = self.db_path_var.get().strip()
        if not db or not os.path.exists(db): return []
        try:
            conn = sqlite3.connect(db); cur = conn.cursor()
            cur.execute("SELECT id, sku, postCode, price, vendor_id FROM records WHERE run_id=? ORDER BY sku, postCode", (run_id,))
            rows = cur.fetchall(); conn.close(); return rows
        except Exception as e:
            messagebox.showerror("Runs", f"Failed to load records: {e}"); return []

    def _db_fetch_records_as_docs(self, run_id:int):
        rows = self._db_fetch_records(run_id)
        docs = []
        for (_id, sku, pc, price, vendor_id) in rows:
            docs.append({"sku": sku, "postCode": pc, "price": float(price), "vendor_id": vendor_id or ""})
        return docs

    def _db_fetch_errors(self, run_id:int):
        db = self.db_path_var.get().strip()
        if not db or not os.path.exists(db): return []
        try:
            conn = sqlite3.connect(db); cur = conn.cursor()
            cur.execute("SELECT row_no, context, error FROM errors WHERE run_id=? ORDER BY row_no", (run_id,))
            rows = cur.fetchall(); conn.close(); return rows
        except Exception as e:
            messagebox.showerror("Runs", f"Failed to load errors: {e}"); return []

    # ---- popup viewers ----
    def _popup_records(self, rows, title="Records"):
        win = tk.Toplevel(self.root); win.title(title); win.geometry("900x600")
        win.columnconfigure(0, weight=1); win.rowconfigure(1, weight=1)

        search_var = tk.StringVar(value="")
        top = ttk.Frame(win); top.grid(row=0, column=0, sticky="ew", pady=(6,4))
        ttk.Label(top, text="Search (sku/postCode):").pack(side="left")
        ttk.Entry(top, textvariable=search_var, width=30).pack(side="left", padx=(8,8))
        tv = ttk.Treeview(win, columns=("id","sku","postCode","price","vendor_id"), show="headings")
        for c in ("id","sku","postCode","price","vendor_id"):
            tv.heading(c, text=c); tv.column(c, width=140 if c!="id" else 80, anchor="center")
        tv.grid(row=1, column=0, sticky="nsew")
        sy = ttk.Scrollbar(win, orient="vertical", command=tv.yview); sy.grid(row=1, column=1, sticky="ns"); tv.configure(yscrollcommand=sy.set)

        def refresh_table():
            q = search_var.get().strip().lower()
            tv.delete(*tv.get_children())
            for r in rows:
                if not q or q in str(r[1]).lower() or q in str(r[2]).lower():
                    tv.insert("", "end", values=r)
        refresh_table()
        ttk.Button(top, text="Apply", command=refresh_table, bootstyle=INFO).pack(side="left")

    def _popup_errors(self, rows, title="Errors"):
        win = tk.Toplevel(self.root); win.title(title); win.geometry("900x500")
        win.columnconfigure(0, weight=1); win.rowconfigure(0, weight=1)
        tv = ttk.Treeview(win, columns=("row_no","context","error"), show="headings")
        for c in ("row_no","context","error"):
            tv.heading(c, text=c); tv.column(c, width=160 if c!="error" else 600, anchor="w")
        tv.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(win, orient="vertical", command=tv.yview); sy.grid(row=0, column=1, sticky="ns"); tv.configure(yscrollcommand=sy.set)
        for r in rows:
            tv.insert("", "end", values=r)

    def _popup_text(self, text, title="Info"):
        win = tk.Toplevel(self.root); win.title(title); win.geometry("900x600")
        win.columnconfigure(0, weight=1); win.rowconfigure(0, weight=1)
        txt = tk.Text(win, wrap=tk.NONE)
        txt.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(win, orient="vertical", command=txt.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(win, orient="horizontal", command=txt.xview); sx.grid(row=1, column=0, sticky="ew")
        txt.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        txt.insert("1.0", text)
        txt.config(state="disabled")

    def _open_db_folder(self):
        db = self.db_path_var.get().strip()
        if not db:
            messagebox.showinfo("Runs", "No DB path configured.")
            return
        folder = os.path.abspath(os.path.dirname(db) or ".")
        try: self._open_folder(folder)
        except Exception: pass

    # -------------------- Run --------------------
    def _open_folder(self, path):
        sys = platform.system()
        if sys == "Windows": subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sys == "Darwin": subprocess.Popen(["open", path])
        else: subprocess.Popen(["xdg-open", path])

# -------------------- Run App --------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = CSVConverterApp(root)
    root.mainloop()



# =======================


# pre_azure_cosmos_db_validation.py


from __future__ import annotations

import os
import csv
import json
import math
import platform
import subprocess
from collections import defaultdict
from statistics import mean
from datetime import datetime
from typing import Any, Mapping, Optional, Iterable

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS

# --- SQLAlchemy plumbing (models live in models.py) ---
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

# Adjust these imports to match models.py (see “Expected models API” above)
from models import Base, IngestRun, VendorRecord  # noqa: F401
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column
from sqlalchemy import String, Float, Integer, DateTime, UniqueConstraint, ForeignKey, func

Base = declarative_base()

# class IngestRun(Base):
#     __tablename__ = "ingest_runs"
#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     profile_name: Mapped[str] = mapped_column(String(64))
#     vendor_id: Mapped[str] = mapped_column(String(128))
#     source_name: Mapped[str] = mapped_column(String(256))
#     created_at: Mapped = mapped_column(DateTime(timezone=True), server_default=func.now())
#     rows_total: Mapped[int] = mapped_column(Integer)
#     rows_valid: Mapped[int] = mapped_column(Integer)
#     rows_invalid: Mapped[int] = mapped_column(Integer)
#     duplicates: Mapped[int] = mapped_column(Integer)
#     unique_skus: Mapped[int] = mapped_column(Integer)

# class VendorRecord(Base):
#     __tablename__ = "vendor_records"
#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     vendor_id: Mapped[str] = mapped_column(String(128), index=True)
#     sku: Mapped[str] = mapped_column(String(128), index=True)
#     postCode: Mapped[str] = mapped_column(String(16), index=True)
#     price: Mapped[float] = mapped_column(Float)
#     run_id: Mapped[int] = mapped_column(ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True)
#     __table_args__ = (UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),)



# ==================== App Settings ====================

APP_DIR = os.path.join(os.path.expanduser("~"), ".csvjson_app")
CONFIG_PATH = os.path.join(APP_DIR, "app_settings.json")

DEFAULT_SETTINGS = {
    "vendor_id": "",
    "export": {
        "folder": os.path.abspath("export"),
        "open_folder_after": True,
        "filename_pattern": "{base}_{batch}_{group}_{ts}.{ext}",
        "formats": {"csv": True, "json": True},
    },
    "batch": {
        "enabled": True,
        "mode": "rows",           # rows | group
        "rows_per_file": 1000,
        "group_column": "postcode"
    },
    # DB is managed here now (connection & save)
    "db": {
        "engine": "sqlite",       # sqlite | postgres | mysql
        "dedupe_scope": "global", # global or per_run (we still enforce unique triple)
        "dsn": "",                # if provided, overrides everything below
        "sqlite_path": os.path.abspath("export_runs.sqlite"),
        "pg": {"host": "localhost", "port": 5432, "user": "", "password": "", "dbname": "", "sslmode": "require"},
        "mysql": {"host": "localhost", "port": 3306, "user": "", "password": "", "dbname": "", "ssl": True},
        "profile_name": "default",
        "echo": False,
        "create_tables": True
    }
}

def _ensure_app_dir() -> None:
    os.makedirs(APP_DIR, exist_ok=True)

def load_settings() -> dict:
    import json as _json
    _ensure_app_dir()
    if not os.path.exists(CONFIG_PATH):
        return _json.loads(_json.dumps(DEFAULT_SETTINGS))
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = _json.load(f)
        def deep_merge(d, default):
            for k, v in default.items():
                if k not in d:
                    d[k] = v
                elif isinstance(v, dict) and isinstance(d[k], dict):
                    deep_merge(d[k], v)
            return d
        return deep_merge(data, _json.loads(_json.dumps(DEFAULT_SETTINGS)))
    except Exception:
        return _json.loads(_json.dumps(DEFAULT_SETTINGS))

def save_settings(cfg: dict) -> None:
    import json as _json
    _ensure_app_dir()
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        _json.dump(cfg, f, indent=2)

# ==================== Validation helpers ====================

CSV_FIELD_ALIASES = {
    "sku": ["sku", "productcode", "productCode", "product_code", "product id", "productid"],
    "postCode": ["postcode", "postCode", "post_code", "post code", "zip", "zip_code"],
    "price": ["price", "unit_price", "unitPrice", "unitprice", "unit price", "amount"],
}

def normalize_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v).strip()
    return str(v).strip().strip('"').strip()

def _lower_keys(d: Mapping[str, Any]) -> dict[str, Any]:
    return {(k or "").strip().lower(): v for k, v in d.items()}

def field_from_row(row: Mapping[str, Any], key: str) -> Any:
    for alias in CSV_FIELD_ALIASES.get(key, []):
        a = alias.lower()
        if a in row:
            return row.get(a)
    return None

def is_valid_sku(s: str) -> tuple[bool, str]:
    s = normalize_str(s)
    if not s:
        return False, "sku empty"
    for ch in s:
        if not (ch.isalnum() or ch in "-_./"):
            return False, "sku has invalid characters"
    if len(s) > 64:
        return False, "sku too long"
    return True, ""

def is_valid_postcode(pc: str) -> tuple[bool, str]:
    pc = normalize_str(pc)
    if not pc:
        return False, "postCode empty"
    if not pc.isdigit() or len(pc) != 4:
        return False, "postCode must be 4 digits"
    return True, ""

def normalize_price(p: str) -> tuple[bool, float, str]:
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

def build_doc(raw_sku: str, raw_pc: str, price_val: float) -> dict[str, Any]:
    return {"sku": normalize_str(raw_sku), "postCode": normalize_str(raw_pc), "price": float(price_val)}

def _validate_from_reader(reader: csv.DictReader) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs: list[dict] = []
    errors: list[dict] = []
    warnings: list[str] = []
    seen_ids: set[str] = set()

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
        if not raw_sku and not raw_pc and not raw_price:
            continue

        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
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

        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            continue

        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))

    return valid_docs, errors, warnings

def validate_csv(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    try:
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return _validate_from_reader(reader)
    except Exception as e:
        return [], [{"row": 0, "context": "file", "error": f"Read error: {e}"}], []

def validate_json(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs: list[dict] = []
    errors: list[dict] = []
    warnings: list[str] = []
    seen_ids: set[str] = set()

    def validate_obj(obj: dict, idx_for_report: int) -> None:
        obj_lc = _lower_keys(obj)
        raw_sku = normalize_str(field_from_row(obj_lc, "sku"))
        raw_pc = normalize_str(field_from_row(obj_lc, "postCode"))
        raw_price = normalize_str(field_from_row(obj_lc, "price"))

        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
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
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            return

        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            return

        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))

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

def validate_pasted_csv_text(text: str) -> tuple[list[dict], list[dict], list[str]]:
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return [], [{"row": 1, "context": "header", "error": "No content"}], []
    reader = csv.DictReader(lines)
    return _validate_from_reader(reader)

# ==================== DB Utils ====================

def _build_sqlalchemy_url(cfg: dict) -> str:
    dsn = (cfg.get("dsn") or "").strip()
    if dsn:
        return dsn
    eng = (cfg.get("engine") or "sqlite").lower()
    if eng == "sqlite":
        path = cfg.get("sqlite_path") or os.path.abspath("export_runs.sqlite")
        if path and not path.startswith("sqlite:///"):
            return f"sqlite:///{path}"
        return path
    if eng == "postgres":
        pg = cfg.get("pg", {})
        user = pg.get("user", "")
        pwd = pg.get("password", "")
        host = pg.get("host", "localhost")
        port = str(pg.get("port", 5432))
        db   = pg.get("dbname", "")
        sslmode = pg.get("sslmode", "require")
        auth = f"{user}:{pwd}@" if user or pwd else ""
        return f"postgresql+psycopg2://{auth}{host}:{port}/{db}?sslmode={sslmode}"
    if eng == "mysql":
        my = cfg.get("mysql", {})
        user = my.get("user", "")
        pwd = my.get("password", "")
        host = my.get("host", "localhost")
        port = str(my.get("port", 3306))
        db   = my.get("dbname", "")
        ssl  = my.get("ssl", True)
        auth = f"{user}:{pwd}@" if user or pwd else ""
        sslq = "&ssl=true" if ssl else ""
        return f"mysql+pymysql://{auth}{host}:{port}/{db}?charset=utf8mb4{sslq}"
    # fallback
    return "sqlite:///export_runs.sqlite"

def get_engine_from_settings(cfg: dict) -> Engine:
    url = _build_sqlalchemy_url(cfg)
    echo = bool(cfg.get("echo", False))
    if url.startswith("sqlite"):
        # better concurrency for SQLite
        return create_engine(url, echo=echo, future=True)
    return create_engine(url, echo=echo, future=True)

def try_create_tables(engine: Engine, cfg: dict) -> None:
    if cfg.get("create_tables", True):
        Base.metadata.create_all(engine)

def test_db_connection(engine: Engine) -> tuple[bool, str]:
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        return True, "Connection OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def save_run_and_rows(
    engine: Engine,
    profile_name: str,
    vendor_id: str,
    source_name: str,
    stats: dict[str, Any],
    rows: list[dict]
) -> tuple[int, int]:
    """
    Inserts an IngestRun and upserts VendorRecord rows with dedupe on (vendor_id, sku, postCode).
    Returns: (run_id, inserted_count)
    """
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    with SessionLocal() as db:
        run = IngestRun(
            profile_name=profile_name,
            vendor_id=vendor_id,
            source_name=source_name,
            rows_total=int(stats.get("rows_total") or 0),
            rows_valid=int(stats.get("rows_valid") or 0),
            rows_invalid=int(stats.get("rows_invalid") or 0),
            duplicates=int(stats.get("duplicates") or 0),
            unique_skus=int(stats.get("unique_skus") or 0),
        )
        db.add(run)
        db.flush()  # get run.id

        # fetch existing keys to avoid duplicates in bulk
        keys = {(vendor_id, r["sku"], r["postCode"]) for r in rows}
        existing: set[tuple[str, str, str]] = set()
        if keys:
            stmt = select(VendorRecord.vendor_id, VendorRecord.sku, VendorRecord.postCode).where(
                (VendorRecord.vendor_id == vendor_id) &
                (VendorRecord.sku.in_([k[1] for k in keys])) &
                (VendorRecord.postCode.in_([k[2] for k in keys]))
            )
            for v, s, p in db.execute(stmt):
                existing.add((v, s, p))

        to_insert = []
        for r in rows:
            k = (vendor_id, r["sku"], r["postCode"])
            if k in existing:
                continue
            to_insert.append(
                VendorRecord(
                    vendor_id=vendor_id,
                    sku=r["sku"],
                    postCode=r["postCode"],
                    price=float(r["price"]),
                    run_id=run.id
                )
            )
        if to_insert:
            db.add_all(to_insert)

        db.commit()
        return run.id, len(to_insert)

# ==================== GUI App ====================

class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("CSV/JSON → Clean CSV & JSON")
        self.root.geometry("1180x860")
        self.root.resizable(True, True)
        self.style = Style(theme="darkly")

        self.settings = load_settings()

        # state
        self.file_path: Optional[str] = None
        self.headers: list[str] = []
        self.last_valid_docs: list[dict] = []
        self.last_errors: list[dict] = []
        self.last_warnings: list[str] = []
        self.cached_stats: dict[str, Any] = {}

        # tk vars
        self.vendor_id_var = tk.StringVar(value=self.settings["vendor_id"])

        self.output_folder_var = tk.StringVar(value=self.settings["export"]["folder"])
        self.open_folder_after_var = tk.BooleanVar(value=self.settings["export"]["open_folder_after"])
        self.filename_pattern_var = tk.StringVar(value=self.settings["export"]["filename_pattern"])
        self.export_csv_var = tk.BooleanVar(value=self.settings["export"]["formats"]["csv"])
        self.export_json_var = tk.BooleanVar(value=self.settings["export"]["formats"]["json"])

        self.enable_batch_var = tk.BooleanVar(value=self.settings["batch"]["enabled"])
        self.batch_mode_var = tk.StringVar(value=self.settings["batch"]["mode"])
        self.rows_per_file_var = tk.IntVar(value=self.settings["batch"]["rows_per_file"])
        self.group_column_var = tk.StringVar(value=self.settings["batch"]["group_column"])

        # DB (active in this app)
        self.db_profile_name_var = tk.StringVar(value=self.settings["db"]["profile_name"])
        self.db_engine_var = tk.StringVar(value=self.settings["db"]["engine"])
        self.db_dedupe_var = tk.StringVar(value=self.settings["db"]["dedupe_scope"])
        self.db_dsn_var = tk.StringVar(value=self.settings["db"]["dsn"])
        self.sqlite_path_var = tk.StringVar(value=self.settings["db"]["sqlite_path"])
        self.pg_host_var = tk.StringVar(value=self.settings["db"]["pg"]["host"])
        self.pg_port_var = tk.IntVar(value=self.settings["db"]["pg"]["port"])
        self.pg_user_var = tk.StringVar(value=self.settings["db"]["pg"]["user"])
        self.pg_pass_var = tk.StringVar(value=self.settings["db"]["pg"]["password"])
        self.pg_dbname_var = tk.StringVar(value=self.settings["db"]["pg"]["dbname"])
        self.pg_sslmode_var = tk.StringVar(value=self.settings["db"]["pg"]["sslmode"])
        self.my_host_var = tk.StringVar(value=self.settings["db"]["mysql"]["host"])
        self.my_port_var = tk.IntVar(value=self.settings["db"]["mysql"]["port"])
        self.my_user_var = tk.StringVar(value=self.settings["db"]["mysql"]["user"])
        self.my_pass_var = tk.StringVar(value=self.settings["db"]["mysql"]["password"])
        self.my_dbname_var = tk.StringVar(value=self.settings["db"]["mysql"]["dbname"])
        self.my_ssl_var = tk.BooleanVar(value=self.settings["db"]["mysql"]["ssl"])
        self.db_echo_var = tk.BooleanVar(value=self.settings["db"].get("echo", False))
        self.db_create_tables_var = tk.BooleanVar(value=self.settings["db"].get("create_tables", True))

        # DB runtime
        self._engine: Optional[Engine] = None
        self._SessionLocal: Optional[sessionmaker] = None

        self._ensure_export_dir()
        self._build_ui()

    # ---------- UI ----------
    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        nb = ttk.Notebook(self.root)
        nb.grid(row=0, column=0, sticky="nsew")

        self.tab_source = ttk.Frame(nb, padding=12); self._tab_source(self.tab_source); nb.add(self.tab_source, text="Source")
        self.tab_preview = ttk.Frame(nb, padding=12); self._tab_preview(self.tab_preview); nb.add(self.tab_preview, text="Preview")
        self.tab_settings = ttk.Frame(nb, padding=0); self._tab_settings(self.tab_settings); nb.add(self.tab_settings, text="Settings")

    def _tab_source(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)

        file_fr = ttk.LabelFrame(parent, text="File", padding=10)
        file_fr.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        file_fr.columnconfigure(1, weight=1)
        ttk.Label(file_fr, text="Selected:").grid(row=0, column=0, sticky="w")
        self.file_label = ttk.Label(file_fr, text="No file selected", anchor="w")
        self.file_label.grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(file_fr, text="Select CSV/JSON", command=self.load_file, bootstyle=PRIMARY).grid(row=0, column=2)

        paste_fr = ttk.LabelFrame(parent, text="Paste CSV Content", padding=10)
        paste_fr.grid(row=1, column=0, sticky="nsew")
        paste_fr.columnconfigure(0, weight=1)
        paste_fr.rowconfigure(0, weight=1)
        self.paste_box = tk.Text(paste_fr, wrap=tk.NONE, height=8)
        self.paste_box.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(paste_fr, orient="vertical", command=self.paste_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(paste_fr, orient="horizontal", command=self.paste_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.paste_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)

        ttk.Button(parent, text="Validate & Preview", command=self.preview_data, bootstyle=INFO)\
            .grid(row=3, column=0, sticky="w", pady=(10, 0))

    def _tab_preview(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=3)
        parent.columnconfigure(1, weight=2)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent); top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(top, text="Preview shows first 100 validated rows (green). Invalid items are summarized on the right (red).").pack(side="left")
        ttk.Button(top, text="Export Files", command=self.export_files, bootstyle=SUCCESS).pack(side="right", padx=(8, 0))
        ttk.Button(top, text="Save to DB", command=self._save_to_db_clicked, bootstyle=PRIMARY).pack(side="right", padx=(8, 0))

        pv_fr = ttk.LabelFrame(parent, text="Preview", padding=10); pv_fr.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        pv_fr.columnconfigure(0, weight=1); pv_fr.rowconfigure(0, weight=1)
        self.preview_box = tk.Text(pv_fr, wrap=tk.NONE, state="disabled")
        self.preview_box.grid(row=0, column=0, sticky="nsew")
        self.preview_box.tag_configure("good", foreground="#5bd75b")  # green
        self.preview_box.tag_configure("head", foreground="#9ecbff")
        sy = ttk.Scrollbar(pv_fr, orient="vertical", command=self.preview_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(pv_fr, orient="horizontal", command=self.preview_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.preview_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)

        st_fr = ttk.LabelFrame(parent, text="Data Quality / Stats", padding=10); st_fr.grid(row=1, column=1, sticky="nsew")
        st_fr.columnconfigure(0, weight=1); st_fr.rowconfigure(0, weight=1)
        self.stats_box = tk.Text(st_fr, height=12, wrap=tk.WORD, state="normal")
        self.stats_box.grid(row=0, column=0, sticky="nsew")
        self.stats_box.tag_configure("good", foreground="#5bd75b")
        self.stats_box.tag_configure("bad", foreground="#ff6b6b")
        sy2 = ttk.Scrollbar(st_fr, orient="vertical", command=self.stats_box.yview); sy2.grid(row=0, column=1, sticky="ns")
        self.stats_box.config(yscrollcommand=sy2.set)

    def _tab_settings(self, parent: ttk.Frame) -> None:
        # Scrollable container with mouse wheel support
        parent.columnconfigure(0, weight=1)
        canvas = tk.Canvas(parent, highlightthickness=0)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scroll = ttk.Frame(canvas, padding=12)

        scroll.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        # mouse wheel bindings for all platforms
        def _on_mousewheel(event):
            if platform.system() == "Darwin":
                canvas.yview_scroll(int(-1 * (event.delta)), "units")
            else:
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))

        # ---- Formats / Vendor / Destination ----
        fm = ttk.LabelFrame(scroll, text="Formats", padding=10); fm.grid(row=0, column=0, sticky="ew")
        ttk.Checkbutton(fm, text="Export CSV", variable=self.export_csv_var).pack(side="left")
        ttk.Checkbutton(fm, text="Export JSON", variable=self.export_json_var).pack(side="left", padx=(10, 0))

        vm = ttk.LabelFrame(scroll, text="Vendor", padding=10); vm.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(vm, text="Vendor ID:").grid(row=0, column=0, sticky="w")
        ttk.Entry(vm, textvariable=self.vendor_id_var, width=24).grid(row=0, column=1, sticky="w", padx=(8,8))

        out = ttk.LabelFrame(scroll, text="Destination & Naming", padding=10); out.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        out.columnconfigure(1, weight=1)
        ttk.Label(out, text="Output folder:").grid(row=0, column=0, sticky="w")
        self.out_label = ttk.Label(out, text=self.output_folder_var.get(), anchor="w"); self.out_label.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(out, text="Browse", command=self._choose_output_folder).grid(row=0, column=2, sticky="w")
        ttk.Label(out, text="Filename pattern:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.pattern_entry = ttk.Entry(out, textvariable=self.filename_pattern_var)
        self.pattern_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(out, text="Tokens: {base} {batch} {group} {ts} {ext}").grid(row=2, column=1, sticky="w", pady=(6, 0))
        ttk.Checkbutton(out, text="Open folder after export", variable=self.open_folder_after_var).grid(row=3, column=1, sticky="w", pady=(8, 0))

        # ---- Batch ----
        lf = ttk.LabelFrame(scroll, text="Batch Export Configuration", padding=10); lf.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        lf.columnconfigure(1, weight=1)
        ttk.Checkbutton(lf, text="Enable batch export", variable=self.enable_batch_var,
                        command=self._toggle_batch_controls).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        ttk.Label(lf, text="Mode:").grid(row=1, column=0, sticky="w")
        row_mode = ttk.Frame(lf); row_mode.grid(row=1, column=1, sticky="w")
        ttk.Radiobutton(row_mode, text="Rows per file", value="rows", variable=self.batch_mode_var,
                        command=self._on_mode_change).pack(side="left")
        ttk.Radiobutton(row_mode, text="Group by column", value="group", variable=self.batch_mode_var,
                        command=self._on_mode_change).pack(side="left", padx=(10, 0))
        ttk.Label(lf, text="Rows per file:").grid(row=2, column=0, sticky="w")
        self.rows_entry = ttk.Entry(lf, width=10, textvariable=self.rows_per_file_var); self.rows_entry.grid(row=2, column=1, sticky="w", pady=(0, 4))
        ttk.Label(lf, text="Group column:").grid(row=3, column=0, sticky="w")
        self.group_combo = ttk.Combobox(lf, width=20, state="readonly", textvariable=self.group_column_var,
                                        values=["postcode", "productcode", "sku", "state"])
        self.group_combo.grid(row=3, column=1, sticky="w")
        self._toggle_batch_controls(); self._on_mode_change()

        # ---- Database (active) ----
        dbf = ttk.LabelFrame(scroll, text="Database", padding=10)
        dbf.grid(row=4, column=0, sticky="ew", pady=(10, 0))
        dbf.columnconfigure(1, weight=1)

        ttk.Label(dbf, text="Profile Name").grid(row=0, column=0, sticky="w")
        ttk.Entry(dbf, textvariable=self.db_profile_name_var, width=20).grid(row=0, column=1, sticky="w", padx=(8, 8))
        ttk.Button(dbf, text="Test Connection", command=self._test_db_clicked, bootstyle=INFO).grid(row=0, column=2, sticky="w")

        ttk.Label(dbf, text="Engine").grid(row=1, column=0, sticky="w")
        ttk.Combobox(dbf, state="readonly", width=12, textvariable=self.db_engine_var,
                     values=["sqlite", "postgres", "mysql"]).grid(row=1, column=1, sticky="w")
        ttk.Label(dbf, text="De-dup Scope").grid(row=1, column=2, sticky="e", padx=(12, 6))
        ttk.Combobox(dbf, state="readonly", width=10, textvariable=self.db_dedupe_var,
                     values=["per_run", "global"]).grid(row=1, column=3, sticky="w")

        ttk.Label(dbf, text="Echo SQL").grid(row=1, column=4, sticky="e")
        ttk.Checkbutton(dbf, variable=self.db_echo_var).grid(row=1, column=5, sticky="w", padx=(4,0))
        ttk.Label(dbf, text="Create Tables").grid(row=1, column=6, sticky="e")
        ttk.Checkbutton(dbf, variable=self.db_create_tables_var).grid(row=1, column=7, sticky="w", padx=(4,0))

        ttk.Label(dbf, text="DSN (optional)").grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(dbf, textvariable=self.db_dsn_var).grid(row=2, column=1, columnspan=7, sticky="ew", pady=(6, 0))

        sl = ttk.LabelFrame(dbf, text="SQLite", padding=10); sl.grid(row=3, column=0, columnspan=8, sticky="ew", pady=(10, 0))
        sl.columnconfigure(1, weight=1)
        ttk.Label(sl, text="Path").grid(row=0, column=0, sticky="w")
        ttk.Entry(sl, textvariable=self.sqlite_path_var).grid(row=0, column=1, sticky="ew", padx=(8,8))
        ttk.Button(sl, text="Browse", command=self._choose_db_path).grid(row=0, column=2, sticky="w")

        pg = ttk.LabelFrame(dbf, text="PostgreSQL", padding=10); pg.grid(row=4, column=0, columnspan=8, sticky="ew", pady=(10, 0))
        for c in range(8): pg.columnconfigure(c, weight=1 if c in (1,3,5,7) else 0)
        ttk.Label(pg, text="Host").grid(row=0, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_host_var, width=18).grid(row=0, column=1, sticky="w")
        ttk.Label(pg, text="Port").grid(row=0, column=2, sticky="e"); ttk.Entry(pg, textvariable=self.pg_port_var, width=8).grid(row=0, column=3, sticky="w")
        ttk.Label(pg, text="User").grid(row=1, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_user_var, width=18).grid(row=1, column=1, sticky="w")
        ttk.Label(pg, text="Pass").grid(row=1, column=2, sticky="e"); ttk.Entry(pg, textvariable=self.pg_pass_var, show="*", width=18).grid(row=1, column=3, sticky="w")
        ttk.Label(pg, text="DB").grid(row=2, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_dbname_var, width=18).grid(row=2, column=1, sticky="w")
        ttk.Label(pg, text="SSL").grid(row=2, column=2, sticky="e"); ttk.Combobox(pg, state="readonly", width=12, textvariable=self.pg_sslmode_var, values=["disable","require","verify-ca","verify-full"]).grid(row=2, column=3, sticky="w")

        my = ttk.LabelFrame(dbf, text="MySQL", padding=10); my.grid(row=5, column=0, columnspan=8, sticky="ew", pady=(10, 0))
        for c in range(8): my.columnconfigure(c, weight=1 if c in (1,3,5,7) else 0)
        ttk.Label(my, text="Host").grid(row=0, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_host_var, width=18).grid(row=0, column=1, sticky="w")
        ttk.Label(my, text="Port").grid(row=0, column=2, sticky="e"); ttk.Entry(my, textvariable=self.my_port_var, width=8).grid(row=0, column=3, sticky="w")
        ttk.Label(my, text="User").grid(row=1, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_user_var, width=18).grid(row=1, column=1, sticky="w")
        ttk.Label(my, text="Pass").grid(row=1, column=2, sticky="e"); ttk.Entry(my, textvariable=self.my_pass_var, show="*", width=18).grid(row=1, column=3, sticky="w")
        ttk.Label(my, text="DB").grid(row=2, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_dbname_var, width=18).grid(row=2, column=1, sticky="w")
        ttk.Checkbutton(my, text="SSL", variable=self.my_ssl_var).grid(row=2, column=2, sticky="w")

        # Save / Load settings actions
        act = ttk.Frame(scroll); act.grid(row=6, column=0, sticky="w", pady=(12, 0))
        ttk.Button(act, text="Save Settings", command=self._save_all_settings, bootstyle=SUCCESS).pack(side="left")
        ttk.Button(act, text="Open Config Folder", command=self._open_config_folder).pack(side="left", padx=(8, 0))

    # ---------- helpers ----------
    def _ensure_export_dir(self) -> None:
        folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        self.output_folder_var.set(folder)
        os.makedirs(folder, exist_ok=True)

    def _choose_output_folder(self) -> None:
        path = filedialog.askdirectory(initialdir=self.output_folder_var.get())
        if path:
            self.output_folder_var.set(path)
            self.out_label.config(text=path)

    def _toggle_batch_controls(self) -> None:
        enabled = self.enable_batch_var.get()
        rows_state = "normal" if (enabled and self.batch_mode_var.get() == "rows") else "disabled"
        group_state = "readonly" if (enabled and self.batch_mode_var.get() == "group") else "disabled"
        self.rows_entry.configure(state=rows_state)
        self.group_combo.configure(state=group_state)

    def _on_mode_change(self) -> None:
        self._toggle_batch_controls()

    def _update_group_columns(self, headers: list[str]) -> None:
        if headers:
            self.group_combo.configure(values=headers)
            cur = self.group_column_var.get()
            if cur not in headers:
                self.group_column_var.set("postcode" if "postcode" in headers else headers[0])

    # ---------- Source & Preview ----------
    def load_file(self) -> None:
        file_path = filedialog.askopenfilename(filetypes=[("CSV/JSON files", "*.csv *.json")])
        if not file_path:
            return
        self.file_path = file_path
        self.file_label.config(text=os.path.basename(file_path))
        if file_path.lower().endswith(".csv"):
            try:
                with open(file_path, newline="", encoding="utf-8-sig") as f:
                    rdr = csv.DictReader(f)
                    self.headers = [(h or "").strip().lower() for h in (rdr.fieldnames or [])]
                    self._update_group_columns(self.headers)
            except Exception:
                pass
        else:
            self.headers = ["sku","postCode","price"]
            self._update_group_columns(self.headers)

    def preview_data(self) -> None:
        if self.file_path:
            ext = os.path.splitext(self.file_path)[1].lower()
            if ext == ".csv":
                valid_docs, errors, warnings = validate_csv(self.file_path)
            elif ext == ".json":
                valid_docs, errors, warnings = validate_json(self.file_path)
            else:
                messagebox.showerror("Error", "Unsupported file type.")
                return
        else:
            txt = self.paste_box.get("1.0", tk.END).strip()
            if not txt:
                messagebox.showerror("Error", "No file selected or pasted content.")
                return
            valid_docs, errors, warnings = validate_pasted_csv_text(txt)
            first = [ln for ln in txt.splitlines() if ln.strip()][:1]
            self.headers = [h.strip().lower() for h in next(csv.reader(first))] if first else ["sku","postCode","price"]
            self._update_group_columns(self.headers)

        self.last_valid_docs, self.last_errors, self.last_warnings = valid_docs, errors, warnings

        # Preview
        self.preview_box.config(state="normal")
        self.preview_box.delete("1.0", tk.END)
        self.preview_box.insert(tk.END, "postCode,sku,price,vendor_id\n", ("head",))
        v_id = self.vendor_id_var.get().strip() or ""
        for d in valid_docs[:100]:
            line = f"{d['postCode']},{d['sku']},{d['price']},{v_id}\n"
            self.preview_box.insert(tk.END, line, ("good",))
        self.preview_box.config(state="disabled")

        # Stats
        total_rows_est = len(valid_docs) + len(errors)
        dup_count = sum(1 for e in errors if "Duplicate id" in e.get("error", ""))
        uniq_skus = len(set(d["sku"] for d in valid_docs))
        prices = [d["price"] for d in valid_docs]
        pmin = min(prices) if prices else None
        pmax = max(prices) if prices else None
        pavg = round(mean(prices), 6) if prices else None
        warn_count = len(warnings)

        self.cached_stats = {
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

        self.stats_box.config(state="normal")
        self.stats_box.delete("1.0", tk.END)

        def put(line: str, tag: Optional[str] = None):
            if tag:
                self.stats_box.insert(tk.END, line + "\n", (tag,))
            else:
                self.stats_box.insert(tk.END, line + "\n")

        put(f"Rows (estimated): {total_rows_est}", "good" if total_rows_est > 0 else "bad")
        put(f"Valid rows: {len(valid_docs)}", "good" if len(valid_docs) > 0 else "bad")
        put(f"Invalid rows: {len(errors)}", "bad" if len(errors) > 0 else "good")
        put(f"Duplicates: {dup_count}", "bad" if dup_count > 0 else "good")
        put(f"Unique SKUs: {uniq_skus}", "good" if uniq_skus > 0 else "bad")

        if pmin is not None:
            put(f"Price min/max/avg: {pmin} / {pmax} / {pavg}", "good")
        else:
            put("Price statistics: N/A", "bad")

        put(f"Warnings: {warn_count}", "bad" if warn_count > 0 else "good")

        if errors:
            self.stats_box.insert(tk.END, "\nIssues (first 50):\n", ("bad",))
            for e in errors[:50]:
                self.stats_box.insert(tk.END, f"Row {e.get('row')}: {e.get('context','')} -> {e.get('error')}\n", ("bad",))
        self.stats_box.config(state="disabled")

    # ---------- Export ----------
    def export_files(self) -> None:
        if not (self.vendor_id_var.get() or "").strip():
            val = simpledialog.askstring("Vendor ID Required", "Enter Vendor ID before export:")
            if not val:
                messagebox.showwarning("Export", "Export cancelled (Vendor ID required).")
                return
            self.vendor_id_var.set(val.strip())

        if not self.last_valid_docs and not self.last_errors:
            self.preview_data()
        if not self.last_valid_docs and self.last_errors:
            messagebox.showerror("Error", "No valid rows to export (all invalid). Check errors in Preview.")
            return

        vendor_id = self.vendor_id_var.get().strip()
        include_vendor = True

        csv_rows = [{
            "postCode": d["postCode"].lstrip("0") if d["postCode"] else "",
            "sku": d["sku"], "price": d["price"],
            **({"vendor_id": vendor_id} if include_vendor else {})
        } for d in self.last_valid_docs]

        json_rows = [{
            "postCode": d["postCode"], "sku": d["sku"], "price": d["price"],
            **({"vendor_id": vendor_id} if include_vendor else {})
        } for d in self.last_valid_docs]

        base_name = os.path.splitext(os.path.basename(self.file_path or 'pasted'))[0]
        base_name_snake = base_name.lower().replace("-", "_").replace(" ", "_")
        export_folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        os.makedirs(export_folder, exist_ok=True)

        if self.enable_batch_var.get():
            mode = self.batch_mode_var.get()
            if mode == "rows":
                try:
                    chunk = max(1, int(self.rows_per_file_var.get()))
                except Exception:
                    chunk = 1000
                self._export_by_rows(export_folder, base_name_snake, csv_rows, json_rows, chunk)
            else:
                group_col = (self.group_column_var.get() or "").strip()
                self._export_by_group(export_folder, base_name_snake, json_rows, csv_rows, group_col)
        else:
            self._export_single(export_folder, base_name_snake, csv_rows, json_rows)

        # errors file
        if self.last_errors:
            error_path = os.path.join(export_folder, f"{base_name_snake}_errors.csv")
            try:
                with open(error_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["row", "context", "error"])
                    writer.writeheader(); writer.writerows(self.last_errors)
            except Exception as e:
                messagebox.showwarning("Warning", f"Failed to write error file:\n{e}")

        if self.open_folder_after_var.get():
            try:
                self._open_folder(export_folder)
            except Exception:
                pass

        messagebox.showinfo("Success", f"Export completed.\nFiles saved in:\n{export_folder}")

    def _export_single(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict]) -> None:
        ts = self._ts()
        if self.export_csv_var.get():
            csv_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="csv")
            fields = ["postCode", "sku", "price"] + (["vendor_id"] if "vendor_id" in (csv_rows[0] if csv_rows else {}) else [])
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader(); w.writerows(csv_rows)
        if self.export_json_var.get():
            json_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_rows, f, indent=4)

    def _export_by_rows(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict], chunk_size: int) -> None:
        total = len(csv_rows)
        if total == 0:
            return
        ts = self._ts()
        parts = (total + chunk_size - 1) // chunk_size
        fields = ["postCode", "sku", "price"] + (["vendor_id"] if "vendor_id" in (csv_rows[0] if csv_rows else {}) else [])
        for i in range(parts):
            start, end = i * chunk_size, min((i+1) * chunk_size, total)
            batch_id = f"part{(i+1):03d}"
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields)
                    w.writeheader(); w.writerows(csv_rows[start:end])
            if self.export_json_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(json_rows[start:end], f, indent=4)

    def _export_by_group(self, folder: str, base: str, json_rows: list[dict], csv_rows: list[dict], group_col: str) -> None:
        ts = self._ts()
        key_lower = (group_col or "").lower()

        def key_for_json(d: dict) -> str:
            if key_lower in ("postcode", "post_code", "post code"): return d.get("postCode", "") or "UNK"
            if key_lower == "sku": return d.get("sku", "") or "UNK"
            if key_lower == "price": return str(d.get("price", "")) or "UNK"
            return str(d.get(key_lower, "") or "UNK")

        groups_json: dict[str, list[dict]] = defaultdict(list)
        groups_csv: dict[str, list[dict]] = defaultdict(list)
        for j, c in zip(json_rows, csv_rows):
            g = (key_for_json(j) or "UNK").strip() or "UNK"
            groups_json[g].append(j)
            groups_csv[g].append(c)

        fields = ["postCode", "sku", "price"] + (["vendor_id"] if "vendor_id" in (csv_rows[0] if csv_rows else {}) else [])
        for gval, rows_csv in groups_csv.items():
            safe_group = self._sanitize_group(gval)
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields)
                    w.writeheader(); w.writerows(rows_csv)
            if self.export_json_var.get():
                rows_json = groups_json[gval]
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(rows_json, f, indent=4)

    # ---------- DB actions ----------
    def _ensure_engine(self) -> Engine:
        # commit UI vars to settings first
        self._save_all_settings(silent=True)
        db_cfg = self.settings["db"]
        # set echo/create flags from UI
        db_cfg["echo"] = bool(self.db_echo_var.get())
        db_cfg["create_tables"] = bool(self.db_create_tables_var.get())
        engine = get_engine_from_settings(db_cfg)
        if db_cfg.get("create_tables", True):
            try_create_tables(engine, db_cfg)
        self._engine = engine
        return engine

    def _test_db_clicked(self) -> None:
        try:
            engine = self._ensure_engine()
            ok, msg = test_db_connection(engine)
            if ok:
                messagebox.showinfo("DB", msg)
            else:
                messagebox.showerror("DB", msg)
        except Exception as e:
            messagebox.showerror("DB", f"{type(e).__name__}: {e}")

    def _save_to_db_clicked(self) -> None:
        if not self.last_valid_docs and not self.last_errors:
            self.preview_data()
        if not self.last_valid_docs:
            messagebox.showerror("DB", "No valid rows to save.")
            return

        vendor_id = (self.vendor_id_var.get() or "").strip()
        if not vendor_id:
            val = simpledialog.askstring("Vendor ID Required", "Enter Vendor ID before saving to database:")
            if not val:
                messagebox.showwarning("DB", "Save cancelled (Vendor ID required).")
                return
            vendor_id = val.strip()
            self.vendor_id_var.set(vendor_id)

        engine = self._ensure_engine()
        source_name = os.path.basename(self.file_path) if self.file_path else "pasted"
        profile_name = self.db_profile_name_var.get().strip() or "default"

        # normalize rows to DB shape & enforce composite key triple
        rows = [{"sku": r["sku"], "postCode": r["postCode"], "price": float(r["price"])} for r in self.last_valid_docs]
        run_id, inserted = save_run_and_rows(
            engine=engine,
            profile_name=profile_name,
            vendor_id=vendor_id,
            source_name=source_name,
            stats=self.cached_stats,
            rows=rows
        )
        messagebox.showinfo("DB", f"Saved Run #{run_id}. Inserted rows: {inserted}.")

    # ---------- Settings ----------
    def _save_all_settings(self, silent: bool = False) -> None:
        self.settings["vendor_id"] = self.vendor_id_var.get().strip()
        self.settings["export"]["folder"] = self.output_folder_var.get().strip()
        self.settings["export"]["open_folder_after"] = bool(self.open_folder_after_var.get())
        self.settings["export"]["filename_pattern"] = self.filename_pattern_var.get().strip()
        self.settings["export"]["formats"]["csv"] = bool(self.export_csv_var.get())
        self.settings["export"]["formats"]["json"] = bool(self.export_json_var.get())

        self.settings["batch"]["enabled"] = bool(self.enable_batch_var.get())
        self.settings["batch"]["mode"] = self.batch_mode_var.get()
        self.settings["batch"]["rows_per_file"] = int(self.rows_per_file_var.get() or 1000)
        self.settings["batch"]["group_column"] = self.group_column_var.get().strip()

        self.settings["db"]["profile_name"] = self.db_profile_name_var.get().strip() or "default"
        self.settings["db"]["engine"] = self.db_engine_var.get()
        self.settings["db"]["dedupe_scope"] = self.db_dedupe_var.get()
        self.settings["db"]["dsn"] = self.db_dsn_var.get().strip()
        self.settings["db"]["sqlite_path"] = self.sqlite_path_var.get().strip()
        self.settings["db"]["pg"]["host"] = self.pg_host_var.get().strip()
        self.settings["db"]["pg"]["port"] = int(self.pg_port_var.get() or 5432)
        self.settings["db"]["pg"]["user"] = self.pg_user_var.get().strip()
        self.settings["db"]["pg"]["password"] = self.pg_pass_var.get()
        self.settings["db"]["pg"]["dbname"] = self.pg_dbname_var.get().strip()
        self.settings["db"]["pg"]["sslmode"] = self.pg_sslmode_var.get().strip()
        self.settings["db"]["mysql"]["host"] = self.my_host_var.get().strip()
        self.settings["db"]["mysql"]["port"] = int(self.my_port_var.get() or 3306)
        self.settings["db"]["mysql"]["user"] = self.my_user_var.get().strip()
        self.settings["db"]["mysql"]["password"] = self.my_pass_var.get()
        self.settings["db"]["mysql"]["dbname"] = self.my_dbname_var.get().strip()
        self.settings["db"]["mysql"]["ssl"] = bool(self.my_ssl_var.get())
        self.settings["db"]["echo"] = bool(self.db_echo_var.get())
        self.settings["db"]["create_tables"] = bool(self.db_create_tables_var.get())

        save_settings(self.settings)
        if not silent:
            messagebox.showinfo("Settings", "Settings saved.")

    # ---------- misc ----------
    def _render_path(self, folder: str, base: str, batch: str, group: str, ts: str, ext: str) -> str:
        pattern = (self.filename_pattern_var.get() or "{base}_{batch}_{group}_{ts}.{ext}").strip()
        vals = {"base": base, "batch": batch, "group": group if group else "all", "ts": ts, "ext": ext.lstrip(".")}
        return os.path.join(folder, pattern.format(**vals))

    def _ts(self) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _sanitize_group(self, s: str) -> str:
        out = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)
        return out[:80] if out else "UNK"

    def _open_folder(self, path: str) -> None:
        sys = platform.system()
        if sys == "Windows":
            subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sys == "Darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])

    def _choose_db_path(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".sqlite", filetypes=[("SQLite DB", "*.sqlite *.db")],
                                            initialfile=os.path.basename(self.sqlite_path_var.get() or "export_runs.sqlite"))
        if path:
            self.sqlite_path_var.set(path)

    def _open_config_folder(self) -> None:
        try:
            if platform.system() == "Windows":
                os.startfile(APP_DIR)  # type: ignore
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", APP_DIR])
            else:
                subprocess.Popen(["xdg-open", APP_DIR])
        except Exception:
            messagebox.showwarning("Open", f"Folder: {APP_DIR}")

# -------------------- Run --------------------
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()


# ====================

# models.py
# SQLAlchemy 2.x declarative models + multi-backend engine builder
# Composite de-dup key: (vendor_id, sku, postCode)
# Dedupe scope: "per_run" (unique within a run) or "global" (unique across all runs)

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Iterable, Mapping, Any

from datetime import datetime
from sqlalchemy import (
    String, Integer, Float, Text, DateTime, ForeignKey, Index, create_engine,
    event, select
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Insert

# ------------- Declarative base -------------

class Base(DeclarativeBase):
    pass

from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column
from sqlalchemy import String, Float, Integer, DateTime, UniqueConstraint, ForeignKey, func

Base = declarative_base()

class IngestRun(Base):
    __tablename__ = "ingest_runs"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(64))
    vendor_id: Mapped[str] = mapped_column(String(128))
    source_name: Mapped[str] = mapped_column(String(256))
    created_at: Mapped = mapped_column(DateTime(timezone=True), server_default=func.now())
    rows_total: Mapped[int] = mapped_column(Integer)
    rows_valid: Mapped[int] = mapped_column(Integer)
    rows_invalid: Mapped[int] = mapped_column(Integer)
    duplicates: Mapped[int] = mapped_column(Integer)
    unique_skus: Mapped[int] = mapped_column(Integer)

class VendorRecord(Base):
    __tablename__ = "vendor_records"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vendor_id: Mapped[str] = mapped_column(String(128), index=True)
    sku: Mapped[str] = mapped_column(String(128), index=True)
    postCode: Mapped[str] = mapped_column(String(16), index=True)
    price: Mapped[float] = mapped_column(Float)
    run_id: Mapped[int] = mapped_column(ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True)
    __table_args__ = (UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),)

# ------------- Models -------------

class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    vendor_id: Mapped[str] = mapped_column(String(128), nullable=True)

    rows_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_invalid: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicates: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unique_skus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    price_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_max: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    warnings: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    records: Mapped[list["Record"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )
    errors: Mapped[list["ErrorRow"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )

    def __repr__(self) -> str:
        return f"<Run id={self.run_id} ts={self.created_at.isoformat()} src={self.source_name} vendor={self.vendor_id}>"


class Record(Base):
    __tablename__ = "records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.run_id", ondelete="CASCADE"), index=True, nullable=False)

    vendor_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    sku: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    postCode: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)

    run: Mapped["Run"] = relationship(back_populates="records")

    # Generic performance indexes (unique index applied conditionally at runtime)
    __table_args__ = (
        Index("ix_records_sku_postcode", "sku", "postCode"),
        Index("ix_records_vendor_postcode", "vendor_id", "postCode"),
    )

    def __repr__(self) -> str:
        return f"<Record run={self.run_id} vendor={self.vendor_id} {self.sku}@{self.postCode} ${self.price:.6f}>"


class ErrorRow(Base):
    __tablename__ = "errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.run_id", ondelete="CASCADE"), index=True, nullable=False)

    row_no: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    context: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped["Run"] = relationship(back_populates="errors")

    def __repr__(self) -> str:
        return f"<Error run={self.run_id} row={self.row_no} err={self.error[:80]!r}>"


# ------------- Config & Engine Helpers -------------

@dataclass
class DBConfig:
    engine: Literal["sqlite", "postgres", "mysql"] = "sqlite"
    dedupe_scope: Literal["per_run", "global"] = "per_run"  # affects which unique index we create
    dsn: Optional[str] = None

    # sqlite
    sqlite_path: Optional[str] = None

    # postgres
    pg_host: Optional[str] = None
    pg_port: int = 5432
    pg_user: Optional[str] = None
    pg_password: Optional[str] = None
    pg_dbname: Optional[str] = None
    pg_sslmode: Optional[str] = None  # "disable" | "require" | "verify-ca" | "verify-full"

    # mysql
    my_host: Optional[str] = None
    my_port: int = 3306
    my_user: Optional[str] = None
    my_password: Optional[str] = None
    my_dbname: Optional[str] = None
    my_ssl: bool = True


def build_dsn(cfg: DBConfig) -> str:
    if cfg.dsn:
        return cfg.dsn

    if cfg.engine == "sqlite":
        path = cfg.sqlite_path or "export_runs.sqlite"
        return f"sqlite:///{path}"

    if cfg.engine == "postgres":
        user = cfg.pg_user or "user"
        pw = cfg.pg_password or "pass"
        host = cfg.pg_host or "localhost"
        port = cfg.pg_port or 5432
        db = cfg.pg_dbname or "appdb"
        # psycopg recommended driver in SA 2.x
        return f"postgresql+psycopg://{user}:{pw}@{host}:{port}/{db}"

    if cfg.engine == "mysql":
        user = cfg.my_user or "user"
        pw = cfg.my_password or "pass"
        host = cfg.my_host or "localhost"
        port = cfg.my_port or 3306
        db = cfg.my_dbname or "appdb"
        # pymysql is commonly available
        return f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"

    raise ValueError(f"Unsupported engine: {cfg.engine}")


def create_engine_from_config(cfg: DBConfig, *, echo: bool = False) -> Engine:
    dsn = build_dsn(cfg)
    connect_args: dict[str, Any] = {}

    if cfg.engine == "sqlite":
        # WAL mode and better concurrency can be enabled via PRAGMAs after connect
        connect_args["check_same_thread"] = False

    engine = create_engine(dsn, echo=echo, pool_pre_ping=True, future=True, connect_args=connect_args)

    if cfg.engine == "sqlite":
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.close()

    return engine


def create_schema(engine: Engine, *, dedupe_scope: Literal["per_run", "global"] = "per_run") -> None:
    """
    Create tables, then apply the appropriate unique index for de-duplication.
    - per_run: UNIQUE (run_id, vendor_id, sku, postCode)
    - global : UNIQUE (vendor_id, sku, postCode)
    """
    Base.metadata.create_all(engine)

    # Build the desired unique index object
    if dedupe_scope == "per_run":
        idx = Index(
            "ux_records_per_run", Record.run_id, Record.vendor_id, Record.sku, Record.postCode, unique=True
        )
    elif dedupe_scope == "global":
        idx = Index(
            "ux_records_global", Record.vendor_id, Record.sku, Record.postCode, unique=True
        )
    else:
        raise ValueError("dedupe_scope must be 'per_run' or 'global'")

    # Create only the wanted unique index
    idx.create(bind=engine, checkfirst=True)


# ------------- Insert/Upsert Utilities -------------

def insert_run(
    session: Session,
    *,
    created_at: Optional[datetime] = None,
    source_name: str,
    vendor_id: Optional[str],
    stats: Mapping[str, Any],
) -> Run:
    run = Run(
        created_at=created_at or datetime.now(),
        source_name=source_name,
        vendor_id=vendor_id or None,
        rows_total=int(stats.get("rows_total", 0)),
        rows_valid=int(stats.get("rows_valid", 0)),
        rows_invalid=int(stats.get("rows_invalid", 0)),
        duplicates=int(stats.get("duplicates", 0)),
        unique_skus=int(stats.get("unique_skus", 0)),
        price_min=stats.get("price_min"),
        price_max=stats.get("price_max"),
        price_avg=stats.get("price_avg"),
        warnings=int(stats.get("warnings", 0)),
    )
    session.add(run)
    session.flush()  # to get run_id
    return run


def insert_records(
    session: Session,
    run_id: int,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: Literal["ignore", "raise"] = "ignore",
) -> int:
    """
    Insert validated records. Each row must include vendor_id, sku, postCode, price.
    on_conflict:
      - "ignore": skip duplicates (recommended with unique index)
      - "raise" : let IntegrityError bubble up
    Returns number of successfully inserted rows (best-effort).
    """
    count = 0
    for r in rows:
        obj = Record(
            run_id=run_id,
            vendor_id=str(r["vendor_id"]),
            sku=str(r["sku"]),
            postCode=str(r["postCode"]),
            price=float(r["price"]),
        )
        session.add(obj)
        try:
            session.flush()
            count += 1
        except IntegrityError:
            session.rollback()
            if on_conflict == "raise":
                raise
            # ignore duplicate
    return count


def insert_errors(
    session: Session,
    run_id: int,
    errors: Iterable[Mapping[str, Any]],
) -> int:
    """
    Insert validation errors (row, context, error).
    """
    count = 0
    for e in errors:
        obj = ErrorRow(
            run_id=run_id,
            row_no=int(e.get("row") or 0),
            context=str(e.get("context") or ""),
            error=str(e.get("error") or ""),
        )
        session.add(obj)
        count += 1
    session.flush()
    return count


# ------------- Example wiring -------------

if __name__ == "__main__":
    # Example config (SQLite)
    cfg = DBConfig(
        engine="sqlite",
        sqlite_path="export_runs.sqlite",
        dedupe_scope="global",  # or "global"
    )

    engine = create_engine_from_config(cfg, echo=False)
    create_schema(engine, dedupe_scope=cfg.dedupe_scope)

    # Example usage
    with Session(engine) as s:
        # Create a run
        stats = {
            "rows_total": 5,
            "rows_valid": 5,
            "rows_invalid": 0,
            "duplicates": 0,
            "unique_skus": 1,
            "price_min": 1.0,
            "price_max": 1.0,
            "price_avg": 1.0,
            "warnings": 0,
        }
        run = insert_run(
            s,
            source_name="demo.csv",
            vendor_id="vendor_001",
            stats=stats,
        )

        # Insert records (dedupe handled by unique index)
        rows = [
            {"vendor_id": "vendor_001", "sku": "SKU126", "postCode": "2500", "price": 1.0},
            {"vendor_id": "vendor_001", "sku": "SKU126", "postCode": "2502", "price": 1.0},
        ]
        inserted = insert_records(s, run.run_id, rows, on_conflict="ignore")

        # Insert errors (none here)
        insert_errors(s, run.run_id, [])

        s.commit()

        # Fetch to verify
        all_runs = s.scalars(select(Run).order_by(Run.run_id.desc())).all()
        print(all_runs)


# ===============


# db_admin_gui.py


# db_admin_gui.py
# Standalone TTKBootstrap GUI for ALL database concerns:
# - Connection profiles (SQLite/Postgres/MySQL) + DSN
# - Dedupe scope toggle (per_run / global)
# - Create/verify application schema (delegates to models.create_schema)
# - Auth schema management (users table): create, list, add, reset password, deactivate/reactivate, delete
# - Inspect tables
#
# NOTE:
#   - This app intentionally encapsulates *all* DB code.
#   - Your main data-converter app should no longer touch the DB directly.
#   - Requires `models.py` (from earlier step) in the same folder.
#
# External deps: sqlalchemy, ttkbootstrap, (optional) bcrypt
# If bcrypt is not installed, falls back to PBKDF2-HMAC (SHA256).

from __future__ import annotations

import os
import json
import platform
import secrets
import string
from dataclasses import asdict
from datetime import datetime
from typing import Any, Optional

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS, WARNING, DANGER

from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

# ---- Import shared DB helpers / app schema (runs, records, errors) from models.py ----
from models import (
    DBConfig,
    create_engine_from_config,
    create_schema,  # creates the application's tables (runs, records, errors) + unique index per dedupe scope
)

# =========================
# Password hashing helpers
# =========================

def _bcrypt_available() -> bool:
    try:
        import bcrypt  # type: ignore
        _ = bcrypt.__version__
        return True
    except Exception:
        return False

def hash_password(password: str) -> str:
    """
    Returns an encoded hash string:
      - If bcrypt is available: 'bcrypt$<hash>'
      - Else PBKDF2-HMAC-SHA256: 'pbkdf2$<salt_hex>$<hash_hex>'
    """
    if _bcrypt_available():
        import bcrypt  # type: ignore
        salt = bcrypt.gensalt(rounds=12)
        h = bcrypt.hashpw(password.encode("utf-8"), salt)
        return f"bcrypt${h.decode('utf-8')}"
    else:
        import hashlib, os, binascii
        salt = os.urandom(16)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
        return f"pbkdf2${binascii.hexlify(salt).decode()}${binascii.hexlify(dk).decode()}"

def verify_password(password: str, encoded: str) -> bool:
    try:
        if encoded.startswith("bcrypt$"):
            if not _bcrypt_available():
                return False
            import bcrypt  # type: ignore
            h = encoded.split("$", 1)[1].encode("utf-8")
            return bcrypt.checkpw(password.encode("utf-8"), h)
        if encoded.startswith("pbkdf2$"):
            import hashlib, binascii
            _, salt_hex, hash_hex = encoded.split("$", 2)
            salt = binascii.unhexlify(salt_hex)
            expected = binascii.unhexlify(hash_hex)
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
            return secrets.compare_digest(dk, expected)
    except Exception:
        return False
    return False

def random_password(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))

# =========================
# Auth schema DDL (minimal)
# =========================

AUTH_USERS_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username VARCHAR(128) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role VARCHAR(64) NOT NULL DEFAULT 'admin',
    is_active BOOLEAN NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL
);
"""

# PostgreSQL/MySQL variant (autoincrement style) — executed conditionally if needed
AUTH_USERS_CREATE_PG = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(128) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role VARCHAR(64) NOT NULL DEFAULT 'admin',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL
);
"""

AUTH_USERS_CREATE_MY = """
CREATE TABLE IF NOT EXISTS users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(128) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role VARCHAR(64) NOT NULL DEFAULT 'admin',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def ensure_auth_schema(engine: Engine, engine_kind: str) -> None:
    ddl = AUTH_USERS_CREATE
    if engine_kind == "postgres":
        ddl = AUTH_USERS_CREATE_PG
    elif engine_kind == "mysql":
        ddl = AUTH_USERS_CREATE_MY
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

# =========================
# Connection profile storage
# =========================

PROFILE_PATH = os.path.join(os.path.expanduser("~"), ".csvjson_app", "db_profiles.json")

def _ensure_profile_dir():
    d = os.path.dirname(PROFILE_PATH)
    os.makedirs(d, exist_ok=True)

def load_profiles() -> dict[str, Any]:
    _ensure_profile_dir()
    if not os.path.exists(PROFILE_PATH):
        return {}
    try:
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_profiles(profiles: dict[str, Any]) -> None:
    _ensure_profile_dir()
    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)

# =========================
# GUI
# =========================

class DBAdminApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Database Admin • CSV/JSON Converter")
        self.root.geometry("1080x780")
        self.root.resizable(True, True)
        self.style = Style(theme="darkly")

        # Current engine + cfg
        self.engine: Optional[Engine] = None
        self.cfg = DBConfig(engine="sqlite", dedupe_scope="per_run", sqlite_path=os.path.abspath("export_runs.sqlite"))
        self.connected_label_var = tk.StringVar(value="Not connected")

        # Connection UI state
        self.db_engine_var = tk.StringVar(value="sqlite")        # sqlite|postgres|mysql
        self.db_dedupe_var = tk.StringVar(value="per_run")       # per_run|global
        self.db_dsn_var = tk.StringVar(value="")                 # optional DSN

        self.sqlite_path_var = tk.StringVar(value=self.cfg.sqlite_path or os.path.abspath("export_runs.sqlite"))

        self.pg_host_var = tk.StringVar(value="localhost")
        self.pg_port_var = tk.IntVar(value=5432)
        self.pg_user_var = tk.StringVar(value="")
        self.pg_pass_var = tk.StringVar(value="")
        self.pg_dbname_var = tk.StringVar(value="")
        self.pg_sslmode_var = tk.StringVar(value="require")

        self.my_host_var = tk.StringVar(value="localhost")
        self.my_port_var = tk.IntVar(value=3306)
        self.my_user_var = tk.StringVar(value="")
        self.my_pass_var = tk.StringVar(value="")
        self.my_dbname_var = tk.StringVar(value="")
        self.my_ssl_var = tk.BooleanVar(value=True)

        # Profiles
        self.profiles = load_profiles()
        self.profile_name_var = tk.StringVar(value="default")

        self._build_ui()

    # ---------- UI ----------
    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        nb = ttk.Notebook(self.root)
        nb.grid(row=0, column=0, sticky="nsew")

        self.tab_conn = ttk.Frame(nb, padding=12)
        nb.add(self.tab_conn, text="Connection")
        self._tab_connection(self.tab_conn)

        self.tab_schema = ttk.Frame(nb, padding=12)
        nb.add(self.tab_schema, text="Schema")
        self._tab_schema(self.tab_schema)

        self.tab_auth = ttk.Frame(nb, padding=12)
        nb.add(self.tab_auth, text="Auth")
        self._tab_auth(self.tab_auth)

        self.tab_tools = ttk.Frame(nb, padding=12)
        nb.add(self.tab_tools, text="Tools")
        self._tab_tools(self.tab_tools)

    def _tab_connection(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(1, weight=1)

        top = ttk.LabelFrame(parent, text="Connection Profile", padding=10)
        top.grid(row=0, column=0, columnspan=2, sticky="ew")
        ttk.Label(top, text="Profile Name:").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.profile_name_var, width=20).grid(row=0, column=1, sticky="w", padx=(8, 8))
        ttk.Button(top, text="Load", command=self._profile_load).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(top, text="Save", command=self._profile_save, bootstyle=SUCCESS).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(top, text="Delete", command=self._profile_delete, bootstyle=DANGER).grid(row=0, column=4, padx=(8, 0))

        lf = ttk.LabelFrame(parent, text="Engine & DSN", padding=10)
        lf.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        lf.columnconfigure(1, weight=1)
        ttk.Label(lf, text="Engine:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(lf, state="readonly", width=12, textvariable=self.db_engine_var,
                     values=["sqlite", "postgres", "mysql"]).grid(row=0, column=1, sticky="w")
        ttk.Label(lf, text="De-dup Scope:").grid(row=0, column=2, sticky="e")
        ttk.Combobox(lf, state="readonly", width=10, textvariable=self.db_dedupe_var,
                     values=["per_run", "global"]).grid(row=0, column=3, sticky="w")
        ttk.Label(lf, text="DSN (optional, overrides structured fields)").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(lf, textvariable=self.db_dsn_var).grid(row=1, column=1, columnspan=3, sticky="ew", pady=(8, 0))

        sl = ttk.LabelFrame(parent, text="SQLite", padding=10)
        sl.grid(row=2, column=0, columnspan=2, sticky="ew")
        sl.columnconfigure(1, weight=1)
        ttk.Label(sl, text="Path:").grid(row=0, column=0, sticky="w")
        ttk.Entry(sl, textvariable=self.sqlite_path_var).grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(sl, text="Browse", command=self._choose_sqlite_path).grid(row=0, column=2, sticky="w")

        pg = ttk.LabelFrame(parent, text="PostgreSQL", padding=10)
        pg.grid(row=3, column=0, columnspan=2, sticky="ew")
        for c in range(6): pg.columnconfigure(c, weight=1 if c == 1 else 0)
        ttk.Label(pg, text="Host").grid(row=0, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_host_var, width=18).grid(row=0, column=1, sticky="w")
        ttk.Label(pg, text="Port").grid(row=0, column=2, sticky="e"); ttk.Entry(pg, textvariable=self.pg_port_var, width=8).grid(row=0, column=3, sticky="w")
        ttk.Label(pg, text="User").grid(row=1, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_user_var, width=18).grid(row=1, column=1, sticky="w")
        ttk.Label(pg, text="Pass").grid(row=1, column=2, sticky="e"); ttk.Entry(pg, textvariable=self.pg_pass_var, show="*", width=18).grid(row=1, column=3, sticky="w")
        ttk.Label(pg, text="DB").grid(row=2, column=0, sticky="w"); ttk.Entry(pg, textvariable=self.pg_dbname_var, width=18).grid(row=2, column=1, sticky="w")
        ttk.Label(pg, text="SSL").grid(row=2, column=2, sticky="e"); ttk.Combobox(pg, state="readonly", width=12, textvariable=self.pg_sslmode_var, values=["disable","require","verify-ca","verify-full"]).grid(row=2, column=3, sticky="w")

        my = ttk.LabelFrame(parent, text="MySQL", padding=10)
        my.grid(row=4, column=0, columnspan=2, sticky="ew")
        for c in range(6): my.columnconfigure(c, weight=1 if c == 1 else 0)
        ttk.Label(my, text="Host").grid(row=0, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_host_var, width=18).grid(row=0, column=1, sticky="w")
        ttk.Label(my, text="Port").grid(row=0, column=2, sticky="e"); ttk.Entry(my, textvariable=self.my_port_var, width=8).grid(row=0, column=3, sticky="w")
        ttk.Label(my, text="User").grid(row=1, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_user_var, width=18).grid(row=1, column=1, sticky="w")
        ttk.Label(my, text="Pass").grid(row=1, column=2, sticky="e"); ttk.Entry(my, textvariable=self.my_pass_var, show="*", width=18).grid(row=1, column=3, sticky="w")
        ttk.Label(my, text="DB").grid(row=2, column=0, sticky="w"); ttk.Entry(my, textvariable=self.my_dbname_var, width=18).grid(row=2, column=1, sticky="w")
        ttk.Checkbutton(my, text="SSL", variable=self.my_ssl_var).grid(row=2, column=2, sticky="w")

        actions = ttk.Frame(parent); actions.grid(row=5, column=0, columnspan=2, sticky="w", pady=(12, 0))
        ttk.Button(actions, text="Connect", command=self._connect, bootstyle=SUCCESS).pack(side="left")
        ttk.Button(actions, text="Disconnect", command=self._disconnect, bootstyle=WARNING).pack(side="left", padx=(8, 0))
        ttk.Label(actions, textvariable=self.connected_label_var).pack(side="left", padx=(12, 0))

    def _tab_schema(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        info = ttk.LabelFrame(parent, text="Application Schema", padding=10)
        info.grid(row=0, column=0, sticky="ew")
        ttk.Label(info, text="This creates/ensures the core application tables (runs, records, errors)\n"
                             "and applies the de-dup unique index based on the selected scope.").grid(row=0, column=0, sticky="w")
        ttk.Button(info, text="Create/Ensure App Schema", command=self._ensure_app_schema, bootstyle=PRIMARY).grid(row=0, column=1, padx=(10, 0))

        mid = ttk.LabelFrame(parent, text="Database Objects", padding=10)
        mid.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        mid.columnconfigure(0, weight=1)
        mid.rowconfigure(0, weight=1)
        self.tables_list = tk.Listbox(mid, height=16)
        self.tables_list.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(mid, orient="vertical", command=self.tables_list.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self.tables_list.configure(yscrollcommand=sy.set)
        ttk.Button(mid, text="Refresh", command=self._refresh_tables, bootstyle=INFO).grid(row=1, column=0, sticky="w", pady=(8, 0))

    def _tab_auth(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)

        top = ttk.Frame(parent); top.grid(row=0, column=0, sticky="ew")
        ttk.Button(top, text="Ensure Auth Schema (users)", command=self._ensure_auth_schema, bootstyle=PRIMARY).pack(side="left")
        ttk.Button(top, text="Refresh Users", command=self._refresh_users, bootstyle=INFO).pack(side="left", padx=(8, 0))

        form = ttk.LabelFrame(parent, text="User Management", padding=10)
        form.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        for c in range(6): form.columnconfigure(c, weight=1 if c in (1, 3) else 0)

        self.user_username_var = tk.StringVar(value="")
        self.user_role_var = tk.StringVar(value="admin")
        self.user_password_var = tk.StringVar(value="")
        self.user_active_var = tk.BooleanVar(value=True)

        ttk.Label(form, text="Username").grid(row=0, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.user_username_var, width=18).grid(row=0, column=1, sticky="w")
        ttk.Label(form, text="Role").grid(row=0, column=2, sticky="e")
        ttk.Combobox(form, state="readonly", width=12, textvariable=self.user_role_var, values=["admin","editor","viewer"]).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="Password").grid(row=1, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.user_password_var, show="*", width=18).grid(row=1, column=1, sticky="w")
        ttk.Checkbutton(form, text="Active", variable=self.user_active_var).grid(row=1, column=2, sticky="w")

        btns = ttk.Frame(form); btns.grid(row=2, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Add/Upsert User", command=self._add_user, bootstyle=SUCCESS).pack(side="left")
        ttk.Button(btns, text="Random Password", command=self._random_password, bootstyle=WARNING).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Reset Password", command=self._reset_password, bootstyle=WARNING).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Deactivate/Activate", command=self._toggle_active, bootstyle=INFO).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Delete User", command=self._delete_user, bootstyle=DANGER).pack(side="left", padx=(8, 0))

        # Users table
        table = ttk.LabelFrame(parent, text="Users", padding=10)
        table.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        table.columnconfigure(0, weight=1)
        table.rowconfigure(0, weight=1)
        cols = ("id","username","role","is_active","created_at")
        self.users_tree = ttk.Treeview(table, columns=cols, show="headings", height=12)
        for c in cols:
            self.users_tree.heading(c, text=c.replace("_"," ").title())
            self.users_tree.column(c, width=140 if c != "id" else 80, anchor="center")
        self.users_tree.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(table, orient="vertical", command=self.users_tree.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self.users_tree.configure(yscrollcommand=sy.set)

    def _tab_tools(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        info = ttk.LabelFrame(parent, text="Utilities", padding=10)
        info.grid(row=0, column=0, sticky="ew")
        ttk.Button(info, text="Export Connection Profile to JSON", command=self._export_profile_json).pack(side="left")
        ttk.Button(info, text="Open Profile Folder", command=self._open_profile_folder).pack(side="left", padx=(8, 0))

        out = ttk.LabelFrame(parent, text="Inspector Output", padding=10)
        out.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        out.columnconfigure(0, weight=1)
        out.rowconfigure(0, weight=1)
        self.out_box = tk.Text(out, wrap="word")
        self.out_box.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(out, orient="vertical", command=self.out_box.yview)
        sy.grid(row=0, column=1, sticky="ns")
        self.out_box.configure(yscrollcommand=sy.set)
        ttk.Button(out, text="Inspect Current Connection", command=self._inspect, bootstyle=INFO).grid(row=1, column=0, sticky="w", pady=(8, 0))

    # ---------- Connection Logic ----------

    def _collect_cfg(self) -> DBConfig:
        return DBConfig(
            engine=self.db_engine_var.get(),
            dedupe_scope=self.db_dedupe_var.get(),
            dsn=self.db_dsn_var.get().strip() or None,
            sqlite_path=self.sqlite_path_var.get().strip() or None,
            pg_host=self.pg_host_var.get().strip() or None,
            pg_port=int(self.pg_port_var.get() or 5432),
            pg_user=self.pg_user_var.get().strip() or None,
            pg_password=self.pg_pass_var.get().strip() or None,
            pg_dbname=self.pg_dbname_var.get().strip() or None,
            pg_sslmode=self.pg_sslmode_var.get().strip() or None,
            my_host=self.my_host_var.get().strip() or None,
            my_port=int(self.my_port_var.get() or 3306),
            my_user=self.my_user_var.get().strip() or None,
            my_password=self.my_pass_var.get().strip() or None,
            my_dbname=self.my_dbname_var.get().strip() or None,
            my_ssl=bool(self.my_ssl_var.get()),
        )

    def _connect(self) -> None:
        try:
            self.cfg = self._collect_cfg()
            self.engine = create_engine_from_config(self.cfg, echo=False)
            # simple probe
            with self.engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            self.connected_label_var.set(f"Connected: {self.cfg.engine}")
            messagebox.showinfo("DB", "Connected OK.")
        except Exception as e:
            self.engine = None
            self.connected_label_var.set("Not connected")
            messagebox.showerror("DB", f"Connection failed:\n{e}")

    def _disconnect(self) -> None:
        self.engine = None
        self.connected_label_var.set("Not connected")
        messagebox.showinfo("DB", "Disconnected.")

    def _choose_sqlite_path(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".sqlite",
            filetypes=[("SQLite DB", "*.sqlite *.db")],
            initialfile=os.path.basename(self.sqlite_path_var.get() or "export_runs.sqlite"),
        )
        if path:
            self.sqlite_path_var.set(path)

    # ---------- Profiles ----------

    def _profile_save(self) -> None:
        name = self.profile_name_var.get().strip() or "default"
        self.profiles[name] = {
            "engine": self.db_engine_var.get(),
            "dedupe": self.db_dedupe_var.get(),
            "dsn": self.db_dsn_var.get(),
            "sqlite_path": self.sqlite_path_var.get(),
            "pg": {
                "host": self.pg_host_var.get(),
                "port": int(self.pg_port_var.get() or 5432),
                "user": self.pg_user_var.get(),
                "password": self.pg_pass_var.get(),
                "dbname": self.pg_dbname_var.get(),
                "sslmode": self.pg_sslmode_var.get(),
            },
            "mysql": {
                "host": self.my_host_var.get(),
                "port": int(self.my_port_var.get() or 3306),
                "user": self.my_user_var.get(),
                "password": self.my_pass_var.get(),
                "dbname": self.my_dbname_var.get(),
                "ssl": bool(self.my_ssl_var.get()),
            },
        }
        save_profiles(self.profiles)
        messagebox.showinfo("Profiles", f"Saved profile '{name}'.")

    def _profile_load(self) -> None:
        name = self.profile_name_var.get().strip() or "default"
        p = self.profiles.get(name)
        if not p:
            messagebox.showwarning("Profiles", f"No profile named '{name}'.")
            return
        self.db_engine_var.set(p.get("engine", "sqlite"))
        self.db_dedupe_var.set(p.get("dedupe", "per_run"))
        self.db_dsn_var.set(p.get("dsn", ""))
        self.sqlite_path_var.set(p.get("sqlite_path", self.sqlite_path_var.get()))
        pg = p.get("pg", {})
        self.pg_host_var.set(pg.get("host", "localhost"))
        self.pg_port_var.set(pg.get("port", 5432))
        self.pg_user_var.set(pg.get("user", ""))
        self.pg_pass_var.set(pg.get("password", ""))
        self.pg_dbname_var.set(pg.get("dbname", ""))
        self.pg_sslmode_var.set(pg.get("sslmode", "require"))
        my = p.get("mysql", {})
        self.my_host_var.set(my.get("host", "localhost"))
        self.my_port_var.set(my.get("port", 3306))
        self.my_user_var.set(my.get("user", ""))
        self.my_pass_var.set(my.get("password", ""))
        self.my_dbname_var.set(my.get("dbname", ""))
        self.my_ssl_var.set(my.get("ssl", True))
        messagebox.showinfo("Profiles", f"Loaded profile '{name}'.")

    def _profile_delete(self) -> None:
        name = self.profile_name_var.get().strip() or "default"
        if name in self.profiles:
            del self.profiles[name]
            save_profiles(self.profiles)
            messagebox.showinfo("Profiles", f"Deleted profile '{name}'.")
        else:
            messagebox.showwarning("Profiles", f"No profile named '{name}'.")

    # ---------- Schema ----------

    def _ensure_app_schema(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        try:
            create_schema(self.engine, dedupe_scope=self.db_dedupe_var.get())
            messagebox.showinfo("Schema", "Application schema ensured.")
            self._refresh_tables()
        except Exception as e:
            messagebox.showerror("Schema", f"Failed to ensure schema:\n{e}")

    def _refresh_tables(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        try:
            self.tables_list.delete(0, "end")
            insp = inspect(self.engine)
            for tbl in sorted(insp.get_table_names()):
                self.tables_list.insert("end", tbl)
        except Exception as e:
            messagebox.showerror("Inspector", f"Error listing tables:\n{e}")

    # ---------- Auth ----------

    def _ensure_auth_schema(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        try:
            ensure_auth_schema(self.engine, self.db_engine_var.get())
            messagebox.showinfo("Auth", "Auth schema (users) ensured.")
            self._refresh_users()
            self._refresh_tables()
        except Exception as e:
            messagebox.showerror("Auth", f"Failed to ensure auth schema:\n{e}")

    def _refresh_users(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        try:
            self.users_tree.delete(*self.users_tree.get_children())
            with self.engine.begin() as conn:
                res = conn.exec_driver_sql("SELECT id, username, role, is_active, created_at FROM users ORDER BY id DESC")
                for row in res:
                    created = row[4]
                    if hasattr(created, "isoformat"):
                        created = created.isoformat(sep=" ", timespec="seconds")
                    self.users_tree.insert("", "end", values=(row[0], row[1], row[2], row[3], str(created)))
        except SQLAlchemyError as e:
            messagebox.showerror("Auth", f"Failed to query users:\n{e}")

    def _selected_user(self) -> Optional[dict]:
        sel = self.users_tree.selection()
        if not sel:
            return None
        vals = self.users_tree.item(sel[0], "values")
        try:
            return {
                "id": int(vals[0]),
                "username": vals[1],
                "role": vals[2],
                "is_active": bool(int(vals[3])) if isinstance(vals[3], str) and vals[3].isdigit() else (vals[3] in (True, "True", "true", 1, "1")),
                "created_at": vals[4],
            }
        except Exception:
            return None

    def _add_user(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        username = self.user_username_var.get().strip()
        password = self.user_password_var.get()
        if not username:
            messagebox.showwarning("Auth", "Username required.")
            return
        if not password:
            password = random_password()
            self.user_password_var.set(password)
            messagebox.showinfo("Auth", f"No password provided. Generated: {password}")

        role = (self.user_role_var.get() or "admin").strip()
        is_active = 1 if self.user_active_var.get() else 0
        ph = hash_password(password)
        try:
            with self.engine.begin() as conn:
                # Upsert-like behavior: try update first, else insert
                # Compatible across engines by two-step approach.
                res = conn.exec_driver_sql("SELECT id FROM users WHERE username = :u", {"u": username}).fetchone()
                if res:
                    conn.exec_driver_sql(
                        "UPDATE users SET password_hash=:p, role=:r, is_active=:a WHERE id=:id",
                        {"p": ph, "r": role, "a": is_active, "id": res[0]},
                    )
                else:
                    conn.exec_driver_sql(
                        "INSERT INTO users (username, password_hash, role, is_active, created_at) "
                        "VALUES (:u, :p, :r, :a, :ts)",
                        {"u": username, "p": ph, "r": role, "a": is_active, "ts": datetime.now()},
                    )
            messagebox.showinfo("Auth", "User upserted.")
            self._refresh_users()
        except SQLAlchemyError as e:
            messagebox.showerror("Auth", f"Failed to upsert user:\n{e}")

    def _reset_password(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        sel = self._selected_user()
        if not sel:
            messagebox.showwarning("Auth", "Select a user row.")
            return
        password = self.user_password_var.get().strip() or random_password()
        self.user_password_var.set(password)
        ph = hash_password(password)
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql("UPDATE users SET password_hash=:p WHERE id=:id", {"p": ph, "id": sel["id"]})
            messagebox.showinfo("Auth", f"Password reset. New password: {password}")
            self._refresh_users()
        except SQLAlchemyError as e:
            messagebox.showerror("Auth", f"Failed to reset password:\n{e}")

    def _toggle_active(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        sel = self._selected_user()
        if not sel:
            messagebox.showwarning("Auth", "Select a user row.")
            return
        new_val = 0 if sel["is_active"] else 1
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql("UPDATE users SET is_active=:a WHERE id=:id", {"a": new_val, "id": sel["id"]})
            messagebox.showinfo("Auth", f"User {'activated' if new_val else 'deactivated'}.")
            self._refresh_users()
        except SQLAlchemyError as e:
            messagebox.showerror("Auth", f"Failed to toggle active:\n{e}")

    def _delete_user(self) -> None:
        if not self.engine:
            messagebox.showwarning("DB", "Not connected.")
            return
        sel = self._selected_user()
        if not sel:
            messagebox.showwarning("Auth", "Select a user row.")
            return
        if not messagebox.askyesno("Confirm", f"Delete user '{sel['username']}'?"):
            return
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql("DELETE FROM users WHERE id=:id", {"id": sel["id"]})
            messagebox.showinfo("Auth", "User deleted.")
            self._refresh_users()
        except SQLAlchemyError as e:
            messagebox.showerror("Auth", f"Failed to delete user:\n{e}")

    def _random_password(self) -> None:
        self.user_password_var.set(random_password())

    # ---------- Tools ----------

    def _inspect(self) -> None:
        self.out_box.delete("1.0", "end")
        if not self.engine:
            self.out_box.insert("end", "Not connected.\n")
            return
        try:
            insp = inspect(self.engine)
            lines = [f"Engine: {self.db_engine_var.get()}",
                     f"Tables: {', '.join(sorted(insp.get_table_names())) or '(none)'}",
                     ""]
            for t in sorted(insp.get_table_names()):
                cols = insp.get_columns(t)
                lines.append(f"[{t}]")
                for c in cols:
                    lines.append(f"  - {c['name']}: {c.get('type')} (nullable={c.get('nullable')})")
                lines.append("")
            self.out_box.insert("end", "\n".join(lines))
        except Exception as e:
            self.out_box.insert("end", f"Inspector error: {e}\n")

    def _export_profile_json(self) -> None:
        prof = self.profile_name_var.get().strip() or "default"
        p = self.profiles.get(prof)
        if not p:
            messagebox.showwarning("Profiles", f"Profile '{prof}' not found. Save it first.")
            return
        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON", "*.json")],
                                            initialfile=f"{prof}.json")
        if not path:
            return
        with open(path, "w", encoding="utf-8") as f:
            json.dump(p, f, indent=2)
        messagebox.showinfo("Profiles", f"Exported profile to:\n{path}")

    def _open_profile_folder(self) -> None:
        folder = os.path.dirname(PROFILE_PATH)
        os.makedirs(folder, exist_ok=True)
        try:
            sys = platform.system()
            if sys == "Windows":
                os.startfile(folder)
            elif sys == "Darwin":
                import subprocess; subprocess.Popen(["open", folder])
            else:
                import subprocess; subprocess.Popen(["xdg-open", folder])
        except Exception:
            messagebox.showwarning("Open", f"Folder: {folder}")

# -------------------- Run --------------------
if __name__ == "__main__":
    root = tk.Tk()
    DBAdminApp(root)
    root.mainloop()

