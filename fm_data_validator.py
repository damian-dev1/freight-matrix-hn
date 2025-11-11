from __future__ import annotations
import os, csv, json, math, platform, subprocess
from collections import defaultdict
from statistics import mean
from datetime import datetime
from typing import Any, Mapping, Optional
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, sessionmaker
from sqlalchemy import select, String, Float, Integer, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy import create_engine, String, Float, Integer, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects import mysql as my_dialect

Base = declarative_base()

class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(64))
    vendor_id: Mapped[str] = mapped_column(String(128))
    source_name: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
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
    run_id: Mapped[int | None] = mapped_column(ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True)

    __table_args__ = (
        UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),
    )

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
        "mode": "rows",
        "rows_per_file": 80000,
        "group_column": "postcode"
    },
    "db": {
        "engine": "sqlite",
        "dedupe_scope": "global",
        "dsn": "",
        "sqlite_path": os.path.abspath("export_runs.sqlite"),
        "pg": {"host": "localhost", "port": 5432, "user": "", "password": "", "dbname": "", "sslmode": "require"},
        "mysql": {"host": "localhost", "port": 3306, "user": "", "password": "", "dbname": "", "ssl": True},
        "profile_name": "default",
        "echo": False,
        "create_tables": True
    }
}

CSV_FIELD_ALIASES = {
    "sku": ["sku", "productcode", "productCode", "product_code", "product id", "productid"],
    "postCode": ["postcode", "postCode", "post_code", "post code", "zip", "zip_code"],
    "price": ["price", "unit_price", "unitPrice", "unitprice", "unit price", "amount"],
}

def _ensure_app_dir(): os.makedirs(APP_DIR, exist_ok=True)
def load_settings() -> dict:
    _ensure_app_dir()
    if not os.path.exists(CONFIG_PATH): return json.loads(json.dumps(DEFAULT_SETTINGS))
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f: data = json.load(f)
        def deep_merge(d, default):
            for k, v in default.items():
                if k not in d: d[k] = v
                elif isinstance(v, dict) and isinstance(d[k], dict): deep_merge(d[k], v)
            return d
        return deep_merge(data, json.loads(json.dumps(DEFAULT_SETTINGS)))
    except Exception:
        return json.loads(json.dumps(DEFAULT_SETTINGS))
def save_settings(cfg: dict) -> None:
    _ensure_app_dir()
    with open(CONFIG_PATH, "w", encoding="utf-8") as f: json.dump(cfg, f, indent=2)

def normalize_str(v: Any) -> str:
    if v is None: return ""
    if isinstance(v, (int, float)): return str(v).strip()
    return str(v).strip().strip('"').strip()
def _lower_keys(d: Mapping[str, Any]) -> dict[str, Any]: return {(k or "").strip().lower(): v for k, v in d.items()}
def field_from_row(row: Mapping[str, Any], key: str) -> Any:
    for alias in CSV_FIELD_ALIASES.get(key, []):
        a = alias.lower()
        if a in row: return row.get(a)
    return None
def is_valid_sku(s: str) -> tuple[bool, str]:
    s = normalize_str(s)
    if not s: return False, "sku empty"
    for ch in s:
        if not (ch.isalnum() or ch in "-_./"): return False, "sku has invalid characters"
    if len(s) > 64: return False, "sku too long"
    return True, ""
def is_valid_postcode(pc: str) -> tuple[bool, str]:
    pc = normalize_str(pc)
    if not pc: return False, "postCode empty"
    if not pc.isdigit() or len(pc) != 4: return False, "postCode must be 4 digits"
    return True, ""
def normalize_price(p: str) -> tuple[bool, float, str]:
    s = normalize_str(p)
    if s == "": return False, 0.0, "price empty"
    clean = s.replace(",", "")
    for sym in "$€£AUDaud ": clean = clean.replace(sym, "")
    try: val = float(clean)
    except Exception: return False, 0.0, "price not a number"
    if val < 0: return False, 0.0, "price negative"
    if math.isinf(val) or math.isnan(val): return False, 0.0, "price not finite"
    return True, round(val, 2), ""
def build_doc(raw_sku: str, raw_pc: str, price_val: float) -> dict[str, Any]:
    return {"sku": normalize_str(raw_sku), "postCode": normalize_str(raw_pc), "price": float(price_val)}
def _validate_from_reader(reader: csv.DictReader) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs, errors, warnings, seen_ids = [], [], [], set()
    raw_fieldnames = reader.fieldnames or []
    fieldnames_lc = [(h or "").strip().lower() for h in raw_fieldnames]
    if not fieldnames_lc:
        errors.append({"row": 1, "context": "header", "error": "Missing header row"})
        return valid_docs, errors, warnings
    missing_min = []
    for key in ["sku", "postCode", "price"]:
        if not any(alias.lower() in fieldnames_lc for alias in CSV_FIELD_ALIASES[key]): missing_min.append(key)
    if missing_min:
        errors.append({"row": 1, "context": "header", "error": f"Missing required columns: {', '.join(missing_min)}"})
        return valid_docs, errors, warnings
    for idx, row in enumerate(reader, start=2):
        row = _lower_keys(row)
        raw_sku = normalize_str(field_from_row(row, "sku"))
        raw_pc = normalize_str(field_from_row(row, "postCode"))
        raw_price = normalize_str(field_from_row(row, "price"))
        if not raw_sku and not raw_pc and not raw_price: continue
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
def validate_csv(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    try:
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return _validate_from_reader(reader)
    except Exception as e:
        return [], [{"row": 0, "context": "file", "error": f"Read error: {e}"}], []
def validate_json(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs, errors, warnings, seen_ids = [], [], [], set()
    def validate_obj(obj: dict, idx_for_report: int) -> None:
        obj_lc = _lower_keys(obj)
        raw_sku = normalize_str(field_from_row(obj_lc, "sku"))
        raw_pc = normalize_str(field_from_row(obj_lc, "postCode"))
        raw_price = normalize_str(field_from_row(obj_lc, "price"))
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
        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            return
        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))
    try:
        with open(file_path, encoding="utf-8") as f: data = json.load(f)
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
def validate_pasted_csv_text(text: str) -> tuple[list[dict], list[dict], list[str]]:
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines: return [], [{"row": 1, "context": "header", "error": "No content"}], []
    reader = csv.DictReader(lines)
    return _validate_from_reader(reader)

def _build_sqlalchemy_url(cfg: dict) -> str:
    dsn = (cfg.get("dsn") or "").strip()
    if dsn: return dsn
    eng = (cfg.get("engine") or "sqlite").lower()
    if eng == "sqlite":
        path = cfg.get("sqlite_path") or os.path.abspath("export_runs.sqlite")
        return path if path.startswith("sqlite:///") else f"sqlite:///{path}"
    if eng == "postgres":
        pg = cfg.get("pg", {})
        user, pwd = pg.get("user",""), pg.get("password","")
        host, port, db = pg.get("host","localhost"), str(pg.get("port",5432)), pg.get("dbname","")
        sslmode = pg.get("sslmode","require")
        auth = f"{user}:{pwd}@" if user or pwd else ""
        return f"postgresql+psycopg2://{auth}{host}:{port}/{db}?sslmode={sslmode}"
    if eng == "mysql":
        my = cfg.get("mysql", {})
        user, pwd = my.get("user",""), my.get("password","")
        host, port, db = my.get("host","localhost"), str(my.get("port",3306)), my.get("dbname","")
        ssl = my.get("ssl", True)
        auth = f"{user}:{pwd}@" if user or pwd else ""
        sslq = "&ssl=true" if ssl else ""
        return f"mysql+pymysql://{auth}{host}:{port}/{db}?charset=utf8mb4{sslq}"
    return "sqlite:///export_runs.sqlite"
def get_engine_from_settings(cfg: dict) -> Engine:
    url = _build_sqlalchemy_url(cfg)
    echo = bool(cfg.get("echo", False))
    return create_engine(url, echo=echo, future=True)
def try_create_tables(engine: Engine, cfg: dict) -> None:
    if cfg.get("create_tables", True): Base.metadata.create_all(engine)
def test_db_connection(engine: Engine) -> tuple[bool, str]:
    try:
        with engine.connect() as conn: conn.exec_driver_sql("SELECT 1")
        return True, "Connection OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
def save_run_and_rows(engine, profile_name, vendor_id, source_name, stats, rows):
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    dialect = engine.dialect.name  # 'sqlite' | 'postgresql' | 'mysql'
    inserted_total = 0

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
        db.flush()

        payload = [
            {
                "vendor_id": vendor_id,
                "sku": r["sku"],
                "postCode": r["postCode"],
                "price": float(r["price"]),
                "run_id": run.id,
            }
            for r in rows
        ]

        chunk_size = 500  # safe for SQLite param limits

        if dialect == "sqlite":
            ins = sqlite_dialect.insert(VendorRecord)
            for i in range(0, len(payload), chunk_size):
                chunk = payload[i:i + chunk_size]
                stmt = ins.values(chunk).on_conflict_do_nothing(
                    index_elements=["vendor_id", "sku", "postCode"]
                )
                res = db.execute(stmt)
                inserted_total += res.rowcount or 0

        elif dialect == "postgresql":
            ins = pg_dialect.insert(VendorRecord)
            for i in range(0, len(payload), chunk_size):
                chunk = payload[i:i + chunk_size]
                stmt = ins.values(chunk).on_conflict_do_nothing(
                    index_elements=["vendor_id", "sku", "postCode"]
                )
                res = db.execute(stmt)
                inserted_total += res.rowcount or 0

        elif dialect == "mysql":
            ins = my_dialect.insert(VendorRecord)
            # Do-nothing equivalent: ON DUPLICATE KEY UPDATE id = id
            for i in range(0, len(payload), chunk_size):
                chunk = payload[i:i + chunk_size]
                stmt = ins.values(chunk).on_duplicate_key_update(id=VendorRecord.id)
                res = db.execute(stmt)
                # MySQL rowcount may include affected-but-not-inserted; best-effort:
                inserted_total += max(res.rowcount or 0, 0)

        else:
            # Fallback: row-by-row with IntegrityError swallow
            for rec in payload:
                try:
                    db.add(VendorRecord(**rec))
                    db.flush()
                    inserted_total += 1
                except IntegrityError:
                    db.rollback()

        db.commit()
        return run.id, inserted_total


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("CSV/JSON → Clean CSV & JSON")
        self.root.geometry("500x360")
        self.root.resizable(True, True)
        self.style = Style(theme="darkly")
        self.settings = load_settings()
        self.file_path: Optional[str] = None
        self.headers: list[str] = []
        self.last_valid_docs: list[dict] = []
        self.last_errors: list[dict] = []
        self.last_warnings: list[str] = []
        self.cached_stats: dict[str, Any] = {}
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
        self._engine: Optional[Engine] = None
        self._ensure_export_dir()
        self._build_ui()

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1); self.root.rowconfigure(0, weight=1)
        nb = ttk.Notebook(self.root); nb.grid(row=0, column=0, sticky="nsew")
        self.tab_source = ttk.Frame(nb, padding=12); self._tab_source(self.tab_source); nb.add(self.tab_source, text="Source")
        self.tab_preview = ttk.Frame(nb, padding=12); self._tab_preview(self.tab_preview); nb.add(self.tab_preview, text="Preview")
        self.tab_settings = ttk.Frame(nb, padding=0); self._tab_settings(self.tab_settings); nb.add(self.tab_settings, text="Settings")

    def _tab_source(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1); parent.rowconfigure(2, weight=1)
        file_fr = ttk.LabelFrame(parent, text="File", padding=10); file_fr.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        file_fr.columnconfigure(1, weight=1)
        ttk.Label(file_fr, text="Selected:").grid(row=0, column=0, sticky="w")
        self.file_label = ttk.Label(file_fr, text="No file selected", anchor="w"); self.file_label.grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(file_fr, text="Select CSV/JSON", command=self.load_file, bootstyle=PRIMARY).grid(row=0, column=2)
        paste_fr = ttk.LabelFrame(parent, text="Paste CSV Content", padding=10); paste_fr.grid(row=1, column=0, sticky="nsew")
        paste_fr.columnconfigure(0, weight=1); paste_fr.rowconfigure(0, weight=1)
        self.paste_box = tk.Text(paste_fr, wrap=tk.NONE, height=8); self.paste_box.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(paste_fr, orient="vertical", command=self.paste_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(paste_fr, orient="horizontal", command=self.paste_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.paste_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)
        ttk.Button(parent, text="Validate & Preview", command=self.preview_data, bootstyle=INFO).grid(row=3, column=0, sticky="w", pady=(10, 0))

    def _tab_preview(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=3); parent.columnconfigure(1, weight=2); parent.rowconfigure(1, weight=1)
        top = ttk.Frame(parent); top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(top, text="Preview shows first 100 validated rows.").pack(side="left")
        ttk.Button(top, text="Export Files", command=self.export_files, bootstyle=SUCCESS).pack(side="right", padx=(8, 0))
        ttk.Button(top, text="Save to DB", command=self._save_to_db_clicked, bootstyle=PRIMARY).pack(side="right", padx=(8, 0))
        pv_fr = ttk.LabelFrame(parent, text="Preview", padding=10); pv_fr.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        pv_fr.columnconfigure(0, weight=1); pv_fr.rowconfigure(0, weight=1)
        self.preview_box = tk.Text(pv_fr, wrap=tk.NONE, state="disabled"); self.preview_box.grid(row=0, column=0, sticky="nsew")
        self.preview_box.tag_configure("good", foreground="#5bd75b"); self.preview_box.tag_configure("head", foreground="#9ecbff")
        sy = ttk.Scrollbar(pv_fr, orient="vertical", command=self.preview_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(pv_fr, orient="horizontal", command=self.preview_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.preview_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)
        st_fr = ttk.LabelFrame(parent, text="Data Quality / Stats", padding=10); st_fr.grid(row=1, column=1, sticky="nsew")
        st_fr.columnconfigure(0, weight=1); st_fr.rowconfigure(0, weight=1)
        self.stats_box = tk.Text(st_fr, height=12, wrap=tk.WORD, state="normal"); self.stats_box.grid(row=0, column=0, sticky="nsew")
        self.stats_box.tag_configure("good", foreground="#5bd75b"); self.stats_box.tag_configure("bad", foreground="#ff6b6b")
        sy2 = ttk.Scrollbar(st_fr, orient="vertical", command=self.stats_box.yview); sy2.grid(row=0, column=1, sticky="ns")
        self.stats_box.config(yscrollcommand=sy2.set)

    def _tab_settings(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        canvas = tk.Canvas(parent, highlightthickness=0); vsb = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scroll = ttk.Frame(canvas, padding=12)
        scroll.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll, anchor="nw"); canvas.configure(yscrollcommand=vsb.set)
        canvas.grid(row=0, column=0, sticky="nsew"); vsb.grid(row=0, column=1, sticky="ns")
        parent.rowconfigure(0, weight=1); parent.columnconfigure(0, weight=1)
        def _on_mousewheel(event):
            if platform.system() == "Darwin": canvas.yview_scroll(int(-1 * (event.delta)), "units")
            else: canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))
        fm = ttk.LabelFrame(scroll, text="Formats", padding=10); fm.grid(row=0, column=0, sticky="ew")
        ttk.Checkbutton(fm, text="Export CSV", variable=self.export_csv_var).pack(side="left")
        ttk.Checkbutton(fm, text="Export JSON", variable=self.export_json_var).pack(side="left", padx=(10, 0))
        vm = ttk.LabelFrame(scroll, text="Vendor", padding=10); vm.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(vm, text="Vendor ID:").grid(row=0, column=0, sticky="w"); ttk.Entry(vm, textvariable=self.vendor_id_var, width=24).grid(row=0, column=1, sticky="w", padx=(8,8))
        out = ttk.LabelFrame(scroll, text="Destination & Naming", padding=10); out.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        out.columnconfigure(1, weight=1)
        ttk.Label(out, text="Output folder:").grid(row=0, column=0, sticky="w")
        self.out_label = ttk.Label(out, text=self.output_folder_var.get(), anchor="w"); self.out_label.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(out, text="Browse", command=self._choose_output_folder).grid(row=0, column=2, sticky="w")
        ttk.Label(out, text="Filename pattern:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.pattern_entry = ttk.Entry(out, textvariable=self.filename_pattern_var); self.pattern_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(out, text="Tokens: {base} {batch} {group} {ts} {ext}").grid(row=2, column=1, sticky="w", pady=(6, 0))
        ttk.Checkbutton(out, text="Open folder after export", variable=self.open_folder_after_var).grid(row=3, column=1, sticky="w", pady=(8, 0))
        lf = ttk.LabelFrame(scroll, text="Batch Export Configuration", padding=10); lf.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        lf.columnconfigure(1, weight=1)
        ttk.Checkbutton(lf, text="Enable batch export", variable=self.enable_batch_var, command=self._toggle_batch_controls).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        ttk.Label(lf, text="Mode:").grid(row=1, column=0, sticky="w")
        row_mode = ttk.Frame(lf); row_mode.grid(row=1, column=1, sticky="w")
        ttk.Radiobutton(row_mode, text="Rows per file", value="rows", variable=self.batch_mode_var, command=self._on_mode_change).pack(side="left")
        ttk.Radiobutton(row_mode, text="Group by column", value="group", variable=self.batch_mode_var, command=self._on_mode_change).pack(side="left", padx=(10, 0))
        ttk.Label(lf, text="Rows per file:").grid(row=2, column=0, sticky="w"); self.rows_entry = ttk.Entry(lf, width=10, textvariable=self.rows_per_file_var); self.rows_entry.grid(row=2, column=1, sticky="w", pady=(0, 4))
        ttk.Label(lf, text="Group column:").grid(row=3, column=0, sticky="w")
        self.group_combo = ttk.Combobox(lf, width=20, state="readonly", textvariable=self.group_column_var, values=["postcode", "productcode", "sku", "state"])
        self.group_combo.grid(row=3, column=1, sticky="w"); self._toggle_batch_controls(); self._on_mode_change()
        dbf = ttk.LabelFrame(scroll, text="Database", padding=10); dbf.grid(row=4, column=0, sticky="ew", pady=(10, 0)); dbf.columnconfigure(1, weight=1)
        ttk.Label(dbf, text="Profile Name").grid(row=0, column=0, sticky="w"); ttk.Entry(dbf, textvariable=self.db_profile_name_var, width=20).grid(row=0, column=1, sticky="w", padx=(8, 8))
        ttk.Button(dbf, text="Test Connection", command=self._test_db_clicked, bootstyle=INFO).grid(row=0, column=2, sticky="w")
        ttk.Label(dbf, text="Engine").grid(row=1, column=0, sticky="w")
        ttk.Combobox(dbf, state="readonly", width=12, textvariable=self.db_engine_var, values=["sqlite", "postgres", "mysql"]).grid(row=1, column=1, sticky="w")
        ttk.Label(dbf, text="De-dup Scope").grid(row=1, column=2, sticky="e", padx=(12, 6))
        ttk.Combobox(dbf, state="readonly", width=10, textvariable=self.db_dedupe_var, values=["per_run", "global"]).grid(row=1, column=3, sticky="w")
        ttk.Label(dbf, text="Echo SQL").grid(row=1, column=4, sticky="e"); ttk.Checkbutton(dbf, variable=self.db_echo_var).grid(row=1, column=5, sticky="w", padx=(4,0))
        ttk.Label(dbf, text="Create Tables").grid(row=1, column=6, sticky="e"); ttk.Checkbutton(dbf, variable=self.db_create_tables_var).grid(row=1, column=7, sticky="w", padx=(4,0))
        ttk.Label(dbf, text="DSN (optional)").grid(row=2, column=0, sticky="w", pady=(6, 0)); ttk.Entry(dbf, textvariable=self.db_dsn_var).grid(row=2, column=1, columnspan=7, sticky="ew", pady=(6, 0))
        sl = ttk.LabelFrame(dbf, text="SQLite", padding=10); sl.grid(row=3, column=0, columnspan=8, sticky="ew", pady=(10, 0)); sl.columnconfigure(1, weight=1)
        ttk.Label(sl, text="Path").grid(row=0, column=0, sticky="w"); ttk.Entry(sl, textvariable=self.sqlite_path_var).grid(row=0, column=1, sticky="ew", padx=(8,8))
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
        act = ttk.Frame(scroll); act.grid(row=6, column=0, sticky="w", pady=(12, 0))
        ttk.Button(act, text="Save Settings", command=self._save_all_settings, bootstyle=SUCCESS).pack(side="left")
        ttk.Button(act, text="Open Config Folder", command=self._open_config_folder).pack(side="left", padx=(8, 0))

    def _ensure_export_dir(self) -> None:
        folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        self.output_folder_var.set(folder)
        os.makedirs(folder, exist_ok=True)
    def _choose_output_folder(self) -> None:
        path = filedialog.askdirectory(initialdir=self.output_folder_var.get())
        if path:
            self.output_folder_var.set(path); self.out_label.config(text=path)
    def _toggle_batch_controls(self) -> None:
        enabled = self.enable_batch_var.get()
        rows_state = "normal" if (enabled and self.batch_mode_var.get() == "rows") else "disabled"
        group_state = "readonly" if (enabled and self.batch_mode_var.get() == "group") else "disabled"
        self.rows_entry.configure(state=rows_state); self.group_combo.configure(state=group_state)
    def _on_mode_change(self) -> None: self._toggle_batch_controls()
    def _update_group_columns(self, headers: list[str]) -> None:
        if headers:
            self.group_combo.configure(values=headers)
            cur = self.group_column_var.get()
            if cur not in headers: self.group_column_var.set("postcode" if "postcode" in headers else headers[0])

    def load_file(self) -> None:
        file_path = filedialog.askopenfilename(filetypes=[("CSV/JSON files", "*.csv *.json")])
        if not file_path: return
        self.file_path = file_path; self.file_label.config(text=os.path.basename(file_path))
        if file_path.lower().endswith(".csv"):
            try:
                with open(file_path, newline="", encoding="utf-8-sig") as f:
                    rdr = csv.DictReader(f)
                    self.headers = [(h or "").strip().lower() for h in (rdr.fieldnames or [])]
                    self._update_group_columns(self.headers)
            except Exception: pass
        else:
            self.headers = ["sku","postCode","price"]; self._update_group_columns(self.headers)

    def preview_data(self) -> None:
        if self.file_path:
            ext = os.path.splitext(self.file_path)[1].lower()
            if ext == ".csv": valid_docs, errors, warnings = validate_csv(self.file_path)
            elif ext == ".json": valid_docs, errors, warnings = validate_json(self.file_path)
            else:
                messagebox.showerror("Error", "Unsupported file type."); return
        else:
            txt = self.paste_box.get("1.0", tk.END).strip()
            if not txt:
                messagebox.showerror("Error", "No file selected or pasted content."); return
            valid_docs, errors, warnings = validate_pasted_csv_text(txt)
            first = [ln for ln in txt.splitlines() if ln.strip()][:1]
            self.headers = [h.strip().lower() for h in next(csv.reader(first))] if first else ["sku","postCode","price"]
            self._update_group_columns(self.headers)
        self.last_valid_docs, self.last_errors, self.last_warnings = valid_docs, errors, warnings
        self.preview_box.config(state="normal"); self.preview_box.delete("1.0", tk.END)
        self.preview_box.insert(tk.END, "postCode,sku,price,vendor_id\n", ("head",))
        v_id = self.vendor_id_var.get().strip() or ""
        for d in valid_docs[:100]:
            line = f"{d['postCode']},{d['sku']},{d['price']},{v_id}\n"
            self.preview_box.insert(tk.END, line, ("good",))
        self.preview_box.config(state="disabled")
        total_rows_est = len(valid_docs) + len(errors)
        dup_count = sum(1 for e in errors if "Duplicate id" in e.get("error", ""))
        uniq_skus = len(set(d["sku"] for d in valid_docs))
        prices = [d["price"] for d in valid_docs]
        pmin = min(prices) if prices else None
        pmax = max(prices) if prices else None
        pavg = round(mean(prices), 6) if prices else None
        warn_count = len(warnings)
        self.cached_stats = {"rows_total": total_rows_est,"rows_valid": len(valid_docs),"rows_invalid": len(errors),"duplicates": dup_count,"unique_skus": uniq_skus,"price_min": pmin,"price_max": pmax,"price_avg": pavg,"warnings": warn_count}
        self.stats_box.config(state="normal"); self.stats_box.delete("1.0", tk.END)
        def put(line: str, tag: Optional[str] = None):
            if tag: self.stats_box.insert(tk.END, line + "\n", (tag,))
            else: self.stats_box.insert(tk.END, line + "\n")
        put(f"Rows (estimated): {total_rows_est}", "good" if total_rows_est > 0 else "bad")
        put(f"Valid rows: {len(valid_docs)}", "good" if len(valid_docs) > 0 else "bad")
        put(f"Invalid rows: {len(errors)}", "bad" if len(errors) > 0 else "good")
        put(f"Duplicates: {dup_count}", "bad" if dup_count > 0 else "good")
        put(f"Unique SKUs: {uniq_skus}", "good" if uniq_skus > 0 else "bad")
        if pmin is not None: put(f"Price min/max/avg: {pmin} / {pmax} / {pavg}", "good")
        else: put("Price statistics: N/A", "bad")
        put(f"Warnings: {warn_count}", "bad" if warn_count > 0 else "good")
        if errors:
            self.stats_box.insert(tk.END, "\nIssues (first 50):\n", ("bad",))
            for e in errors[:50]:
                self.stats_box.insert(tk.END, f"Row {e.get('row')}: {e.get('context','')} -> {e.get('error')}\n", ("bad",))
        self.stats_box.config(state="disabled")

    def export_files(self) -> None:
        if not (self.vendor_id_var.get() or "").strip():
            val = simpledialog.askstring("Vendor ID Required", "Enter Vendor ID before export:")
            if not val:
                messagebox.showwarning("Export", "Export cancelled (Vendor ID required)."); return
            self.vendor_id_var.set(val.strip())
        if not self.last_valid_docs and not self.last_errors: self.preview_data()
        if not self.last_valid_docs and self.last_errors:
            messagebox.showerror("Error", "No valid rows to export (all invalid)."); return
        vendor_id = self.vendor_id_var.get().strip()
        csv_rows = [{"postCode": d["postCode"].lstrip("0") if d["postCode"] else "","sku": d["sku"], "price": d["price"], "vendor_id": vendor_id} for d in self.last_valid_docs]
        json_rows = [{"postCode": d["postCode"], "sku": d["sku"], "price": d["price"], "vendor_id": vendor_id} for d in self.last_valid_docs]
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
                self._export_by_group(export_folder, base_name_snake, json_rows, csv_rows, group_col)
        else:
            self._export_single(export_folder, base_name_snake, csv_rows, json_rows)
        if self.last_errors:
            error_path = os.path.join(export_folder, f"{base_name_snake}_errors.csv")
            try:
                with open(error_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["row", "context", "error"])
                    writer.writeheader(); writer.writerows(self.last_errors)
            except Exception as e:
                messagebox.showwarning("Warning", f"Failed to write error file:\n{e}")
        if self.open_folder_after_var.get():
            try: self._open_folder(export_folder)
            except Exception: pass
        messagebox.showinfo("Success", f"Export completed.\nFiles saved in:\n{export_folder}")

    def _export_single(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict]) -> None:
        ts = self._ts()
        if self.export_csv_var.get():
            csv_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="csv")
            fields = ["postCode", "sku", "price"] + (["vendor_id"] if csv_rows and "vendor_id" in csv_rows[0] else [])
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(csv_rows)
        if self.export_json_var.get():
            json_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="json")
            with open(json_path, 'w', encoding='utf-8') as f: json.dump(json_rows, f, indent=4)

    def _export_by_rows(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict], chunk_size: int) -> None:
        total = len(csv_rows)
        if total == 0: return
        ts = self._ts(); parts = (total + chunk_size - 1) // chunk_size
        fields = ["postCode", "sku", "price"] + (["vendor_id"] if csv_rows and "vendor_id" in csv_rows[0] else [])
        for i in range(parts):
            start, end = i * chunk_size, min((i+1) * chunk_size, total)
            batch_id = f"part{(i+1):03d}"
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(csv_rows[start:end])
            if self.export_json_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f: json.dump(json_rows[start:end], f, indent=4)

    def _export_by_group(self, folder: str, base: str, json_rows: list[dict], csv_rows: list[dict], group_col: str) -> None:
        ts = self._ts(); key_lower = (group_col or "").lower()
        def key_for_json(d: dict) -> str:
            if key_lower in ("postcode", "post_code", "post code"): return d.get("postCode", "") or "UNK"
            if key_lower == "sku": return d.get("sku", "") or "UNK"
            if key_lower == "price": return str(d.get("price", "")) or "UNK"
            return str(d.get(key_lower, "") or "UNK")
        groups_json, groups_csv = defaultdict(list), defaultdict(list)
        for j, c in zip(json_rows, csv_rows):
            g = (key_for_json(j) or "UNK").strip() or "UNK"
            groups_json[g].append(j); groups_csv[g].append(c)
        fields = ["postCode", "sku", "price"] + (["vendor_id"] if csv_rows and "vendor_id" in csv_rows[0] else [])
        for gval, rows_csv in groups_csv.items():
            safe_group = self._sanitize_group(gval)
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows_csv)
            if self.export_json_var.get():
                rows_json = groups_json[gval]
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f: json.dump(rows_json, f, indent=4)

    def _ensure_engine(self) -> Engine:
        self._save_all_settings(silent=True)
        db_cfg = self.settings["db"]
        db_cfg["echo"] = bool(self.db_echo_var.get())
        db_cfg["create_tables"] = bool(self.db_create_tables_var.get())
        engine = get_engine_from_settings(db_cfg)
        try_create_tables(engine, db_cfg)
        self._engine = engine
        return engine
    def _test_db_clicked(self) -> None:
        try:
            engine = self._ensure_engine()
            ok, msg = test_db_connection(engine)
            if ok: messagebox.showinfo("DB", msg)
            else: messagebox.showerror("DB", msg)
        except Exception as e:
            messagebox.showerror("DB", f"{type(e).__name__}: {e}")
    def _save_to_db_clicked(self) -> None:
        if not self.last_valid_docs and not self.last_errors: self.preview_data()
        if not self.last_valid_docs:
            messagebox.showerror("DB", "No valid rows to save."); return
        vendor_id = (self.vendor_id_var.get() or "").strip()
        if not vendor_id:
            val = simpledialog.askstring("Vendor ID Required", "Enter Vendor ID before saving to database:")
            if not val:
                messagebox.showwarning("DB", "Save cancelled (Vendor ID required)."); return
            vendor_id = val.strip(); self.vendor_id_var.set(vendor_id)
        engine = self._ensure_engine()
        source_name = os.path.basename(self.file_path) if self.file_path else "pasted"
        profile_name = self.db_profile_name_var.get().strip() or "default"
        rows = [{"sku": r["sku"], "postCode": r["postCode"], "price": float(r["price"])} for r in self.last_valid_docs]
        run_id, inserted = save_run_and_rows(engine=engine, profile_name=profile_name, vendor_id=vendor_id, source_name=source_name, stats=self.cached_stats, rows=rows)
        messagebox.showinfo("DB", f"Saved Run #{run_id}. Inserted rows: {inserted}.")

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
        if not silent: messagebox.showinfo("Settings", "Settings saved.")

    def _render_path(self, folder: str, base: str, batch: str, group: str, ts: str, ext: str) -> str:
        pattern = (self.filename_pattern_var.get() or "{base}_{batch}_{group}_{ts}.{ext}").strip()
        vals = {"base": base, "batch": batch, "group": group if group else "all", "ts": ts, "ext": ext.lstrip(".")}
        return os.path.join(folder, pattern.format(**vals))
    def _ts(self) -> str: return datetime.now().strftime("%Y%m%d_%H%M%S")
    def _sanitize_group(self, s: str) -> str:
        out = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)
        return out[:80] if out else "UNK"
    def _open_folder(self, path: str) -> None:
        sys = platform.system()
        if sys == "Windows": subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sys == "Darwin": subprocess.Popen(["open", path])
        else: subprocess.Popen(["xdg-open", path])
    def _choose_db_path(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".sqlite", filetypes=[("SQLite DB", "*.sqlite *.db")], initialfile=os.path.basename(self.sqlite_path_var.get() or "export_runs.sqlite"))
        if path: self.sqlite_path_var.set(path)
    def _open_config_folder(self) -> None:
        try:
            if platform.system() == "Windows": os.startfile(APP_DIR)
            elif platform.system() == "Darwin": subprocess.Popen(["open", APP_DIR])
            else: subprocess.Popen(["xdg-open", APP_DIR])
        except Exception: messagebox.showwarning("Open", f"Folder: {APP_DIR}")

if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
