import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from azure.cosmos import CosmosClient, exceptions
import logging
import csv
import json
import os
import re
from decimal import Decimal, InvalidOperation
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
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
SETTINGS_FILE = "app_settings.json"
DEFAULT_SETTINGS = {
    "connection_string": "",
    "database_name": "soh",
    "container_name": "dropshipPricing",
    "allow_partial_upload": False,
    "log_level": "INFO",
    "max_workers": 4,
    "use_upsert": True 
}
app_state = DEFAULT_SETTINGS.copy()
def load_settings():
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
            apply_log_level(app_state.get("log_level", "INFO"))
            logger.info("Settings loaded from %s", SETTINGS_FILE)
        except Exception as e:
            logger.error("Failed to load settings: %s", e)
    else:
        apply_log_level(app_state.get("log_level", "INFO"))
def save_settings():
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
def apply_log_level(level_name: str):
    level = logging.INFO
    if level_name.upper() == "DEBUG":
        level = logging.DEBUG
    elif level_name.upper() == "WARNING":
        level = logging.WARNING
    elif level_name.upper() == "ERROR":
        level = logging.ERROR
    logger.setLevel(level)
def get_cosmos_container():
    """
    Return a ContainerProxy based on current app_state settings.
    """
    conn = app_state["connection_string"].strip()
    dbname = app_state["database_name"].strip()
    cname = app_state["container_name"].strip()
    client = CosmosClient.from_connection_string(conn)
    db = client.get_database_client(dbname)
    return db.get_container_client(cname)
def preflight_cosmos_connection():
    """
    Validate current connection string, database and container exist/access.
    """
    try:
        client = CosmosClient.from_connection_string(app_state["connection_string"].strip())
        db = client.get_database_client(app_state["database_name"].strip())
        _ = db.read()
        container = db.get_container_client(app_state["container_name"].strip())
        _ = container.read()
        return True, None
    except Exception as e:
        return False, e
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
    """
    Accepts str/number, returns (ok, normalized_str_price, error)
    Normalizes to 2 decimal places as string to match schema.
    """
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
def upload_item_with_retry(doc, container, MAX_RETRIES=5, use_upsert=True):
    """Worker function to upload a single document with exponential backoff and retry."""
    retry_count = 0
    backoff_time = 1.0
    while retry_count < MAX_RETRIES:
        try:
            if use_upsert:
                container.upsert_item(doc)
            else:
                container.create_item(doc)
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
        except exceptions.CosmosResourceExistsError:
            if not use_upsert:
                error_msg = f"Item {doc['id']} already exists (Insert-only mode). Skipping."
                return False, Decimal("0"), error_msg
            error_msg = f"Fatal error uploading {doc['id']}: Resource already exists unexpectedly."
            return False, Decimal("0"), error_msg
        except Exception as e:
            error_msg = f"Fatal error uploading {doc['id']}: {e}"
            return False, Decimal("0"), error_msg
    return False, Decimal("0"), f"Failed to upload {doc['id']} after all attempts (unknown error)."
def upload_sku_price(raw_sku, raw_pc, raw_price, log_area, root):
    log_area.insert(tk.END, "--- Starting Single SKU Update ---\n")
    log_area.see(tk.END)
    ok_sku, sku_err = is_valid_sku(raw_sku)
    ok_pc, pc_err = is_valid_postcode(raw_pc)
    ok_price, norm_price, price_err = normalize_price(raw_price)
    errs = []
    if not ok_sku: errs.append(f"SKU error: {sku_err}")
    if not ok_pc: errs.append(f"Postcode error: {pc_err}")
    if not ok_price: errs.append(f"Price error: {price_err}")
    if errs:
        msg = f"Validation failed:\n- " + "\n- ".join(errs)
        log_area.insert(tk.END, msg + "\n")
        log_area.see(tk.END)
        messagebox.showerror("Validation Error", msg)
        return
    ok_conn, err_conn = preflight_cosmos_connection()
    if not ok_conn:
        msg = f"Cosmos connection failed. Please verify connection string / DB / container.\nDetails: {err_conn}"
        log_area.insert(tk.END, msg + "\n")
        log_area.see(tk.END)
        messagebox.showerror("Connection Error", msg)
        return
    doc = build_doc(raw_sku, raw_pc, norm_price)
    container = get_cosmos_container()
    use_upsert = app_state.get("use_upsert", DEFAULT_SETTINGS["use_upsert"])
    log_area.insert(tk.END, f"Attempting upload for ID: {doc['id']} (Mode: {'Upsert' if use_upsert else 'Insert-Only'})...\n")
    log_area.see(tk.END)
    success, ru, error_msg = upload_item_with_retry(doc, container, MAX_RETRIES=5, use_upsert=use_upsert)
    if success:
        final_msg = f"SUCCESS: Item {doc['id']} updated. RU charge: {ru:.2f}"
        log_area.insert(tk.END, final_msg + "\n")
        messagebox.showinfo("Success", final_msg)
    else:
        final_msg = f"FAILURE: Item {doc['id']} failed. Reason: {error_msg}"
        log_area.insert(tk.END, final_msg + "\n")
        messagebox.showerror("Upload Failed", final_msg)
    log_area.see(tk.END)
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
        for idx, row in enumerate(reader, start=2):  # header is row 1
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
def bulk_upload_task(file_path, log_area, bulk_button, root, progress_bar, total_items_var, processed_count_var, uploaded_count_var, failed_count_var, ru_count_var):
    """
    Performs validation, error reporting, and concurrent upload execution 
    in a background thread, updating the UI safely.
    """
    def update_progress_ui(total, processed, uploaded, failed, total_ru):
        progress_bar.config(maximum=total, value=processed)
        total_items_var.set(f"Total Items: {total}")
        processed_count_var.set(f"Processed: {processed}")
        uploaded_count_var.set(f"Uploaded: {uploaded}")
        failed_count_var.set(f"Failed: {failed}")
        ru_count_var.set(f"Total RU: {total_ru:.2f}")
    def update_ui_state(log_msg=None, enable_button=True, show_cursor=False, show_box=None, title=None):
        if log_msg:
            log_area.insert(tk.END, log_msg + "\n")
            log_area.see(tk.END)
        bulk_button.config(state=tk.NORMAL if enable_button else tk.DISABLED)
        root.config(cursor="" if not show_cursor else "wait")
        if enable_button:
            progress_bar.config(value=0, maximum=1)
            total_items_var.set("Total Items: N/A")
            processed_count_var.set("Processed: 0")
            uploaded_count_var.set("Uploaded: 0")
            failed_count_var.set("Failed: 0")
            ru_count_var.set("Total RU: 0.00")
        if show_box == "info":
            messagebox.showinfo(title, log_msg)
        elif show_box == "error":
            messagebox.showerror(title, log_msg)
    root.after(0, update_ui_state, "--- Starting Bulk Upload (Background Thread) ---", False, True, None, None)
    root.after(0, update_progress_ui, 1, 0, 0, 0, Decimal("0"))
    ok_conn, err_conn = preflight_cosmos_connection()
    if not ok_conn:
        msg = f"Cosmos connection failed. Please verify connection string / DB / container.\n\nDetails:\n{err_conn}"
        root.after(0, update_ui_state, msg, True, False, "error", "Connection Error")
        logger.error(msg)
        return
    try:
        root.after(0, update_ui_state, f"Validating file: {file_path}", False, True)
        valid_docs, errors, warnings = validate_file(file_path)
        for w in warnings:
            root.after(0, update_ui_state, f"Warning: {w}")
            logger.warning(w)
        report_path = f"{file_path}.errors.csv"
        allow_partial = app_state.get("allow_partial_upload", False)
        if errors and not allow_partial:
            write_error_report(errors, report_path)
            msg = (
                f"Validation failed.\n"
                f"Valid rows: {len(valid_docs)}\n"
                f"Errors: {len(errors)}\n\n"
                f"Error report written to:\n{report_path}\n\n"
                f"No data has been uploaded (strict mode)."
            )
            root.after(0, update_ui_state, msg, True, False, "error", "Validation Failed")
            logger.error(f"Bulk validation failed with {len(errors)} errors. Report: {report_path}")
            return
        if errors and allow_partial:
            write_error_report(errors, report_path)
            log_msg = f"Partial mode: {len(errors)} validation errors logged to {report_path}. Uploading {len(valid_docs)} valid rows..."
            root.after(0, update_ui_state, log_msg)
            logger.warning(f"Partial upload enabled. Errors: {len(errors)}; Proceeding with {len(valid_docs)} valid rows.")
        if not valid_docs:
            root.after(0, update_ui_state, "No valid documents to upload.", True, False, "info", "Bulk Upload Complete")
            return
        container = get_cosmos_container()
        uploaded = 0
        failed_network = 0
        total_ru = Decimal("0")
        max_workers = app_state.get("max_workers", DEFAULT_SETTINGS["max_workers"])
        if not isinstance(max_workers, int) or max_workers <= 0:
            max_workers = DEFAULT_SETTINGS["max_workers"]
        total_to_upload = len(valid_docs)
        root.after(0, update_progress_ui, total_to_upload, 0, 0, 0, Decimal("0"))
        log_msg = f"Starting concurrent upload using {max_workers} threads for {total_to_upload} documents (Mode: {'Upsert' if use_upsert else 'Insert-Only'})..."
        root.after(0, update_ui_state, log_msg)
        use_upsert = app_state.get("use_upsert", DEFAULT_SETTINGS["use_upsert"])
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_doc = {
                executor.submit(upload_item_with_retry, doc, container, 5, use_upsert): doc
                for doc in valid_docs
            }
            for i, future in enumerate(as_completed(future_to_doc), start=1):
                doc = future_to_doc[future]
                try:
                    success, ru, error_msg = future.result()
                    if success:
                        uploaded += 1
                        total_ru += ru
                    else:
                        failed_network += 1
                        root.after(0, update_ui_state, f"Failed item: {doc['id']}. Reason: {error_msg}")
                except Exception as e:
                    failed_network += 1
                    root.after(0, update_ui_state, f"Unexpected error during thread execution for {doc['id']}: {e}")
                    logger.error(f"Unexpected error during thread execution for {doc['id']}: {e}")
                if i % 100 == 0 or i == total_to_upload:
                    root.after(0, update_progress_ui, total_to_upload, i, uploaded, failed_network, total_ru)
        total_failed_combined = len(errors) + failed_network # Validation errors + network errors
        final_msg = (
            f"Bulk Upload Complete. Total Processed: {total_to_upload}.\n"
            f"Uploaded successfully: {uploaded}.\n"
            f"Failed (Total): {total_failed_combined} (Validation: {len(errors)}, Network: {failed_network}).\n"
            f"Approx total RU: {total_ru:.2f}"
        )
        root.after(0, update_progress_ui, total_to_upload, total_to_upload, uploaded, failed_network, total_ru)
        root.after(0, update_ui_state, final_msg, True, False, "info", "Bulk Upload Complete")
        logger.info(f"Bulk upload complete. Uploaded: {uploaded}. Total failed: {total_failed_combined}. Approx total RU: {total_ru}")
    except Exception as e:
        error_msg = f"FATAL ERROR during bulk upload: {e}"
        root.after(0, update_ui_state, error_msg, True, False, "error", "Bulk Upload Error")
        logger.error(f"Bulk upload failed: {e}")
def select_file_and_upload(log_area, bulk_button, root, progress_bar, total_items_var, processed_count_var, uploaded_count_var, failed_count_var, ru_count_var):
    """Handles file selection and starts the upload task in a new thread."""
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json")])
    if file_path:
        threading.Thread(target=bulk_upload_task, args=(
            file_path, log_area, bulk_button, root, progress_bar, 
            total_items_var, processed_count_var, uploaded_count_var, 
            failed_count_var, ru_count_var
        ), daemon=True).start()
def apply_settings_from_ui(conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var, upsert_var):
    app_state["connection_string"] = conn_var.get().strip()
    app_state["database_name"] = db_var.get().strip()
    app_state["container_name"] = cont_var.get().strip()
    app_state["allow_partial_upload"] = bool(partial_var.get())
    app_state["log_level"] = loglevel_var.get()
    app_state["use_upsert"] = bool(upsert_var.get())
    try:
        app_state["max_workers"] = int(max_workers_var.get())
    except ValueError:
        app_state["max_workers"] = DEFAULT_SETTINGS["max_workers"]
        logger.error("Invalid value for Max Workers. Defaulting to %d.", app_state["max_workers"])
    apply_log_level(app_state["log_level"])
def test_connection_action():
    ok, err = preflight_cosmos_connection()
    if ok:
        messagebox.showinfo("Connection Test", "Connection successful!")
    else:
        messagebox.showerror("Connection Test Failed", str(err))
def build_app():
    load_settings()
    root = tk.Tk()
    root.title("Freight Matrix Loader")
    root.geometry("800x650")
    root.minsize(720, 480)
    style = ttk.Style()
    try:
        style.theme_use("default")
    except Exception:
        pass
    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)
    tab_upload = ttk.Frame(notebook)
    notebook.add(tab_upload, text="Upload")
    left = ttk.Frame(tab_upload, padding=10)
    left.grid(row=0, column=0, sticky="nsw")
    right = ttk.Frame(tab_upload, padding=10)
    right.grid(row=0, column=1, sticky="nsew")
    tab_upload.columnconfigure(1, weight=1)
    tab_upload.rowconfigure(0, weight=1)
    ttk.Label(left, text="SKU:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
    sku_entry = ttk.Entry(left, width=28)
    sku_entry.grid(row=0, column=1, sticky="w", padx=4, pady=4)
    ttk.Label(left, text="Postcode:").grid(row=1, column=0, sticky="e", padx=4, pady=4)
    postcode_entry = ttk.Entry(left, width=28)
    postcode_entry.grid(row=1, column=1, sticky="w", padx=4, pady=4)
    ttk.Label(left, text="Price:").grid(row=2, column=0, sticky="e", padx=4, pady=4)
    price_entry = ttk.Entry(left, width=28)
    price_entry.grid(row=2, column=1, sticky="w", padx=4, pady=4)
    send_button = ttk.Button(left, text="Single SKU Update",
                             command=lambda: upload_sku_price(sku_entry.get(), postcode_entry.get(), price_entry.get(), log_area, root))
    send_button.grid(row=3, column=0, columnspan=2, sticky="ew", padx=4, pady=(8, 4))
    bulk_button = ttk.Button(left, text="Bulk Upload CSV/JSON (Validate First)")
    bulk_button.grid(row=4, column=0, columnspan=2, sticky="ew", padx=4, pady=4)
    total_items_var = tk.StringVar(value="Total Items: N/A")
    processed_count_var = tk.StringVar(value="Processed: 0")
    uploaded_count_var = tk.StringVar(value="Uploaded: 0")
    failed_count_var = tk.StringVar(value="Failed: 0 (Net)")
    ru_count_var = tk.StringVar(value="Total RU: 0.00")
    progress_frame = ttk.LabelFrame(left, text="Upload Progress", padding=8)
    progress_frame.grid(row=5, column=0, columnspan=2, sticky="ew", padx=4, pady=8)
    progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
    progress_bar.pack(fill="x", pady=4)
    ttk.Label(progress_frame, textvariable=total_items_var, font=('Arial', 10, 'bold')).pack(anchor="w")
    ttk.Label(progress_frame, textvariable=processed_count_var).pack(anchor="w")
    ttk.Label(progress_frame, textvariable=uploaded_count_var, foreground='green').pack(anchor="w")
    ttk.Label(progress_frame, textvariable=failed_count_var, foreground='red').pack(anchor="w")
    ttk.Label(progress_frame, textvariable=ru_count_var).pack(anchor="w")
    bulk_button.config(command=lambda: select_file_and_upload(
        log_area, bulk_button, root, progress_bar, 
        total_items_var, processed_count_var, uploaded_count_var, 
        failed_count_var, ru_count_var
    ))
    log_area = scrolledtext.ScrolledText(right, width=80, height=24)
    log_area.pack(fill="both", expand=True)
    tab_settings = ttk.Frame(notebook)
    notebook.add(tab_settings, text="Settings")
    conn_var = tk.StringVar(value=app_state["connection_string"])
    db_var = tk.StringVar(value=app_state["database_name"])
    cont_var = tk.StringVar(value=app_state["container_name"])
    partial_var = tk.IntVar(value=1 if app_state["allow_partial_upload"] else 0)
    loglevel_var = tk.StringVar(value=app_state["log_level"])
    max_workers_var = tk.IntVar(value=app_state["max_workers"])
    upsert_var = tk.IntVar(value=1 if app_state["use_upsert"] else 0)
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
    upsert_check = ttk.Checkbutton(tab_settings, text="Use Upsert (Overwrite Existing Records)", variable=upsert_var)
    upsert_check.grid(row=6, column=1, sticky="w", padx=6, pady=6)
    btn_frame = ttk.Frame(tab_settings)
    btn_frame.grid(row=7, column=1, sticky="w", padx=6, pady=10)
    apply_btn = ttk.Button(btn_frame, text="Apply (No Save)", 
                           command=lambda: (apply_settings_from_ui(conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var, upsert_var), messagebox.showinfo("Settings", "Settings applied.")))
    apply_btn.grid(row=0, column=0, padx=4)
    save_btn = ttk.Button(btn_frame, text="Save Settings", 
                          command=lambda: (apply_settings_from_ui(conn_var, db_var, cont_var, partial_var, loglevel_var, max_workers_var, upsert_var), save_settings()))
    save_btn.grid(row=0, column=1, padx=4)
    test_btn = ttk.Button(btn_frame, text="Test Connection", command=test_connection_action)
    test_btn.grid(row=0, column=2, padx=4)
    help_text = (
        "Tips:\n"
        "- Enter the exact Primary connection string from Azure Portal → Cosmos DB → Keys.\n"
        "- Use 'Apply' to test without saving; 'Save' persists to app_settings.json.\n"
        "- **Use Upsert** controls if existing documents are overwritten (Checked) or if upload fails on conflict (Unchecked/Insert-Only).\n"
        "- **Max Concurrent Threads** controls how many documents are uploaded at once (default: 4).\n"
        "- All uploads now use concurrent threads with **automatic retry logic** and exponential backoff to handle throttling (HTTP 429) errors reliably."
    )
    ttk.Label(tab_settings, text=help_text, foreground="#555", justify=tk.LEFT).grid(row=8, column=0, columnspan=2, sticky="w", padx=6, pady=6)
    return root
if __name__ == "__main__":
    root = build_app()
    root.mainloop()
