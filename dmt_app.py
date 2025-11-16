import os
import fnmatch
import json
import subprocess
import concurrent.futures
import threading
import queue as thread_queue
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText
import ttkbootstrap as tb
from ttkbootstrap.constants import *

BASE_DIR = Path(__file__).resolve().parent
DMT_DIR = BASE_DIR / "win-x64-package"
DMT_PATH = DMT_DIR / "dmt.exe"
MIGRATION_SETTINGS = DMT_DIR / "migrationsettings.json"
CONFIG_PATH = BASE_DIR / "cosmos_loader_config.json"
HISTORY_PATH = BASE_DIR / "cosmos_loader_history.json"

class DMTGui(tb.Window):
    MAX_HISTORY = 200

    def __init__(self):
        super().__init__(
            title="Cosmos DB JSON Batch Loader (Parallel)",
            themename="darkly",
        )
        self.geometry("980x640")
        self.minsize(720, 400)
        self.resizable(True, True)

        self.colors = self.style.colors

        self.ui_queue = thread_queue.Queue()
        self.stop_flag = threading.Event()
        self.executor: concurrent.futures.ThreadPoolExecutor | None = None
        self.futures: list[concurrent.futures.Future] = []
        self.process_lock = threading.Lock()
        self.active_processes: set[subprocess.Popen] = set()

        self.queue_items: dict[str, str] = {}  # file_path -> iid
        self.iid_to_path: dict[str, str] = {}  # iid -> file_path
        self.history_rows: list[dict] = []
        self._completed = 0
        self._total = 0
        self._next_id = 1

        self.status_var = tk.StringVar(value="Ready.")

        self._register_button_styles()
        self._build_layout()
        self._load_config()
        self._load_history()
        self._apply_text_theme()

        self.after(80, self._drain_ui_queue)


    def _surface_bg(self) -> str:
        try:
            val = None
            try:
                val = self.colors.get("surface")
            except TypeError:
                val = self.colors.get("surface")
        except Exception:
            val = None
        if not val:
            try:
                val = self.colors.get("bg")
            except Exception:
                val = None
        return val or self.style.lookup("TFrame", "background")

    def _fg_color(self) -> str:
        try:
            val = self.colors.get("fg")
        except Exception:
            val = None
        return val or self.style.lookup("TLabel", "foreground")

    def _register_button_styles(self):
        """
        Buttons with border + hover feedback, based on theme palette.
        """
        surf = self._surface_bg()

        def themed_color(key: str, fallback: str) -> str:
            try:
                c = self.colors.get(key)
            except Exception:
                c = None
            return c or fallback

        style = self.style

        primary = themed_color("primary", "#1f6feb")
        danger = themed_color("danger", "#e55353")
        info = themed_color("info", "#17a2b8")
        success = themed_color("success", "#28a745")
        secondary = themed_color("secondary", "#6c757d")
        warning = themed_color("warning", "#ffc107")

        def make(style_name: str, fg: str):
            sname = f"{style_name}.TButton"
            style.configure(
                sname,
                background=surf,
                foreground=fg,
                borderwidth=1,
                relief="ridge",
                focusthickness=1,
                focuscolor=fg,
                padding=(10, 5),
            )
            style.map(
                sname,
                background=[("active", surf), ("pressed", surf)],
                foreground=[("active", fg)],
                relief=[("pressed", "sunken")],
                bordercolor=[("active", fg)],
            )

        make("Primary", primary)
        make("Danger", danger)
        make("Info", info)
        make("Success", success)
        make("Secondary", secondary)
        make("Warning", warning)

    def _refresh_button_styles(self):
        self.colors = self.style.colors
        self._register_button_styles()
        self._apply_text_theme()


    def _build_layout(self):
        preferred_themes = [
            "cosmo",
            "flatly",
            "journal",
            "litera",
            "lumen",
            "minty",
            "pulse",
            "sandstone",
            "united",
            "yeti",
            "darkly",
            "cyborg",
            "solar",
            "superhero",
            "vapor",
        ]
        style_themes = set(self.style.theme_names())
        self.available_themes = [t for t in preferred_themes if t in style_themes] or list(
            style_themes
        )

        menubar = Menu(self)
        filem = Menu(menubar, tearoff=0)
        filem.add_command(label="Open Config…", command=self._open_config_dialog)
        filem.add_command(label="Save Config", command=self._save_config)
        filem.add_separator()
        filem.add_command(label="Exit", command=self.destroy)
        menubar.add_cascade(label="File", menu=filem)

        themem = Menu(menubar, tearoff=0)
        for tname in self.available_themes:
            themem.add_command(
                label=tname.capitalize(),
                command=lambda name=tname: self._apply_theme(name),
            )
        menubar.add_cascade(label="Theme", menu=themem)
        self.config(menu=menubar)

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(10, 0))

        self.tab_queue = ttk.Frame(self.nb)
        self.tab_history = ttk.Frame(self.nb)
        self.tab_settings = ttk.Frame(self.nb)
        self.tab_cli = ttk.Frame(self.nb)
        self.tab_logs = ttk.Frame(self.nb)

        self.nb.add(self.tab_queue, text=" Queue ")
        self.nb.add(self.tab_history, text=" History ")
        self.nb.add(self.tab_settings, text=" Settings ")
        self.nb.add(self.tab_cli, text=" CLI ")
        self.nb.add(self.tab_logs, text=" Logs ")

        self._build_queue_tab()
        self._build_history_tab()
        self._build_settings_tab()
        self._build_cli_tab()
        self._build_logs_tab()

        status_bar = ttk.Frame(self)
        status_bar.pack(fill="x", side="bottom", padx=6, pady=4)
        ttk.Label(
            status_bar, textvariable=self.status_var, anchor="w"
        ).pack(side="left", fill="x", expand=True)


    def _build_queue_tab(self):
        top = ttk.Frame(self.tab_queue)
        top.pack(fill="x", padx=6, pady=(6, 2))

        ttk.Label(top, text="JSON Folder:", width=14).pack(side="left")
        self.folder_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.folder_var).pack(
            side="left", fill="x", expand=True, padx=(4, 6)
        )
        ttk.Button(
            top,
            text="Browse",
            command=self._browse_folder,
            style="Secondary.TButton",
        ).pack(side="left")

        opts = ttk.Frame(self.tab_queue)
        opts.pack(fill="x", padx=6, pady=(0, 4))

        self.include_subdirs_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            opts, text="Include subfolders", variable=self.include_subdirs_var
        ).pack(side="left")

        ttk.Label(opts, text="Patterns:").pack(side="left", padx=(12, 4))
        self.pattern_var = tk.StringVar(value="*.json")
        ttk.Entry(opts, textvariable=self.pattern_var, width=28).pack(side="left")
        ttk.Label(opts, text="(use ; to separate)").pack(
            side="left", padx=(6, 0)
        )

        mid = ttk.Frame(self.tab_queue)
        mid.pack(fill="both", expand=True, padx=6, pady=(2, 4))

        cols = ("id", "file", "status", "priority", "started", "finished")
        self.queue_tree = ttk.Treeview(
            mid,
            columns=cols,
            show="headings",
            selectmode="extended",
        )
        self.queue_tree.heading("id", text="ID")
        self.queue_tree.heading("file", text="File")
        self.queue_tree.heading("status", text="Status")
        self.queue_tree.heading("priority", text="Priority")
        self.queue_tree.heading("started", text="Started")
        self.queue_tree.heading("finished", text="Finished")

        self.queue_tree.column("id", width=50, anchor="center", stretch=False)
        self.queue_tree.column("file", width=320, anchor="w")
        self.queue_tree.column("status", width=120, anchor="center")
        self.queue_tree.column("priority", width=90, anchor="center")
        self.queue_tree.column("started", width=150, anchor="center")
        self.queue_tree.column("finished", width=150, anchor="center")

        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.queue_tree.yview)
        self.queue_tree.configure(yscrollcommand=vsb.set)

        self.queue_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="left", fill="y")

        self.queue_tree.tag_configure("success", foreground="#25c26e")
        self.queue_tree.tag_configure("fail", foreground="#e55353")
        self.queue_tree.tag_configure("running", foreground="#f0ad4e")
        self.queue_tree.tag_configure("stopped", foreground="#888888")

        self.queue_tree.bind("<Double-1>", self._on_queue_double_click)

        act = ttk.Frame(self.tab_queue)
        act.pack(fill="x", padx=6, pady=(0, 6))

        left_btns = ttk.Frame(act)
        left_btns.pack(side="left")

        ttk.Button(
            left_btns,
            text="Scan Files",
            command=self._scan_files,
            style="Info.TButton",
        ).pack(side="left")
        ttk.Button(
            left_btns,
            text="Start Batch",
            command=self._start_batch,
            style="Primary.TButton",
        ).pack(side="left", padx=(6, 0))
        ttk.Button(
            left_btns,
            text="Stop",
            command=self._stop_batch,
            style="Danger.TButton",
        ).pack(side="left", padx=(6, 0))
        ttk.Button(
            left_btns,
            text="Clear Queue",
            command=self._clear_queue,
            style="Secondary.TButton",
        ).pack(side="left", padx=(6, 0))

        quick = ttk.Frame(act)
        quick.pack(side="right")

        ttk.Button(
            quick,
            text="Delete Selected",
            command=self._delete_selected,
            style="Danger.TButton",
        ).pack(side="left", padx=(0, 6))

        ttk.Label(quick, text="Priority:").pack(side="left")
        self.bulk_priority_var = tk.StringVar(value="Medium")
        ttk.Combobox(
            quick,
            textvariable=self.bulk_priority_var,
            state="readonly",
            width=8,
            values=("High", "Medium", "Low"),
        ).pack(side="left", padx=(4, 4))

        ttk.Button(
            quick,
            text="Apply to Selected",
            command=self._bulk_update_priority,
            style="Success.TButton",
        ).pack(side="left")


    def _build_history_tab(self):
        wrap = ttk.Frame(self.tab_history)
        wrap.pack(fill="both", expand=True, padx=6, pady=6)

        cols = ("file", "result", "started", "finished")
        self.hist_tree = ttk.Treeview(
            wrap,
            columns=cols,
            show="headings",
            selectmode="browse",
        )
        self.hist_tree.heading("file", text="File")
        self.hist_tree.heading("result", text="Result")
        self.hist_tree.heading("started", text="Started")
        self.hist_tree.heading("finished", text="Finished")

        self.hist_tree.column("file", width=360, anchor="w")
        self.hist_tree.column("result", width=120, anchor="center")
        self.hist_tree.column("started", width=160, anchor="center")
        self.hist_tree.column("finished", width=160, anchor="center")

        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.hist_tree.yview)
        self.hist_tree.configure(yscrollcommand=vsb.set)

        self.hist_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="left", fill="y")

        self.hist_tree.tag_configure("success", foreground="#25c26e")
        self.hist_tree.tag_configure("fail", foreground="#e55353")

        btnbar = ttk.Frame(self.tab_history)
        btnbar.pack(fill="x", padx=6, pady=(0, 6))

        ttk.Button(
            btnbar,
            text="Export History JSON",
            command=self._export_history,
            style="Success.TButton",
        ).pack(side="left")
        ttk.Button(
            btnbar,
            text="Clear History",
            command=self._clear_history,
            style="Secondary.TButton",
        ).pack(side="left", padx=(6, 0))


    def _build_settings_tab(self):
        wrap = ttk.Frame(self.tab_settings)
        wrap.pack(fill="both", expand=True, padx=10, pady=10)

        form = ttk.Frame(wrap)
        form.pack(fill="x", expand=False)
        for c, w in [(0, 0), (1, 1), (2, 0)]:
            form.columnconfigure(c, weight=w)

        r = 0
        ttk.Label(
            form, text="Connection String:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.conn_str_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.conn_str_var).grid(
            row=r, column=1, sticky="ew", padx=(8, 0), pady=4
        )
        ttk.Button(
            form,
            text="Help",
            command=self._noop,
            style="Secondary.TButton",
        ).grid(row=r, column=2, sticky="e", padx=6, pady=4)
        r += 1

        ttk.Label(
            form, text="Database Name:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.db_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.db_var).grid(
            row=r, column=1, sticky="ew", padx=(8, 0), pady=4
        )
        r += 1

        ttk.Label(
            form, text="Container Name:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.container_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.container_var).grid(
            row=r, column=1, sticky="ew", padx=(8, 0), pady=4
        )
        r += 1

        ttk.Label(
            form, text="Partition key path:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.pk_path_var = tk.StringVar(value="/id")
        ttk.Entry(form, textvariable=self.pk_path_var).grid(
            row=r, column=1, sticky="ew", padx=(8, 0), pady=4
        )
        r += 1

        ttk.Label(
            form, text="Allow Partial Upload:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.partial_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(form, variable=self.partial_var).grid(
            row=r, column=1, sticky="w", padx=(8, 0), pady=4
        )
        r += 1

        ttk.Label(
            form, text="Log Level:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.log_level_var = tk.StringVar(value="INFO")
        ttk.Combobox(
            form,
            textvariable=self.log_level_var,
            state="readonly",
            values=("DEBUG", "INFO", "WARNING", "ERROR"),
            width=18,
        ).grid(row=r, column=1, sticky="w", padx=(8, 0), pady=4)
        r += 1

        ttk.Label(
            form, text="Max Workers:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.workers_var = tk.IntVar(value=4)
        ttk.Spinbox(
            form,
            from_=1,
            to=64,
            textvariable=self.workers_var,
            width=8,
        ).grid(row=r, column=1, sticky="w", padx=(8, 0), pady=4)
        r += 1

        ttk.Label(
            form, text="Write Mode:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        self.write_mode_var = tk.StringVar(value="Insert")
        ttk.Combobox(
            form,
            textvariable=self.write_mode_var,
            state="readonly",
            values=("Insert", "Upsert"),
            width=18,
        ).grid(row=r, column=1, sticky="w", padx=(8, 0), pady=4)
        r += 1

        ttk.Label(
            form, text="Theme:", font=("Segoe UI", 10, "bold")
        ).grid(row=r, column=0, sticky="w", pady=4)
        current_theme = self.style.theme_use()
        if current_theme not in self.available_themes:
            current_theme = (
                "darkly" if "darkly" in self.available_themes else current_theme
            )
        self.theme_var = tk.StringVar(value=current_theme)
        ttk.Combobox(
            form,
            textvariable=self.theme_var,
            state="readonly",
            values=self.available_themes,
            width=18,
        ).grid(row=r, column=1, sticky="w", padx=(8, 0), pady=4)
        ttk.Button(
            form,
            text="Apply Theme",
            command=self._apply_theme_from_dropdown,
            style="Secondary.TButton",
        ).grid(row=r, column=2, sticky="e", padx=6, pady=4)
        r += 1

        footer = ttk.Frame(wrap)
        footer.pack(fill="x", side="bottom", pady=(12, 0))

        left = ttk.Frame(footer)
        left.pack(side="left")
        ttk.Label(left, text="Config actions:").pack(side="left")

        right = ttk.Frame(footer)
        right.pack(side="right")

        ttk.Button(
            right,
            text="Load Config",
            command=self._open_config_dialog,
            style="Secondary.TButton",
        ).pack(side="left", padx=(0, 6))
        ttk.Button(
            right,
            text="Save Config",
            command=self._save_config,
            style="Success.TButton",
        ).pack(side="left", padx=(0, 6))
        ttk.Button(
            right,
            text="Test Settings",
            command=self._test_settings,
            style="Warning.TButton",
        ).pack(side="left", padx=(0, 6))
        ttk.Button(
            right,
            text="Test with file…",
            command=self._test_with_file,
            style="Info.TButton",
        ).pack(side="left")


    def _build_cli_tab(self):
        wrap = ttk.Frame(self.tab_cli)
        wrap.pack(fill="both", expand=True, padx=8, pady=8)

        top = ttk.Frame(wrap)
        top.pack(fill="x", pady=(0, 6))

        ttk.Label(top, text="Arguments (after dmt.exe):").pack(side="left")
        self.cli_args_var = tk.StringVar(value="-h")
        ttk.Entry(top, textvariable=self.cli_args_var).pack(
            side="left", fill="x", expand=True, padx=(6, 6)
        )
        ttk.Button(
            top,
            text="Run",
            command=self._run_cli,
            style="Primary.TButton",
        ).pack(side="left")

        self.cli_output = ScrolledText(
            wrap,
            height=18,
            wrap="word",
            borderwidth=0,
            highlightthickness=0,
        )
        self.cli_output.pack(fill="both", expand=True)


    def _build_logs_tab(self):
        self.log = ScrolledText(
            self.tab_logs,
            height=18,
            wrap="word",
            borderwidth=0,
            highlightthickness=0,
        )
        self.log.pack(fill="both", expand=True, padx=8, pady=8)

        ctx = Menu(self.log, tearoff=0, bg=self._surface_bg(), fg=self._fg_color())
        ctx.add_command(label="Copy", command=lambda: self.log.event_generate("<<Copy>>"))
        ctx.add_command(label="Clear", command=self._clear_log)
        self.log.bind("<Button-3>", lambda e: ctx.tk_popup(e.x_root, e.y_root))


    def _load_config(self, path=CONFIG_PATH):
        defaults = {
            "connection_string": "",
            "database_name": "",
            "container_name": "",
            "partition_key_path": "/id",
            "allow_partial_upload": True,
            "log_level": "INFO",
            "max_workers": 4,
            "last_folder": "",
            "theme": "darkly",
            "write_mode": "Insert",
        }
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for k in defaults:
                    defaults[k] = data.get(k, defaults[k])
            except Exception as e:
                messagebox.showwarning("Config", f"Failed to load config: {e}")

        self.conn_str_var.set(defaults["connection_string"])
        self.db_var.set(defaults["database_name"])
        self.container_var.set(defaults["container_name"])
        self.pk_path_var.set(defaults["partition_key_path"])
        self.partial_var.set(defaults["allow_partial_upload"])
        self.log_level_var.set(defaults["log_level"])
        self.workers_var.set(int(defaults["max_workers"]))
        self.folder_var.set(defaults.get("last_folder", ""))
        self.write_mode_var.set(defaults.get("write_mode", "Insert"))

        theme_value = defaults.get("theme", "darkly")
        if theme_value not in self.available_themes:
            theme_value = (
                "darkly" if "darkly" in self.available_themes else self.style.theme_use()
            )
        self.theme_var.set(theme_value)
        self._apply_theme_from_dropdown()

    def _save_config(self, path=CONFIG_PATH):
        data = {
            "connection_string": self.conn_str_var.get().strip(),
            "database_name": self.db_var.get().strip(),
            "container_name": self.container_var.get().strip(),
            "partition_key_path": self.pk_path_var.get().strip() or "/id",
            "allow_partial_upload": bool(self.partial_var.get()),
            "log_level": self.log_level_var.get(),
            "max_workers": int(self.workers_var.get()),
            "last_folder": self.folder_var.get().strip(),
            "theme": self.theme_var.get(),
            "write_mode": self.write_mode_var.get(),
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            self._log_ui("Config saved.\n")
        except Exception as e:
            messagebox.showerror("Config", f"Save failed: {e}")

    def _open_config_dialog(self):
        path = filedialog.askopenfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
        )
        if path:
            self._load_config(path)
            self._apply_text_theme()

    def _load_history(self):
        self.history_rows.clear()
        if os.path.exists(HISTORY_PATH):
            try:
                with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                    self.history_rows = json.load(f)
            except Exception:
                self.history_rows = []
        if len(self.history_rows) > self.MAX_HISTORY:
            self.history_rows = self.history_rows[-self.MAX_HISTORY :]
        self._refresh_history_tree()

    def _save_history(self):
        try:
            with open(HISTORY_PATH, "w", encoding="utf-8") as f:
                json.dump(self.history_rows, f, indent=2)
        except Exception:
            pass

    def _clear_history(self):
        if not self.history_rows:
            return
        if not messagebox.askyesno("Clear History", "Clear all history records?"):
            return
        self.history_rows.clear()
        self._refresh_history_tree()
        self._save_history()
        self._log_ui("History cleared.\n")

    def _refresh_history_tree(self):
        for iid in self.hist_tree.get_children():
            self.hist_tree.delete(iid)
        for row in self.history_rows:
            tags = ()
            if row.get("result") == "Success":
                tags = ("success",)
            elif row.get("result") in {"Failed", "Error", "WriteError", "Config Error"}:
                tags = ("fail",)
            self.hist_tree.insert(
                "",
                "end",
                values=(row["file"], row["result"], row["started"], row["finished"]),
                tags=tags,
            )

    def _export_history(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            initialfile="migration_history.json",
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.history_rows, f, indent=2)
            messagebox.showinfo("History", "History exported.")
        except Exception as e:
            messagebox.showerror("History", f"Export failed: {e}")


    def _browse_folder(self):
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.folder_var.set(folder_path)

    def _scan_files(self):
        folder = self.folder_var.get().strip()
        if not os.path.isdir(folder):
            messagebox.showerror("Error", "Invalid folder path!")
            return
        patterns = [p.strip() for p in self.pattern_var.get().split(";") if p.strip()]
        recurse = self.include_subdirs_var.get()
        found: list[str] = []
        if recurse:
            for root, _, files in os.walk(folder):
                for name in files:
                    for pat in patterns:
                        if fnmatch.fnmatch(name.lower(), pat.lower()):
                            found.append(os.path.join(root, name))
                            break
        else:
            for name in os.listdir(folder):
                for pat in patterns:
                    if fnmatch.fnmatch(name.lower(), pat.lower()):
                        found.append(os.path.join(folder, name))
                        break
        if not found:
            messagebox.showwarning("No Files", "No matching files found.")
            return

        added = 0
        for f in found:
            if f in self.queue_items:
                continue
            status = "Pending"
            iid = self.queue_tree.insert(
                "",
                "end",
                values=(self._next_id, os.path.basename(f), status, "Medium", "", ""),
            )
            self.queue_items[f] = iid
            self.iid_to_path[iid] = f
            self._next_id += 1
            added += 1
        self._log_ui(f"Scanned {len(found)} files, added {added} to queue.\n")
        self.status_var.set(f"Queue: {len(self.queue_items)} files.")

    def _clear_queue(self):
        if self.queue_items and not messagebox.askyesno(
            "Clear Queue", "Clear all queued files?"
        ):
            return
        for iid in self.queue_tree.get_children():
            self.queue_tree.delete(iid)
        self.queue_items.clear()
        self.iid_to_path.clear()
        self.status_var.set("Queue cleared.")

    def _delete_selected(self):
        sel = self.queue_tree.selection()
        if not sel:
            return
        if not messagebox.askyesno(
            "Delete Selected", f"Remove {len(sel)} item(s) from queue?"
        ):
            return
        for iid in sel:
            path = self.iid_to_path.pop(iid, None)
            if path and path in self.queue_items:
                del self.queue_items[path]
            self.queue_tree.delete(iid)
        self.status_var.set(f"Queue: {len(self.queue_items)} files.")

    def _bulk_update_priority(self):
        sel = self.queue_tree.selection()
        if not sel:
            return
        priority = self.bulk_priority_var.get()
        for iid in sel:
            vals = list(self.queue_tree.item(iid, "values"))
            if not vals:
                continue
            vals[3] = priority
            self.queue_tree.item(iid, values=tuple(vals))
        self._log_ui(f"Updated priority to '{priority}' for {len(sel)} item(s).\n")

    def _start_batch(self):
        if self.executor is not None:
            messagebox.showinfo("Batch", "Batch already running.")
            return
        self._save_config()
        files = list(self.queue_items.keys())
        if not files:
            messagebox.showwarning("Batch", "Queue is empty. Scan files first.")
            return

        def priority_rank(path: str) -> int:
            iid = self.queue_items[path]
            vals = self.queue_tree.item(iid, "values")
            pr = vals[3] if len(vals) > 3 else "Medium"
            return {"High": 0, "Medium": 1, "Low": 2}.get(pr, 1)

        files.sort(key=priority_rank)

        max_workers = max(1, int(self.workers_var.get()))
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.futures.clear()
        self.stop_flag.clear()
        self._completed = 0
        self._total = len(files)
        self._log_ui(
            f"Starting batch with {max_workers} workers for {self._total} file(s)...\n"
        )
        self.status_var.set("Batch running...")

        for f in files:
            iid = self.queue_items[f]
            self._update_queue_row(iid, status="Running", started=self._ts(), finished="")
            fut = self.executor.submit(self._migrate_file_worker, f)
            self.futures.append(fut)

        self.after(300, self._check_futures_done)

    def _stop_batch(self):
        if not self.executor and not self.active_processes:
            self._log_ui("No active batch to stop.\n")
            self.status_var.set("No active batch.")
            return

        self.stop_flag.set()
        self._log_ui("Stop signal sent. Waiting for workers to finish...\n")
        self.status_var.set("Stopping batch...")

        with self.process_lock:
            procs = list(self.active_processes)
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass

    def _check_futures_done(self):
        if self.executor is None:
            with self.process_lock:
                still = bool(self.active_processes)
            if not still:
                self.status_var.set("Idle.")
            return

        done = all(f.done() for f in self.futures)
        if done:
            self.executor.shutdown(wait=False, cancel_futures=False)
            self.executor = None
            self._log_ui("Batch finished.\n")
            ok = sum(
                1
                for r in self.history_rows[-self._total :]
                if r["result"] == "Success"
            )
            fail = self._total - ok
            self._log_ui(
                f"Summary: {ok} succeeded, {fail} failed, total {self._total}.\n"
            )
            self.status_var.set("Batch completed.")
        else:
            self.after(400, self._check_futures_done)


    def _migrate_file_worker(self, file_path: str):
        if self.stop_flag.is_set():
            self._post_ui(lambda: self._mark_result(file_path, "Stopped"))
            return

        conn_str = self.conn_str_var.get().strip()
        db = self.db_var.get().strip()
        container = self.container_var.get().strip()
        pk_path = self.pk_path_var.get().strip() or "/id"
        write_mode = self.write_mode_var.get() or "Insert"

        if not conn_str:
            self._post_ui(
                lambda: self._log_ui(
                    "⚠ connection_string empty in config. Set it in Settings.\n"
                )
            )
            self._post_ui(lambda: self._mark_result(file_path, "Config Error"))
            return

        settings = {
            "Source": "json",
            "Sink": "cosmos-nosql",
            "SourceSettings": {
                "FilePath": file_path,
            },
            "SinkSettings": {
                "ConnectionString": conn_str,
                "Database": db,
                "Container": container,
                "PartitionKeyPath": pk_path,
                "WriteMode": write_mode,
            },
        }

        try:
            with open(MIGRATION_SETTINGS, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            self._post_ui(lambda: self._log_ui(f"⚠ Failed to write migration settings: {e}\n"))
            self._post_ui(lambda: self._mark_result(file_path, "WriteError"))
            return

        try:
            process = subprocess.Popen(
                [str(DMT_PATH), "--settings", str(MIGRATION_SETTINGS)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except Exception as e:
            if self.partial_var.get():
                self._post_ui(
                    lambda: self._log_ui(
                        f"⚠ Error starting dmt.exe for {os.path.basename(file_path)}: {e}\n"
                    )
                )
                self._post_ui(lambda: self._mark_result(file_path, "Error"))
            else:
                self._post_ui(lambda: self._log_ui(f"✖ Fatal start error: {e}\n"))
                self._post_ui(lambda: self._mark_result(file_path, "Error"))
            return

        with self.process_lock:
            self.active_processes.add(process)

        try:
            assert process.stdout is not None
            for line in process.stdout:
                if self.stop_flag.is_set():
                    try:
                        process.terminate()
                    except Exception:
                        pass
                    break
                self._post_ui(lambda l=line: self._log_ui(l))
            rc = process.wait()
        finally:
            with self.process_lock:
                self.active_processes.discard(process)

        if self.stop_flag.is_set():
            self._post_ui(lambda: self._mark_result(file_path, "Stopped"))
            return

        if rc != 0:
            self._post_ui(
                lambda: self._log_ui(
                    f"✖ Migration failed for {os.path.basename(file_path)} (rc={rc})\n"
                )
            )
            self._post_ui(lambda: self._mark_result(file_path, "Failed"))
        else:
            self._post_ui(
                lambda: self._log_ui(
                    f"✔ Migrated {os.path.basename(file_path)}\n"
                )
            )
            self._post_ui(lambda: self._mark_result(file_path, "Success"))


    def _post_ui(self, fn):
        self.ui_queue.put(fn)

    def _drain_ui_queue(self):
        try:
            while True:
                fn = self.ui_queue.get_nowait()
                try:
                    fn()
                except Exception:
                    pass
        except thread_queue.Empty:
            pass
        self.after(80, self._drain_ui_queue)

    def _log_ui(self, text: str):
        self.log.insert("end", text)
        self.log.see("end")

    def _clear_log(self):
        self.log.delete("1.0", "end")


    def _update_queue_row(self, iid, status=None, started=None, finished=None):
        vals = list(self.queue_tree.item(iid, "values"))
        if not vals:
            return
        if status is not None:
            vals[2] = status
        if started is not None:
            vals[4] = started
        if finished is not None:
            vals[5] = finished
        self.queue_tree.item(iid, values=tuple(vals))

        tag = None
        if vals[2] == "Success":
            tag = "success"
        elif vals[2] in {"Failed", "Error", "WriteError", "Config Error"}:
            tag = "fail"
        elif vals[2] == "Running":
            tag = "running"
        elif vals[2] == "Stopped":
            tag = "stopped"
        if tag:
            self.queue_tree.item(iid, tags=(tag,))

    def _mark_result(self, file_path: str, result: str):
        iid = self.queue_items.get(file_path)
        if iid:
            started = self.queue_tree.item(iid, "values")[4]
            self._update_queue_row(iid, status=result, finished=self._ts())
            row = {
                "file": os.path.basename(file_path),
                "result": result,
                "started": started,
                "finished": self.queue_tree.item(iid, "values")[5],
            }
            self.history_rows.append(row)
            if len(self.history_rows) > self.MAX_HISTORY:
                self.history_rows = self.history_rows[-self.MAX_HISTORY :]
            self._save_history()
            self._refresh_history_tree()
            self._completed += 1
            self._log_ui(f"Progress: {self._completed}/{self._total}\n")


    def _apply_theme(self, themename: str):
        try:
            self.style.theme_use(themename)
        except tk.TclError as e:
            messagebox.showerror("Theme", f"Failed to apply theme '{themename}': {e}")
            return
        self._refresh_button_styles()
        self._apply_text_theme()
        self.theme_var.set(themename)

    def _apply_theme_from_dropdown(self):
        self._apply_theme(self.theme_var.get())

    def _apply_text_theme(self):
        if hasattr(self, "log"):
            self.log.configure(
                bg=self._surface_bg(),
                fg=self._fg_color(),
                insertbackground=self._fg_color(),
            )
        if hasattr(self, "cli_output"):
            self.cli_output.configure(
                bg=self._surface_bg(),
                fg=self._fg_color(),
                insertbackground=self._fg_color(),
            )

    def _ts(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _noop(self):
        messagebox.showinfo(
            "Info",
            "Paste a full Cosmos connection string here.\n"
            "Example: AccountEndpoint=...;AccountKey=...;",
        )


    def _test_settings(self):
        issues = []
        if not self.conn_str_var.get().strip():
            issues.append("connection_string is empty")
        if not self.db_var.get().strip():
            issues.append("database_name is empty")
        if not self.container_var.get().strip():
            issues.append("container_name is empty")
        if not os.path.isfile(DMT_PATH):
            issues.append(f"dmt.exe not found at: {DMT_PATH}")
        if issues:
            self._log_ui("⚠ Test failed:\n  - " + "\n  - ".join(issues) + "\n")
            self.status_var.set("Test failed.")
            return
        try:
            proc = subprocess.Popen(
                [str(DMT_PATH), "-h"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert proc.stdout is not None
            for _ in range(5):
                line = proc.stdout.readline()
                if not line:
                    break
                self._log_ui(line)
            proc.terminate()
        except Exception as e:
            self._log_ui(f"⚠ dmt.exe invocation error: {e}\n")
            self.status_var.set("Test failed.")
            return
        self._log_ui("✔ Test passed: config looks OK and dmt.exe is reachable.\n")
        self.status_var.set("Test passed.")

    def _test_with_file(self):
        path = filedialog.askopenfilename(
            filetypes=[("JSON", "*.json"), ("All files", "*.*")]
        )
        if not path:
            return
        conn_str = self.conn_str_var.get().strip()
        db = self.db_var.get().strip()
        container = self.container_var.get().strip()
        pk_path = self.pk_path_var.get().strip() or "/id"
        write_mode = self.write_mode_var.get() or "Insert"
        if not conn_str or not db or not container:
            messagebox.showwarning(
                "Test with file",
                "Set connection string, database and container in Settings first.",
            )
            return

        settings = {
            "Source": "json",
            "Sink": "cosmos-nosql",
            "SourceSettings": {
                "FilePath": path,
            },
            "SinkSettings": {
                "ConnectionString": conn_str,
                "Database": db,
                "Container": container,
                "PartitionKeyPath": pk_path,
                "WriteMode": write_mode,
            },
        }

        try:
            with open(MIGRATION_SETTINGS, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            self._log_ui(f"⚠ Failed to write migration settings: {e}\n")
            return

        self._log_ui(f"\n[Dry-run] Testing file: {os.path.basename(path)}\n")
        self.status_var.set("Running test with file...")

        try:
            proc = subprocess.Popen(
                [str(DMT_PATH), "--settings", str(MIGRATION_SETTINGS)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                self._log_ui(line)
            rc = proc.wait()
            if rc == 0:
                self._log_ui("✔ Test with file completed successfully.\n")
                self.status_var.set("Test with file OK.")
            else:
                self._log_ui(f"✖ Test with file failed (rc={rc}).\n")
                self.status_var.set("Test with file failed.")
        except Exception as e:
            self._log_ui(f"✖ Test with file error: {e}\n")
            self.status_var.set("Test with file failed.")

    def _run_cli(self):
        args = self.cli_args_var.get().strip()
        if not args:
            args_list = []
        else:
            args_list = args.split()

        self.cli_output.delete("1.0", "end")
        cmd = [str(DMT_PATH)] + args_list
        self.cli_output.insert("end", f"$ {' '.join(cmd)}\n\n")
        self.status_var.set("Running CLI command...")

        def worker():
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            except Exception as e:
                self._post_ui(
                    lambda: self._append_cli_output(f"Error starting process: {e}\n")
                )
                self._post_ui(lambda: self.status_var.set("CLI failed."))
                return
            assert proc.stdout is not None
            for line in proc.stdout:
                self._post_ui(lambda l=line: self._append_cli_output(l))
            rc = proc.wait()
            if rc == 0:
                self._post_ui(
                    lambda: self._append_cli_output("\n✔ CLI command completed.\n")
                )
                self._post_ui(lambda: self.status_var.set("CLI done."))
            else:
                self._post_ui(
                    lambda: self._append_cli_output(
                        f"\n✖ CLI command failed (rc={rc}).\n"
                    )
                )
                self._post_ui(lambda: self.status_var.set("CLI failed."))

        threading.Thread(target=worker, daemon=True).start()

    def _append_cli_output(self, text: str):
        self.cli_output.insert("end", text)
        self.cli_output.see("end")


    def _on_queue_double_click(self, event):
        item_id = self.queue_tree.identify_row(event.y)
        if not item_id:
            return
        self._open_item_detail(item_id)

    def _open_item_detail(self, iid: str):
        vals = self.queue_tree.item(iid, "values")
        if not vals:
            return
        file_id, fname, status, priority, started, finished = vals
        path = self.iid_to_path.get(iid, "")

        win = tb.Toplevel(self)
        win.title(f"Details - {fname}")
        win.geometry("640x480")

        info = ttk.Frame(win)
        info.pack(fill="x", padx=8, pady=8)

        def row(label: str, value: str, r: int):
            ttk.Label(info, text=label + ":", width=14).grid(
                row=r, column=0, sticky="w", pady=2
            )
            ttk.Label(info, text=value or "-", width=60).grid(
                row=r, column=1, sticky="w", pady=2
            )

        row("ID", str(file_id), 0)
        row("File", fname, 1)
        row("Status", status, 2)
        row("Priority", priority, 3)
        row("Started", started, 4)
        row("Finished", finished, 5)
        row("Full path", path, 6)

        ttk.Label(win, text="File preview (first 2000 chars):").pack(
            anchor="w", padx=8, pady=(8, 0)
        )
        preview = ScrolledText(win, height=14, wrap="word")
        preview.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        try:
            if path and os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = f.read(2000)
                preview.insert("end", data)
            else:
                preview.insert("end", "File not found on disk.")
        except Exception as e:
            preview.insert("end", f"Error reading file: {e}")
        preview.configure(state="disabled")

        btnbar = ttk.Frame(win)
        btnbar.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(
            btnbar,
            text="Close",
            command=win.destroy,
            style="Secondary.TButton",
        ).pack(side="right")



if __name__ == "__main__":
    app = DMTGui()
    app.mainloop()

