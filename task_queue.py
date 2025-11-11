import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import subprocess, json, os, concurrent.futures, queue, threading
from datetime import datetime
import tkinter.font as tkFont
import requests
DMT_PATH = r"./dmt-2.9.0-win-x64/win-x64-package/dmt.exe"
MIGRATION_SETTINGS = r"./dmt-2.9.0-win-x64/win-x64-package/migrationsettings.json"
PROFILES_DIR = "./profiles"
os.makedirs(PROFILES_DIR, exist_ok=True)
class DMTGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Cosmos DB JSON Batch Loader")
        self.geometry("1000x680")
        self.minsize(750, 520)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.bg_color = "#120D18"
        self.fg_color = "#FFFFFF"
        self.configure(bg=self.bg_color)
        self.font_h1 = tkFont.Font(family="Helvetica", size=20, weight="normal")
        self.font_h2 = tkFont.Font(family="Helvetica", size=15, weight="normal")
        self.font_h3 = tkFont.Font(family="Helvetica", size=11)
        style = ttk.Style(self)
        style.theme_use('default')
        style.configure('.', background=self.bg_color, foreground=self.fg_color)
        style.configure('TFrame', background=self.bg_color)
        style.configure('TLabel', background=self.bg_color, foreground=self.fg_color, padding=5)
        style.configure('TButton',
                        padding=(10, 6),
                        relief="flat",
                        background="#020000",
                        foreground=self.fg_color)
        style.map('TButton',
                  background=[('active', "#1F0808"), ('disabled', '#333333')],
                  foreground=[('active', self.fg_color)])
        style.configure('TEntry', fieldbackground="#240213", foreground=self.fg_color,
                        insertcolor=self.fg_color)
        style.configure('TCheckbutton', background=self.bg_color, foreground=self.fg_color)
        style.configure('Treeview',
                        background="#0E0101",
                        fieldbackground="#0F0404",
                        foreground=self.fg_color,
                        rowheight=24)
        style.map('Treeview', background=[('selected', "#0F141B")])
        style.configure('TNotebook', background=self.bg_color)
        style.configure('TNotebook.Tab',
                        background="#110505",
                        foreground=self.fg_color,
                        padding=[14, 8])
        style.map('TNotebook.Tab',
                  background=[('selected', "#071838")],
                  foreground=[('selected', 'white')])
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=15, pady=15)
        self.tab_migrate   = ttk.Frame(self.notebook)
        self.tab_queue     = ttk.Frame(self.notebook)
        self.tab_profiles  = ttk.Frame(self.notebook)
        self.tab_settings  = ttk.Frame(self.notebook)
        self.tab_logs      = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_migrate,  text="Migration")
        self.notebook.add(self.tab_queue,    text="Task Queue")
        self.notebook.add(self.tab_profiles, text="Profiles")
        self.notebook.add(self.tab_settings, text="Settings")
        self.notebook.add(self.tab_logs,     text="Logs")
        self.task_queue = queue.Queue()
        self.pending_tasks = queue.Queue()
        self.running_tasks = {}
        self.task_counter = 0
        self.task_status = {}
        self.task_files = {}
        self.task_start_time = {}
        self.stop_event = threading.Event()
        self.workers = []
        self._build_migration_tab()
        self._build_queue_tab()
        self._build_profiles_tab()
        self._build_settings_tab()
        self._build_logs_tab()
        self._update_queue_display()
    def _build_migration_tab(self):
        f = self.tab_migrate
        f.grid_columnconfigure((0, 1, 2), weight=1)
        ttk.Label(f, text="Batch JSON → Cosmos DB", font=self.font_h1, anchor="center")\
            .grid(row=0, column=0, columnspan=3, pady=(5, 20), sticky="ew")
        ttk.Label(f, text="JSON Folder:", font=self.font_h2)\
            .grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.folder_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.folder_var, width=55)\
            .grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(f, text="Browse", command=self.browse_folder)\
            .grid(row=1, column=2, padx=5, pady=5)
        self.endpoint_var   = tk.StringVar()
        self.key_var        = tk.StringVar()
        self.db_var         = tk.StringVar()
        self.container_var  = tk.StringVar()
        self._labeled_entry(f, "Endpoint:",   self.endpoint_var,   row=2)
        self._labeled_entry(f, "Key:",        self.key_var,        row=3)
        self._labeled_entry(f, "Database:",   self.db_var,         row=4)
        self._labeled_entry(f, "Container:",  self.container_var,  row=5)
        ttk.Button(f, text="Test Connection", command=self.test_connection)\
            .grid(row=6, column=0, columnspan=3, pady=10)
        opt = ttk.Frame(f)
        opt.grid(row=7, column=0, columnspan=3, sticky="w", pady=12, padx=5)
        ttk.Label(opt, text="Parallel workers:", font=self.font_h3).pack(side="left")
        self.workers_var = tk.IntVar(value=4)
        ttk.Entry(opt, textvariable=self.workers_var, width=5).pack(side="left", padx=5)
        self.skip_errors_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opt, text="Skip errors & continue", variable=self.skip_errors_var)\
            .pack(side="right", padx=20)
        ttk.Button(f, text="Enqueue Files", command=self.enqueue_files)\
            .grid(row=8, column=0, columnspan=3, pady=15, sticky="ew")
    def _labeled_entry(self, parent, label, var, row):
        ttk.Label(parent, text=label, font=self.font_h3)\
            .grid(row=row, column=0, sticky="w", padx=5, pady=3)
        ttk.Entry(parent, textvariable=var, width=55)\
            .grid(row=row, column=1, columnspan=2, padx=5, pady=3, sticky="ew")
    def browse_folder(self):
        p = filedialog.askdirectory()
        if p:
            self.folder_var.set(p)
    def test_connection(self):
        endpoint = self.endpoint_var.get().strip()
        key = self.key_var.get().strip()
        if not endpoint or not key:
            self._toast("Endpoint and Key are required.")
            return
        if not endpoint.startswith("https://"):
            self._toast("Endpoint must start with https://")
            return
        self._log(f"\n[TEST] Connecting to {endpoint}...\n")
        self._toast("Testing connection...")
        def run_test():
            try:
                url = f"{endpoint.rstrip('/')}/dbs"
                headers = {
                    "Authorization": f"type=master&ver=1.0&sig={self._generate_auth_token('GET', 'dbs', '', key)}",
                    "x-ms-date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
                    "x-ms-version": "2018-12-31"
                }
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    dbs = response.json().get("Databases", [])
                    db_names = [db["id"] for db in dbs]
                    msg = f"Success: Connected! Found {len(db_names)} database(s): {', '.join(db_names) or 'None'}"
                    self._log(f"[TEST] {msg}\n")
                    self._toast("Connection successful!")
                else:
                    msg = f"Failed: {response.status_code} {response.reason}"
                    self._log(f"[TEST] {msg}\n")
                    self._toast(msg)
            except Exception as e:
                msg = f"Error: {str(e)}"
                self._log(f"[TEST] {msg}\n")
                self._toast("Connection failed.")
        threading.Thread(target=run_test, daemon=True).start()
    def _generate_auth_token(self, verb, resource_type, resource_id, key):
        import hmac, hashlib, base64
        key_bytes = base64.b64decode(key)
        string_to_sign = f"{verb.lower()}\n{resource_type.lower()}\n{resource_id}\n\n\n"
        signature = hmac.new(key_bytes, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        return base64.b64encode(signature).decode('utf-8')
    def _build_queue_tab(self):
        f = self.tab_queue
        f.grid_rowconfigure(1, weight=1)
        f.grid_columnconfigure(0, weight=1)
        ttk.Label(f, text="Task Queue Progress", font=self.font_h1, anchor="center")\
            .grid(row=0, column=0, pady=(5, 10), sticky="ew")
        cols = ("id", "status", "file", "duration")
        self.queue_tree = ttk.Treeview(f, columns=cols, show="headings", selectmode="none")
        self.queue_tree.heading("id",       text="ID")
        self.queue_tree.heading("status",   text="Status")
        self.queue_tree.heading("file",     text="File")
        self.queue_tree.heading("duration", text="Time")
        self.queue_tree.column("id",       width=60,  anchor="center")
        self.queue_tree.column("status",   width=110, anchor="center")
        self.queue_tree.column("file",     width=420, anchor="w")
        self.queue_tree.column("duration", width=100, anchor="center")
        vsb = ttk.Scrollbar(f, orient="vertical", command=self.queue_tree.yview)
        self.queue_tree.configure(yscrollcommand=vsb.set)
        self.queue_tree.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        vsb.grid(row=1, column=1, sticky="ns")
        self.queue_tree.tag_configure("running",  foreground="#FFD700", font=("Helvetica", 10, "bold"))
        self.queue_tree.tag_configure("success",  foreground="#00FF00")
        self.queue_tree.tag_configure("error",    foreground="#FF4444")
        self.queue_tree.tag_configure("pending",  foreground="#AAAAAA")
        btn_frame = ttk.Frame(f)
        btn_frame.grid(row=2, column=0, pady=10, sticky="ew")
        btn_frame.grid_columnconfigure((0,1,2), weight=1)
        self.btn_start = ttk.Button(btn_frame, text="Start", command=self.start_processing)
        self.btn_stop  = ttk.Button(btn_frame, text="Stop",  command=self.stop_processing, state="disabled")
        self.btn_clear = ttk.Button(btn_frame, text="Clear", command=self.clear_completed)
        self.btn_start.grid(row=0, column=0, padx=5, sticky="ew")
        self.btn_stop.grid(row=0, column=1, padx=5, sticky="ew")
        self.btn_clear.grid(row=0, column=2, padx=5, sticky="ew")
    def enqueue_files(self):
        folder = self.folder_var.get()
        if not folder or not os.path.isdir(folder):
            self._toast("Select a valid folder.")
            return
        files = [os.path.join(folder, f) for f in os.listdir(folder)
                 if f.lower().endswith(".json")]
        if not files:
            self._toast("No JSON files.")
            return
        while not self.pending_tasks.empty():
            try: self.pending_tasks.get_nowait()
            except: pass
        self.task_counter = 0
        self.task_status.clear()
        self.task_files.clear()
        self.task_start_time.clear()
        for item in self.queue_tree.get_children():
            self.queue_tree.delete(item)
        for file_path in files:
            self.task_counter += 1
            task_id = self.task_counter
            self.pending_tasks.put((task_id, file_path))
            self.task_queue.put((task_id, "Pending", "Queued", "—"))
            self.task_files[task_id] = file_path
        self._toast(f"Enqueued {len(files)} files.")
        self.btn_start.config(state="normal")
    def start_processing(self):
        if self.pending_tasks.empty():
            self._toast("No tasks.")
            return
        self.stop_event.clear()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        workers = max(1, self.workers_var.get())
        self.workers = []
        for _ in range(workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self.workers.append(t)
        self._toast(f"Started {workers} workers.")
    def stop_processing(self):
        self.stop_event.set()
        self.btn_stop.config(state="disabled")
        self.btn_start.config(state="normal")
        self._toast("Stopping...")
    def clear_completed(self):
        for item in self.queue_tree.get_children():
            status = self.queue_tree.item(item, "values")[1]
            if status in ("Success", "Error"):
                self.queue_tree.delete(item)
        self._toast("Cleared completed.")
    def _worker_loop(self):
        while not self.stop_event.is_set():
            try:
                task_id, file_path = self.pending_tasks.get(timeout=0.5)
            except queue.Empty:
                continue
            start_time = datetime.now()
            self.task_start_time[task_id] = start_time
            self.task_queue.put((task_id, "Running", "Started", "0s"))
            try:
                proc = self._run_migration(file_path, task_id)
                self.running_tasks[task_id] = proc
                proc.wait()
                del self.running_tasks[task_id]
                duration = str(datetime.now() - start_time).split('.')[0]
                if proc.returncode == 0:
                    self.task_queue.put((task_id, "Success", "Done", duration))
                else:
                    self.task_queue.put((task_id, "Error", f"Code {proc.returncode}", duration))
            except Exception as e:
                duration = str(datetime.now() - start_time).split('.')[0]
                self.task_queue.put((task_id, "Error", str(e), duration))
            finally:
                self.pending_tasks.task_done()
    def _run_migration(self, file_path, task_id):
        settings = {
            "Source": {"File": file_path, "Type": "Json"},
            "Sink": {
                "Type": "CosmosDBNoSql",
                "ConnectionString": f"AccountEndpoint={self.endpoint_var.get()};AccountKey={self.key_var.get()};",
                "Database": self.db_var.get(),
                "Container": self.container_var.get()
            }
        }
        with open(MIGRATION_SETTINGS, "w", encoding="utf-8") as fp:
            json.dump(settings, fp, indent=4)
        proc = subprocess.Popen(
            [DMT_PATH, "-s", MIGRATION_SETTINGS],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        for line in proc.stdout:
            if not self.stop_event.is_set():
                self.task_queue.put((task_id, "Running", line.strip(), "—"))
        return proc
    def _update_queue_display(self):
        while not self.task_queue.empty():
            try:
                task_id, status, message, duration = self.task_queue.get_nowait()
                file_name = os.path.basename(self.task_files.get(task_id, "Unknown"))
                tag = status.lower()
                ts = datetime.now().strftime("%H:%M:%S")
                self._log(f"[{ts}] [TASK {task_id:03d}] [{status}] {file_name} | {message}\n")
                found = False
                for item in self.queue_tree.get_children():
                    if self.queue_tree.item(item, "values")[0] == str(task_id):
                        self.queue_tree.item(item, values=(task_id, status, file_name, duration), tags=(tag,))
                        found = True
                        break
                if not found:
                    self.queue_tree.insert("", "end", values=(task_id, status, file_name, duration), tags=(tag,))
            except queue.Empty:
                break
        children = self.queue_tree.get_children()
        if children:
            self.queue_tree.see(children[-1])
        self.after(300, self._update_queue_display)
    def _build_logs_tab(self):
        f = self.tab_logs
        f.grid_rowconfigure(0, weight=1)
        f.grid_columnconfigure(0, weight=1)
        ttk.Label(f, text="Detailed Execution Log", font=self.font_h1, anchor="center")\
            .grid(row=0, column=0, pady=(5, 10), sticky="ew")
        txt_frame = ttk.Frame(f)
        txt_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        txt_frame.grid_rowconfigure(0, weight=1)
        txt_frame.grid_columnconfigure(0, weight=1)
        self.log = tk.Text(txt_frame,
                           bg="#02000A", fg=self.fg_color,
                           insertbackground=self.fg_color,
                           wrap="word", font=("Consolas", 10))
        sb = ttk.Scrollbar(txt_frame, orient="vertical", command=self.log.yview)
        self.log.configure(yscrollcommand=sb.set)
        self.log.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")
    def _log(self, txt):
        if hasattr(self, 'log'):
            self.log.insert("end", txt)
            self.log.see("end")
            self.update_idletasks()
    def _build_profiles_tab(self):
        f = self.tab_profiles
        f.grid_columnconfigure(0, weight=1)
        f.grid_rowconfigure(1, weight=1)
        ttk.Label(f, text="Saved Connection Profiles", font=self.font_h1, anchor="center")\
            .grid(row=0, column=0, pady=(5, 15), sticky="ew")
        cols = ("name", "endpoint", "db", "container")
        self.tree = ttk.Treeview(f, columns=cols, show="headings", selectmode="browse")
        self.tree.heading("name",      text="Profile")
        self.tree.heading("endpoint",  text="Endpoint")
        self.tree.heading("db",        text="Database")
        self.tree.heading("container", text="Container")
        self.tree.column("name",      width=150)
        self.tree.column("endpoint",  width=260)
        self.tree.column("db",        width=120)
        self.tree.column("container", width=120)
        sb = ttk.Scrollbar(f, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        self.tree.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        sb.grid(row=1, column=1, sticky="ns")
        btns = ttk.Frame(f)
        btns.grid(row=2, column=0, pady=10, sticky="ew")
        ttk.Button(btns, text="Save Current",   command=self.save_profile).pack(side="left", padx=5)
        ttk.Button(btns, text="Load Selected", command=self.load_profile).pack(side="left", padx=5)
        ttk.Button(btns, text="Delete",        command=self.delete_profile).pack(side="left", padx=5)
        self._refresh_profiles()
    def _current_profile_data(self):
        return {
            "endpoint":   self.endpoint_var.get(),
            "key":        self.key_var.get(),
            "database":   self.db_var.get(),
            "container":  self.container_var.get()
        }
    def save_profile(self):
        data = self._current_profile_data()
        if not data["endpoint"] or not data["key"]:
            self._toast("Endpoint and Key required.")
            return
        name = simpledialog.askstring("Profile name", "Enter name:", parent=self)
        if not name: return
        path = os.path.join(PROFILES_DIR, f"{name}.json")
        try:
            with open(path, "w", encoding="utf-8") as fp:
                json.dump({"name": name, **data}, fp, indent=4)
            self._toast(f"Profile '{name}' saved.")
            self._refresh_profiles()
        except Exception as e:
            self._toast(f"Save failed: {e}")
    def load_profile(self):
        sel = self.tree.selection()
        if not sel:
            self._toast("Select a profile.")
            return
        tag = self.tree.item(sel[0], "tags")[0]
        path = os.path.join(PROFILES_DIR, tag)
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            self.endpoint_var.set(data.get("endpoint",""))
            self.key_var.set(data.get("key",""))
            self.db_var.set(data.get("database",""))
            self.container_var.set(data.get("container",""))
            self._toast(f"Loaded '{data.get('name')}'")
        except Exception as e:
            self._toast(f"Load failed: {e}")
    def delete_profile(self):
        sel = self.tree.selection()
        if not sel: return
        tag = self.tree.item(sel[0], "tags")[0]
        if messagebox.askyesno("Delete", f"Delete '{tag}'?"):
            try:
                os.remove(os.path.join(PROFILES_DIR, tag))
                self._refresh_profiles()
            except Exception as e:
                self._toast(f"Delete failed: {e}")
    def _refresh_profiles(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for fn in os.listdir(PROFILES_DIR):
            if fn.lower().endswith(".json"):
                try:
                    with open(os.path.join(PROFILES_DIR, fn), "r") as fp:
                        d = json.load(fp)
                    ep = d.get("endpoint","")
                    if len(ep)>35: ep = ep[:32]+"..."
                    self.tree.insert("", "end",
                                     values=(d.get("name",fn), ep, d.get("database",""), d.get("container","")),
                                     tags=(fn,))
                except: pass
    def _build_settings_tab(self):
        f = self.tab_settings
        f.grid_columnconfigure(0, weight=1)
        ttk.Label(f, text="Application Settings", font=self.font_h1, anchor="center")\
            .grid(row=0, column=0, pady=(5, 20), sticky="ew")
        ttk.Label(f, text="DMT Executable:", font=self.font_h3)\
            .grid(row=1, column=0, sticky="w", padx=10, pady=5)
        self.dmt_var = tk.StringVar(value=DMT_PATH)
        dmt_f = ttk.Frame(f)
        dmt_f.grid(row=2, column=0, sticky="ew", padx=10, pady=2)
        ttk.Entry(dmt_f, textvariable=self.dmt_var, width=70).pack(side="left", fill="x", expand=True)
        ttk.Button(dmt_f, text="Browse", command=self.browse_dmt).pack(side="right", padx=5)
        ttk.Label(f, text="Migration Settings JSON:", font=self.font_h3)\
            .grid(row=3, column=0, sticky="w", padx=10, pady=(15,5))
        self.mig_var = tk.StringVar(value=MIGRATION_SETTINGS)
        mig_f = ttk.Frame(f)
        mig_f.grid(row=4, column=0, sticky="ew", padx=10, pady=2)
        ttk.Entry(mig_f, textvariable=self.mig_var, width=70).pack(side="left", fill="x", expand=True)
        ttk.Button(mig_f, text="Browse", command=self.browse_mig).pack(side="right", padx=5)
        ttk.Button(f, text="Save Settings", command=self.save_app_settings)\
            .grid(row=5, column=0, pady=25)
    def browse_dmt(self):
        p = filedialog.askopenfilename(filetypes=[("Executable", "*.exe"), ("All", "*.*")])
        if p: self.dmt_var.set(p)
    def browse_mig(self):
        p = filedialog.askopenfilename(filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if p: self.mig_var.set(p)
    def save_app_settings(self):
        global DMT_PATH, MIGRATION_SETTINGS
        DMT_PATH = self.dmt_var.get()
        MIGRATION_SETTINGS = self.mig_var.get()
        self._toast("Settings saved.")
    def _toast(self, text, duration=3000):
        toast = tk.Toplevel(self)
        toast.overrideredirect(True)
        toast.configure(bg="#133602", bd=0)
        lbl = tk.Label(toast, text=text, bg="#112707", fg="white",
                       font=("Helvetica", 11), padx=20, pady=10)
        lbl.pack()
        self.update_idletasks()
        x = self.winfo_rootx() + self.winfo_width()//2 - toast.winfo_reqwidth()//2
        y = self.winfo_rooty() + self.winfo_height()//2 - toast.winfo_reqheight()//2
        toast.geometry(f"+{x}+{y}")
        toast.after(duration, toast.destroy)
    def _on_close(self):
        if messagebox.askokcancel("Quit", "Stop all tasks and exit?"):
            self.stop_processing()
            self.stop_event.set()
            for proc in self.running_tasks.values():
                if proc.poll() is None:
                    proc.terminate()
            self.destroy()
if __name__ == "__main__":
    app = DMTGui()
    app.mainloop()
